"""Plan S0 acceptance: exchange flow over a DirChannel.

Convergence, idempotent re-pull, update/replay behavior, self-skip,
and fault isolation — all with real linecite stores.
"""
from __future__ import annotations

from linecite.generic_ingester import ingest_paths

from linecite_sync import DirChannel, pull, push
from linecite_sync.metrics import (convergence, last_import_age_seconds,
                                   published_age_seconds,
                                   roundtrip_fidelity)
from linecite_sync.sync import published_manifest, stream_name

from tests.conftest import make_host_store, make_repo


def test_stream_names_encode_slashy_repos(host_a, tmp_path):
    store_a, _ = host_a
    from linecite.generic_ingester import ingest_paths as ip
    root = make_repo(tmp_path / "slashy", {"a.md": "# A\nslashy body\n"})
    ip(store_a, root, host="host-a", origin_repo="team/proj v2")
    ch = DirChannel(tmp_path / "channel")
    name = push(store_a, "team/proj v2", ch)
    assert "--" in name and "/" not in name and " " not in name
    from linecite_sync.channels import decode_stream_name
    assert decode_stream_name(name) == ("host-a", "team/proj v2")
    store_b = make_host_store("host-b",
                              make_repo(tmp_path / "b2",
                                        {"b.md": "# B\nother\n"}))
    [o] = [o for o in pull(store_b, ch) if o.name == name]
    assert o.status == "accepted"


def test_host_collision_stream_not_masked_as_self(host_a, tmp_path):
    # Review blocker: a stream that SHARES our host id but carries a
    # different fingerprint must reach import_snapshot so contract
    # §8.5 host_collision applies — never be silently skipped_self.
    store_a, _ = host_a
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    imposter_root = make_repo(tmp_path / "imposter-repo",
                              {"docs/x.md": "# X\nimposter body\n"})
    imposter = make_host_store("host-a", imposter_root)  # same id!
    imposter_ch = DirChannel(tmp_path / "imposter-ch")
    push(imposter, "proj", imposter_ch)
    # imposter's stream lands on OUR channel under our stream name
    pkg = imposter_ch.fetch(stream_name("host-a", "proj"),
                            tmp_path / "tmp2")
    ch.publish(pkg, stream_name("host-a", "proj"))
    [o] = pull(store_a, ch)
    assert o.status == "import_rejected"
    assert "host_collision" in o.detail


def _mini_pkg(tmp_path, v):
    pkg = tmp_path / f"pkg{v}"
    pkg.mkdir(exist_ok=True)
    (pkg / "manifest.json").write_text('{"v":%d}' % v)
    (pkg / "chunks.jsonl").write_text("")
    return pkg


def test_dirchannel_grace_retains_previous_version(tmp_path):
    # Review blocker (round 2): superseded versions must outlive the
    # pointer swap by a grace window so in-flight readers can finish.
    from linecite_sync.channels import DirChannel as DC
    ch = DC(tmp_path / "ch")                 # default grace: 300s
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    got = ch.fetch("h--r", tmp_path / "out")
    assert '"v":2' in (got / "manifest.json").read_text()
    vdirs = [d for d in (tmp_path / "ch").iterdir()
             if d.is_dir() and d.name.startswith(".h--r.v-")]
    assert len(vdirs) == 2                    # old survives the grace


def test_dirchannel_grace_measured_from_supersession(tmp_path):
    # Review blocker (round 3): a version that has been LIVE longer
    # than the grace window must still survive the moment it is
    # superseded — grace counts from retirement, not creation.
    import os, time
    from linecite_sync.channels import DirChannel as DC
    ch = DC(tmp_path / "ch")                  # default grace 300s
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    old_dir = ch._live_dir("h--r")
    aged = time.time() - 400                  # older than the grace
    os.utime(old_dir, (aged, aged))
    ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    assert old_dir.exists()                   # retired, not reaped
    vdirs = [d for d in (tmp_path / "ch").iterdir()
             if d.is_dir() and d.name.startswith(".h--r.v-")]
    assert len(vdirs) == 2


def test_dirchannel_fetch_retries_across_races(tmp_path):
    # Interleaving regression: a reader that resolved the OLD pointer
    # and lost its version dir to GC must re-resolve and succeed.
    from linecite_sync.channels import DirChannel as DC, _copy_package
    import linecite_sync.channels as chmod
    ch = DC(tmp_path / "ch", gc_grace_seconds=0)   # worst case
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    stale_src = ch._live_dir("h--r")
    # publisher swaps + GCs the old version out from under the reader
    ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    assert not stale_src.exists()             # the race is real
    # a raw copy from the stale path would fail...
    import pytest as _pytest
    with _pytest.raises(Exception):
        _copy_package(stale_src, tmp_path / "raw")
    # ...but the public fetch re-resolves and lands on v2
    got = ch.fetch("h--r", tmp_path / "out")
    assert '"v":2' in (got / "manifest.json").read_text()


def test_two_host_exchange_converges(host_a, host_b, tmp_path):
    (store_a, _), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    push(store_b, "proj", ch)
    out_a = {o.name: o.status for o in pull(store_a, ch)}
    out_b = {o.name: o.status for o in pull(store_b, ch)}
    assert out_a[stream_name("host-b", "proj")] == "accepted"
    assert out_a[stream_name("host-a", "proj")] == "skipped_self"
    assert out_b[stream_name("host-a", "proj")] == "accepted"
    # Convergence (metrics.S4): same scoped queries, same answers.
    queries = ["exponential backoff", "fifo order", "lru eviction"]
    for scope in (["host-a"], ["host-b"]):
        assert convergence(store_a, store_b, queries, scope)


def test_repull_is_idempotent_noop(host_a, host_b, tmp_path):
    (store_a, _), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    assert {o.status for o in pull(store_b, ch)} == {"accepted"}
    again = pull(store_b, ch)
    assert [o.status for o in again] == ["noop"]


def test_update_advances_and_stale_republish_refuses(
        host_a, host_b, tmp_path):
    (store_a, root_a), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    # keep a copy of the OLD published package (a stale republisher)
    old = ch.fetch(stream_name("host-a", "proj"), tmp_path / "old")
    pull(store_b, ch)
    # host-a edits and re-pushes; B sees the new seq
    (root_a / "docs/retry.md").write_text("# Retry\ncircuit breaker\n",
                                          encoding="utf-8")
    ingest_paths(store_a, root_a, host="host-a", origin_repo="proj")
    push(store_a, "proj", ch)
    [outcome] = [o for o in pull(store_b, ch)
                 if o.name == stream_name("host-a", "proj")]
    assert outcome.status == "accepted" and outcome.snapshot_seq == 2
    # a stale actor republishes the old package: replay must refuse
    ch.publish(old, stream_name("host-a", "proj"))
    [outcome] = [o for o in pull(store_b, ch)
                 if o.name == stream_name("host-a", "proj")]
    assert outcome.status == "import_rejected"
    assert "replay" in outcome.detail


def test_bad_package_does_not_abort_others(host_a, host_b, tmp_path):
    (store_a, _), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    # plant a corrupted stream alongside the good one
    good = ch.fetch(stream_name("host-a", "proj"), tmp_path / "tmp")
    (good / "chunks.jsonl").write_bytes(b"garbage\n")
    ch.publish(good, stream_name("host-x", "proj"))
    statuses = {o.name: o.status for o in pull(store_b, ch)}
    assert statuses[stream_name("host-x", "proj")] == "gate_refused"
    assert statuses[stream_name("host-a", "proj")] == "accepted"


def test_roundtrip_fidelity_and_age(host_a, host_b, tmp_path):
    (store_a, _), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    pull(store_b, ch)
    pkg = ch.fetch(stream_name("host-a", "proj"), tmp_path / "check")
    rep = roundtrip_fidelity(pkg, store_b)
    assert rep.ok and rep.total > 0
    age = published_age_seconds(ch, "host-a", "proj")
    assert 0 <= age < 3600
    m = published_manifest(ch, "host-a", "proj")
    assert m["host"] == "host-a"


def test_semantic_reindex_is_local_choice(host_a, host_b, tmp_path):
    # Embeddings never travel: after pull, B may build its own index
    # over A's chunks via the public reembed API (contract §9
    # local-metadata exemption).
    (store_a, _), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    push(store_a, "proj", ch)
    pull(store_b, ch)

    class TinyEmbedder:
        model, dim = "fake/tiny", 4

        def embed(self, texts):
            return [[float(len(t) % 7), 1.0, 0.0, 0.0] for t in texts]

    n = store_b.reembed(TinyEmbedder(), filters={"host": "host-a"})
    assert n > 0


def test_last_import_age_tracks_receiver_not_channel(
        host_a, host_b, tmp_path):
    # Plan S0.4 staleness is the RECEIVER's: a newer publish must not
    # move the metric until the receiver actually pulls it.
    from linecite.generic_ingester import ingest_paths as ip
    (store_a, root_a), (store_b, _) = host_a, host_b
    ch = DirChannel(tmp_path / "channel")
    receipts = tmp_path / "receipts.jsonl"
    from datetime import datetime, timezone
    push(store_a, "proj", ch)
    pull(store_b, ch, receipt_log=receipts)
    now = datetime.now(timezone.utc)       # single reference instant
    age1 = last_import_age_seconds(receipts, "host-a", "proj", now=now)
    # producer publishes a NEWER snapshot; receiver has not pulled
    import time
    time.sleep(1.1)                        # ensure a later exported_at
    (root_a / "docs/retry.md").write_text("# Retry\nnewer\n",
                                          encoding="utf-8")
    ip(store_a, root_a, host="host-a", origin_repo="proj")
    push(store_a, "proj", ch)
    age2 = last_import_age_seconds(receipts, "host-a", "proj", now=now)
    assert age2 == age1                    # unmoved by publish alone
    pull(store_b, ch, receipt_log=receipts)
    age3 = last_import_age_seconds(receipts, "host-a", "proj", now=now)
    assert age3 < age2                     # refreshed by the pull


def test_dirchannel_rejects_hostile_pointer_and_symlinks(tmp_path):
    import os
    from linecite_sync.channels import ChannelError, DirChannel as DC
    ch = DC(tmp_path / "ch")
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    # hostile pointer: absolute / traversal target
    (tmp_path / "ch" / "h--r.ptr").write_text("../../etc")
    import pytest as _pytest
    with _pytest.raises(ChannelError, match="unexpected target"):
        ch.fetch("h--r", tmp_path / "out")
    # publish over the hostile VALUE also refuses (round-7 class fix),
    # so reset the stream by removing the poisoned pointer first
    with _pytest.raises(ChannelError, match="unexpected target"):
        ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    (tmp_path / "ch" / "h--r.ptr").unlink()
    # symlinked package file refused even with a valid-shaped pointer
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    live = ch._live_dir("h--r")
    (live / "chunks.jsonl").unlink()
    os.symlink("/etc/hosts", live / "chunks.jsonl")
    with _pytest.raises(ChannelError, match="plain-file"):
        ch.fetch("h--r", tmp_path / "out2")


def test_dirchannel_rejects_symlinked_pointer_file(tmp_path):
    import os
    from linecite_sync.channels import ChannelError, DirChannel as DC
    ch = DC(tmp_path / "ch")
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    ptr = tmp_path / "ch" / "h--r.ptr"
    ptr.unlink()
    os.symlink("/etc/hosts", ptr)      # hostile: read arbitrary file
    import pytest as _pytest
    with _pytest.raises(ChannelError,
                        match="symlink|not a regular file"):
        ch.fetch("h--r", tmp_path / "out")
    # publish over a symlinked pointer must ALSO refuse before reading
    with _pytest.raises(ChannelError,
                        match="symlink|not a regular file"):
        ch.publish(_mini_pkg(tmp_path, 2), "h--r")


def test_dirchannel_publish_refuses_hostile_pointer_value(tmp_path):
    from linecite_sync.channels import ChannelError, DirChannel as DC
    ch = DC(tmp_path / "ch")
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    (tmp_path / "ch" / "h--r.ptr").write_text("../../outside")
    import pytest as _pytest
    with _pytest.raises(ChannelError, match="unexpected target"):
        ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    # refused publish cleaned up its staged version dir
    staged = [d for d in (tmp_path / "ch").iterdir()
              if d.is_dir() and d.name.startswith(".h--r.v-")]
    assert len(staged) == 1              # only the original remains


def test_dirchannel_whitespace_padded_pointer_refused(tmp_path):
    # Round-8 variant: .strip() must not launder padded hostile values
    # past the exact-shape validation — on fetch AND publish.
    from linecite_sync.channels import ChannelError, DirChannel as DC
    ch = DC(tmp_path / "ch")
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    ptr = tmp_path / "ch" / "h--r.ptr"
    ptr.write_text("\n" + ptr.read_text() + "\n")
    import pytest as _pytest
    with _pytest.raises(ChannelError, match="unexpected target"):
        ch.fetch("h--r", tmp_path / "out")
    with _pytest.raises(ChannelError, match="unexpected target"):
        ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    staged = [d for d in (tmp_path / "ch").iterdir()
              if d.is_dir() and d.name.startswith(".h--r.v-")]
    assert len(staged) == 1               # refused publish cleaned up


def test_dirchannel_invalid_utf8_pointer_refused(tmp_path):
    from linecite_sync.channels import ChannelError, DirChannel as DC
    ch = DC(tmp_path / "ch")
    ch.publish(_mini_pkg(tmp_path, 1), "h--r")
    (tmp_path / "ch" / "h--r.ptr").write_bytes(b"\xff\xfe garbage")
    import pytest as _pytest
    with _pytest.raises(ChannelError, match="not valid UTF-8"):
        ch.fetch("h--r", tmp_path / "out")
    with _pytest.raises(ChannelError, match="not valid UTF-8"):
        ch.publish(_mini_pkg(tmp_path, 2), "h--r")
    staged = [d for d in (tmp_path / "ch").iterdir()
              if d.is_dir() and d.name.startswith(".h--r.v-")]
    assert len(staged) == 1               # refused publish cleaned up
