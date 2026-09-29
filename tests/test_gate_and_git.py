"""Gate refusals (secret spot-check, digest tamper, version cap),
GitChannel roundtrip over a local bare repo, and the interop check
over linecite's frozen golden vectors (two digest implementations
must agree)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import linecite
from linecite import ChunkStore, import_snapshot
from linecite.snapshot import SnapshotError

from linecite_sync import DirChannel, GateRefused, GitChannel, check_package, pull, push
from linecite_sync.gate import _canonical_json
from linecite_sync.sync import stream_name

from tests.conftest import make_host_store, make_repo


def _redigest(pkg: Path) -> None:
    import hashlib
    cb = (pkg / "chunks.jsonl").read_bytes()
    m = json.loads((pkg / "manifest.json").read_bytes())
    m["chunk_count"] = len([l for l in cb.split(b"\n") if l])
    m["content_digest"] = hashlib.sha256(cb).hexdigest()
    probe = {k: v for k, v in m.items() if k != "manifest_digest"}
    m["manifest_digest"] = hashlib.sha256(
        cb + _canonical_json(probe)).hexdigest()
    (pkg / "manifest.json").write_bytes(_canonical_json(m))


class TestGate:
    def _pkg(self, tmp_path):
        root = make_repo(tmp_path / "repo", {
            "docs/a.md": "# A\nplain content\n"})
        store = make_host_store("gate-host", root)
        ch = DirChannel(tmp_path / "ch")
        push(store, "proj", ch)
        return ch.fetch(stream_name("gate-host", "proj"),
                        tmp_path / "out")

    def test_clean_package_passes(self, tmp_path):
        rep = check_package(self._pkg(tmp_path))
        assert rep.host == "gate-host" and rep.chunk_count > 0

    def test_digest_tamper_refused_by_gate(self, tmp_path):
        pkg = self._pkg(tmp_path)
        m = json.loads((pkg / "manifest.json").read_bytes())
        m["origin_repo"] = "evil"
        (pkg / "manifest.json").write_bytes(_canonical_json(m))
        with pytest.raises(GateRefused) as e:
            check_package(pkg)
        assert e.value.code == "manifest_digest"

    def test_planted_secret_refused_even_with_valid_digests(
            self, tmp_path):
        # The exporter's deny-list is gate 1; transport is gate 2 —
        # a leaked credential with VALID digests must still refuse.
        pkg = self._pkg(tmp_path)
        rows = [json.loads(l) for l in
                (pkg / "chunks.jsonl").read_bytes().splitlines() if l]
        rows[0]["text"] = "password = hunter2-super-secret"
        # (identity now stale, but the gate runs BEFORE the importer's
        # identity checks — the secret must trip first)
        (pkg / "chunks.jsonl").write_bytes(
            b"".join(_canonical_json(r) + b"\n" for r in rows))
        _redigest(pkg)
        with pytest.raises(GateRefused) as e:
            check_package(pkg)
        assert e.value.code == "secret_material"

    def test_future_format_version_refused(self, tmp_path):
        pkg = self._pkg(tmp_path)
        m = json.loads((pkg / "manifest.json").read_bytes())
        m["format_version"] = 99
        (pkg / "manifest.json").write_bytes(_canonical_json(m))
        _redigest_keep_fv = json.loads(
            (pkg / "manifest.json").read_bytes())
        # redigest so ONLY the version gate can trip
        cb = (pkg / "chunks.jsonl").read_bytes()
        import hashlib
        probe = {k: v for k, v in _redigest_keep_fv.items()
                 if k != "manifest_digest"}
        _redigest_keep_fv["manifest_digest"] = hashlib.sha256(
            cb + _canonical_json(probe)).hexdigest()
        (pkg / "manifest.json").write_bytes(
            _canonical_json(_redigest_keep_fv))
        with pytest.raises(GateRefused) as e:
            check_package(pkg)
        assert e.value.code == "unknown_version"


class TestGitChannel:
    def test_publish_fetch_roundtrip_via_bare_repo(self, tmp_path):
        bare = tmp_path / "wire.git"
        subprocess.run(["git", "init", "--bare", "-q", str(bare)],
                       check=True)
        clone_a = tmp_path / "clone-a"
        clone_b = tmp_path / "clone-b"
        for c in (clone_a, clone_b):
            subprocess.run(["git", "clone", "-q", str(bare), str(c)],
                           check=True)
            subprocess.run(["git", "-C", str(c), "commit",
                            "--allow-empty", "-q", "-m", "init"],
                           check=True)
        subprocess.run(["git", "-C", str(clone_a), "push", "-q"],
                       check=True)
        subprocess.run(["git", "-C", str(clone_b), "pull", "-q"],
                       check=True)

        root = make_repo(tmp_path / "repo", {
            "docs/a.md": "# A\ngit channel body\n"})
        store_a = make_host_store("git-a", root)
        push(store_a, "proj", GitChannel(clone_a))

        store_b = ChunkStore(dim=4)
        store_b.init_local_identity("git-b")
        outcomes = pull(store_b, GitChannel(clone_b))
        assert [o.status for o in outcomes] == ["accepted"]

    def test_dirty_clone_refused(self, tmp_path):
        bare = tmp_path / "wire.git"
        subprocess.run(["git", "init", "--bare", "-q", str(bare)],
                       check=True)
        clone = tmp_path / "clone"
        subprocess.run(["git", "clone", "-q", str(bare), str(clone)],
                       check=True)
        (clone / "unrelated.txt").write_text("dirt")
        root = make_repo(tmp_path / "repo", {"a.md": "# A\nx\n"})
        store = make_host_store("git-c", root)
        from linecite_sync.channels import ChannelError
        with pytest.raises(ChannelError, match="dirty"):
            push(store, "proj", GitChannel(clone))


class TestVectorInterop:
    """linecite's frozen golden vectors, fetched through a channel and
    gated by OUR independent digest implementation, must land on their
    expected outcomes — the two contract implementations agree."""

    def test_frozen_vectors_through_channel_and_gate(self, tmp_path):
        vectors = (Path(linecite.__file__).resolve()
                   .parents[2] / "contract" / "vectors")
        if not vectors.is_dir():
            pytest.skip("linecite contract vectors not present "
                        "(non-editable install)")
        ch = DirChannel(tmp_path / "ch")
        for d in sorted(vectors.iterdir()):
            if not d.is_dir():
                continue
            expected = json.loads((d / "expected.json").read_text())
            from linecite_sync import stream_name as _sn
            name = _sn("vec-host", d.name)
            ch.publish(d, name)
            fetched = ch.fetch(name, tmp_path / d.name)
            if d.name == "v3-tampered-manifest":
                with pytest.raises(GateRefused) as e:
                    check_package(fetched)
                assert e.value.code == "manifest_digest"
                continue
            check_package(fetched)     # gate agrees with linecite
            store = ChunkStore(dim=4)
            store.init_local_identity("interop-runner")
            if expected["outcome"] == "accepted":
                res = import_snapshot(store, fetched)
                assert res.reconcile is None or \
                    res.reconcile.inserted == expected["inserted"]
            else:
                with pytest.raises(SnapshotError) as e:
                    import_snapshot(store, fetched)
                assert e.value.code == expected["code"]
            store.close()
