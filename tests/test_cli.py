"""CLI coverage: unit (channel spec parsing, store-dim reopen) and
integration (end-to-end push/pull through main() over a DirChannel
with real on-disk stores, receipts wired, exit codes)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from linecite_sync.channels import DirChannel, GitChannel
from linecite_sync.cli import _open_store, _parse_channel, main
from linecite_sync.sync import stream_name

from tests.conftest import make_host_store, make_repo


class TestUnits:
    def test_parse_channel_specs(self, tmp_path):
        assert isinstance(_parse_channel(f"dir:{tmp_path}"), DirChannel)
        with pytest.raises(ValueError, match="dir:PATH or git:PATH"):
            _parse_channel("nopath")
        with pytest.raises(ValueError, match="unknown channel kind"):
            _parse_channel(f"ftp:{tmp_path}")
        with pytest.raises(Exception):      # not a git clone
            _parse_channel(f"git:{tmp_path}")

    def test_open_store_retries_persisted_dim(self, tmp_path):
        from linecite import ChunkStore
        db = tmp_path / "kb.duckdb"
        ChunkStore(db, dim=4).close()       # persisted dim != default
        s = _open_store(ChunkStore, str(db))
        assert s.dim == 4
        s.close()


class TestEndToEnd:
    def test_push_pull_roundtrip_with_receipts(self, tmp_path, capsys):
        # producer store on disk
        root = make_repo(tmp_path / "repo",
                         {"docs/a.md": "# A\ncli body line\n"})
        db_a = tmp_path / "a.duckdb"
        from linecite import ChunkStore
        from linecite.generic_ingester import ingest_paths
        sa = ChunkStore(db_a, dim=4)
        sa.init_local_identity("cli-a")
        ingest_paths(sa, root, host="cli-a", origin_repo="proj")
        sa.close()
        # receiver store on disk
        db_b = tmp_path / "b.duckdb"
        sb = ChunkStore(db_b, dim=4)
        sb.init_local_identity("cli-b")
        sb.close()

        ch = f"dir:{tmp_path / 'channel'}"
        receipts = tmp_path / "receipts.jsonl"
        assert main(["push", "--db", str(db_a), "--repo", "proj",
                     "--channel", ch]) == 0
        out = capsys.readouterr().out
        assert stream_name("cli-a", "proj") in out

        assert main(["pull", "--db", str(db_b), "--channel", ch,
                     "--receipts", str(receipts)]) == 0
        out = capsys.readouterr().out
        assert "accepted" in out
        rows = [json.loads(l) for l in
                receipts.read_text().splitlines() if l]
        assert rows and rows[-1]["host"] == "cli-a" \
            and rows[-1]["status"] == "accepted"

        # re-pull: noop, still exit 0, second receipt appended
        assert main(["pull", "--db", str(db_b), "--channel", ch,
                     "--receipts", str(receipts)]) == 0
        assert "noop" in capsys.readouterr().out

    def test_pull_exit_1_on_bad_stream(self, tmp_path, capsys):
        from linecite import ChunkStore
        db = tmp_path / "b.duckdb"
        s = ChunkStore(db, dim=4)
        s.init_local_identity("cli-b")
        s.close()
        chroot = tmp_path / "channel"
        ch = DirChannel(chroot)
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "manifest.json").write_text("{}")
        (pkg / "chunks.jsonl").write_text("")
        ch.publish(pkg, stream_name("evil", "proj"))
        assert main(["pull", "--db", str(db),
                     "--channel", f"dir:{chroot}"]) == 1
        assert "gate_refused" in capsys.readouterr().out
