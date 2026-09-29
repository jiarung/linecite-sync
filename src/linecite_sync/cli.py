"""linecite-sync CLI: push/pull a store over a channel.

    linecite-sync push --db kb.duckdb --repo my-repo --channel dir:/mnt/team
    linecite-sync pull --db kb.duckdb --channel git:/path/to/sync-clone
"""
from __future__ import annotations

import argparse
from typing import Optional

from linecite_sync.channels import Channel, DirChannel, GitChannel


def _parse_channel(spec: str) -> Channel:
    kind, _, path = spec.partition(":")
    if not path:
        raise ValueError(f"channel spec must be dir:PATH or git:PATH, "
                         f"got {spec!r}")
    if kind == "dir":
        return DirChannel(path)
    if kind == "git":
        return GitChannel(path)
    raise ValueError(f"unknown channel kind {kind!r} (dir|git)")


def _open_store(chunk_store_cls, db: str):
    """Open an existing store at its persisted dimension using ONLY
    linecite's public surface: try the default, and on the store's
    documented open-time dim-mismatch error, retry with the persisted
    dim it names. (No DuckDB internals — the boundary holds.)"""
    import re
    try:
        return chunk_store_cls(db)
    except ValueError as e:
        m = re.search(r"DB persists dim=(\d+)", str(e))
        if not m:
            raise
        return chunk_store_cls(db, dim=int(m.group(1)))


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(prog="linecite-sync",
                                 description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_push = sub.add_parser("push", help="export + publish the local "
                                         "partition of one repo")
    p_push.add_argument("--db", required=True)
    p_push.add_argument("--repo", required=True,
                        help="origin_repo partition to publish")
    p_push.add_argument("--channel", required=True)
    p_pull = sub.add_parser("pull", help="fetch, gate, and import every "
                                         "stream on the channel")
    p_pull.add_argument("--db", required=True)
    p_pull.add_argument("--channel", required=True)
    p_pull.add_argument("--receipts", default=None,
                        help="jsonl receipt log for accepted imports "
                             "(feeds the last-import staleness metric)")
    args = ap.parse_args(argv)

    from linecite import ChunkStore
    from linecite_sync import sync
    try:
        channel = _parse_channel(args.channel)
    except ValueError as e:
        ap.error(str(e))
    store = _open_store(ChunkStore, args.db)
    try:
        if args.cmd == "push":
            name = sync.push(store, args.repo, channel)
            print(f"published {name}")
            return 0
        outcomes = sync.pull(store, channel,
                             receipt_log=args.receipts)
        bad = 0
        for o in outcomes:
            extra = (f" seq={o.snapshot_seq}" if o.snapshot_seq
                     is not None else "")
            stats = (f" +{o.inserted}/-{o.deleted}"
                     if o.status == "accepted" else "")
            print(f"{o.status:<16} {o.name}{extra}{stats}"
                  f"{('  ' + o.detail) if o.detail else ''}")
            bad += o.status in ("gate_refused", "import_rejected")
        return 1 if bad else 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
