"""push/pull orchestration over a channel.

Everything store-shaped goes through linecite's PUBLIC API only
(export_snapshot / import_snapshot / ChunkStore.local_identity);
this module never opens the DuckDB file itself.
"""
from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from linecite import export_snapshot, import_snapshot
from linecite.snapshot import SnapshotError

from linecite_sync.channels import Channel, encode_stream_name
from linecite_sync.gate import GateRefused, check_package


@dataclass(frozen=True)
class PullOutcome:
    name: str
    status: str            # accepted | noop | skipped_self |
    #                        gate_refused | import_rejected
    detail: str = ""
    snapshot_seq: Optional[int] = None
    inserted: int = 0
    deleted: int = 0
    host: Optional[str] = None
    origin_repo: Optional[str] = None
    exported_at: Optional[str] = None


def stream_name(host: str, origin_repo: str) -> str:
    """Reversible encoded channel name — any legal
    host/origin_repo (repos may contain '/', spaces, ...)."""
    return encode_stream_name(host, origin_repo)


def push(store, origin_repo: str, channel: Channel, *,
         meta: Optional[dict] = None) -> str:
    """Export the local partition and publish it. Returns the stream
    name. The gate self-checks our own export before it leaves the
    machine — a misconfigured deny-list fails HERE, not at a
    teammate's pull."""
    ident = store.local_identity()
    if ident is None:
        raise ValueError("store has no local identity; ingest first")
    host = ident[0]
    with tempfile.TemporaryDirectory(prefix="lcsync-push-") as td:
        pkg = export_snapshot(store, origin_repo, Path(td) / "pkg",
                              meta=meta)
        check_package(pkg)                 # self-gate (raises)
        name = stream_name(host, origin_repo)
        channel.publish(pkg, name)
    return name


def pull(store, channel: Channel, *,
         receipt_log: Optional[Path | str] = None) -> list[PullOutcome]:
    """Fetch every stream on the channel, gate it, and import what
    belongs to other hosts. One bad package never aborts the rest.

    receipt_log: optional jsonl path — every accepted/noop import
    appends a receipt (ts_imported, host, origin_repo, snapshot_seq,
    exported_at, status). This transport-owned record is what the
    last-import staleness metric reads (plan S0.4): linecite's import
    stream state is store-internal, and the receiver's freshness is a
    transport concern anyway."""
    ident = store.local_identity()
    if ident is None:
        raise ValueError("store has no local identity; ingest first")
    local_host, local_fp = ident
    outcomes: list[PullOutcome] = []
    for name in channel.list_names():
        with tempfile.TemporaryDirectory(prefix="lcsync-pull-") as td:
            try:
                pkg = channel.fetch(name, td)
            except Exception as e:          # noqa: BLE001 — report, move on
                outcomes.append(PullOutcome(name, "gate_refused",
                                            f"fetch failed: {e}"))
                continue
            try:
                report = check_package(pkg)
            except GateRefused as e:
                outcomes.append(PullOutcome(name, "gate_refused",
                                            str(e)))
                continue
            if report.host == local_host and \
                    report.host_identity_fingerprint == local_fp:
                # Genuinely our own stream. A stream that merely SHARES
                # our host id (different fingerprint) is NOT skipped:
                # it goes to import_snapshot so contract §8.5's
                # host_collision semantics apply and get reported.
                outcomes.append(PullOutcome(
                    name, "skipped_self",
                    snapshot_seq=report.snapshot_seq,
                    host=report.host, origin_repo=report.origin_repo,
                    exported_at=report.exported_at))
                continue
            try:
                res = import_snapshot(store, pkg)
            except SnapshotError as e:
                outcomes.append(PullOutcome(
                    name, "import_rejected", str(e),
                    snapshot_seq=report.snapshot_seq,
                    host=report.host, origin_repo=report.origin_repo,
                    exported_at=report.exported_at))
                continue
            outcome = PullOutcome(
                name, res.status,
                snapshot_seq=res.snapshot_seq,
                inserted=(res.reconcile.inserted if res.reconcile else 0),
                deleted=(res.reconcile.deleted if res.reconcile else 0),
                host=report.host, origin_repo=report.origin_repo,
                exported_at=report.exported_at)
            outcomes.append(outcome)
            if receipt_log is not None:
                from datetime import datetime, timezone
                rl = Path(receipt_log)
                rl.parent.mkdir(parents=True, exist_ok=True)
                with rl.open("a") as f:
                    f.write(json.dumps({
                        "ts_imported": datetime.now(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "host": report.host,
                        "origin_repo": report.origin_repo,
                        "snapshot_seq": res.snapshot_seq,
                        "exported_at": report.exported_at,
                        "status": res.status}) + "\n")
    return outcomes


def published_manifest(channel: Channel, host: str,
                       origin_repo: str) -> dict:
    """Read a stream's live manifest from the channel (metrics use)."""
    with tempfile.TemporaryDirectory(prefix="lcsync-meta-") as td:
        pkg = channel.fetch(stream_name(host, origin_repo), td)
        return json.loads((Path(pkg) / "manifest.json")
                          .read_text("utf-8"))
