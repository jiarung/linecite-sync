"""Protocol metrics (split-plan S.4) — sync correctness, not IR.

These measure the EXCHANGE, using only linecite's public retrieval
API and the channel's published manifests:

- convergence: after an exchange, two stores answer the same scoped
  queries with the same (source_path, content_sha) sets.
- roundtrip fidelity: every row of a published package is present in
  the receiving store with identical contract fields (v0 approach:
  per-row scoped lexical lookup + field comparison via the public
  Chunk dataclass — no store internals).
- snapshot age: now − exported_at of a stream's LIVE published
  manifest (manifest granularity per the contract; per-chunk
  verification timestamps are explicitly out of contract v0).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from linecite import Retriever

from linecite_sync.channels import Channel
from linecite_sync.sync import published_manifest

_CONTRACT_FIELDS = ("chunk_uid", "chunk_id", "host", "origin_repo",
                    "source_path", "source_type", "start_line",
                    "end_line", "text", "raw_text", "content_sha",
                    "chunk_ordinal", "heading_path", "artifact_type",
                    "language", "quality_tag")


def convergence(store_a, store_b, queries: list[str],
                scope: list[str], k: int = 10) -> bool:
    """True when both stores agree on every scoped query's result
    set, compared as (source_path, content_sha) frozensets."""
    ra, rb = Retriever(store_a), Retriever(store_b)
    for q in queries:
        a = {(c.source_path, c.content_sha)
             for c in ra.retrieve(q, k=k, scope=scope).chunks}
        b = {(c.source_path, c.content_sha)
             for c in rb.retrieve(q, k=k, scope=scope).chunks}
        if a != b:
            return False
    return True


@dataclass(frozen=True)
class FidelityReport:
    total: int
    matched: int

    @property
    def ok(self) -> bool:
        return self.total == self.matched


def roundtrip_fidelity(pkg_dir: Path | str, store) -> FidelityReport:
    """Every package row must exist in `store` with identical contract
    fields. v0 lookup: scoped lexical retrieval on the row's own text,
    matched by chunk_uid (public API only; adequate at snapshot scale)."""
    rows = [json.loads(l) for l in
            (Path(pkg_dir) / "chunks.jsonl").read_text("utf-8")
            .splitlines() if l]
    r = Retriever(store)
    matched = 0
    # k scales with package size so a valid row can't fall off the
    # candidate list among many similar chunks.
    k = max(10, 2 * len(rows))
    for row in rows:
        res = r.retrieve(row["text"], k=k, scope=[row["host"]])
        found = next((c for c in res.chunks
                      if c.chunk_uid == row["chunk_uid"]), None)
        if found and all(getattr(found, f) == row[f]
                         for f in _CONTRACT_FIELDS):
            matched += 1
    return FidelityReport(total=len(rows), matched=matched)


def published_age_seconds(channel: Channel, host: str,
                          origin_repo: str,
                          now: datetime | None = None) -> float:
    """Age of a stream's live PUBLISHED snapshot (channel-side —
    how stale the producer's publish is)."""
    m = published_manifest(channel, host, origin_repo)
    exported = datetime.strptime(
        m["exported_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return (now - exported).total_seconds()


def last_import_age_seconds(receipt_log: Path | str, host: str,
                            origin_repo: str,
                            now: datetime | None = None) -> float:
    """Age of the RECEIVER's last accepted import for a stream (plan
    S0.4 staleness): now − exported_at of the newest accepted/noop
    receipt. Reads the transport-owned receipt log written by
    pull(receipt_log=...); publishing a newer snapshot does NOT move
    this metric until the receiver actually pulls it."""
    last = None
    for line in Path(receipt_log).read_text("utf-8").splitlines():
        if not line:
            continue
        r = json.loads(line)
        if r["host"] == host and r["origin_repo"] == origin_repo \
                and r["status"] in ("accepted", "noop"):
            last = r
    if last is None:
        raise ValueError(
            f"no accepted import receipt for {host}/{origin_repo}")
    exported = datetime.strptime(
        last["exported_at"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return (now - exported).total_seconds()
