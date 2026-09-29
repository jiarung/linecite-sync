"""Transport gate — independent contract verification (second gate).

Deliberately does NOT import linecite's serializer: this is a second
implementation of the snapshot contract's canonical-bytes and digest
rules (contract §3, required-key subset of §8.1), so the two
implementations cross-check each other (interop is asserted over
linecite's frozen golden vectors in tests). The gate runs BEFORE a
fetched package is handed to `import_snapshot`; linecite's importer
remains the authoritative full validator.

Secret spot-check: linecite's export deny-hook is the first gate and
`excluded_count` is audit metadata — trusted, but verified here by
scanning the actual chunk text for high-risk patterns (trust, then
verify: a misconfigured exporter must not leak through transport).
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

# The contract version this transport targets (linecite contract/ v0).
PINNED_FORMAT_VERSION = 0

_MANIFEST_REQUIRED = ("format_version", "host", "host_identity_fingerprint",
                      "origin_repo", "content_digest", "snapshot_seq",
                      "manifest_digest", "chunk_count", "exported_at",
                      "excluded_count", "empty", "meta")

# High-risk content patterns for the transport spot-check.
SECRET_PATTERNS = tuple(re.compile(p) for p in (
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    r"\bAKIA[0-9A-Z]{16}\b",
    r"\bghp_[A-Za-z0-9]{36}\b",
    r"\bsk-[A-Za-z0-9]{20,}\b",
    r"(?i)\b(?:api[_-]?key|secret[_-]?access[_-]?key|password)\s*[=:]\s*\S+",
))


class GateRefused(Exception):
    """Package refused by the transport gate; `code` names the check."""

    def __init__(self, code: str, msg: str):
        super().__init__(f"{code}: {msg}")
        self.code = code


@dataclass(frozen=True)
class GateReport:
    host: str
    host_identity_fingerprint: str
    origin_repo: str
    snapshot_seq: int
    content_digest: str
    exported_at: str
    chunk_count: int


_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_HEX32_RE = re.compile(r"^[0-9a-f]{32}$")
_EXPORTED_AT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _canonical_json(obj) -> bytes:
    """Contract §3 canonical JSON — independent implementation."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def check_package(pkg_dir: Path | str,
                  secret_patterns=SECRET_PATTERNS) -> GateReport:
    """Verify a fetched snapshot package; raise GateRefused on any
    failure. Digests are recomputed from raw bytes per contract §3."""
    pkg = Path(pkg_dir)
    try:
        manifest_bytes = (pkg / "manifest.json").read_bytes()
        chunks_bytes = (pkg / "chunks.jsonl").read_bytes()
    except OSError as e:
        raise GateRefused("unreadable", str(e))
    def _no_dups(pairs):
        keys = [k for k, _ in pairs]
        if len(keys) != len(set(keys)):
            raise GateRefused("bad_manifest_json", "duplicate keys")
        return dict(pairs)

    def _no_nonfinite(tok):
        raise GateRefused("bad_manifest_json", f"non-finite {tok!r}")

    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"),
                              object_pairs_hook=_no_dups,
                              parse_constant=_no_nonfinite)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise GateRefused("bad_manifest_json", str(e))
    if not isinstance(manifest, dict):
        raise GateRefused("bad_manifest_json", "manifest is not an object")
    for key in _MANIFEST_REQUIRED:
        if key not in manifest:
            raise GateRefused("missing_key", key)
    fv = manifest["format_version"]
    if not isinstance(fv, int) or isinstance(fv, bool) or fv < 0:
        raise GateRefused("bad_format_version", repr(fv))
    if fv > PINNED_FORMAT_VERSION:
        raise GateRefused(
            "unknown_version",
            f"format_version {fv} > pinned {PINNED_FORMAT_VERSION} — "
            "upgrade linecite-sync")

    # Field shape checks for everything the transport itself consumes
    # (contract §2.2 subset) — bad-but-digest-valid packages must be
    # consistently gate_refused, not produce odd names downstream.
    def _shape(key, ok):
        if not ok(manifest[key]):
            raise GateRefused("bad_field", key)
    _int = lambda v: isinstance(v, int) and not isinstance(v, bool)  # noqa: E731
    _shape("host", lambda v: isinstance(v, str) and _HOST_RE.match(v))
    _shape("host_identity_fingerprint",
           lambda v: isinstance(v, str) and _HEX32_RE.match(v))
    _shape("origin_repo", lambda v: isinstance(v, str) and v)
    _shape("snapshot_seq", lambda v: _int(v) and v >= 0)
    _shape("chunk_count", lambda v: _int(v) and v >= 0)
    _shape("exported_at",
           lambda v: isinstance(v, str) and _EXPORTED_AT_RE.match(v))
    _shape("content_digest",
           lambda v: isinstance(v, str) and len(v) == 64)
    _shape("manifest_digest",
           lambda v: isinstance(v, str) and len(v) == 64)

    # Independent digest recompute (contract §3). Canonicalization
    # failures are gate refusals too — fault isolation must hold.
    if hashlib.sha256(chunks_bytes).hexdigest() != \
            manifest["content_digest"]:
        raise GateRefused("content_digest", "recompute mismatch")
    probe = {k: v for k, v in manifest.items() if k != "manifest_digest"}
    try:
        probe_bytes = _canonical_json(probe)
    except (TypeError, ValueError) as e:
        raise GateRefused("canonicalization", str(e))
    if hashlib.sha256(chunks_bytes + probe_bytes).hexdigest() != \
            manifest["manifest_digest"]:
        raise GateRefused("manifest_digest", "recompute mismatch")

    n_rows = len([l for l in chunks_bytes.split(b"\n") if l])
    if n_rows != manifest["chunk_count"]:
        raise GateRefused("chunk_count",
                          f"{manifest['chunk_count']} declared, "
                          f"{n_rows} rows")

    # Secret spot-check over the actual verbatim payload.
    text = chunks_bytes.decode("utf-8", errors="replace")
    for pat in secret_patterns:
        m = pat.search(text)
        if m:
            raise GateRefused(
                "secret_material",
                f"pattern {pat.pattern!r} matched in chunk payload — "
                "exporter deny-list misconfigured?")

    return GateReport(host=manifest["host"],
                      host_identity_fingerprint=manifest[
                          "host_identity_fingerprint"],
                      origin_repo=manifest["origin_repo"],
                      snapshot_seq=manifest["snapshot_seq"],
                      content_digest=manifest["content_digest"],
                      exported_at=manifest["exported_at"],
                      chunk_count=manifest["chunk_count"])
