# linecite-sync — plan (founding doc)

Transport companion for [linecite](https://github.com/jiarung/linecite)'s
cross-host snapshot exchange. Implements **Phase S** of linecite's
`docs/design/cross-host-split-plan.md` against the frozen snapshot
contract (linecite `contract/` v0 UNSTABLE +
`docs/design/snapshot-contract.md`).

## Boundary (non-negotiable, from the split plan)

- **Transport only.** This repo moves snapshot directories between
  machines and orchestrates export/import. It NEVER touches DuckDB
  internals — every store interaction goes through linecite's public
  API (`export_snapshot`, `import_snapshot`, `ChunkStore` public
  methods).
- The transport gate re-verifies package digests **independently**
  (own hashlib + canonical-JSON implementation per contract §3) — a
  deliberate second implementation of the contract's canonical-bytes
  rules, exercising interoperability rather than importing linecite's
  serializer.
- Contract changes happen in linecite's repo, never here. This repo
  pins the linecite version it targets.

## Phase S0 — framework (this repo's first commit)

1. **Channel protocol** (`channels.py`): `publish(pkg_dir, name)` /
   `fetch(name, dest)` / `list_names()`. Two v0 channels:
   - `DirChannel` — a shared filesystem directory (NFS, Dropbox,
     rsync target, USB stick). Publish = atomic directory copy
     (temp + rename); fetch = copy out.
   - `GitChannel` — a git repository as the wire (works with any
     remote; tests use a local bare repo, zero network). Publish =
     copy under `snapshots/<name>/`, commit, push; fetch = pull, copy
     out. Requires a clean dedicated clone; refuses dirty worktrees.
2. **Transport gate** (`gate.py`, contract §3/§8.1 subset):
   independent recompute of `content_digest` and `manifest_digest`
   over the fetched bytes; manifest required-key presence; secret
   spot-check — deny-pattern scan over `chunks.jsonl` raw text as the
   SECOND gate (first gate is linecite's export-side deny hook;
   manifest `excluded_count` is audit metadata, trusted but verified
   here by content scan). Gate failures refuse to hand the package to
   import.
3. **Orchestration** (`sync.py`):
   - `push(store, origin_repo, channel, *, meta=None)` — export via
     linecite → gate (self-check) → publish. Publish name =
     `<host>--<origin_repo>` (one live snapshot per stream; history
     is the channel's concern, e.g. git log).
   - `pull(store, channel)` — list → fetch each → gate → skip own
     host (self-import is rejected by linecite anyway; skipping keeps
     pull idempotent and quiet) → `import_snapshot`; per-package
     outcome report (accepted / noop / skipped_self / gate_refused /
     import_rejected(code)). A bad package never aborts the others.
4. **Protocol metrics** (`metrics.py`, per split-plan S.4):
   - `convergence(store_a, store_b, scope)` — same scoped lexical
     query set → identical (source_path, content_sha) result sets.
   - `roundtrip_fidelity(src_store, dst_store, host, repo)` —
     contract-field equality across the exchange.
   - `snapshot_age(store, host, repo)` — now − exported_at of the
     last accepted import (manifest granularity, per contract; the
     import stream state is read via linecite's public reporting,
     v0: the ImportResult + manifest.json retained beside history).
5. **CLI** `linecite-sync push|pull --db ... --channel dir:PATH|git:PATH`.
6. **Onboarding** (README, the promised ~10 lines): ingest → push;
   teammate: pull → retrieve with `scope=` → `reembed` locally for
   semantic search.
7. **Tests** (all local, zero network):
   - convergence: A exports→channel→B imports; scoped queries agree;
     re-pull is a no-op (idempotence)
   - update flow: A edits repo, re-push; B pull sees seq advance;
     replay of the old directory refuses
   - gate: package with an undeclared planted secret is refused
     BEFORE import; tampered digest refused by the gate (not only by
     linecite)
   - GitChannel roundtrip via local bare repo
   - interop: linecite's frozen conformance vectors fetched through a
     channel + gate still yield the expected import outcomes
     (v1/v2 accept; v3/v4 gate- or import-refused per their
     expected.json)

## Deliberately NOT in S0

- Discovery/auth/daemons — the channel is dumb storage by design
- Forwarding re-export of remote partitions (contract v0 exclusion)
- Conflict resolution beyond the contract's replay rules
- Multi-snapshot history retention policies (git channel gets history
  for free; dir channel keeps latest only)
- Scheduling (cron/watchers) — compose externally

## Dependency & versioning

- `linecite` as a path/git dependency pinned to the contract-v0
  commit; CI installs it before tests. When linecite publishes to
  PyPI, switch to a version pin.
- Contract compatibility is asserted at runtime: gate refuses
  `format_version` above the pinned contract version (mirrors the
  importer rule).

## Risks

- Digest reimplementation drift — mitigated by the interop test over
  linecite's frozen golden vectors (both implementations must agree).
- GitChannel shelling out to `git` — kept minimal (clone assumed to
  exist; explicit errors on dirty/missing), no libgit dependency.
- Per-phase codex review before each commit (house convention).
