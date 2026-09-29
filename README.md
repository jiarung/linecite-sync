# linecite-sync

Transport companion for [linecite](https://github.com/jiarung/linecite)'s
cross-host snapshot exchange. Channels are dumb storage (a shared
directory, a git repo); the frozen snapshot contract does the
guaranteeing — digest verification, replay protection, host identity.
**No server, no daemon, no key.**

This repo never touches linecite's database internals: every store
interaction goes through linecite's public API, and the transport gate
re-verifies package digests with its **own independent implementation**
of the contract's canonical-bytes rules (cross-checked against
linecite's frozen golden vectors in tests).

## Onboarding (the whole flow)

```bash
pip install linecite-sync            # pulls in linecite

# you, on your machine:
linecite-ingest ./my-repo --db kb.duckdb --host laptop-a
linecite-sync push --db kb.duckdb --repo my-repo --channel dir:/mnt/team

# a teammate:
linecite-sync pull --db kb.duckdb --channel dir:/mnt/team
```

```python
# teammate's queries, scoped to your memory:
Retriever(store).retrieve("how do we retry", k=5, scope=["laptop-a"])
# optional: build a local semantic index over your chunks
store.reembed(LocalEmbedder())
```

Your citations arrive verbatim and verify as `remote_unverified` on
their machine — the files live with you, and linecite won't pretend
otherwise.

## Channels

- `dir:PATH` — any shared directory (NFS, Dropbox, rsync target, a
  USB stick). Atomic per-stream replace; latest snapshot only.
- `git:PATH` — a dedicated git clone as the wire (any remote works).
  History for free; publish refuses a dirty worktree.

## The two gates

1. linecite's export-side deny-list keeps secrets out of packages.
2. This repo's transport gate re-checks every fetched package before
   import: independent digest recompute, manifest required keys,
   pinned contract version, and a secret spot-check over the actual
   chunk payload (trust the exporter, verify the bytes).

A refused package never reaches the importer, and one bad stream
never aborts a pull of the others.

## Status

Phase S0 (see docs/plan.md): channels, gates, push/pull, protocol
metrics (convergence / roundtrip fidelity / snapshot age), CLI.
Targets linecite snapshot contract **v0 (UNSTABLE)** — this package
version-locks to the contract and refuses newer `format_version`s.

## License

Apache-2.0
