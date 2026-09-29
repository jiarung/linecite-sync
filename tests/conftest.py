"""Shared fixtures: real linecite stores exchanging over real channels
— zero network, zero mocks of the contract."""
from __future__ import annotations

from pathlib import Path

import pytest

from linecite import ChunkStore
from linecite.generic_ingester import ingest_paths


def make_repo(root: Path, files: dict) -> Path:
    for rel, text in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
    return root


def make_host_store(host: str, repo_root: Path,
                    origin_repo: str = "proj") -> ChunkStore:
    s = ChunkStore(dim=4)
    s.init_local_identity(host)
    ingest_paths(s, repo_root, host=host, origin_repo=origin_repo)
    return s


@pytest.fixture()
def host_a(tmp_path):
    root = make_repo(tmp_path / "a-repo", {
        "docs/retry.md": "# Retry\nexponential backoff with jitter\n",
        "docs/queue.md": "# Queue\nmessages drain in fifo order\n"})
    store = make_host_store("host-a", root)
    return store, root


@pytest.fixture()
def host_b(tmp_path):
    root = make_repo(tmp_path / "b-repo", {
        "docs/cache.md": "# Cache\nlru eviction under pressure\n"})
    store = make_host_store("host-b", root)
    return store, root
