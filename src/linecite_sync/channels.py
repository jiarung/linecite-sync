"""Snapshot channels — dumb storage the packages travel over.

A channel knows three verbs and nothing about the contract:
publish(pkg_dir, name), fetch(name, dest_dir) -> Path, list_names().
Names are one-per-stream (encoded `<host>--<origin_repo>`, see
encode_stream_name): a publish replaces the stream's live snapshot;
history is the channel's own concern (git keeps it for free, a
directory does not).
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import urllib.parse
import uuid
from pathlib import Path
from typing import Protocol

# Encoded-part alphabet: '-' is percent-encoded INSIDE parts so the
# '--' separator can never be ambiguous (hosts may legally contain
# '-' and even '--'; origin_repo is only required to be non-empty).
_PART_RE = r"[A-Za-z0-9._~%]+"
_NAME_RE = re.compile(rf"^{_PART_RE}--{_PART_RE}$")


class ChannelError(RuntimeError):
    pass


class PointerMissing(ChannelError):
    """The stream has no pointer yet (first publish) — the ONLY
    pointer-read failure publish may treat as benign."""


def _encode_part(part: str) -> str:
    if not part:
        raise ChannelError("empty stream-name part")
    return urllib.parse.quote(part, safe="._~").replace("-", "%2D")


def encode_stream_name(host: str, origin_repo: str) -> str:
    """Reversible channel name for a stream. Any legal host /
    origin_repo (repos may contain '/', spaces, …) round-trips."""
    return f"{_encode_part(host)}--{_encode_part(origin_repo)}"


def decode_stream_name(name: str) -> tuple[str, str]:
    host_enc, sep, repo_enc = name.partition("--")
    if not sep or not host_enc or not repo_enc:
        raise ChannelError(f"invalid stream name: {name!r}")
    return (urllib.parse.unquote(host_enc),
            urllib.parse.unquote(repo_enc))


def _check_name(name: str) -> None:
    if not _NAME_RE.match(name):
        raise ChannelError(f"invalid snapshot name: {name!r} "
                           "(expected encoded <host>--<origin_repo>)")


def _copy_package(src: Path, dst: Path) -> None:
    """Copy the two package files; refuse malformed packages early.
    Channel storage is only semi-trusted: symlinked package files are
    refused so a hostile channel cannot make us read arbitrary paths
    before the gate even runs."""
    if src.is_symlink():
        raise ChannelError(f"symlinked package dir refused: {src}")
    for f in ("manifest.json", "chunks.jsonl"):
        fp = src / f
        if fp.is_symlink() or not fp.is_file():
            raise ChannelError(
                f"not a plain-file snapshot package ({f}): {src}")
    dst.mkdir(parents=True, exist_ok=True)
    for f in ("manifest.json", "chunks.jsonl"):
        shutil.copy2(src / f, dst / f)



def _read_ptr_nofollow(ptr: Path) -> str:
    """Read a pointer file with no-follow semantics, atomically on one
    fd: open O_NOFOLLOW (symlink -> refuse), fstat THAT fd (no TOCTOU
    window), enforce regular-file + size cap, read from the fd."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(ptr, flags)
    except OSError as e:
        import errno
        if e.errno == getattr(errno, "ELOOP", None):
            raise ChannelError(f"pointer is a symlink: {ptr}")
        if e.errno == errno.ENOENT:
            raise PointerMissing(f"no such stream pointer: {ptr}")
        raise ChannelError(f"unreadable stream pointer {ptr.name!r}: {e}")
    try:
        import stat as _stat
        st = os.fstat(fd)
        if not _stat.S_ISREG(st.st_mode):
            raise ChannelError(f"pointer is not a regular file: {ptr}")
        if st.st_size > 4096:
            raise ChannelError(f"pointer is oversized: {ptr}")
        raw = os.read(fd, 4096)
        try:
            # RAW value, no normalization: stripping would launder
            # whitespace-padded hostile values past the exact-shape
            # validation. publish() writes the bare version string,
            # so exact equality is the honest comparison.
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ChannelError(f"pointer is not valid UTF-8: {ptr} "
                               f"({e})")
    finally:
        os.close(fd)


class Channel(Protocol):
    def publish(self, pkg_dir: Path | str, name: str) -> None: ...
    def fetch(self, name: str, dest_dir: Path | str) -> Path: ...
    def list_names(self) -> list[str]: ...


class DirChannel:
    """A shared filesystem directory (NFS/Dropbox/rsync target/USB).

    Publish keeps the PREVIOUS snapshot visible until the new one is
    fully committed: packages land in versioned directories
    (`.<name>.v-<uuid>/`) and the live version is named by a pointer
    file (`<name>.ptr`) swapped with an atomic os.replace — a
    concurrent fetch sees the old or the new package, never a gap and
    never a half-written directory. A crash mid-publish leaves the
    old snapshot live.

    Superseded versions are GC'd only after `gc_grace_seconds` (an
    in-flight reader that resolved the old pointer gets time to finish
    copying), and fetch() re-resolves the pointer and retries if its
    version directory vanishes mid-copy — old-or-new, never a gap."""

    def __init__(self, root: Path | str, *, gc_grace_seconds: int = 300):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.gc_grace_seconds = gc_grace_seconds

    def _ptr(self, name: str) -> Path:
        return self.root / f"{name}.ptr"

    def _validated_version(self, name: str, version: str) -> Path:
        """ONE validation path for every pointer VALUE: exact shape
        publish() writes, confined under the channel root. Every use
        of a pointer value (fetch resolution, publish GC bookkeeping)
        goes through here — the read path is _read_ptr_nofollow, the
        value path is this."""
        if not re.match(rf"^\.{re.escape(name)}\.v-[0-9a-f]{{32}}$",
                        version):
            raise ChannelError(
                f"stream {name!r} pointer has an unexpected target "
                f"{version!r}")
        d = self.root / version
        try:
            d.resolve().relative_to(self.root.resolve())
        except ValueError:
            raise ChannelError(
                f"stream {name!r} escapes the channel root")
        return d

    def _live_dir(self, name: str) -> Path:
        # ONE no-follow fd-based read path for every pointer access.
        version = _read_ptr_nofollow(self._ptr(name))
        d = self._validated_version(name, version)
        if d.is_symlink() or not d.is_dir():
            raise ChannelError(f"stream {name!r} pointer is dangling "
                               "or symlinked")
        return d

    def publish(self, pkg_dir: Path | str, name: str) -> None:
        _check_name(name)
        version = f".{name}.v-{uuid.uuid4().hex}"
        new_dir = self.root / version
        _copy_package(Path(pkg_dir), new_dir)
        # Pointer read + value validation share fetch's exact paths;
        # ONLY a missing pointer (first publish) is benign — every
        # other pointer failure refuses the publish and cleans the
        # staged version dir.
        old_version = None
        try:
            raw = _read_ptr_nofollow(self._ptr(name))
        except PointerMissing:
            raw = None
        except ChannelError:
            shutil.rmtree(new_dir, ignore_errors=True)
            raise
        if raw is not None:
            try:
                self._validated_version(name, raw)
            except ChannelError:
                shutil.rmtree(new_dir, ignore_errors=True)
                raise
            old_version = raw
        # Atomic pointer swap: write tmp, fsync, os.replace.
        fd, tmp = tempfile.mkstemp(prefix=f".{name}.ptr.",
                                   dir=self.root)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(version)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._ptr(name))
        except Exception:
            Path(tmp).unlink(missing_ok=True)
            shutil.rmtree(new_dir, ignore_errors=True)
            raise
        # Retirement mark + grace-based GC (post-commit). Grace is
        # measured from SUPERSESSION, not creation: touching the old
        # version dir right after the pointer swap restamps its mtime
        # as the retirement time — otherwise a snapshot that had been
        # live longer than the grace window would be deleted the
        # instant it is superseded, re-opening the reader race.
        import time
        if old_version and old_version != version:
            try:
                os.utime(self.root / old_version)
            except OSError:
                pass
        cutoff = time.time() - self.gc_grace_seconds
        for d in self.root.glob(f".{name}.v-*"):
            if d.name != version and d.is_dir() and \
                    d.stat().st_mtime < cutoff:
                shutil.rmtree(d, ignore_errors=True)

    def fetch(self, name: str, dest_dir: Path | str) -> Path:
        _check_name(name)
        dest = Path(dest_dir) / name
        last_err: Exception | None = None
        for _ in range(3):
            src = self._live_dir(name)
            try:
                _copy_package(src, dest)
                return dest
            except (OSError, ChannelError) as e:
                # Version dir vanished mid-copy (publisher GC beat the
                # grace window, e.g. gc_grace_seconds=0): re-resolve
                # the pointer and retry — old or new, never a gap.
                last_err = e
                shutil.rmtree(dest, ignore_errors=True)
        raise ChannelError(
            f"fetch of {name!r} kept losing its version dir: {last_err}")

    def list_names(self) -> list[str]:
        return sorted(p.name[:-4] for p in self.root.glob("*.ptr")
                      if _NAME_RE.match(p.name[:-4]))


class GitChannel:
    """A git working clone as the wire (any git remote works; tests
    use a local bare repo — zero network dependency in this package).

    The clone is DEDICATED to sync: publish refuses a dirty worktree
    rather than committing unrelated changes. Snapshots live under
    snapshots/<name>/ in the repo; git's own commit machinery makes
    each publish atomic and keeps history."""

    def __init__(self, clone_path: Path | str):
        self.clone = Path(clone_path)
        if not (self.clone / ".git").exists():
            raise ChannelError(f"not a git clone: {self.clone}")

    def _git(self, *args: str) -> str:
        res = subprocess.run(["git", *args], cwd=self.clone,
                             capture_output=True, text=True)
        if res.returncode != 0:
            raise ChannelError(
                f"git {' '.join(args)} failed: {res.stderr.strip()}")
        return res.stdout

    def _has_remote(self) -> bool:
        return bool(self._git("remote").strip())

    def _sync_down(self) -> None:
        if self._has_remote():
            self._git("pull", "--ff-only")

    def publish(self, pkg_dir: Path | str, name: str) -> None:
        _check_name(name)
        if self._git("status", "--porcelain").strip():
            raise ChannelError(
                f"refusing to publish from a dirty clone: {self.clone} "
                "(this clone must be dedicated to linecite-sync)")
        self._sync_down()
        dest = self.clone / "snapshots" / name
        if dest.exists():
            shutil.rmtree(dest)
        _copy_package(Path(pkg_dir), dest)
        self._git("add", "-A", "snapshots")
        if self._git("status", "--porcelain").strip():
            self._git("commit", "-m", f"sync: publish {name}")
            if self._has_remote():
                self._git("push")

    def fetch(self, name: str, dest_dir: Path | str) -> Path:
        _check_name(name)
        self._sync_down()
        src = self.clone / "snapshots" / name
        dest = Path(dest_dir) / name
        _copy_package(src, dest)
        return dest

    def list_names(self) -> list[str]:
        self._sync_down()
        root = self.clone / "snapshots"
        if not root.is_dir():
            return []
        return sorted(p.name for p in root.iterdir()
                      if p.is_dir() and _NAME_RE.match(p.name))
