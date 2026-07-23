"""Cross-process writer lock for one project's EOD production pipeline."""

from __future__ import annotations

import json
import errno
import os
import re
import secrets
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping


EOD_WRITER_LOCK_RELATIVE_PATH = Path(
    "data/audit/vrp_hybrid_v2_eod/.eod_writer.lock"
)
EOD_CANONICAL_WRITER_LOCK_RELATIVE_PATH = Path(
    "data/audit/vrp_hybrid_v2_eod/.eod_canonical_writer.lock"
)
EOD_WRITER_LOCK_PATH_ENV = "VRP_EOD_WRITER_LOCK_PATH"
EOD_WRITER_LOCK_TOKEN_ENV = "VRP_EOD_WRITER_LOCK_TOKEN"
_TOKEN_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class EodWriterAlreadyRunningError(RuntimeError):
    """Raised when another process owns the project-wide EOD writer lock."""


class EodWriterDelegationError(RuntimeError):
    """Raised when a child cannot prove its caller owns the writer lock."""


@dataclass(frozen=True)
class EodWriterLease:
    path: Path
    token: str


def eod_writer_lock_path(project_root: Path) -> Path:
    """Return the deterministic ignored lock path for one project checkout."""

    parent = (
        project_root.expanduser().resolve()
        / EOD_WRITER_LOCK_RELATIVE_PATH.parent
    ).resolve()
    return parent / EOD_WRITER_LOCK_RELATIVE_PATH.name


def eod_canonical_writer_lock_path(project_root: Path) -> Path:
    """Return the lock retained by the legacy child while it can publish files."""

    parent = (
        project_root.expanduser().resolve()
        / EOD_CANONICAL_WRITER_LOCK_RELATIVE_PATH.parent
    ).resolve()
    return parent / EOD_CANONICAL_WRITER_LOCK_RELATIVE_PATH.name


def _ensure_lock_byte(handle) -> None:
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
        os.fsync(handle.fileno())


def _try_lock(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _is_lock_contention(exc: OSError) -> bool:
    return exc.errno in {errno.EACCES, errno.EAGAIN}


def _reject_unsafe_existing_lock(path: Path) -> None:
    if not os.path.lexists(path):
        return
    is_junction = getattr(path, "is_junction", lambda: False)
    if path.is_symlink() or is_junction() or not path.is_file():
        raise OSError(f"EOD lock must be a regular file: {path}")


def _unlock(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _owner_path(lock_path: Path) -> Path:
    return lock_path.with_name(f"{lock_path.name}.owner.json")


def _write_owner(lock_path: Path, token: str) -> None:
    payload = (
        json.dumps(
        {
            "contract": "vrp.hybrid_v2.eod_writer_lock",
            "owner_pid": os.getpid(),
            "token": token,
        },
        sort_keys=True,
        separators=(",", ":"),
        )
        + "\n"
    )
    destination = _owner_path(lock_path)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)


def _read_owner(lock_path: Path) -> dict[str, object]:
    path = _owner_path(lock_path)
    _reject_unsafe_existing_lock(path)
    try:
        before = path.read_bytes()
        after = path.read_bytes()
        if before != after:
            raise ValueError("writer-lock metadata changed while it was read")
        payload = json.loads(before.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise EodWriterDelegationError(
            "cannot validate the caller-owned EOD writer lock"
        ) from exc
    if not isinstance(payload, dict):
        raise EodWriterDelegationError("EOD writer-lock metadata must be an object")
    return payload


@contextmanager
def exclusive_eod_writer_lock(project_root: Path) -> Iterator[EodWriterLease]:
    """Hold the operation gate until the complete file and DB operation exits."""

    lock_path = eod_writer_lock_path(project_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    _reject_unsafe_existing_lock(lock_path)
    handle = lock_path.open("a+b")
    acquired = False
    try:
        _ensure_lock_byte(handle)
        try:
            _try_lock(handle)
        except OSError as exc:
            if not _is_lock_contention(exc):
                raise
            raise EodWriterAlreadyRunningError(
                f"another EOD writer is already running for {project_root.resolve()}"
            ) from exc
        acquired = True
        token = secrets.token_hex(32)
        _write_owner(lock_path, token)
        yield EodWriterLease(path=lock_path, token=token)
    finally:
        if acquired:
            _owner_path(lock_path).unlink(missing_ok=True)
            try:
                _unlock(handle)
            except OSError:
                # Closing the descriptor also releases an OS-owned lock.
                pass
        handle.close()


@contextmanager
def exclusive_eod_canonical_writer_lock(project_root: Path) -> Iterator[Path]:
    """Prevent overlap while a legacy child can change canonical file outputs."""

    lock_path = eod_canonical_writer_lock_path(project_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    _reject_unsafe_existing_lock(lock_path)
    handle = lock_path.open("a+b")
    acquired = False
    try:
        _ensure_lock_byte(handle)
        try:
            _try_lock(handle)
        except OSError as exc:
            if not _is_lock_contention(exc):
                raise
            raise EodWriterAlreadyRunningError(
                f"another canonical EOD writer is already running for {project_root.resolve()}"
            ) from exc
        acquired = True
        yield lock_path
    finally:
        if acquired:
            try:
                _unlock(handle)
            except OSError:
                pass
        handle.close()


def delegated_eod_writer_environment(lease: EodWriterLease) -> dict[str, str]:
    """Return the proof a legacy child needs to use its caller's held lock."""

    return {
        EOD_WRITER_LOCK_PATH_ENV: str(lease.path),
        EOD_WRITER_LOCK_TOKEN_ENV: lease.token,
    }


def _validate_delegated_lock(
    project_root: Path,
    *,
    supplied_path: str,
    supplied_token: str,
) -> None:
    expected_path = eod_writer_lock_path(project_root)
    try:
        observed_path = Path(supplied_path).expanduser().resolve(strict=True)
    except OSError as exc:
        raise EodWriterDelegationError("delegated EOD writer-lock path is invalid") from exc
    if observed_path != expected_path:
        raise EodWriterDelegationError(
            "delegated EOD writer lock is outside the expected project path"
        )
    if _TOKEN_PATTERN.fullmatch(supplied_token) is None:
        raise EodWriterDelegationError("delegated EOD writer-lock token is invalid")

    owner = _read_owner(observed_path)
    if (
        owner.get("contract") != "vrp.hybrid_v2.eod_writer_lock"
        or owner.get("token") != supplied_token
    ):
        raise EodWriterDelegationError(
            "delegated EOD writer-lock ownership evidence does not match"
        )

    probe = observed_path.open("a+b")
    unexpectedly_acquired = False
    try:
        _ensure_lock_byte(probe)
        try:
            _try_lock(probe)
        except OSError as exc:
            if not _is_lock_contention(exc):
                raise EodWriterDelegationError(
                    "cannot probe the delegated EOD writer lock"
                ) from exc
            return
        unexpectedly_acquired = True
    finally:
        if unexpectedly_acquired:
            try:
                _unlock(probe)
            except OSError:
                pass
        probe.close()
    raise EodWriterDelegationError(
        "delegated EOD writer lock is not held by the calling process"
    )


@contextmanager
def eod_writer_execution_lock(
    project_root: Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> Iterator[EodWriterLease]:
    """Acquire the lock directly or validate a wrapper-owned delegated lock."""

    source = os.environ if environment is None else environment
    supplied_path = source.get(EOD_WRITER_LOCK_PATH_ENV)
    supplied_token = source.get(EOD_WRITER_LOCK_TOKEN_ENV)
    if bool(supplied_path) != bool(supplied_token):
        raise EodWriterDelegationError(
            "delegated EOD writer-lock path and token must be supplied together"
        )
    if supplied_path and supplied_token:
        _validate_delegated_lock(
            project_root,
            supplied_path=supplied_path,
            supplied_token=supplied_token,
        )
        removed: dict[str, str] = {}
        if environment is None:
            for key in (EOD_WRITER_LOCK_PATH_ENV, EOD_WRITER_LOCK_TOKEN_ENV):
                value = os.environ.pop(key, None)
                if value is not None:
                    removed[key] = value
        try:
            yield EodWriterLease(path=Path(supplied_path).resolve(), token=supplied_token)
        finally:
            if environment is None:
                os.environ.update(removed)
        return

    with exclusive_eod_writer_lock(project_root) as lease:
        yield lease
