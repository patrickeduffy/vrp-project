"""Database-session coordination for reference and EOD shadow writers."""

from __future__ import annotations

import hashlib
import os
import re
from contextlib import contextmanager
from typing import Iterator


FINALIZATION_LEASE_TOKEN_ENV = "VRP_POSTGRES_FINALIZATION_LEASE_TOKEN"
GLOBAL_FINALIZATION_ADVISORY_KEY = int.from_bytes(
    hashlib.sha256(b"vrp.reference-data-and-eod-shadow.v1").digest()[:8],
    byteorder="big",
    signed=True,
)
DELEGATED_CHILD_ACTIVE_ADVISORY_KEY = int.from_bytes(
    hashlib.sha256(b"vrp.reference-data-and-eod-shadow.child-active.v1").digest()[:8],
    byteorder="big",
    signed=True,
)
_TOKEN = re.compile(r"^[0-9a-f]{64}$")


class FinalizationCoordinationError(RuntimeError):
    """A mutating loader is outside the required finalization lease."""


def token_advisory_key(token: str) -> int:
    if _TOKEN.fullmatch(token) is None:
        raise FinalizationCoordinationError(
            "PostgreSQL finalization lease token is invalid"
        )
    return int.from_bytes(
        hashlib.sha256(f"vrp.finalization-token:{token}".encode("ascii")).digest()[:8],
        byteorder="big",
        signed=True,
    )


def _try_lock(connection, key: int) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        row = cursor.fetchone()
    return bool(row and row[0] is True)


def _unlock(connection, key: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (key,))


def verify_delegated_finalization_lease(connection) -> str:
    """Prove a parent session owns both locks (diagnostic compatibility API)."""

    token = os.environ.get(FINALIZATION_LEASE_TOKEN_ENV, "")
    token_key = token_advisory_key(token)
    unexpectedly_acquired: list[int] = []
    try:
        for key in (GLOBAL_FINALIZATION_ADVISORY_KEY, token_key):
            if _try_lock(connection, key):
                unexpectedly_acquired.append(key)
        if unexpectedly_acquired:
            raise FinalizationCoordinationError(
                "mutating loader is not owned by the active PostgreSQL finalizer"
            )
        return token
    finally:
        for key in reversed(unexpectedly_acquired):
            try:
                _unlock(connection, key)
            except Exception:
                pass
        try:
            connection.rollback()
        except Exception:
            pass


@contextmanager
def delegated_finalization_child_lease(connection) -> Iterator[str]:
    """Keep a delegated child visible for its entire mutating operation.

    The child-active lock closes the parent-death hole inherent in a one-time
    lease check.  A replacement coordinator cannot start while a child from a
    dead parent still owns this lock.  Acquiring the child lock *before*
    proving the parent locks also makes the startup race safe: if a replacement
    coordinator wins the global lock, the old token proof fails and this child
    never mutates.
    """

    token = os.environ.get(FINALIZATION_LEASE_TOKEN_ENV, "")
    token_key = token_advisory_key(token)
    child_acquired = False
    unexpectedly_acquired: list[int] = []
    try:
        child_acquired = _try_lock(connection, DELEGATED_CHILD_ACTIVE_ADVISORY_KEY)
        if not child_acquired:
            raise FinalizationCoordinationError(
                "another delegated PostgreSQL mutating child is already active"
            )
        for key in (GLOBAL_FINALIZATION_ADVISORY_KEY, token_key):
            if _try_lock(connection, key):
                unexpectedly_acquired.append(key)
        if unexpectedly_acquired:
            raise FinalizationCoordinationError(
                "mutating loader is not owned by the active PostgreSQL finalizer"
            )
        try:
            connection.rollback()
        except Exception:
            pass
        yield token
    finally:
        for key in reversed(unexpectedly_acquired):
            try:
                _unlock(connection, key)
            except Exception:
                pass
        if child_acquired:
            try:
                _unlock(connection, DELEGATED_CHILD_ACTIVE_ADVISORY_KEY)
            except Exception:
                pass
        try:
            connection.rollback()
        except Exception:
            pass


@contextmanager
def standalone_finalization_lease(connection) -> Iterator[None]:
    """Serialize a standalone loader and reject a surviving delegated child."""

    acquired_keys: list[int] = []
    try:
        if not _try_lock(connection, GLOBAL_FINALIZATION_ADVISORY_KEY):
            raise FinalizationCoordinationError(
                "another PostgreSQL finalization writer is already running"
            )
        acquired_keys.append(GLOBAL_FINALIZATION_ADVISORY_KEY)
        if not _try_lock(connection, DELEGATED_CHILD_ACTIVE_ADVISORY_KEY):
            raise FinalizationCoordinationError(
                "a delegated PostgreSQL mutating child is still active"
            )
        acquired_keys.append(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY)
        try:
            connection.rollback()
        except Exception:
            pass
        yield
    finally:
        for key in reversed(acquired_keys):
            try:
                _unlock(connection, key)
            except Exception:
                pass
        try:
            connection.rollback()
        except Exception:
            pass


def has_delegated_finalization_lease() -> bool:
    return bool(os.environ.get(FINALIZATION_LEASE_TOKEN_ENV))


__all__ = [
    "DELEGATED_CHILD_ACTIVE_ADVISORY_KEY",
    "FINALIZATION_LEASE_TOKEN_ENV",
    "GLOBAL_FINALIZATION_ADVISORY_KEY",
    "FinalizationCoordinationError",
    "delegated_finalization_child_lease",
    "has_delegated_finalization_lease",
    "standalone_finalization_lease",
    "token_advisory_key",
    "verify_delegated_finalization_lease",
]
