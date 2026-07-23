from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.storage.finalization_coordination import (  # noqa: E402
    DELEGATED_CHILD_ACTIVE_ADVISORY_KEY,
    FINALIZATION_LEASE_TOKEN_ENV,
    GLOBAL_FINALIZATION_ADVISORY_KEY,
    FinalizationCoordinationError,
    delegated_finalization_child_lease,
    standalone_finalization_lease,
    token_advisory_key,
    verify_delegated_finalization_lease,
)
from vrp.orchestration.eod_postgres import (  # noqa: E402
    DatabaseFinalizationAlreadyRunningError,
    exclusive_database_finalization_lock,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        key = params[0]
        if "pg_try_advisory_lock" in query:
            acquired = key not in self.connection.held
            if acquired:
                self.connection.held.add(key)
                self.connection.owned.add(key)
            self.row = (acquired,)
        elif "pg_advisory_unlock" in query:
            released = key in self.connection.owned
            self.connection.owned.discard(key)
            self.connection.held.discard(key)
            self.row = (released,)
        else:
            raise AssertionError(query)

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, *, held=(), shared_held=None):
        self.held = shared_held if shared_held is not None else set(held)
        self.owned: set[int] = set()
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FinalizationCoordinationTests(unittest.TestCase):
    def test_delegated_loader_proves_both_parent_session_locks(self):
        token = "a" * 64
        connection = FakeConnection(
            held=(GLOBAL_FINALIZATION_ADVISORY_KEY, token_advisory_key(token))
        )
        with patch.dict(
            "os.environ",
            {FINALIZATION_LEASE_TOKEN_ENV: token},
            clear=False,
        ):
            self.assertEqual(verify_delegated_finalization_lease(connection), token)
        self.assertEqual(connection.owned, set())
        self.assertGreater(connection.rollbacks, 0)

    def test_delegation_fails_if_either_parent_lock_is_not_held(self):
        token = "b" * 64
        connection = FakeConnection(held=(GLOBAL_FINALIZATION_ADVISORY_KEY,))
        with patch.dict(
            "os.environ",
            {FINALIZATION_LEASE_TOKEN_ENV: token},
            clear=False,
        ):
            with self.assertRaisesRegex(
                FinalizationCoordinationError,
                "not owned",
            ):
                verify_delegated_finalization_lease(connection)
        self.assertEqual(connection.owned, set())

    def test_standalone_loader_holds_and_releases_the_global_lock(self):
        connection = FakeConnection()
        with standalone_finalization_lease(connection):
            self.assertIn(GLOBAL_FINALIZATION_ADVISORY_KEY, connection.owned)
            self.assertIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, connection.owned)
        self.assertNotIn(GLOBAL_FINALIZATION_ADVISORY_KEY, connection.held)
        self.assertNotIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, connection.held)

    def test_standalone_loader_rejects_an_active_finalizer(self):
        connection = FakeConnection(held=(GLOBAL_FINALIZATION_ADVISORY_KEY,))
        with self.assertRaisesRegex(
            FinalizationCoordinationError,
            "already running",
        ):
            with standalone_finalization_lease(connection):
                self.fail("standalone loader bypassed the active finalizer")

    def test_surviving_child_blocks_replacement_after_parent_session_dies(self):
        token = "c" * 64
        token_key = token_advisory_key(token)
        shared_held = {GLOBAL_FINALIZATION_ADVISORY_KEY, token_key}
        child = FakeConnection(shared_held=shared_held)
        replacement = FakeConnection(shared_held=shared_held)

        with patch.dict(
            "os.environ",
            {FINALIZATION_LEASE_TOKEN_ENV: token},
            clear=False,
        ):
            with delegated_finalization_child_lease(child):
                self.assertIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, shared_held)

                # Simulate PostgreSQL releasing the dead parent's session locks
                # while the mutating child connection remains alive.
                shared_held.discard(GLOBAL_FINALIZATION_ADVISORY_KEY)
                shared_held.discard(token_key)

                with self.assertRaisesRegex(
                    FinalizationCoordinationError,
                    "still active",
                ):
                    with standalone_finalization_lease(replacement):
                        self.fail("replacement overlapped the orphaned child")

        self.assertNotIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, shared_held)
        with standalone_finalization_lease(replacement):
            self.assertIn(GLOBAL_FINALIZATION_ADVISORY_KEY, replacement.owned)

    def test_late_child_fails_old_token_proof_after_replacement_wins_global_lock(self):
        old_token = "d" * 64
        shared_held = {GLOBAL_FINALIZATION_ADVISORY_KEY}
        child = FakeConnection(shared_held=shared_held)

        with patch.dict(
            "os.environ",
            {FINALIZATION_LEASE_TOKEN_ENV: old_token},
            clear=False,
        ):
            with self.assertRaisesRegex(
                FinalizationCoordinationError,
                "not owned",
            ):
                with delegated_finalization_child_lease(child):
                    self.fail("late child mutated under a replacement parent")

        self.assertNotIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, shared_held)
        self.assertNotIn(token_advisory_key(old_token), shared_held)

    def test_parent_coordinator_probes_child_active_before_granting_lease(self):
        shared_held = {DELEGATED_CHILD_ACTIVE_ADVISORY_KEY}
        connection = FakeConnection(shared_held=shared_held)
        fake_psycopg = SimpleNamespace(connect=lambda *_args, **_kwargs: connection)

        with (
            patch.dict(sys.modules, {"psycopg": fake_psycopg}),
            patch.dict("os.environ", {"VRP_DATABASE_URL": "postgresql://test"}),
        ):
            with self.assertRaisesRegex(
                DatabaseFinalizationAlreadyRunningError,
                "still active",
            ):
                with exclusive_database_finalization_lock(
                    SimpleNamespace(
                        run_dir=ROOT / "missing-audit/20260721_170000",
                        environment="test",
                    )
                ):
                    self.fail("parent lease ignored an orphaned child")

        self.assertNotIn(GLOBAL_FINALIZATION_ADVISORY_KEY, shared_held)
        self.assertTrue(connection.closed)

    def test_parent_startup_barrier_is_released_only_after_token_exists(self):
        connection = FakeConnection()
        fake_psycopg = SimpleNamespace(connect=lambda *_args, **_kwargs: connection)

        with (
            patch.dict(sys.modules, {"psycopg": fake_psycopg}),
            patch.dict("os.environ", {"VRP_DATABASE_URL": "postgresql://test"}),
        ):
            with exclusive_database_finalization_lock(
                SimpleNamespace(
                    run_dir=ROOT / "missing-audit/20260721_170000",
                    environment="test",
                )
            ) as token:
                self.assertIn(GLOBAL_FINALIZATION_ADVISORY_KEY, connection.owned)
                self.assertIn(token_advisory_key(token), connection.owned)
                self.assertNotIn(
                    DELEGATED_CHILD_ACTIVE_ADVISORY_KEY,
                    connection.held,
                )

        self.assertEqual(connection.held, set())
        self.assertTrue(connection.closed)

    def test_parent_verifies_prior_database_continuity_behind_startup_barrier(self):
        connection = FakeConnection()
        fake_psycopg = SimpleNamespace(connect=lambda *_args, **_kwargs: connection)
        observed = []

        def verify(active_connection, evidence):
            self.assertIs(active_connection, connection)
            self.assertEqual(evidence, ["prior-run"])
            self.assertIn(GLOBAL_FINALIZATION_ADVISORY_KEY, connection.owned)
            self.assertIn(DELEGATED_CHILD_ACTIVE_ADVISORY_KEY, connection.owned)
            observed.append("verified")

        with (
            patch.dict(sys.modules, {"psycopg": fake_psycopg}),
            patch.dict("os.environ", {"VRP_DATABASE_URL": "postgresql://test"}),
            patch(
                "vrp.orchestration.eod_postgres.completed_eod_finalization_evidence",
                return_value=["prior-run"],
            ),
            patch(
                "vrp.orchestration.eod_postgres.verify_database_finalization_continuity",
                side_effect=verify,
            ),
        ):
            with exclusive_database_finalization_lock(
                SimpleNamespace(
                    run_dir=ROOT / "missing-audit/20260721_170000",
                    environment="test",
                )
            ):
                self.assertEqual(observed, ["verified"])

        self.assertEqual(connection.held, set())


if __name__ == "__main__":
    unittest.main(verbosity=2)
