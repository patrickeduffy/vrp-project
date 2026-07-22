from __future__ import annotations

import os
import sys
import unittest
from collections import deque
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.storage.postgres import (
    DatabaseConfigurationError,
    PostgresReferenceDataRepository,
    ReferenceDataConflict,
    connect_from_environment,
)
from vrp.storage.reference_data import (
    ReferenceDataRelease,
    ReferenceRateObservation,
    StoredReferenceRateObservation,
)


class RecordingCursor:
    class Connection:
        def __init__(self, autocommit=False):
            self.autocommit = autocommit

    def __init__(self, responses=(), *, autocommit=False):
        self.responses = deque(responses)
        self.calls = []
        self.current = None
        self.connection = self.Connection(autocommit=autocommit)

    def execute(self, query, params=None):
        self.calls.append((" ".join(query.split()), params))
        self.current = self.responses.popleft() if self.responses else None
        return self

    def fetchone(self):
        result = self.current
        self.current = None
        return result


def make_release():
    return ReferenceDataRelease(
        release_id=uuid4(),
        dataset_key="FRED_SOFR",
        dataset_kind="REFERENCE_RATE",
        dataset_schema_version="v1",
        normalized_content_sha256="c" * 64,
        source_system="FRED",
        loader_version="loader-v1",
        normalized_data_asset_id=uuid4(),
        vintage_kind="LATEST_REVISED",
        retrieved_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
        source_row_count=2071,
        persisted_row_count=1,
        observation_start_date=date(2018, 4, 3),
        observation_end_date=date(2026, 7, 20),
    )


class ReferenceDataRepositoryTests(unittest.TestCase):
    def test_new_release_returns_inserted_identity(self):
        release = make_release()
        cursor = RecordingCursor(responses=[(release.release_id,)])
        result = PostgresReferenceDataRepository(cursor).register_release(release)
        self.assertTrue(result.inserted)
        self.assertEqual(result.record_id, release.release_id)
        self.assertIn("ON CONFLICT", cursor.calls[0][0])
        self.assertNotIn("DO UPDATE", cursor.calls[0][0])
        self.assertIsInstance(cursor.calls[0][1][-1], str)

    def test_existing_release_is_a_true_noop(self):
        release = make_release()
        existing_id = uuid4()
        cursor = RecordingCursor(
            responses=[
                None,
                (
                    existing_id,
                    "REFERENCE_RATE",
                    "FRED",
                    "LATEST_REVISED",
                    date(2018, 4, 3),
                    date(2026, 7, 20),
                    2071,
                    1,
                ),
            ]
        )
        result = PostgresReferenceDataRepository(cursor).register_release(release)
        self.assertFalse(result.inserted)
        self.assertEqual(result.record_id, existing_id)
        self.assertEqual(len(cursor.calls), 2)

    def test_missing_existing_release_after_conflict_fails_closed(self):
        cursor = RecordingCursor(responses=[None, None])
        with self.assertRaises(ReferenceDataConflict):
            PostgresReferenceDataRepository(cursor).register_release(make_release())

    def test_rate_insert_uses_bound_values_and_never_updates(self):
        cursor = RecordingCursor()
        row = StoredReferenceRateObservation(
            observation_id=uuid4(),
            release_id=uuid4(),
            value=ReferenceRateObservation(date(2026, 7, 20), Decimal("3.57")),
        )
        PostgresReferenceDataRepository(cursor).append_reference_rate(row)
        sql, params = cursor.calls[0]
        self.assertIn("INSERT INTO vrp.reference_rate_observations", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertEqual(params[4], Decimal("3.57"))
        self.assertNotIn("3.57", sql)

    def test_dataset_lock_is_transaction_scoped(self):
        cursor = RecordingCursor()
        PostgresReferenceDataRepository(cursor).acquire_dataset_lock(" FRED_SOFR ")
        self.assertIn("pg_advisory_xact_lock", cursor.calls[0][0])
        self.assertEqual(cursor.calls[0][1], ("vrp/reference-data/FRED_SOFR",))

    def test_dataset_lock_rejects_autocommit_cursor(self):
        cursor = RecordingCursor(autocommit=True)
        with self.assertRaisesRegex(DatabaseConfigurationError, "non-autocommit"):
            PostgresReferenceDataRepository(cursor).acquire_dataset_lock("FRED_SOFR")

    def test_connection_requires_environment_configuration(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(DatabaseConfigurationError):
                connect_from_environment()


if __name__ == "__main__":
    unittest.main(verbosity=2)
