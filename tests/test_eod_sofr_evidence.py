from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from vrp.eod_shadow.sofr_evidence import (
    SofrEvidenceError,
    load_sofr_updater_evidence,
)
from vrp.reference_history.sources import normalize_sofr_csv as real_normalize_sofr_csv


TIMESTAMP = "20260405_210000_UTC"
SNAPSHOT_NAME = "fred_sofr_history_refreshed_snapshot.csv"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SofrUpdaterEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.audit_directory = self.root / TIMESTAMP
        self.audit_directory.mkdir()
        self.snapshot_path = self.audit_directory / SNAPSHOT_NAME
        self.snapshot_path.write_text(
            "observation_date,SOFR\n"
            "2018-04-03,1.83\n"
            "2018-04-04,1.74\n",
            encoding="utf-8",
        )
        self.manifest_path = self.audit_directory / "sofr_update_manifest.json"
        self.payload = {
            "status": "PUBLISHED",
            "published": True,
            "hard_checks_passed": True,
            "source": "FRED_SOFR",
            "run_timestamp_utc": TIMESTAMP,
            # These intentionally retain an old Windows root. Resolution is by the
            # timestamp directory and snapshot filename, not by the recorded root.
            "audit_directory": f"C:\\old-project\\data\\audit\\{TIMESTAMP}",
            "audit_files": {
                "refreshed_snapshot": (
                    f"C:\\old-project\\data\\audit\\{TIMESTAMP}\\{SNAPSHOT_NAME}"
                )
            },
            "new_min_date": "2018-04-03",
            "new_max_date": "2018-04-04",
            "new_rows_total": 2,
        }
        self._write_manifest()
        self.run_manifest = {"sofr_manifest": str(self.manifest_path)}

    def _write_manifest(self) -> None:
        self.manifest_path.write_text(
            json.dumps(self.payload, sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _test_normalizer(path: Path):
        return real_normalize_sofr_csv(path, enforce_production_coverage=False)

    def _load(self, valuation_date: date = date(2018, 4, 5)):
        with patch(
            "vrp.eod_shadow.sofr_evidence.normalize_sofr_csv",
            side_effect=self._test_normalizer,
        ) as normalizer:
            evidence = load_sofr_updater_evidence(
                self.run_manifest,
                valuation_date,
            )
        normalizer.assert_called_once_with(self.snapshot_path.resolve())
        return evidence

    def test_returns_content_addressed_latest_observation_and_is_immutable(self):
        expected_history = self._test_normalizer(self.snapshot_path)
        evidence = self._load()

        self.assertEqual(evidence.updater_manifest_path, self.manifest_path.resolve())
        self.assertEqual(evidence.updater_manifest_sha256, _sha256(self.manifest_path))
        self.assertEqual(evidence.refreshed_snapshot_path, self.snapshot_path.resolve())
        self.assertEqual(evidence.refreshed_snapshot_sha256, _sha256(self.snapshot_path))
        self.assertEqual(
            evidence.normalized_content_sha256,
            expected_history.content_sha256,
        )
        self.assertEqual(evidence.start_date, date(2018, 4, 3))
        self.assertEqual(evidence.end_date, date(2018, 4, 4))
        self.assertEqual(evidence.row_count, 2)
        self.assertEqual(evidence.observation_date, date(2018, 4, 4))
        self.assertEqual(evidence.rate_decimal, Decimal("0.017400000000"))
        self.assertEqual(evidence.row_sha256, expected_history.rows[-1].row_sha256)
        with self.assertRaises(FrozenInstanceError):
            evidence.row_count = 3  # type: ignore[misc]

    def test_requires_every_published_hard_check_marker_and_source(self):
        invalid_values = {
            "status": "FAILED",
            "published": False,
            "hard_checks_passed": False,
            "source": "OTHER",
        }
        for field, invalid in invalid_values.items():
            with self.subTest(field=field):
                original = self.payload[field]
                self.payload[field] = invalid
                self._write_manifest()
                with self.assertRaisesRegex(SofrEvidenceError, field):
                    self._load()
                self.payload[field] = original

    def test_manifest_date_range_and_row_count_must_match_normalized_snapshot(self):
        invalid_values = {
            "new_min_date": "2018-04-02",
            "new_max_date": "2018-04-03",
            "new_rows_total": 3,
        }
        for field, invalid in invalid_values.items():
            with self.subTest(field=field):
                original = self.payload[field]
                self.payload[field] = invalid
                self._write_manifest()
                with self.assertRaisesRegex(SofrEvidenceError, field):
                    self._load()
                self.payload[field] = original

    def test_snapshot_must_end_strictly_before_valuation_date(self):
        with self.assertRaisesRegex(SofrEvidenceError, "strictly before"):
            self._load(date(2018, 4, 4))

    def test_detects_snapshot_change_during_normalization(self):
        def mutate_after_read(path: Path):
            history = self._test_normalizer(path)
            path.write_bytes(path.read_bytes() + b"\n")
            return history

        with patch(
            "vrp.eod_shadow.sofr_evidence.normalize_sofr_csv",
            side_effect=mutate_after_read,
        ):
            with self.assertRaisesRegex(SofrEvidenceError, "changed"):
                load_sofr_updater_evidence(
                    self.run_manifest,
                    date(2018, 4, 5),
                )

    def test_rejects_manifest_outside_its_declared_timestamp_directory(self):
        self.payload["run_timestamp_utc"] = "20260405_210001_UTC"
        self._write_manifest()
        with self.assertRaisesRegex(SofrEvidenceError, "run_timestamp_utc"):
            self._load()


if __name__ == "__main__":
    unittest.main()
