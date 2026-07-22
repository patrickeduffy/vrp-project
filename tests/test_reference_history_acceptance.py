from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from vrp.reference_history.canonical import sha256_file
from vrp.reference_history.sources import normalize_sofr_csv, normalize_spy_daily_files

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/reference_history_20260721_baseline.json"


class ReferenceHistoryAcceptanceTests(unittest.TestCase):
    def test_pinned_acceptance_fixture_is_complete(self):
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(payload["fixture_schema"], "vrp-reference-history-acceptance-v1")
        datasets = payload["datasets"]
        self.assertEqual(set(datasets), {"sofr", "spy_eod", "spy_rsi14", "spy_rv21d"})
        for contract in datasets.values():
            self.assertRegex(contract["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(contract["row_count"], 0)
        self.assertEqual(datasets["spy_rv21d"]["source_column"], "rv21d_vol_pct")
        self.assertEqual(datasets["spy_rv21d"]["warmup_row_count"], 21)
        self.assertEqual(datasets["sofr"]["row_count"], 2071)
        self.assertEqual(datasets["sofr"]["start_date"], "2018-04-03")
        self.assertEqual(datasets["sofr"]["end_date"], "2026-07-20")
        self.assertRegex(datasets["sofr"]["normalized_content_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(datasets["spy_eod"]["row_count"], 2148)
        self.assertEqual(datasets["spy_eod"]["start_date"], "2018-01-02")
        self.assertEqual(datasets["spy_eod"]["end_date"], "2026-07-21")
        self.assertRegex(
            datasets["spy_eod"]["normalized_content_sha256"], r"^[0-9a-f]{64}$"
        )
        self.assertRegex(
            datasets["spy_eod"]["feature_definition_content_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertEqual(
            datasets["spy_rsi14"]["formula_version"],
            "wilder_rsi14_spy_close_v3_clean_session_rebuild",
        )

    def test_local_accepted_files_match_fixture_when_requested(self):
        source_root = os.environ.get("VRP_REFERENCE_SOURCE_ROOT")
        if not source_root:
            self.skipTest("Set VRP_REFERENCE_SOURCE_ROOT to run full source acceptance.")
        payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
        root = Path(source_root)
        for name, contract in payload["datasets"].items():
            path = root / contract["path"]
            self.assertTrue(path.is_file(), f"missing {name}: {path}")
            self.assertEqual(sha256_file(path), contract["sha256"], name)
        sofr = normalize_sofr_csv(root / payload["datasets"]["sofr"]["path"])
        self.assertEqual(len(sofr.rows), payload["datasets"]["sofr"]["row_count"])
        self.assertEqual(sofr.start_date.isoformat(), payload["datasets"]["sofr"]["start_date"])
        self.assertEqual(sofr.end_date.isoformat(), payload["datasets"]["sofr"]["end_date"])
        self.assertEqual(
            sofr.content_sha256,
            payload["datasets"]["sofr"]["normalized_content_sha256"],
        )
        daily = normalize_spy_daily_files(
            root / payload["datasets"]["spy_eod"]["path"],
            root / payload["datasets"]["spy_rsi14"]["path"],
            root / payload["datasets"]["spy_rv21d"]["path"],
        )
        self.assertEqual(len(daily.rows), payload["datasets"]["spy_eod"]["row_count"])
        self.assertEqual(daily.start_date.isoformat(), payload["datasets"]["spy_eod"]["start_date"])
        self.assertEqual(daily.end_date.isoformat(), payload["datasets"]["spy_eod"]["end_date"])
        self.assertEqual(
            daily.content_sha256,
            payload["datasets"]["spy_eod"]["normalized_content_sha256"],
        )
        self.assertEqual(
            daily.definition.content_sha256,
            payload["datasets"]["spy_eod"]["feature_definition_content_sha256"],
        )
        self.assertEqual(
            str(daily.rows[-1].spy_close),
            f"{payload['datasets']['spy_eod']['latest_spy_close']:.8f}",
        )
        self.assertAlmostEqual(
            daily.rows[-1].rsi14,
            payload["datasets"]["spy_rsi14"]["latest_rsi14"],
            places=12,
        )
        self.assertAlmostEqual(
            daily.rows[-1].rv21d_volatility_pct,
            payload["datasets"]["spy_rv21d"]["latest_volatility_pct"],
            places=12,
        )
        self.assertEqual(
            daily.definition.rsi_formula_version,
            payload["datasets"]["spy_rsi14"]["formula_version"],
        )


if __name__ == "__main__":
    unittest.main()
