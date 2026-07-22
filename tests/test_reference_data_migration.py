from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations/0002_reference_data.sql"


class ReferenceDataMigrationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = " ".join(cls.sql.lower().split())

    def test_migration_is_transactional_and_self_records(self):
        self.assertTrue(self.sql.lstrip().startswith("BEGIN;"))
        self.assertTrue(self.sql.rstrip().endswith("COMMIT;"))
        self.assertIn("VALUES ('0002'", self.sql)

    def test_compact_reference_tables_exist(self):
        expected = {
            "reference_data_releases",
            "reference_rate_observations",
            "daily_market_feature_definitions",
            "daily_market_features",
        }
        actual = set(re.findall(r"create\s+table\s+vrp\.([a-z0-9_]+)", self.sql, re.I))
        self.assertEqual(actual, expected)

    def test_source_units_and_formula_contract_are_explicit(self):
        self.assertIn("rate_percent", self.normalized)
        self.assertIn("rate_decimal", self.normalized)
        self.assertIn("annual_percentage_points", self.normalized)
        self.assertIn("rv_sample_ddof", self.normalized)
        self.assertIn("rv21d_volatility_pct", self.normalized)
        self.assertIn("rsi_formula_version", self.normalized)
        self.assertIn("price_adjustment", self.normalized)

    def test_corrections_are_append_only_and_current_rows_are_views(self):
        self.assertIn("supersedes_observation_id", self.normalized)
        self.assertIn("supersedes_daily_market_feature_id", self.normalized)
        self.assertIn("create view vrp.current_reference_rate_observations", self.normalized)
        self.assertIn("create view vrp.current_daily_market_features", self.normalized)
        self.assertEqual(self.normalized.count("before update or delete"), 4)
        self.assertNotIn("on conflict do update", self.normalized)

    def test_operational_rows_can_pin_exact_reference_rows(self):
        self.assertIn("market_snapshots_sofr_observation_fk", self.normalized)
        self.assertIn("market_snapshots_sofr_observation_id_fk", self.normalized)
        self.assertIn("market_snapshots_daily_feature_id_fk", self.normalized)
        self.assertIn("signal_features_daily_feature_fk", self.normalized)
        self.assertIn("signal_features_daily_rv_values_fk", self.normalized)
        self.assertIn("sofr_observation_date < valuation_date", self.normalized)

    def test_releases_are_typed_and_sealed_to_one_transaction(self):
        self.assertIn("dataset_kind", self.normalized)
        self.assertIn("load_transaction_id", self.normalized)
        self.assertIn("default (pg_current_xact_id()::text)", self.normalized)
        self.assertIn("force_current_load_transaction", self.normalized)
        self.assertIn("assert_compatible_reference_data_releases", self.normalized)

    def test_bulk_option_quotes_remain_outside_postgresql(self):
        for forbidden_table in ("option_quotes", "option_chains", "raw_quotes"):
            self.assertNotRegex(
                self.sql,
                rf"create\s+table\s+vrp\.{forbidden_table}\b",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
