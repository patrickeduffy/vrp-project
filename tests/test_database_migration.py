from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations/0001_operational_schema.sql"


class OperationalSchemaContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = " ".join(cls.sql.lower().split())

    def test_migration_is_transactional_and_self_records(self):
        self.assertTrue(self.sql.lstrip().startswith("BEGIN;"))
        self.assertTrue(self.sql.rstrip().endswith("COMMIT;"))
        self.assertIn("INSERT INTO vrp.schema_migrations", self.sql)

    def test_required_operational_tables_exist(self):
        expected = {
            "schema_migrations",
            "model_versions",
            "configuration_versions",
            "pipeline_runs",
            "pipeline_run_stages",
            "data_assets",
            "pipeline_run_data_assets",
            "market_snapshots",
            "implied_variance_term_structure",
            "forecast_variance_term_structure",
            "signal_features",
            "signal_evaluations",
            "selected_signals",
            "qa_results",
            "signal_publications",
        }
        actual = set(re.findall(r"create\s+table\s+vrp\.([a-z0-9_]+)", self.sql, re.I))
        self.assertEqual(actual, expected)

    def test_publication_is_gated_and_latest_is_a_view(self):
        self.assertIn("run_status = 'COMPLETED'", self.sql)
        self.assertIn("run_qa_status = 'PASS'", self.sql)
        self.assertIn("CREATE VIEW vrp.latest_published_snapshot", self.sql)
        self.assertNotIn("is_latest", self.normalized)

    def test_schema_requires_no_postgresql_extension(self):
        self.assertNotIn("create extension", self.normalized)
        self.assertIn("UUID PRIMARY KEY", self.sql)

    def test_raw_option_quotes_remain_outside_postgresql(self):
        for forbidden_table in ("option_quotes", "option_chains", "raw_quotes"):
            self.assertNotRegex(
                self.sql,
                rf"create\s+table\s+vrp\.{forbidden_table}\b",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
