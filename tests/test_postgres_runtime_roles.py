from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROVISIONING = ROOT / "ops/postgres/provision_shadow_runtime_roles.sql"


def _grant_tables(sql: str, privilege: str, role: str) -> set[str]:
    matches = re.finditer(
        rf"grant\s+{privilege}\s+on\s+table\s+(.*?)\s+to\s+([a-z0-9_,\s]+?)\s*;",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in matches:
        roles = {item.strip() for item in match.group(2).split(",")}
        if role in roles:
            return {
                item.strip().removeprefix("vrp.")
                for item in match.group(1).split(",")
                if item.strip()
            }
    raise AssertionError(f"missing {privilege} grant for {role}")


class PostgresRuntimeRoleContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = PROVISIONING.read_text(encoding="utf-8")
        cls.normalized = " ".join(cls.sql.lower().split())

    def test_roles_are_non_login_and_non_privileged(self):
        for role in ("vrp_reference_loader", "vrp_eod_shadow_writer"):
            self.assertRegex(
                self.normalized,
                rf"alter role {role} nologin nosuperuser nocreatedb "
                rf"nocreaterole noreplication inherit",
            )

    def test_eod_writer_cannot_publish_or_mutate_reference_history(self):
        inserts = _grant_tables(
            self.sql, "insert", "vrp_eod_shadow_writer"
        )
        self.assertNotIn("signal_publications", inserts)
        self.assertNotIn("reference_data_releases", inserts)
        self.assertNotIn("reference_rate_observations", inserts)
        self.assertNotIn("daily_market_features", inserts)
        self.assertIn("selected_signals", inserts)

    def test_reference_loader_is_append_only_for_reference_rows(self):
        inserts = _grant_tables(self.sql, "insert", "vrp_reference_loader")
        self.assertIn("reference_rate_observations", inserts)
        self.assertIn("daily_market_features", inserts)
        self.assertNotRegex(
            self.normalized,
            r"grant update .* on vrp\.(reference_rate_observations|daily_market_features)",
        )

    def test_no_broad_or_destructive_grants_exist(self):
        self.assertNotIn("grant all", self.normalized)
        self.assertNotIn("alter default privileges", self.normalized)
        self.assertNotRegex(self.normalized, r"grant\s+(delete|truncate)")
        self.assertNotIn("grant create on schema", self.normalized)

    def test_no_sequence_privileges_are_needed(self):
        self.assertNotIn("on all sequences", self.normalized)
        self.assertNotRegex(self.normalized, r"grant\s+usage\s+on\s+sequence")

    def test_public_function_execution_is_revoked_before_narrow_grants(self):
        self.assertIn(
            "revoke execute on all functions in schema vrp from public",
            self.normalized,
        )
        self.assertIn(
            "grant execute on function vrp.force_current_load_transaction()",
            self.normalized,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
