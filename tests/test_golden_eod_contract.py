from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.golden import (  # noqa: E402
    CANONICAL_DECISION_REL,
    CANONICAL_SIGNAL_REL,
    DEFAULT_FIXTURE_REL,
    EXPECTED_TENORS,
    GoldenCase,
    _clean_git_head,
    capture_golden_contract,
    load_fixture,
    validate_fixture_payload,
    validate_verification_manifest_for_publication,
    verify_golden_contract,
    verify_golden_contract_with_manifest,
    write_verification_manifest,
)

FIXTURE = ROOT / DEFAULT_FIXTURE_REL


class GoldenFixtureStructureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = load_fixture(FIXTURE)

    def test_fixture_is_structurally_valid(self):
        self.assertEqual(validate_fixture_payload(self.fixture), [])

    def test_fixture_rejects_weakened_tolerances_and_missing_fields(self):
        weakened = copy.deepcopy(self.fixture)
        weakened["comparison"]["relative_tolerance"] = 1.0
        self.assertTrue(validate_fixture_payload(weakened))

        missing_field = copy.deepcopy(self.fixture)
        del missing_field["cases"][0]["signals"][0]["core_threshold_rsi"]
        errors = validate_fixture_payload(missing_field)
        self.assertTrue(any("keys do not match" in error for error in errors))

        invalid_commit = copy.deepcopy(self.fixture)
        invalid_commit["baseline_commit"] = "main"
        self.assertTrue(validate_fixture_payload(invalid_commit))

        invalid_time = copy.deepcopy(self.fixture)
        invalid_time["captured_at_utc"] = "2026-07-21T12:00:00"
        self.assertTrue(validate_fixture_payload(invalid_time))

        unhashable_identifier = copy.deepcopy(self.fixture)
        unhashable_identifier["cases"][0]["case_id"] = ["not", "hashable"]
        self.assertTrue(validate_fixture_payload(unhashable_identifier))

    def test_fixture_is_anchored_to_the_accepted_baseline(self):
        self.assertEqual(
            self.fixture["baseline_commit"],
            "c3857984def9d295bd49dc7eab7c5a8421b0ed5b",
        )

    def test_fixture_covers_no_trade_and_every_selected_category(self):
        expected = {
            "latest_no_trade",
            "dense_core_back_tiebreak",
            "core_middle",
            "secondary_back",
            "secondary_middle",
            "secondary_front",
        }
        self.assertEqual({case["case_id"] for case in self.fixture["cases"]}, expected)
        for case in self.fixture["cases"]:
            self.assertEqual([row["tenor"] for row in case["signals"]], EXPECTED_TENORS)

    def test_staged_candidate_outputs_can_be_verified_before_publication(self):
        signal_rows = [row for case in self.fixture["cases"] for row in case["signals"]]
        decision_rows = [case["decision"] for case in self.fixture["cases"]]
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            signal_path = temp_root / "candidate_signals.parquet"
            decision_path = temp_root / "candidate_decisions.parquet"
            signals = pd.DataFrame(signal_rows)
            decisions = pd.DataFrame(decision_rows)
            signals.to_parquet(signal_path, index=False)
            decisions.to_parquet(decision_path, index=False)

            self.assertEqual(
                verify_golden_contract(
                    temp_root,
                    FIXTURE,
                    signal_history_path=signal_path,
                    selected_decisions_path=decision_path,
                ),
                [],
            )

            signals.loc[0, "model_vrp_log"] = float(signals.loc[0, "model_vrp_log"]) + 0.01
            signals.to_parquet(signal_path, index=False)
            mismatches = verify_golden_contract(
                temp_root,
                FIXTURE,
                signal_history_path=signal_path,
                selected_decisions_path=decision_path,
            )
            self.assertTrue(any("model_vrp_log" in mismatch for mismatch in mismatches))

            signals.loc[0, "model_vrp_log"] = self.fixture["cases"][0]["signals"][0][
                "model_vrp_log"
            ]
            signals.to_parquet(signal_path, index=False)
            manifest_path = temp_root / "golden_verification_manifest.json"
            manifest_mismatches, manifest = verify_golden_contract_with_manifest(
                temp_root,
                FIXTURE,
                signal_history_path=signal_path,
                selected_decisions_path=decision_path,
            )
            self.assertEqual(manifest_mismatches, [])
            self.assertEqual(manifest["status"], "PASS")
            self.assertEqual(manifest["mode"], "STAGED")
            self.assertEqual(len(manifest["verification_id"]), 64)
            write_verification_manifest(manifest_path, manifest)
            self.assertEqual(json.loads(manifest_path.read_text(encoding="utf-8")), manifest)

            repeat_mismatches, repeat_manifest = verify_golden_contract_with_manifest(
                temp_root,
                FIXTURE,
                signal_history_path=signal_path,
                selected_decisions_path=decision_path,
            )
            self.assertEqual(repeat_mismatches, [])
            self.assertEqual(repeat_manifest["verification_id"], manifest["verification_id"])

            publication_errors = validate_verification_manifest_for_publication(
                manifest_path,
                accepted_baseline_commit=self.fixture["baseline_commit"],
                accepted_fixture_path=FIXTURE,
                accepted_fixture_sha256=manifest["fixture"]["sha256"],
                allowed_signal_history_path=signal_path,
                allowed_selected_decisions_path=decision_path,
            )
            self.assertEqual(publication_errors, [])

            tampered_manifest = copy.deepcopy(manifest)
            tampered_manifest["verification_id"] = "0" * 64
            tampered_path = temp_root / "tampered_manifest.json"
            write_verification_manifest(tampered_path, tampered_manifest)
            tamper_errors = validate_verification_manifest_for_publication(
                tampered_path,
                accepted_baseline_commit=self.fixture["baseline_commit"],
                accepted_fixture_path=FIXTURE,
                accepted_fixture_sha256=manifest["fixture"]["sha256"],
                allowed_signal_history_path=signal_path,
                allowed_selected_decisions_path=decision_path,
            )
            self.assertTrue(
                any("verification_id does not match" in error for error in tamper_errors)
            )

            invalid_path_manifest = copy.deepcopy(manifest)
            invalid_path_manifest["artifacts"]["signal_history"]["path"] = "bad\x00path"
            invalid_path = temp_root / "invalid_path_manifest.json"
            invalid_path.write_text(
                json.dumps(invalid_path_manifest),
                encoding="utf-8",
            )
            invalid_path_errors = validate_verification_manifest_for_publication(
                invalid_path,
                accepted_baseline_commit=self.fixture["baseline_commit"],
                accepted_fixture_path=FIXTURE,
                accepted_fixture_sha256=manifest["fixture"]["sha256"],
                allowed_signal_history_path=signal_path,
                allowed_selected_decisions_path=decision_path,
            )
            self.assertTrue(
                any("signal_history path" in error for error in invalid_path_errors)
            )

            for protected_path in (FIXTURE, signal_path, decision_path):
                with self.subTest(protected_path=protected_path):
                    with self.assertRaisesRegex(ValueError, "cannot overwrite protected input"):
                        write_verification_manifest(protected_path, manifest)

            alias_path = temp_root / "signal_alias.parquet"
            try:
                alias_path.symlink_to(signal_path)
            except OSError:
                pass
            else:
                with self.assertRaisesRegex(ValueError, "cannot overwrite protected input"):
                    write_verification_manifest(alias_path, manifest)

            signals.loc[0, "model_vrp_log"] = float(signals.loc[0, "model_vrp_log"]) + 0.01
            signals.to_parquet(signal_path, index=False)
            publication_errors = validate_verification_manifest_for_publication(
                manifest_path,
                accepted_baseline_commit=self.fixture["baseline_commit"],
                accepted_fixture_path=FIXTURE,
                accepted_fixture_sha256=manifest["fixture"]["sha256"],
                allowed_signal_history_path=signal_path,
                allowed_selected_decisions_path=decision_path,
            )
            self.assertTrue(
                any(
                    "signal_history changed after verification" in error
                    for error in publication_errors
                )
            )

    def test_manifest_verifier_returns_fail_evidence_for_bad_inputs(self):
        signal_rows = [row for case in self.fixture["cases"] for row in case["signals"]]
        decision_rows = [case["decision"] for case in self.fixture["cases"]]
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            signal_path = temp_root / "candidate_signals.parquet"
            decision_path = temp_root / "candidate_decisions.parquet"
            pd.DataFrame(signal_rows).to_parquet(signal_path, index=False)
            pd.DataFrame(decision_rows).to_parquet(decision_path, index=False)

            malformed_fixture = temp_root / "malformed_fixture.json"
            malformed_fixture.write_text("{}\n", encoding="utf-8")
            mismatches, manifest = verify_golden_contract_with_manifest(
                temp_root,
                malformed_fixture,
                signal_history_path=signal_path,
                selected_decisions_path=decision_path,
            )
            self.assertTrue(mismatches)
            self.assertEqual(manifest["status"], "FAIL")
            self.assertIsNone(manifest["fixture"]["baseline_commit"])

            mismatches, manifest = verify_golden_contract_with_manifest(
                temp_root,
                FIXTURE,
                signal_history_path=temp_root / "missing_signals.parquet",
                selected_decisions_path=temp_root / "missing_decisions.parquet",
            )
            self.assertTrue(any("file does not exist" in mismatch for mismatch in mismatches))
            self.assertEqual(manifest["status"], "FAIL")

    def test_cli_rejects_manifest_collision_before_verification(self):
        signal_rows = [row for case in self.fixture["cases"] for row in case["signals"]]
        decision_rows = [case["decision"] for case in self.fixture["cases"]]
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            signal_path = temp_root / "candidate_signals.parquet"
            decision_path = temp_root / "candidate_decisions.parquet"
            pd.DataFrame(signal_rows).to_parquet(signal_path, index=False)
            pd.DataFrame(decision_rows).to_parquet(decision_path, index=False)
            fixture_before = FIXTURE.read_bytes()
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "golden_eod.py"),
                    "verify",
                    "--source-root",
                    str(temp_root),
                    "--fixture",
                    str(FIXTURE),
                    "--signal-history",
                    str(signal_path),
                    "--selected-decisions",
                    str(decision_path),
                    "--manifest",
                    str(FIXTURE),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("cannot overwrite protected input", result.stderr)
            self.assertEqual(FIXTURE.read_bytes(), fixture_before)

    def test_staged_candidate_paths_are_all_or_none(self):
        with self.assertRaisesRegex(ValueError, "must be supplied together"):
            verify_golden_contract(
                ROOT,
                FIXTURE,
                signal_history_path=Path("candidate_signals.parquet"),
            )

    def test_capture_requires_clean_matching_git_head_and_explicit_overwrite(self):
        source_case = self.fixture["cases"][0]
        with tempfile.TemporaryDirectory() as temp_dir:
            source_root = Path(temp_dir)
            subprocess.run(["git", "init", "-q", str(source_root)], check=True)
            subprocess.run(
                ["git", "-C", str(source_root), "config", "user.name", "VRP Test"],
                check=True,
            )
            subprocess.run(
                ["git", "-C", str(source_root), "config", "user.email", "vrp@example.invalid"],
                check=True,
            )
            tracked = source_root / "tracked.txt"
            tracked.write_text("baseline\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(source_root), "add", "tracked.txt"], check=True)
            subprocess.run(
                ["git", "-C", str(source_root), "commit", "-q", "-m", "baseline"],
                check=True,
            )
            head = _clean_git_head(source_root)

            signal_path = source_root / CANONICAL_SIGNAL_REL
            decision_path = source_root / CANONICAL_DECISION_REL
            signal_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(source_case["signals"]).to_parquet(signal_path, index=False)
            pd.DataFrame([source_case["decision"]]).to_parquet(decision_path, index=False)
            captured_fixture = source_root / "captured.json"
            cases = [GoldenCase("synthetic", source_case["date"], "Synthetic capture test.")]

            payload = capture_golden_contract(
                source_root,
                captured_fixture,
                baseline_commit=head,
                cases=cases,
            )
            self.assertEqual(validate_fixture_payload(payload), [])
            with self.assertRaises(FileExistsError):
                capture_golden_contract(
                    source_root,
                    captured_fixture,
                    baseline_commit=head,
                    cases=cases,
                )
            overwritten = capture_golden_contract(
                source_root,
                captured_fixture,
                baseline_commit=head,
                cases=cases,
                overwrite=True,
            )
            self.assertEqual(overwritten["baseline_commit"], head)

            tracked.write_text("dirty\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "tracked changes"):
                capture_golden_contract(
                    source_root,
                    source_root / "dirty.json",
                    baseline_commit=head,
                    cases=cases,
                )


class CanonicalGoldenReconciliationTests(unittest.TestCase):
    def test_local_canonical_outputs_match_fixture_when_available(self):
        source_root = Path(os.environ.get("VRP_GOLDEN_SOURCE_ROOT", ROOT))
        required = [source_root / CANONICAL_SIGNAL_REL, source_root / CANONICAL_DECISION_REL]
        if not all(path.is_file() for path in required):
            self.skipTest(
                "Canonical production data is not present. Set VRP_GOLDEN_SOURCE_ROOT "
                "to run production reconciliation."
            )
        mismatches = verify_golden_contract(source_root, FIXTURE)
        self.assertEqual(mismatches, [], "\n".join(mismatches[:50]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
