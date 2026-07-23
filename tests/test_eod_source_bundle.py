from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration import eod_bundle  # noqa: E402


class EodSourceBundleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temporary.name).resolve()
        self.run_dir = (
            self.project_root
            / "data"
            / "audit"
            / "vrp_hybrid_v2_eod"
            / "20260721_174124"
        )
        self.staging = self.run_dir / "staging"
        self.staging.mkdir(parents=True)
        self.sofr_dir = (
            self.project_root
            / "data"
            / "audit"
            / "sofr_eod_update_v1"
            / "20260721_214125_UTC"
        )
        self.sofr_dir.mkdir(parents=True)
        self.sofr_manifest = self.sofr_dir / "sofr_update_manifest.json"
        self.sofr_snapshot = (
            self.sofr_dir / "fred_sofr_history_refreshed_snapshot.csv"
        )
        self.sofr_snapshot.write_text(
            "observation_date,SOFR\n2026-07-20,3.57\n",
            encoding="utf-8",
        )
        self._write_json(
            self.sofr_manifest,
            {
                "source": "FRED_SOFR",
                "status": "PUBLISHED",
                "audit_files": {"refreshed_snapshot": str(self.sofr_snapshot)},
            },
        )

        self.publish_manifest = (
            self.staging / "vrp_hybrid_v2_publish_manifest.json"
        )
        self._write_json(
            self.publish_manifest,
            {
                "release_id": "vrp_corsi_intraday_hybrid_v2",
                "target_date": "2026-07-21",
            },
        )
        self.fixed_binary_paths = {
            "signal_history": self.staging
            / "vrp_hybrid_v2_signal_history.parquet",
            "latest_snapshot": self.staging
            / "vrp_hybrid_v2_latest_snapshot.parquet",
            "selected_decisions": self.staging
            / "vrp_hybrid_v2_selected_decisions.parquet",
            "forecast_history": self.staging
            / "vrp_hybrid_v2_forecast_history.parquet",
            "static_tiebreaks": self.staging
            / "vrp_hybrid_v2_static_tiebreaks.csv",
        }
        for index, (label, path) in enumerate(self.fixed_binary_paths.items(), 1):
            path.write_bytes(f"{index}:{label}:fixed-content\n".encode("utf-8"))

        self.run_manifest = self.run_dir / "run_manifest.json"
        self._write_json(
            self.run_manifest,
            {
                "final_health": "PASS",
                "project_root": str(self.project_root),
                "publish_manifest": str(self.publish_manifest),
                "sofr_manifest": str(self.sofr_manifest),
                "status": "PASS",
                "target_date": "2026-07-21",
            },
        )
        self._write_json(
            self.run_dir / "run_status.json",
            {
                "audit_dir": str(self.run_dir),
                "published": True,
                "status": "PASS",
                "target_date": "2026-07-21",
            },
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _write_json(path: Path, payload) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def test_deterministic_success_has_canonical_frozen_mapping_and_json(self):
        first = eod_bundle.load_eod_source_bundle(self.run_dir)
        second = eod_bundle.load_eod_source_bundle(self.run_dir)

        self.assertEqual(first, second)
        self.assertEqual(
            tuple(first.artifact_sha256),
            eod_bundle.EOD_SOURCE_BUNDLE_LABELS,
        )
        identity = {
            "contract": eod_bundle.EOD_SOURCE_BUNDLE_CONTRACT,
            "schema_version": eod_bundle.EOD_SOURCE_BUNDLE_SCHEMA_VERSION,
            "artifact_sha256": dict(first.artifact_sha256),
        }
        expected = hashlib.sha256(
            json.dumps(
                identity,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(first.content_sha256, expected)
        self.assertEqual(
            first.to_json_dict(),
            {
                **identity,
                "content_sha256": expected,
            },
        )
        with self.assertRaises(TypeError):
            first.artifact_sha256["signal_history"] = "0" * 64  # type: ignore[index]

    def test_changed_fixed_artifact_changes_only_its_digest_and_overall_digest(self):
        before = eod_bundle.load_eod_source_bundle(self.run_dir)
        self.fixed_binary_paths["signal_history"].write_bytes(
            b"changed-signal-history\n"
        )

        after = eod_bundle.load_eod_source_bundle(self.run_dir)

        self.assertNotEqual(before.content_sha256, after.content_sha256)
        self.assertNotEqual(
            before.artifact_sha256["signal_history"],
            after.artifact_sha256["signal_history"],
        )
        unchanged = set(eod_bundle.EOD_SOURCE_BUNDLE_LABELS) - {"signal_history"}
        for label in unchanged:
            self.assertEqual(
                before.artifact_sha256[label],
                after.artifact_sha256[label],
                label,
            )

    def test_missing_sofr_manifest_fails_closed(self):
        self.sofr_manifest.unlink()

        with self.assertRaisesRegex(
            eod_bundle.EodSourceBundleError,
            "missing run sofr_manifest",
        ):
            eod_bundle.load_eod_source_bundle(self.run_dir)

    def test_malformed_sofr_evidence_fails_closed(self):
        with self.subTest("manifest is not an object"):
            self.sofr_manifest.write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(
                eod_bundle.EodSourceBundleError,
                "must contain a JSON object",
            ):
                eod_bundle.load_eod_source_bundle(self.run_dir)

        with self.subTest("audit_files is missing"):
            self._write_json(self.sofr_manifest, {"source": "FRED_SOFR"})
            with self.assertRaisesRegex(
                eod_bundle.EodSourceBundleError,
                "audit_files must be an object",
            ):
                eod_bundle.load_eod_source_bundle(self.run_dir)

    def test_hash_disagreement_is_treated_as_a_stable_read_failure(self):
        original = eod_bundle._sha256_file
        signal_reads = 0

        def unstable_hash(path: Path, label: str) -> str:
            nonlocal signal_reads
            observed = original(path, label)
            if path.name == "vrp_hybrid_v2_signal_history.parquet":
                signal_reads += 1
                if signal_reads == 2:
                    return "0" * 64
            return observed

        with patch.object(eod_bundle, "_sha256_file", side_effect=unstable_hash):
            with self.assertRaisesRegex(
                eod_bundle.EodSourceBundleError,
                "changed while it was being hashed",
            ):
                eod_bundle.load_eod_source_bundle(self.run_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
