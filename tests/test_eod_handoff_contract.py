from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from vrp.orchestration.eod import EodRunRequest, load_completed_eod_handoff
from vrp.orchestration.eod_bundle import load_eod_source_bundle

CODE_VERSION = "a" * 40


class CompletedEodHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.project_root = Path(self.temporary.name).resolve()
        self.runtime_path = (
            self.project_root / "config/vrp_hybrid_v2_eod_runtime_config.json"
        )
        self.runtime_path.parent.mkdir(parents=True)
        self.runtime_path.write_text(
            json.dumps(
                {"outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"}}
            ),
            encoding="utf-8",
        )
        self.run_dir = (
            self.project_root
            / "data/audit/vrp_hybrid_v2_eod/20260721_174124"
        )
        self.run_dir.mkdir(parents=True)
        staging = self.run_dir / "staging"
        staging.mkdir()
        self.publish_manifest = (
            staging / "vrp_hybrid_v2_publish_manifest.json"
        )
        self.publish_manifest.write_text(
            json.dumps({"target_date": "2026-07-21"}),
            encoding="utf-8",
        )
        for filename in (
            "vrp_hybrid_v2_signal_history.parquet",
            "vrp_hybrid_v2_latest_snapshot.parquet",
            "vrp_hybrid_v2_selected_decisions.parquet",
            "vrp_hybrid_v2_forecast_history.parquet",
            "vrp_hybrid_v2_static_tiebreaks.csv",
        ):
            (staging / filename).write_bytes(filename.encode("utf-8"))
        sofr_dir = (
            self.project_root
            / "data/audit/sofr_eod_update_v1/20260721_214125_UTC"
        )
        sofr_dir.mkdir(parents=True)
        self.sofr_manifest = sofr_dir / "sofr_update_manifest.json"
        sofr_snapshot = sofr_dir / "fred_sofr_history_refreshed_snapshot.csv"
        sofr_snapshot.write_text(
            "observation_date,SOFR\n2026-07-20,3.57\n",
            encoding="utf-8",
        )
        self.sofr_manifest.write_text(
            json.dumps(
                {"audit_files": {"refreshed_snapshot": str(sofr_snapshot)}}
            ),
            encoding="utf-8",
        )
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.manifest = {
            "approved_nav": 1_000_000.0,
            "code_version": CODE_VERSION,
            "final_health": "PASS",
            "project_root": str(self.project_root),
            "publish_manifest": str(self.publish_manifest),
            "publish_requested": True,
            "postgres_environment": "local",
            "postgres_postpass_required": True,
            "published_outputs": {"signal_history": "signal.parquet"},
            "run_timestamp": self.run_dir.name,
            "runtime_config": str(self.runtime_path),
            "skip_upstream": False,
            "sofr_manifest": str(self.sofr_manifest),
            "status": "PASS",
            "target_date": "2026-07-21",
        }
        self._write_manifest()
        (self.run_dir / "run_status.json").write_text(
            json.dumps(
                {
                    "audit_dir": str(self.run_dir),
                    "published": True,
                    "status": "PASS",
                    "target_date": "2026-07-21",
                }
            ),
            encoding="utf-8",
        )
        self.handoff_path = self.project_root / "control/eod-result.json"
        self.handoff_path.parent.mkdir()
        self._write_handoff()
        self.request = EodRunRequest(
            project_root=self.project_root,
            target_date="20260721",
            runtime_config=self.runtime_path,
            code_version=CODE_VERSION,
            postgres_environment="local",
            postgres_postpass_required=True,
        )

    def _write_manifest(self) -> None:
        self.manifest_path.write_text(
            json.dumps(self.manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _write_handoff(self, **changes) -> None:
        payload = {
            "contract": "vrp.hybrid_v2.eod_result",
            "code_version": CODE_VERSION,
            "postgres_environment": "local",
            "postgres_postpass_required": True,
            "published": True,
            "run_dir": str(self.run_dir),
            "run_manifest": {
                "path": str(self.manifest_path),
                "sha256": hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
            },
            "schema_version": 1,
            "source_bundle": load_eod_source_bundle(self.run_dir).to_json_dict(),
            "status": "PASS",
            "target_date": "2026-07-21",
        }
        payload.update(changes)
        self.handoff_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def test_returns_only_the_exact_published_pass_run(self):
        result = load_completed_eod_handoff(self.handoff_path, self.request)

        self.assertEqual(result.run_dir, self.run_dir)
        self.assertEqual(result.run_manifest_path, self.manifest_path)
        self.assertEqual(result.target_date.isoformat(), "2026-07-21")
        self.assertEqual(
            result.run_manifest_sha256,
            hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
        )

    def test_rejects_tampered_manifest_and_nonpublished_handoff(self):
        self.manifest["approved_nav"] = 2_000_000.0
        self._write_manifest()
        with self.assertRaisesRegex(ValueError, "digest"):
            load_completed_eod_handoff(self.handoff_path, self.request)

        self.manifest["approved_nav"] = 1_000_000.0
        self._write_manifest()
        self._write_handoff(published=False)
        with self.assertRaisesRegex(ValueError, "published PASS"):
            load_completed_eod_handoff(self.handoff_path, self.request)

    def test_rejects_request_drift_and_missing_sofr_evidence(self):
        self.manifest["skip_upstream"] = True
        self.manifest["sofr_manifest"] = None
        self._write_manifest()
        self._write_handoff_without_bundle_refresh()
        with self.assertRaisesRegex(ValueError, "skip-upstream"):
            load_completed_eod_handoff(self.handoff_path, self.request)

        self.manifest["skip_upstream"] = False
        self._write_manifest()
        self._write_handoff_without_bundle_refresh()
        with self.assertRaisesRegex(ValueError, "SOFR"):
            load_completed_eod_handoff(self.handoff_path, self.request)

    def test_rejects_a_run_outside_the_configured_audit_root(self):
        outside = self.project_root / "data/audit/other/20260721_174124"
        shutil.copytree(self.run_dir, outside)
        outside_manifest = outside / "run_manifest.json"
        outside_payload = dict(self.manifest)
        outside_payload["publish_manifest"] = str(
            outside / "staging/vrp_hybrid_v2_publish_manifest.json"
        )
        outside_manifest.write_text(
            json.dumps(outside_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self.run_dir = outside
        self.manifest_path = outside_manifest
        self._write_handoff()

        with self.assertRaisesRegex(ValueError, "configured audit root"):
            load_completed_eod_handoff(self.handoff_path, self.request)

    def _write_handoff_without_bundle_refresh(self) -> None:
        existing = json.loads(self.handoff_path.read_text(encoding="utf-8"))
        existing["run_manifest"] = {
            "path": str(self.manifest_path),
            "sha256": hashlib.sha256(self.manifest_path.read_bytes()).hexdigest(),
        }
        self.handoff_path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
