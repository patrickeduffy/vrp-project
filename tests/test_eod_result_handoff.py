from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = ROOT / "notebooks"
sys.path.insert(0, str(NOTEBOOKS))


def load_module():
    name = "eod_result_handoff_test_module"
    spec = importlib.util.spec_from_file_location(
        name,
        NOTEBOOKS / "vrp_hybrid_v2_eod_pipeline.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


eod = load_module()
CODE_VERSION = "a" * 40


class EodResultHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.runtime_path = (
            self.root / "config/vrp_hybrid_v2_eod_runtime_config.json"
        )
        self.runtime_path.parent.mkdir(parents=True)
        self.runtime_path.write_text(
            json.dumps(
                {
                    "close_buffer_minutes": 15,
                    "outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"},
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _config(self, *, publish: bool = True):
        return eod.PipelineConfig(
            project_root=self.root,
            runtime_config_path=self.runtime_path,
            target_date=pd.Timestamp("2026-07-21"),
            approved_nav=1_000_000.0,
            skip_upstream=False,
            force_recalculate=False,
            recalc_start_override=None,
            publish=publish,
            code_version=CODE_VERSION if publish else None,
            postgres_postpass_required=publish,
            postgres_environment="local" if publish else None,
            postgres_postpass_bypass_reason=None,
            run_timestamp="20260721_174124",
            run_dir=self.root / "data" / "audit" / "vrp_hybrid_v2_eod" / "20260721_174124",
        )

    def _completed_manifest(self, config, *, published: bool | None = None):
        actually_published = config.publish if published is None else published
        publish_manifest = (
            config.run_dir / "staging/vrp_hybrid_v2_publish_manifest.json"
        )
        sofr_manifest = (
            config.project_root
            / "data/audit/sofr_eod_update_v1/20260721_214125_UTC"
            / "sofr_update_manifest.json"
        )
        return {
            "status": "PASS",
            "code_version": config.code_version,
            "final_health": "PASS",
            "project_root": str(config.project_root.resolve()),
            "publish_manifest": str(publish_manifest.resolve()),
            "target_date": "2026-07-21",
            "run_timestamp": config.run_timestamp,
            "publish_requested": config.publish,
            "postgres_environment": config.postgres_environment,
            "postgres_postpass_required": config.postgres_postpass_required,
            "published_outputs": (
                {"signal_history": str(config.project_root / "signal_history.parquet")}
                if actually_published
                else {}
            ),
            "sofr_manifest": str(sofr_manifest.resolve()),
        }

    def _write_manifest(self, config, manifest) -> Path:
        staging = config.run_dir / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "vrp_hybrid_v2_publish_manifest.json").write_text(
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
        (config.run_dir / "run_status.json").write_text(
            json.dumps(
                {
                    "published": bool(manifest["publish_requested"]),
                    "status": "PASS",
                    "target_date": "2026-07-21",
                }
            ),
            encoding="utf-8",
        )
        sofr_manifest = Path(str(manifest["sofr_manifest"]))
        sofr_manifest.parent.mkdir(parents=True, exist_ok=True)
        sofr_snapshot = (
            sofr_manifest.parent / "fred_sofr_history_refreshed_snapshot.csv"
        )
        sofr_snapshot.write_text(
            "observation_date,SOFR\n2026-07-20,3.57\n",
            encoding="utf-8",
        )
        sofr_manifest.write_text(
            json.dumps(
                {"audit_files": {"refreshed_snapshot": str(sofr_snapshot)}}
            ),
            encoding="utf-8",
        )
        path = config.run_dir / "run_manifest.json"
        path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return path

    def _main_patches(self, pipeline):
        runtime_path = self.runtime_path
        runtime = {
            "close_buffer_minutes": 15,
            "outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"},
        }
        return (
            patch.object(eod, "load_runtime_config", return_value=(runtime, runtime_path)),
            patch.object(eod, "parse_target", return_value=pd.Timestamp("2026-07-21")),
            patch.object(eod, "now_stamp", return_value="20260721_174124"),
            patch.object(
                eod,
                "resolve_clean_code_version",
                return_value=CODE_VERSION,
            ),
            patch.object(eod, "pipeline", side_effect=pipeline),
        )

    def test_success_writes_exact_versioned_handoff_after_pipeline(self):
        destination = self.root / "handoffs" / "completed.json"
        events: list[str] = []

        def pipeline(config):
            events.append("pipeline")
            self.assertFalse(destination.exists())
            manifest = self._completed_manifest(config)
            self._write_manifest(config, manifest)
            return manifest

        original_writer = eod.write_result_handoff

        def tracked_writer(*args, **kwargs):
            events.append("handoff")
            return original_writer(*args, **kwargs)

        patches = self._main_patches(pipeline)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patch.object(
            eod, "write_result_handoff", side_effect=tracked_writer
        ), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = eod.main(
                [
                    "--project-root",
                    str(self.root),
                    "--runtime-config",
                    str(self.runtime_path),
                    "--target-date",
                    "2026-07-21",
                    "--result-handoff",
                    str(destination),
                    "--postgres-postpass-required",
                    "--postgres-environment",
                    "local",
                ]
            )

        self.assertEqual(result, 0)
        self.assertEqual(events, ["pipeline", "handoff"])
        payload = json.loads(destination.read_text(encoding="utf-8"))
        run_dir = (self.root / "data/audit/vrp_hybrid_v2_eod/20260721_174124").resolve()
        manifest_path = (run_dir / "run_manifest.json").resolve()
        self.assertEqual(payload["contract"], eod.RESULT_HANDOFF_CONTRACT)
        self.assertEqual(payload["schema_version"], eod.RESULT_HANDOFF_SCHEMA_VERSION)
        self.assertEqual(payload["status"], "PASS")
        self.assertEqual(payload["target_date"], "2026-07-21")
        self.assertEqual(payload["run_dir"], str(run_dir))
        self.assertEqual(payload["run_manifest"]["path"], str(manifest_path))
        self.assertEqual(
            payload["run_manifest"]["sha256"],
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        )
        self.assertIs(payload["published"], True)
        self.assertEqual(
            payload["source_bundle"]["contract"],
            "vrp.hybrid_v2.eod_source_bundle",
        )
        self.assertEqual(len(payload["source_bundle"]["content_sha256"]), 64)

    def test_no_publish_success_records_actual_publication_false(self):
        config = self._config(publish=False)
        manifest = self._completed_manifest(config)
        self._write_manifest(config, manifest)

        payload = eod.build_result_handoff(config, manifest)

        self.assertIs(payload["published"], False)
        self.assertEqual(payload["status"], "PASS")

    def test_pipeline_failure_never_creates_handoff(self):
        destination = self.root / "handoff.json"

        def pipeline(_config):
            raise RuntimeError("injected pipeline failure")

        patches = self._main_patches(pipeline)
        with patches[0], patches[1], patches[2], patches[3], patches[4], redirect_stdout(
            io.StringIO()
        ), redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "injected pipeline failure"):
                eod.main(
                    [
                        "--project-root",
                        str(self.root),
                        "--runtime-config",
                        str(self.runtime_path),
                        "--result-handoff",
                        str(destination),
                        "--postgres-postpass-required",
                        "--postgres-environment",
                        "local",
                    ]
                )

        self.assertFalse(destination.exists())

    def test_direct_published_legacy_run_requires_postpass_or_audited_bypass(self):
        def pipeline(_config):
            raise AssertionError("pipeline must not run")

        patches = self._main_patches(pipeline)
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4] as run_pipeline,
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            with self.assertRaisesRegex(ValueError, "requires the PostgreSQL post-pass"):
                eod.main(
                    [
                        "--project-root",
                        str(self.root),
                        "--runtime-config",
                        str(self.runtime_path),
                    ]
                )

        run_pipeline.assert_not_called()

    def test_handoff_failure_returns_distinct_code_without_changing_pass_manifest(self):
        destination = self.root / "handoff.json"
        manifest_path: Path | None = None

        def pipeline(config):
            nonlocal manifest_path
            manifest = self._completed_manifest(config)
            manifest_path = self._write_manifest(config, manifest)
            return manifest

        patches = self._main_patches(pipeline)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patch.object(
            eod,
            "write_result_handoff",
            side_effect=OSError("injected handoff failure"),
        ), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            result = eod.main(
                [
                    "--project-root",
                    str(self.root),
                    "--runtime-config",
                    str(self.runtime_path),
                    "--result-handoff",
                    str(destination),
                    "--postgres-postpass-required",
                    "--postgres-environment",
                    "local",
                ]
            )

        self.assertEqual(result, eod.RESULT_HANDOFF_WRITE_FAILED_EXIT_CODE)
        self.assertIsNotNone(manifest_path)
        retained = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(retained["status"], "PASS")
        self.assertFalse(destination.exists())

    def test_existing_destination_is_never_replaced(self):
        config = self._config()
        manifest = self._completed_manifest(config)
        self._write_manifest(config, manifest)
        destination = self.root / "handoff.json"
        destination.write_text("previous-good-handoff", encoding="utf-8")

        with self.assertRaisesRegex(FileExistsError, "refusing to replace"):
            eod.write_result_handoff(destination, config, manifest)

        self.assertEqual(
            destination.read_text(encoding="utf-8"),
            "previous-good-handoff",
        )
        self.assertEqual(list(self.root.glob(".handoff.json.*.tmp")), [])

    def test_audit_directory_claim_is_atomic_and_never_reuses_a_run(self):
        run_dir = self._config().run_dir

        claimed = eod.claim_run_directory(run_dir)

        self.assertEqual(claimed, run_dir.resolve())
        with self.assertRaisesRegex(RuntimeError, "refusing to reuse"):
            eod.claim_run_directory(run_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
