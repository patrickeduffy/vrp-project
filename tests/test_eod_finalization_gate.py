from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    ATTEMPT_FILE_NAME,
    STATUS_FILE_NAME,
    UnresolvedEodFinalizationError,
    assert_no_unresolved_eod_finalizations,
    completed_eod_finalization_evidence,
    find_unresolved_eod_finalizations,
    require_canonical_eod_runtime_config,
    resolve_eod_audit_root,
)


class EodFinalizationGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temporary.name).resolve()
        self.audit_root = (
            self.project_root / "data/audit/vrp_hybrid_v2_eod"
        )
        self.audit_root.mkdir(parents=True)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(
        self,
        timestamp: str,
        *,
        required: bool = True,
        environment: str = "local",
        completed: bool = False,
        retry_status: str | None = None,
    ) -> Path:
        run_dir = self.audit_root / timestamp
        run_dir.mkdir()
        manifest = {
            "code_version": "a" * 40,
            "final_health": "PASS",
            "postgres_environment": environment if required else None,
            "postgres_postpass_required": required,
            "publish_requested": True,
            "published_outputs": {"signal_history": "signal.parquet"},
            "run_timestamp": timestamp,
            "skip_upstream": False,
            "status": "PASS",
        }
        manifest_path = run_dir / "run_manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        if completed:
            status = {
                "code_version": "a" * 40,
                "environment": environment,
                "exit_code": 0,
                "run_dir": str(run_dir),
                "run_manifest_sha256": manifest_sha,
                "source_bundle_sha256": "a" * 64,
                "status": "COMPLETED",
                "preflight": {
                    "snapshot_validation": {"content_sha256": "b" * 64}
                },
                "postgres_shadow": {
                    "result": {
                        "database_projection_sha256": "c" * 64,
                        "database_readback_sha256": "d" * 64,
                        "pipeline_run_id": "00000000-0000-0000-0000-000000000001",
                        "market_snapshot_id": "00000000-0000-0000-0000-000000000002",
                        "selected_signal_id": "00000000-0000-0000-0000-000000000003",
                    }
                },
            }
            (run_dir / STATUS_FILE_NAME).write_text(
                json.dumps(status),
                encoding="utf-8",
            )
            if retry_status is not None:
                retry = dict(status)
                retry["status"] = retry_status
                retry["exit_code"] = 0 if retry_status == "COMPLETED" else 10
                (run_dir / ATTEMPT_FILE_NAME).write_text(
                    json.dumps(retry),
                    encoding="utf-8",
                )
        return run_dir

    def test_legacy_runs_before_rollout_are_not_inferred_as_debt(self):
        self._run("20260720_170000", required=False)

        self.assertEqual(find_unresolved_eod_finalizations(self.audit_root), [])
        assert_no_unresolved_eod_finalizations(self.audit_root)

    def test_missing_manifest_fails_closed_when_completion_evidence_remains(self):
        damaged = self.audit_root / "20260720_170000"
        damaged.mkdir()
        (damaged / "run_status.json").write_text(
            json.dumps({"status": "PASS"}),
            encoding="utf-8",
        )
        abandoned = self.audit_root / "20260720_180000"
        abandoned.mkdir()

        unresolved = find_unresolved_eod_finalizations(self.audit_root)

        self.assertEqual([item.run_dir for item in unresolved], [damaged])
        self.assertIn("manifest is missing", unresolved[0].reason)
        with self.assertRaisesRegex(
            UnresolvedEodFinalizationError,
            "oldest published EOD",
        ):
            assert_no_unresolved_eod_finalizations(self.audit_root)

    def test_oldest_required_run_without_terminal_status_blocks(self):
        older = self._run("20260720_170000")
        self._run("20260721_170000", completed=True)

        unresolved = find_unresolved_eod_finalizations(self.audit_root)

        self.assertEqual([item.run_dir for item in unresolved], [older])
        with self.assertRaisesRegex(
            UnresolvedEodFinalizationError,
            "oldest published EOD",
        ):
            assert_no_unresolved_eod_finalizations(self.audit_root)

    def test_matching_completed_status_resolves_only_its_exact_environment(self):
        run_dir = self._run("20260720_170000", completed=True)
        self.assertEqual(find_unresolved_eod_finalizations(self.audit_root), [])

        status_path = run_dir / STATUS_FILE_NAME
        status = json.loads(status_path.read_text(encoding="utf-8"))
        status["environment"] = "test"
        status_path.write_text(json.dumps(status), encoding="utf-8")

        self.assertEqual(
            [item.run_dir for item in find_unresolved_eod_finalizations(self.audit_root)],
            [run_dir],
        )

    def test_completed_evidence_exposes_exact_database_projection_identity(self):
        run_dir = self._run("20260720_170000", completed=True)

        evidence = completed_eod_finalization_evidence(self.audit_root)

        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0].run_dir, run_dir)
        self.assertEqual(
            evidence[0].pipeline_run_id,
            "00000000-0000-0000-0000-000000000001",
        )
        self.assertEqual(evidence[0].snapshot_content_sha256, "b" * 64)
        self.assertEqual(evidence[0].database_projection_sha256, "c" * 64)
        self.assertEqual(evidence[0].database_readback_sha256, "d" * 64)

    def test_database_continuity_evidence_is_scoped_to_one_environment(self):
        self._run("20260720_170000", completed=True, environment="local")
        production = self._run(
            "20260721_170000",
            completed=True,
            environment="production",
        )

        evidence = completed_eod_finalization_evidence(
            self.audit_root,
            environment="production",
        )

        self.assertEqual([item.run_dir for item in evidence], [production])

    def test_completed_status_without_database_identity_remains_unresolved(self):
        run_dir = self._run("20260720_170000", completed=True)
        status_path = run_dir / STATUS_FILE_NAME
        status = json.loads(status_path.read_text(encoding="utf-8"))
        del status["postgres_shadow"]
        status_path.write_text(json.dumps(status), encoding="utf-8")

        self.assertEqual(
            [item.run_dir for item in find_unresolved_eod_finalizations(self.audit_root)],
            [run_dir],
        )

    def test_failed_latest_retry_reopens_debt_without_erasing_terminal_evidence(self):
        run_dir = self._run(
            "20260720_170000",
            completed=True,
            retry_status="PREFLIGHT_FAILED",
        )

        self.assertEqual(
            [item.run_dir for item in find_unresolved_eod_finalizations(self.audit_root)],
            [run_dir],
        )
        self.assertEqual(
            json.loads((run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8"))[
                "status"
            ],
            "COMPLETED",
        )

    def test_symlinked_retry_sidecar_cannot_hide_retry_state(self):
        run_dir = self._run("20260720_170000", completed=True)
        target = self.project_root / "retry-target.json"
        target.write_text('{"status": "COMPLETED"}', encoding="utf-8")
        attempt = run_dir / ATTEMPT_FILE_NAME
        try:
            attempt.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"file symlinks are unavailable: {exc}")

        self.assertEqual(
            [item.run_dir for item in find_unresolved_eod_finalizations(self.audit_root)],
            [run_dir],
        )

    def test_before_timestamp_excludes_current_and_newer_runs(self):
        older = self._run("20260720_170000")
        self._run("20260721_170000")

        unresolved = find_unresolved_eod_finalizations(
            self.audit_root,
            before_timestamp="20260721_170000",
        )

        self.assertEqual([item.run_dir for item in unresolved], [older])

    def test_runtime_configuration_resolves_the_exact_audit_root(self):
        runtime = self.project_root / "config/runtime.json"
        runtime.parent.mkdir()
        runtime.write_text(
            json.dumps({"outputs": {"audit_dir": "data/audit/custom"}}),
            encoding="utf-8",
        )

        self.assertEqual(
            resolve_eod_audit_root(self.project_root, runtime),
            (self.project_root / "data/audit/custom").resolve(),
        )

    def test_canonical_production_audit_queue_cannot_move_with_configuration(self):
        runtime = (
            self.project_root
            / "config"
            / "vrp_hybrid_v2_eod_runtime_config.json"
        )
        runtime.parent.mkdir()
        runtime.write_text(
            json.dumps({"outputs": {"audit_dir": "data/audit/replacement"}}),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "audit queue cannot be redirected"):
            resolve_eod_audit_root(self.project_root, None)
        with self.assertRaisesRegex(ValueError, "audit queue cannot be redirected"):
            require_canonical_eod_runtime_config(self.project_root, runtime)


if __name__ == "__main__":
    unittest.main(verbosity=2)
