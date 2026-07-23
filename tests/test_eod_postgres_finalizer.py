from __future__ import annotations

import errno
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration.eod_postgres import (  # noqa: E402
    ATTEMPT_FILE_NAME,
    EOD_SNAPSHOT_LOADER,
    EXIT_PREFLIGHT_FAILED,
    EXIT_REFERENCE_SYNC_FAILED,
    EXIT_SHADOW_LOAD_FAILED,
    EXIT_STATUS_WRITE_FAILED,
    REFERENCE_LOADER,
    STATUS_FILE_NAME,
    EodPostgresFinalizerRequest,
    exclusive_run_lock,
    finalize_eod_postgres,
    write_status_atomic,
)
from vrp.orchestration.eod_finalization_gate import (  # noqa: E402
    UnresolvedEodFinalizationError,
    assert_no_unresolved_eod_finalizations,
)
from vrp.storage.finalization_coordination import (  # noqa: E402
    FINALIZATION_LEASE_TOKEN_ENV,
)


CODE_VERSION = "a" * 40
SOFR_DIGEST = "b" * 64
SNAPSHOT_DIGEST = "d" * 64
SOURCE_BUNDLE_DIGEST = "e" * 64
SOFR_SOURCE_DIGEST = "f" * 64


def completed(payload, *, return_code: int = 0, stderr: str = ""):
    stdout = payload if isinstance(payload, str) else json.dumps(payload)
    return subprocess.CompletedProcess([], return_code, stdout=stdout, stderr=stderr)


class QueueRunner:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: list[tuple[list[str], dict]] = []

    def __call__(self, command, **kwargs):
        self.calls.append((list(command), dict(kwargs)))
        if not self.responses:
            raise AssertionError("unexpected child process")
        return self.responses.pop(0)


class EodPostgresFinalizerTests(unittest.TestCase):
    def setUp(self) -> None:
        code_identity = patch(
            "vrp.orchestration.eod_postgres.resolve_clean_code_version",
            side_effect=lambda **kwargs: kwargs["explicit"],
        )
        self.code_identity = code_identity.start()
        self.addCleanup(code_identity.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.project_root = self.root / "project"
        self.run_dir = (
            self.project_root
            / "data"
            / "audit"
            / "vrp_hybrid_v2_eod"
            / "20260721_170000"
        )
        self.artifact_root = self.project_root / "data" / "reference_history"
        self.runtime_config = (
            self.project_root
            / "config"
            / "vrp_hybrid_v2_eod_runtime_config.json"
        )
        self.run_dir.mkdir(parents=True)
        self.runtime_config.parent.mkdir(parents=True)
        self.runtime_config.write_text(
            json.dumps(
                {"outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"}}
            ),
            encoding="utf-8",
        )
        database_lock = patch(
            "vrp.orchestration.eod_postgres.exclusive_database_finalization_lock",
            side_effect=lambda _request: nullcontext(),
        )
        database_lock.start()
        self.addCleanup(database_lock.stop)
        self.sofr_manifest = self.run_dir / "sofr_update_manifest.json"
        self.sofr_manifest.write_text("{}", encoding="utf-8")
        self.sofr_snapshot = (
            self.run_dir / "fred_sofr_history_refreshed_snapshot.csv"
        )
        self.sofr_snapshot.write_text(
            "observation_date,SOFR\n2026-07-20,3.57\n",
            encoding="utf-8",
        )
        self._write_published_contract()
        source_bundle = patch(
            "vrp.orchestration.eod_postgres.load_eod_source_bundle",
            side_effect=self._source_bundle,
        )
        source_bundle.start()
        self.addCleanup(source_bundle.stop)
        self.request = EodPostgresFinalizerRequest(
            project_root=self.project_root,
            run_dir=self.run_dir,
            artifact_root=self.artifact_root,
            environment="local",
            code_version=CODE_VERSION,
            requested_by="unit-test",
            run_manifest_sha256=self._manifest_digest(),
            source_bundle_sha256=SOURCE_BUNDLE_DIGEST,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_published_contract(self, *, published: bool = True) -> None:
        published_outputs = {
            "execution_handoff": str(self.project_root / "execution.csv"),
            "forecast_history": str(self.project_root / "forecast.parquet"),
            "latest_snapshot": str(self.project_root / "latest.parquet"),
            "selected_decisions": str(self.project_root / "decisions.parquet"),
            "signal_history": str(self.project_root / "signals.parquet"),
            "static_tiebreaks": str(self.project_root / "tiebreaks.csv"),
        }
        manifest = {
            "code_version": CODE_VERSION,
            "final_health": "PASS",
            "finished_at": "2026-07-21T21:00:00+00:00",
            "project_root": str(self.project_root),
            "publish_requested": published,
            "postgres_environment": "local",
            "postgres_postpass_required": True,
            "published_outputs": published_outputs if published else {},
            "run_timestamp": "20260721_170000",
            "runtime_config": str(self.runtime_config),
            "skip_upstream": False,
            "sofr_manifest": str(self.sofr_manifest),
            "status": "PASS",
            "target_date": "2026-07-21",
        }
        status = {
            "audit_dir": str(self.run_dir),
            "data_health": {"overall_status": "PASS"},
            "published": published,
            "run_timestamp": "20260721_170000",
            "status": "PASS",
            "target_date": "2026-07-21",
        }
        (self.run_dir / "run_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        (self.run_dir / "run_status.json").write_text(
            json.dumps(status), encoding="utf-8"
        )

    def _manifest_digest(self) -> str:
        return hashlib.sha256(
            (self.run_dir / "run_manifest.json").read_bytes()
        ).hexdigest()

    def _refresh_manifest_digest(self) -> None:
        self.request = replace(
            self.request,
            run_manifest_sha256=self._manifest_digest(),
        )

    def _source_bundle(self, _run_dir):
        run_status_digest = hashlib.sha256(
            (self.run_dir / "run_status.json").read_bytes()
        ).hexdigest()
        evidence = {
            "contract": "vrp.hybrid_v2.eod_source_bundle",
            "schema_version": 1,
            "content_sha256": SOURCE_BUNDLE_DIGEST,
            "artifact_sha256": {
                "run_status": run_status_digest,
                "sofr_refreshed_snapshot": SOFR_SOURCE_DIGEST,
            },
        }
        return SimpleNamespace(
            content_sha256=SOURCE_BUNDLE_DIGEST,
            artifact_sha256=evidence["artifact_sha256"],
            to_json_dict=lambda: evidence,
        )

    def _validation(self):
        return {
            "content_sha256": SNAPSHOT_DIGEST,
            "decision": "NO_TRADE",
            "sofr_normalized_content_sha256": SOFR_DIGEST,
            "sofr_refreshed_snapshot_path": str(self.sofr_snapshot),
            "sofr_refreshed_snapshot_sha256": SOFR_SOURCE_DIGEST,
            "status": "VALID",
            "valuation_date": "2026-07-21",
        }

    @staticmethod
    def _references(*, no_op: bool = False):
        return [
            {
                "content_sha256": SOFR_DIGEST,
                "dataset_key": "FRED_SOFR",
                "no_op": no_op,
            },
            {
                "content_sha256": "c" * 64,
                "dataset_key": "SPY_SIGNAL_DAILY_FEATURES",
                "no_op": no_op,
            },
        ]

    @staticmethod
    def _shadow(*, no_op: bool = False):
        return {
            "database_projection_sha256": "9" * 64,
            "database_readback_sha256": "8" * 64,
            "decision": "NO_TRADE",
            "market_snapshot_id": "00000000-0000-0000-0000-000000000002",
            "no_op": no_op,
            "pipeline_run_id": "00000000-0000-0000-0000-000000000001",
            "selected_signal_id": "00000000-0000-0000-0000-000000000003",
            "status": "COMPLETED",
            "valuation_date": "2026-07-21",
        }

    def test_success_uses_exact_run_dir_and_never_places_credentials_in_commands(self):
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        secret = "do-not-log-this-password"
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": f"postgresql://generic:{secret}@localhost/vrp",
                "VRP_DATABASE_URL": f"postgresql://vrp:{secret}@localhost/vrp",
            },
        ):
            result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.status, "COMPLETED")
        self.assertEqual(len(runner.calls), 3)
        validation, reference, shadow = [item[0] for item in runner.calls]
        self.assertEqual(Path(validation[2]), EOD_SNAPSHOT_LOADER)
        self.assertIn("--validate-only", validation)
        self.assertEqual(Path(reference[2]), REFERENCE_LOADER)
        self.assertEqual(reference[3], "all")
        self.assertEqual(Path(shadow[2]), EOD_SNAPSHOT_LOADER)
        self.assertNotIn("--validate-only", shadow)
        for command in (validation, reference, shadow):
            rendered = " ".join(command)
            self.assertNotIn(secret, rendered)
            self.assertNotIn("VRP_DATABASE_URL", rendered)
        for command in (validation, shadow):
            run_index = command.index("--run-dir") + 1
            self.assertEqual(Path(command[run_index]), self.run_dir)
            digest_index = command.index("--expected-run-manifest-sha256") + 1
            self.assertEqual(command[digest_index], self.request.run_manifest_sha256)
            bundle_index = command.index("--expected-source-bundle-sha256") + 1
            self.assertEqual(command[bundle_index], SOURCE_BUNDLE_DIGEST)
            self.assertNotIn("latest", [part.lower() for part in command])
        content_index = shadow.index("--expected-content-sha256") + 1
        self.assertEqual(shadow[content_index], SNAPSHOT_DIGEST)
        self.assertIn(CODE_VERSION, reference)
        self.assertIn(CODE_VERSION, shadow)
        self.assertIn(str(self.artifact_root), reference)
        self.assertIn(str(self.sofr_snapshot), reference)
        self.assertIn(SOFR_SOURCE_DIGEST, reference)
        validation_environment = runner.calls[0][1]["env"]
        reference_environment = runner.calls[1][1]["env"]
        shadow_environment = runner.calls[2][1]["env"]
        self.assertNotIn("VRP_DATABASE_URL", validation_environment)
        self.assertNotIn("DATABASE_URL", validation_environment)
        self.assertNotIn("PGPASSWORD", validation_environment)
        self.assertIn(secret, reference_environment["VRP_DATABASE_URL"])
        self.assertIn(secret, shadow_environment["VRP_DATABASE_URL"])
        for _, kwargs in runner.calls:
            self.assertGreater(kwargs["timeout"], 0)

        status_path = self.run_dir / STATUS_FILE_NAME
        status_text = status_path.read_text(encoding="utf-8")
        status = json.loads(status_text)
        self.assertEqual(status["status"], "COMPLETED")
        self.assertEqual(status["authoritative_file_result"], "UNCHANGED")
        self.assertFalse(status["database_projection_authoritative"])
        self.assertFalse(status["publishes_signal"])
        self.assertNotIn(secret, status_text)
        self.assertFalse(list(self.run_dir.glob(f".{STATUS_FILE_NAME}.*.tmp")))

    def test_database_lease_spans_both_mutating_children_and_delegates_token(self):
        events: list[str] = []
        token = "9" * 64

        @contextmanager
        def database_lease(_request):
            events.append("database-lock-enter")
            yield token
            events.append("database-lock-exit")

        class TrackingRunner(QueueRunner):
            def __call__(runner_self, command, **kwargs):
                if "--validate-only" in command:
                    events.append("validation")
                    self.assertNotIn(FINALIZATION_LEASE_TOKEN_ENV, kwargs["env"])
                elif Path(command[2]) == REFERENCE_LOADER:
                    events.append("reference")
                    self.assertEqual(
                        kwargs["env"][FINALIZATION_LEASE_TOKEN_ENV],
                        token,
                    )
                else:
                    events.append("shadow")
                    self.assertEqual(
                        kwargs["env"][FINALIZATION_LEASE_TOKEN_ENV],
                        token,
                    )
                return super().__call__(command, **kwargs)

        runner = TrackingRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        result = finalize_eod_postgres(
            self.request,
            runner=runner,
            database_lock_factory=database_lease,
        )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            events,
            [
                "validation",
                "database-lock-enter",
                "reference",
                "shadow",
                "database-lock-exit",
            ],
        )

    def test_checkout_is_revalidated_after_secretless_validation(self):
        self.code_identity.side_effect = [
            CODE_VERSION,
            ValueError("validation child changed the checkout"),
        ]
        runner = QueueRunner(completed(self._validation()))

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(result.status, "PREFLIGHT_FAILED")
        self.assertEqual(len(runner.calls), 1)
        self.assertIn("--validate-only", runner.calls[0][0])
        self.assertIn(
            "changed after secretless validation",
            result.payload["preflight"]["error"]["message"],
        )

    def test_bare_python_executable_remains_path_resolvable(self):
        request = EodPostgresFinalizerRequest(
            project_root=self.project_root,
            run_dir=self.run_dir,
            artifact_root=self.artifact_root,
            environment="local",
            code_version=CODE_VERSION,
            requested_by="unit-test",
            run_manifest_sha256=self.request.run_manifest_sha256,
            source_bundle_sha256=SOURCE_BUNDLE_DIGEST,
            python_executable=Path("python.exe"),
        )
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )

        result = finalize_eod_postgres(request, runner=runner)

        self.assertEqual(result.exit_code, 0)
        self.assertTrue(runner.calls)
        self.assertTrue(all(call[0][0] == "python.exe" for call in runner.calls))

    def test_unpublished_run_fails_preflight_without_starting_a_child(self):
        self._write_published_contract(published=False)
        self._refresh_manifest_digest()
        runner = QueueRunner()
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(result.status, "PREFLIGHT_FAILED")
        self.assertEqual(runner.calls, [])
        self.assertFalse((self.run_dir / STATUS_FILE_NAME).exists())
        self.assertEqual(result.payload["preflight"]["status"], "FAILED")
        self.assertIn("no-publish", result.payload["preflight"]["error"]["message"])

    def test_validation_child_failure_has_preflight_exit_code(self):
        runner = QueueRunner(completed("", return_code=7, stderr="invalid snapshot"))
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(len(runner.calls), 1)
        status = json.loads((self.run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8"))
        self.assertEqual(status["status"], "PREFLIGHT_FAILED")
        self.assertEqual(status["preflight"]["error"]["child_return_code"], 7)

    def test_skip_upstream_run_is_rejected_before_validation_child(self):
        manifest_path = self.run_dir / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["skip_upstream"] = True
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self._refresh_manifest_digest()
        runner = QueueRunner()
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])

    def test_missing_sofr_manifest_is_rejected_before_validation_child(self):
        self.sofr_manifest.unlink()
        runner = QueueRunner()
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])

    def test_reference_failure_stops_before_shadow_and_redacts_diagnostic(self):
        secret = "reference-secret"
        runner = QueueRunner(
            completed(self._validation()),
            completed(
                "",
                return_code=1,
                stderr=f"connection failed postgresql://vrp:{secret}@localhost/vrp",
            ),
        )
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_REFERENCE_SYNC_FAILED)
        self.assertEqual(result.status, "REFERENCE_SYNC_FAILED")
        self.assertEqual(len(runner.calls), 2)
        status_text = (self.run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8")
        self.assertNotIn(secret, status_text)
        self.assertIn("[REDACTED_DATABASE_URL]", status_text)

    def test_shadow_failure_has_distinct_exit_code(self):
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed("", return_code=3, stderr="shadow mismatch"),
        )
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_SHADOW_LOAD_FAILED)
        self.assertEqual(result.status, "SHADOW_LOAD_FAILED")
        self.assertEqual(len(runner.calls), 3)
        status = json.loads((self.run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8"))
        self.assertEqual(status["authoritative_file_result"], "UNCHANGED")
        self.assertEqual(status["postgres_shadow"]["status"], "FAILED")

    def test_reference_results_require_two_unique_rows_and_matching_sofr_digest(self):
        for references in (
            [self._references()[0], self._references()[0]],
            [
                {
                    "content_sha256": "d" * 64,
                    "dataset_key": "FRED_SOFR",
                },
                self._references()[1],
            ],
        ):
            with self.subTest(references=references):
                runner = QueueRunner(
                    completed(self._validation()),
                    completed(references),
                )
                result = finalize_eod_postgres(self.request, runner=runner)
                self.assertEqual(result.exit_code, EXIT_REFERENCE_SYNC_FAILED)
                self.assertEqual(len(runner.calls), 2)

    def test_shadow_valuation_date_must_match_validated_snapshot(self):
        shadow = self._shadow()
        shadow["valuation_date"] = "2026-07-22"
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(shadow),
        )
        result = finalize_eod_postgres(self.request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_SHADOW_LOAD_FAILED)
        status = json.loads((self.run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8"))
        self.assertIn(
            "valuation_date",
            status["postgres_shadow"]["error"]["message"],
        )

    def test_shadow_result_requires_exact_database_identity(self):
        shadow = self._shadow()
        del shadow["selected_signal_id"]
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(shadow),
        )

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_SHADOW_LOAD_FAILED)
        self.assertEqual(result.status, "SHADOW_LOAD_FAILED")
        self.assertIn(
            "selected_signal_id",
            result.payload["postgres_shadow"]["error"]["message"],
        )

    def test_shadow_result_requires_database_projection_digest(self):
        shadow = self._shadow()
        del shadow["database_projection_sha256"]
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(shadow),
        )

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_SHADOW_LOAD_FAILED)
        self.assertIn(
            "projection digest",
            result.payload["postgres_shadow"]["error"]["message"],
        )

    def test_shadow_result_requires_database_readback_digest(self):
        shadow = self._shadow()
        del shadow["database_readback_sha256"]
        runner = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(shadow),
        )

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_SHADOW_LOAD_FAILED)
        self.assertIn(
            "read-back digest",
            result.payload["postgres_shadow"]["error"]["message"],
        )

    def test_status_write_failure_prevents_database_steps_and_has_distinct_exit_code(self):
        runner = QueueRunner()

        def fail_status_write(path, payload):
            raise OSError("disk unavailable")

        result = finalize_eod_postgres(
            self.request,
            runner=runner,
            status_writer=fail_status_write,
        )
        self.assertEqual(result.exit_code, EXIT_STATUS_WRITE_FAILED)
        self.assertEqual(result.status, "STATUS_WRITE_FAILED")
        self.assertEqual(runner.calls, [])
        self.assertIn("disk unavailable", result.payload["status_write_error"]["message"])

    def test_retry_reconciles_database_without_overwriting_terminal_sidecar(self):
        first = QueueRunner(
            completed(self._validation()),
            completed(self._references(no_op=False)),
            completed(self._shadow(no_op=False)),
        )
        repeated = QueueRunner(
            completed(self._validation()),
            completed(self._references(no_op=True)),
            completed(self._shadow(no_op=True)),
        )
        first_result = finalize_eod_postgres(self.request, runner=first)
        second_result = finalize_eod_postgres(self.request, runner=repeated)
        self.assertEqual(first_result.exit_code, 0)
        self.assertEqual(second_result.exit_code, 0)
        status = json.loads((self.run_dir / STATUS_FILE_NAME).read_text(encoding="utf-8"))
        self.assertFalse(status["postgres_shadow"]["result"]["no_op"])
        retry = json.loads((self.run_dir / ATTEMPT_FILE_NAME).read_text(encoding="utf-8"))
        self.assertTrue(retry["postgres_shadow"]["result"]["no_op"])
        self.assertTrue(
            all(item["no_op"] for item in retry["reference_history"]["datasets"])
        )

    def test_terminal_status_wrong_environment_is_preserved_without_children(self):
        first = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        self.assertEqual(finalize_eod_postgres(self.request, runner=first).exit_code, 0)
        status_path = self.run_dir / STATUS_FILE_NAME
        original = status_path.read_bytes()
        wrong_environment = replace(self.request, environment="test")
        runner = QueueRunner()

        result = finalize_eod_postgres(wrong_environment, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertEqual(status_path.read_bytes(), original)
        self.assertFalse((self.run_dir / ATTEMPT_FILE_NAME).exists())

    def test_corrupt_manifest_retry_preserves_primary_and_records_failed_attempt(self):
        first = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        self.assertEqual(finalize_eod_postgres(self.request, runner=first).exit_code, 0)
        status_path = self.run_dir / STATUS_FILE_NAME
        original = status_path.read_bytes()
        manifest_path = self.run_dir / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["target_date"] = "2026-07-22"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        runner = QueueRunner()

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertEqual(status_path.read_bytes(), original)
        attempt = json.loads(
            (self.run_dir / ATTEMPT_FILE_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual(attempt["status"], "PREFLIGHT_FAILED")

    def test_dirty_checkout_retry_preserves_terminal_and_records_failed_attempt(self):
        first = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        self.assertEqual(finalize_eod_postgres(self.request, runner=first).exit_code, 0)
        status_path = self.run_dir / STATUS_FILE_NAME
        original = status_path.read_bytes()
        runner = QueueRunner()

        with patch(
            "vrp.orchestration.eod_postgres.resolve_clean_code_version",
            side_effect=ValueError("working tree is dirty"),
        ):
            result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(result.status, "PREFLIGHT_FAILED")
        self.assertEqual(runner.calls, [])
        self.assertEqual(status_path.read_bytes(), original)
        attempt_path = self.run_dir / ATTEMPT_FILE_NAME
        attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
        self.assertEqual(attempt["status"], "PREFLIGHT_FAILED")
        self.assertEqual(attempt["preserved_terminal_status"], str(status_path))
        self.assertIn(
            "working tree is dirty",
            attempt["preflight"]["error"]["message"],
        )
        with self.assertRaises(UnresolvedEodFinalizationError):
            assert_no_unresolved_eod_finalizations(self.run_dir.parent)

    def test_outside_root_forged_terminal_cannot_authorize_retry_attempt_write(self):
        first = QueueRunner(
            completed(self._validation()),
            completed(self._references()),
            completed(self._shadow()),
        )
        first_result = finalize_eod_postgres(self.request, runner=first)
        self.assertEqual(first_result.exit_code, 0)

        outside_run_dir = self.project_root / "outside" / self.run_dir.name
        outside_run_dir.mkdir(parents=True)
        forged_status = dict(first_result.payload)
        forged_status["run_dir"] = str(outside_run_dir)
        forged_status_path = outside_run_dir / STATUS_FILE_NAME
        forged_status_path.write_text(json.dumps(forged_status), encoding="utf-8")
        original = forged_status_path.read_bytes()
        outside_request = replace(self.request, run_dir=outside_run_dir)
        runner = QueueRunner()

        with patch(
            "vrp.orchestration.eod_postgres.resolve_clean_code_version",
            side_effect=ValueError("working tree is dirty"),
        ) as code_identity:
            result = finalize_eod_postgres(outside_request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        code_identity.assert_not_called()
        self.assertEqual(forged_status_path.read_bytes(), original)
        self.assertFalse((outside_run_dir / ATTEMPT_FILE_NAME).exists())
        self.assertIn(
            "outside the canonical EOD audit root",
            result.payload["preflight"]["error"]["message"],
        )

    def test_nontimestamped_canonical_child_cannot_match_terminal_sidecar(self):
        invalid_run_dir = self.run_dir.parent / "latest"
        invalid_run_dir.mkdir()
        invalid_request = replace(self.request, run_dir=invalid_run_dir)
        forged_status = {
            "artifact_root": str(self.artifact_root),
            "code_version": CODE_VERSION,
            "environment": "local",
            "exit_code": 0,
            "project_root": str(self.project_root),
            "run_dir": str(invalid_run_dir),
            "run_manifest_sha256": self.request.run_manifest_sha256,
            "source_bundle_sha256": SOURCE_BUNDLE_DIGEST,
            "status": "COMPLETED",
        }
        status_path = invalid_run_dir / STATUS_FILE_NAME
        status_path.write_text(json.dumps(forged_status), encoding="utf-8")
        original = status_path.read_bytes()
        runner = QueueRunner()

        with patch(
            "vrp.orchestration.eod_postgres.resolve_clean_code_version",
            side_effect=ValueError("working tree is dirty"),
        ) as code_identity:
            result = finalize_eod_postgres(invalid_request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        code_identity.assert_not_called()
        self.assertEqual(status_path.read_bytes(), original)
        self.assertFalse((invalid_run_dir / ATTEMPT_FILE_NAME).exists())
        self.assertIn(
            "timestamped direct child",
            result.payload["preflight"]["error"]["message"],
        )

    def test_older_obligated_run_blocks_newer_finalizer(self):
        older = self.run_dir.parent / "20260720_170000"
        older.mkdir()
        (older / "run_manifest.json").write_text(
            json.dumps(
                {
                    "code_version": CODE_VERSION,
                    "final_health": "PASS",
                    "postgres_environment": "local",
                    "postgres_postpass_required": True,
                    "publish_requested": True,
                    "published_outputs": {"signal_history": "signal.parquet"},
                    "run_timestamp": older.name,
                    "skip_upstream": False,
                    "status": "PASS",
                }
            ),
            encoding="utf-8",
        )
        runner = QueueRunner()

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertIn("oldest published EOD", result.payload["preflight"]["error"]["message"])

    def test_redirected_runtime_contract_is_rejected_before_children(self):
        redirected = self.project_root / "config/redirected.json"
        redirected.write_text(
            json.dumps(
                {"outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"}}
            ),
            encoding="utf-8",
        )
        manifest_path = self.run_dir / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime_config"] = str(redirected)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self._refresh_manifest_digest()
        runner = QueueRunner()

        result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertIn("canonical production", result.payload["preflight"]["error"]["message"])

    def test_concurrent_finalizer_is_rejected_without_overwriting_status(self):
        status_path = self.run_dir / STATUS_FILE_NAME
        status_path.write_text(
            json.dumps({"status": "COMPLETED", "sentinel": "preserve"}),
            encoding="utf-8",
        )
        original = status_path.read_bytes()
        runner = QueueRunner()

        with exclusive_run_lock(self.run_dir):
            result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(result.status, "ALREADY_RUNNING")
        self.assertEqual(runner.calls, [])
        self.assertEqual(status_path.read_bytes(), original)

    def test_unexpected_exact_run_lock_oserror_is_not_misclassified(self):
        target = "msvcrt.locking" if os.name == "nt" else "fcntl.flock"
        with patch(target, side_effect=OSError(errno.EIO, "storage failure")):
            with self.assertRaisesRegex(OSError, "storage failure"):
                with exclusive_run_lock(self.run_dir):
                    self.fail("unexpected lock error was swallowed")

    def test_invalid_code_version_fails_before_any_child(self):
        request = EodPostgresFinalizerRequest(
            project_root=self.project_root,
            run_dir=self.run_dir,
            artifact_root=self.artifact_root,
            environment="local",
            code_version="short",
            requested_by="unit-test",
            run_manifest_sha256=self.request.run_manifest_sha256,
            source_bundle_sha256=SOURCE_BUNDLE_DIGEST,
        )
        runner = QueueRunner()
        result = finalize_eod_postgres(request, runner=runner)
        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertFalse((self.run_dir / STATUS_FILE_NAME).exists())
        self.assertFalse((self.run_dir / ATTEMPT_FILE_NAME).exists())

    def test_manifest_digest_and_code_identity_are_hard_preflight_gates(self):
        manifest_path = self.run_dir / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["approved_nav"] = 2_000_000
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        runner = QueueRunner()

        digest_result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(digest_result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertIn(
            "caller-pinned digest",
            digest_result.payload["preflight"]["error"]["message"],
        )

        self._refresh_manifest_digest()
        with patch(
            "vrp.orchestration.eod_postgres.resolve_clean_code_version",
            side_effect=ValueError("checkout changed"),
        ):
            identity_result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(identity_result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertIn(
            "code identity",
            identity_result.payload["preflight"]["error"]["message"],
        )

    def test_source_bundle_digest_is_a_hard_preflight_gate(self):
        bundle = self._source_bundle(self.run_dir)
        bundle.content_sha256 = "0" * 64
        runner = QueueRunner()

        with patch(
            "vrp.orchestration.eod_postgres.load_eod_source_bundle",
            return_value=bundle,
        ):
            result = finalize_eod_postgres(self.request, runner=runner)

        self.assertEqual(result.exit_code, EXIT_PREFLIGHT_FAILED)
        self.assertEqual(runner.calls, [])
        self.assertIn(
            "source bundle",
            result.payload["preflight"]["error"]["message"],
        )

    def test_atomic_status_writer_closes_descriptor_if_fdopen_fails(self):
        status_path = self.run_dir / "descriptor-test.json"
        real_close = os.close
        with (
            patch(
                "vrp.orchestration.eod_postgres.os.fdopen",
                side_effect=OSError("fdopen failed"),
            ),
            patch(
                "vrp.orchestration.eod_postgres.os.close",
                wraps=real_close,
            ) as close_descriptor,
        ):
            with self.assertRaisesRegex(OSError, "fdopen failed"):
                write_status_atomic(status_path, {"status": "RUNNING"})
        close_descriptor.assert_called_once()
        self.assertFalse(status_path.exists())
        self.assertFalse(list(self.run_dir.glob(".descriptor-test.json.*.tmp")))

    def test_atomic_status_writer_rejects_symlinked_destination(self):
        destination = self.run_dir / "symlinked-status.json"
        target = self.root / "outside-status-target.json"
        target.write_text('{"sentinel": true}', encoding="utf-8")
        try:
            destination.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"file symlinks are unavailable: {exc}")

        with self.assertRaisesRegex(Exception, "regular file"):
            write_status_atomic(destination, {"status": "RUNNING"})
        self.assertEqual(target.read_text(encoding="utf-8"), '{"sentinel": true}')


if __name__ == "__main__":
    unittest.main(verbosity=2)
