from __future__ import annotations

import json
import signal
import subprocess
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from scripts import run_eod as entrypoint  # noqa: E402
from vrp.orchestration.eod import (  # noqa: E402
    EodRunRequest,
    parse_eod_manifest_line,
    run_eod_observed,
    terminate_process_tree,
)
from vrp.orchestration.post_publish_shadow import (  # noqa: E402
    EOD_DATABASE_ENV,
    REFERENCE_DATABASE_ENV,
    SHADOW_FAILURE_EXIT_CODE,
    ShadowDatabaseTargets,
    ShadowIdentity,
    build_eod_loader_command,
    build_reference_loader_command,
    execute_post_publish_shadow,
    resolve_shadow_database_targets,
    resolve_shadow_identity,
    resolve_shadow_runtime_config,
    validate_shadow_database_roles,
    validate_completed_eod_manifest,
)
from vrp.orchestration import post_publish_shadow as shadow_contract  # noqa: E402


def _write_successful_run(project_root: Path) -> Path:
    run_dir = project_root / "data/audit/vrp_hybrid_v2_eod/20260723_120000"
    run_dir.mkdir(parents=True)
    manifest = run_dir / "run_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "final_health": "PASS",
                "project_root": str(project_root),
                "publish_requested": True,
                "published_outputs": {"latest_snapshot": "snapshot.parquet"},
                "status": "PASS",
            }
        ),
        encoding="utf-8",
    )
    runtime = project_root / "config/vrp_hybrid_v2_eod_runtime_config.json"
    runtime.parent.mkdir(parents=True)
    runtime.write_text("{}", encoding="utf-8")
    for relative in (
        "scripts/load_reference_history.py",
        "scripts/load_eod_snapshot.py",
    ):
        script = project_root / relative
        script.parent.mkdir(parents=True, exist_ok=True)
        script.write_text("# test", encoding="utf-8")
    return manifest


def _role_observation(role: str, current_user: str) -> dict[str, object]:
    is_reference = role == "vrp_reference_loader"
    select_tables = (
        shadow_contract._REFERENCE_SELECT_TABLES
        if is_reference
        else shadow_contract._EOD_SELECT_TABLES
    )
    insert_tables = (
        shadow_contract._REFERENCE_INSERT_TABLES
        if is_reference
        else shadow_contract._EOD_INSERT_TABLES
    )
    update_columns = (
        shadow_contract._REFERENCE_UPDATE_COLUMNS
        if is_reference
        else shadow_contract._EOD_UPDATE_COLUMNS
    )
    execute_functions = (
        shadow_contract._REFERENCE_EXECUTE_FUNCTIONS
        if is_reference
        else frozenset()
    )
    table_privileges = [
        *((table_name, "SELECT") for table_name in select_tables),
        *((table_name, "INSERT") for table_name in insert_tables),
    ]
    return {
        "current_user": current_user,
        "is_superuser": False,
        "can_create_database": False,
        "can_create_role": False,
        "can_replicate": False,
        "can_bypass_rls": False,
        "has_expected_role": True,
        "has_forbidden_role": False,
        "can_create_in_schema": False,
        "can_use_schema": True,
        "can_create_in_database": False,
        "can_publish": False,
        "can_insert_reference_rates": is_reference,
        "can_insert_daily_features": is_reference,
        "can_insert_market_snapshots": not is_reference,
        "can_insert_selected_signals": not is_reference,
        "memberships": (role,),
        "owns_database": False,
        "owns_schema": False,
        "owns_relation": False,
        "owns_function": False,
        "table_privileges": tuple(table_privileges),
        "update_columns": tuple(update_columns),
        "sequence_privileges": (),
        "executable_functions": tuple(execute_functions),
    }


class ShadowPreflightTests(unittest.TestCase):
    def test_explicit_identity_must_be_a_clean_checkout_head(self):
        with patch(
            "vrp.orchestration.post_publish_shadow._run_git",
            side_effect=["", "a" * 40, ""],
        ) as run_git:
            identity = resolve_shadow_identity(
                ROOT,
                explicit_code_version="a" * 40,
                explicit_requested_by="operator",
                environment={},
            )
        self.assertEqual(identity, ShadowIdentity("a" * 40, "operator"))
        self.assertEqual(run_git.call_count, 3)

    def test_database_targets_require_separate_environment_values(self):
        with self.assertRaisesRegex(ValueError, REFERENCE_DATABASE_ENV):
            resolve_shadow_database_targets({})
        with self.assertRaisesRegex(ValueError, EOD_DATABASE_ENV):
            resolve_shadow_database_targets({REFERENCE_DATABASE_ENV: "reference"})
        targets = resolve_shadow_database_targets(
            {
                REFERENCE_DATABASE_ENV: "reference",
                EOD_DATABASE_ENV: "eod",
            }
        )
        self.assertEqual(targets.reference_database_url, "reference")
        self.assertEqual(targets.eod_database_url, "eod")

    def test_loader_commands_never_contain_database_credentials(self):
        identity = ShadowIdentity("b" * 40, "operator")
        reference = build_reference_loader_command(
            project_root=ROOT,
            runtime_config=ROOT / "config/runtime.json",
            environment="local",
            identity=identity,
            python_executable="python.exe",
        )
        snapshot = build_eod_loader_command(
            project_root=ROOT,
            run_dir=ROOT / "data/audit/run",
            environment="local",
            identity=identity,
            python_executable="python.exe",
        )
        rendered = " ".join(reference + snapshot)
        self.assertNotIn("postgresql://", rendered)
        self.assertNotIn(REFERENCE_DATABASE_ENV, rendered)
        self.assertNotIn(EOD_DATABASE_ENV, rendered)

    def test_runtime_files_are_validated_before_publication(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, "shadow runtime files are missing"):
                resolve_shadow_runtime_config(Path(temp), None)

    def test_database_role_preflight_rejects_shared_or_privileged_identity(self):
        reference = _role_observation("vrp_reference_loader", "same_user")
        eod = _role_observation("vrp_eod_shadow_writer", "same_user")
        with patch(
            "vrp.orchestration.post_publish_shadow._inspect_database_role",
            side_effect=[reference, eod],
        ):
            with self.assertRaisesRegex(ValueError, "must be distinct"):
                validate_shadow_database_roles(
                    ShadowDatabaseTargets("reference", "eod")
                )
        privileged = {**reference, "current_user": "reference", "is_superuser": True}
        with patch(
            "vrp.orchestration.post_publish_shadow._inspect_database_role",
            side_effect=[privileged, {**eod, "current_user": "eod"}],
        ):
            with self.assertRaisesRegex(ValueError, "over-privileged"):
                validate_shadow_database_roles(
                    ShadowDatabaseTargets("reference", "eod")
                )

    def test_exact_column_scoped_update_roles_pass_preflight(self):
        reference = _role_observation(
            "vrp_reference_loader", "reference_user"
        )
        eod = _role_observation("vrp_eod_shadow_writer", "eod_user")
        self.assertNotIn(
            "UPDATE",
            {privilege for _, privilege in reference["table_privileges"]},
        )
        self.assertTrue(reference["update_columns"])
        with patch(
            "vrp.orchestration.post_publish_shadow._inspect_database_role",
            side_effect=[reference, eod],
        ):
            self.assertEqual(
                validate_shadow_database_roles(
                    ShadowDatabaseTargets("reference", "eod")
                ),
                ("reference_user", "eod_user"),
            )

    def test_database_role_preflight_rejects_unexpected_privileges(self):
        reference = _role_observation("vrp_reference_loader", "reference_user")
        eod = _role_observation("vrp_eod_shadow_writer", "eod_user")
        reference["memberships"] = (
            "vrp_reference_loader",
            "unrelated_privileged_role",
        )
        with patch(
            "vrp.orchestration.post_publish_shadow._inspect_database_role",
            side_effect=[reference, eod],
        ):
            with self.assertRaisesRegex(ValueError, "unexpected role memberships"):
                validate_shadow_database_roles(
                    ShadowDatabaseTargets("reference", "eod")
                )
        reference = _role_observation("vrp_reference_loader", "reference_user")
        reference["table_privileges"] = (
            *reference["table_privileges"],
            ("selected_signals", "DELETE"),
        )
        with patch(
            "vrp.orchestration.post_publish_shadow._inspect_database_role",
            side_effect=[reference, eod],
        ):
            with self.assertRaisesRegex(ValueError, "unexpected table privileges"):
                validate_shadow_database_roles(
                    ShadowDatabaseTargets("reference", "eod")
                )


class ManifestSelectionTests(unittest.TestCase):
    def test_windows_cancellation_requests_graceful_break_before_force_kill(self):
        process = SimpleNamespace(
            pid=1234,
            poll=lambda: None,
            send_signal=MagicMock(),
            wait=MagicMock(return_value=0),
        )
        with patch("vrp.orchestration.eod.os.name", "nt"), patch(
            "vrp.orchestration.eod.subprocess.run"
        ) as taskkill:
            terminate_process_tree(process, grace_seconds=1)
        process.send_signal.assert_called_once_with(signal.CTRL_BREAK_EVENT)
        process.wait.assert_called_once_with(timeout=1)
        taskkill.assert_not_called()

    def test_windows_cancellation_force_kills_only_after_grace_timeout(self):
        process = SimpleNamespace(
            pid=1234,
            poll=lambda: None,
            send_signal=MagicMock(),
            wait=MagicMock(
                side_effect=[subprocess.TimeoutExpired("process", 1), 0]
            ),
        )
        with patch("vrp.orchestration.eod.os.name", "nt"), patch(
            "vrp.orchestration.eod.subprocess.run"
        ) as taskkill:
            terminate_process_tree(process, grace_seconds=1)
        process.send_signal.assert_called_once_with(signal.CTRL_BREAK_EVENT)
        taskkill.assert_called_once()

    def test_only_exact_eod_audit_manifest_is_accepted(self):
        expected = (
            ROOT
            / "data/audit/vrp_hybrid_v2_eod/20260723_120000/run_manifest.json"
        ).resolve()
        line = f"VRP_EOD_MANIFEST_PATH={expected}"
        self.assertEqual(parse_eod_manifest_line(line, ROOT), expected)
        self.assertIsNone(
            parse_eod_manifest_line(
                f"VRP_EOD_MANIFEST_PATH={ROOT / 'data/audit/sofr/run_manifest.json'}",
                ROOT,
            )
        )
        self.assertIsNone(parse_eod_manifest_line("not a manifest", ROOT))

    def test_manifest_outside_audit_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outside = root / "run_manifest.json"
            outside.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "outside the EOD audit root"):
                validate_completed_eod_manifest(outside, root)

    def test_observed_runner_ignores_upstream_manifest_and_keeps_exact_eod_path(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy = root / "notebooks/vrp_hybrid_v2_eod_pipeline.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("# test", encoding="utf-8")
            expected = (
                root
                / "data/audit/vrp_hybrid_v2_eod/20260723_120000/run_manifest.json"
            ).resolve()
            fake_process = SimpleNamespace(
                stdout=iter(
                    [
                        f"Manifest: {root / 'data/audit/sofr/run_manifest.json'}\n",
                        "VRP_PROGRESS|done|100|PASS\n",
                        f"VRP_EOD_MANIFEST_PATH={expected}\n",
                    ]
                ),
                wait=lambda: 0,
            )
            with patch(
                "vrp.orchestration.eod.subprocess.Popen",
                return_value=fake_process,
            ) as popen, patch("sys.stdout", new_callable=StringIO):
                result = run_eod_observed(
                    EodRunRequest(project_root=root),
                    python_executable="python.exe",
                    process_environment={"PATH": "test"},
                )
            self.assertEqual(result.return_code, 0)
            self.assertEqual(result.manifest_paths, (expected,))
            self.assertEqual(popen.call_args.kwargs["env"], {"PATH": "test"})

    def test_observed_runner_terminates_process_tree_when_output_handling_is_cancelled(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy = root / "notebooks/vrp_hybrid_v2_eod_pipeline.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("# test", encoding="utf-8")
            fake_process = SimpleNamespace(
                stdout=iter(["VRP_PROGRESS|step|1|starting\n"]),
                wait=lambda: 0,
            )
            with patch(
                "vrp.orchestration.eod.subprocess.Popen",
                return_value=fake_process,
            ), patch(
                "vrp.orchestration.eod.terminate_process_tree"
            ) as terminate, patch("sys.stdout", new_callable=StringIO):
                with self.assertRaisesRegex(RuntimeError, "cancelled"):
                    run_eod_observed(
                        EodRunRequest(project_root=root),
                        output_handler=lambda _line: (_ for _ in ()).throw(
                            RuntimeError("cancelled")
                        ),
                    )
            terminate.assert_called_once_with(fake_process)


class ShadowExecutionTests(unittest.TestCase):
    def test_changed_checkout_skips_all_database_loaders(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = _write_successful_run(root)
            with patch(
                "vrp.orchestration.post_publish_shadow.validate_shadow_checkout",
                side_effect=ValueError("dirty checkout"),
            ), patch(
                "vrp.orchestration.post_publish_shadow._run_loader"
            ) as run_loader:
                result = execute_post_publish_shadow(
                    project_root=root,
                    manifest_path=manifest,
                    runtime_config=None,
                    environment="local",
                    identity=ShadowIdentity("c" * 40, "operator"),
                    databases=ShadowDatabaseTargets("reference", "eod"),
                )
            self.assertFalse(result.success)
            run_loader.assert_not_called()
            status = json.loads(result.status_path.read_text(encoding="utf-8"))
            self.assertEqual(status["shadow_status"], "FAILED")
            self.assertTrue(status["file_publication_retained"])
            self.assertIn("changed", status["error"])

    def test_reference_load_precedes_snapshot_and_credentials_are_separated(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = _write_successful_run(root)
            identity = ShadowIdentity("c" * 40, "operator")
            databases = ShadowDatabaseTargets("reference-secret", "eod-secret")
            with patch(
                "vrp.orchestration.post_publish_shadow._run_loader",
                side_effect=[
                    (0, [{"no_op": True, "dataset_key": "FRED_SOFR"}]),
                    (0, {"no_op": True, "decision": "NO_TRADE"}),
                ],
            ) as run_loader, patch(
                "vrp.orchestration.post_publish_shadow.validate_shadow_checkout"
            ) as validate_checkout:
                result = execute_post_publish_shadow(
                    project_root=root,
                    manifest_path=manifest,
                    runtime_config=None,
                    environment="local",
                    identity=identity,
                    databases=databases,
                    python_executable="python.exe",
                    base_environment={"PATH": "path"},
                )
            self.assertTrue(result.success)
            self.assertEqual(run_loader.call_count, 2)
            self.assertEqual(validate_checkout.call_count, 2)
            self.assertEqual(
                [item.kwargs["database_url"] for item in run_loader.call_args_list],
                ["reference-secret", "eod-secret"],
            )
            self.assertIn("load_reference_history.py", run_loader.call_args_list[0].args[0][2])
            self.assertIn("load_eod_snapshot.py", run_loader.call_args_list[1].args[0][2])
            status_text = result.status_path.read_text(encoding="utf-8")
            self.assertNotIn("reference-secret", status_text)
            self.assertNotIn("eod-secret", status_text)
            self.assertEqual(json.loads(status_text)["shadow_status"], "PASS")

    def test_reference_failure_skips_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = _write_successful_run(root)
            with patch(
                "vrp.orchestration.post_publish_shadow._run_loader",
                return_value=(1, None),
            ) as run_loader, patch(
                "vrp.orchestration.post_publish_shadow.validate_shadow_checkout"
            ):
                result = execute_post_publish_shadow(
                    project_root=root,
                    manifest_path=manifest,
                    runtime_config=None,
                    environment="local",
                    identity=ShadowIdentity("d" * 40, "operator"),
                    databases=ShadowDatabaseTargets("reference", "eod"),
                    python_executable="python.exe",
                )
            self.assertFalse(result.success)
            self.assertEqual(run_loader.call_count, 1)
            status = json.loads(result.status_path.read_text(encoding="utf-8"))
            self.assertIsNone(status["snapshot_loader_return_code"])
            self.assertTrue(status["file_publication_retained"])

    def test_loader_timeout_is_recorded_as_shadow_only_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = _write_successful_run(root)
            with patch(
                "vrp.orchestration.post_publish_shadow._run_loader",
                side_effect=subprocess.TimeoutExpired("loader", 1),
            ), patch(
                "vrp.orchestration.post_publish_shadow.validate_shadow_checkout"
            ):
                result = execute_post_publish_shadow(
                    project_root=root,
                    manifest_path=manifest,
                    runtime_config=None,
                    environment="local",
                    identity=ShadowIdentity("d" * 40, "operator"),
                    databases=ShadowDatabaseTargets("reference", "eod"),
                    loader_timeout_seconds=1,
                )
            self.assertFalse(result.success)
            status = json.loads(result.status_path.read_text(encoding="utf-8"))
            self.assertEqual(status["shadow_status"], "FAILED")
            self.assertTrue(status["file_publication_retained"])
            self.assertIn("timed out", status["error"])


class StableEntrypointShadowTests(unittest.TestCase):
    @patch("scripts.run_eod.run_eod")
    def test_default_path_is_unchanged(self, run_eod):
        run_eod.return_value = 7
        self.assertEqual(entrypoint.main(["--project-root", str(ROOT)]), 7)
        run_eod.assert_called_once()

    @patch("scripts.run_eod.resolve_shadow_identity")
    def test_dry_run_has_no_shadow_preflight_side_effects(self, resolve_identity):
        result = entrypoint.main(
            ["--project-root", str(ROOT), "--shadow-write", "--dry-run"]
        )
        self.assertEqual(result, 0)
        resolve_identity.assert_not_called()

    @patch("scripts.run_eod.run_eod_observed")
    def test_shadow_and_no_publish_are_rejected_before_eod(self, observed):
        result = entrypoint.main(
            ["--project-root", str(ROOT), "--shadow-write", "--no-publish"]
        )
        self.assertEqual(result, 2)
        observed.assert_not_called()

    @patch("scripts.run_eod.validate_shadow_checkout")
    @patch("scripts.run_eod.execute_post_publish_shadow")
    @patch("scripts.run_eod.run_eod_observed")
    @patch("scripts.run_eod.validate_shadow_database_roles")
    @patch("scripts.run_eod.resolve_shadow_database_targets")
    @patch("scripts.run_eod.resolve_shadow_identity")
    def test_legacy_failure_skips_shadow(
        self,
        resolve_identity,
        resolve_databases,
        validate_roles,
        observed,
        execute_shadow,
        validate_checkout,
    ):
        resolve_identity.return_value = ShadowIdentity("e" * 40, "operator")
        resolve_databases.return_value = ShadowDatabaseTargets("reference", "eod")
        observed.return_value = SimpleNamespace(return_code=9, manifest_paths=())
        result = entrypoint.main(["--project-root", str(ROOT), "--shadow-write"])
        self.assertEqual(result, 9)
        execute_shadow.assert_not_called()

    @patch("scripts.run_eod.validate_shadow_checkout")
    @patch("scripts.run_eod.execute_post_publish_shadow")
    @patch("scripts.run_eod.run_eod_observed")
    @patch("scripts.run_eod.validate_shadow_database_roles")
    @patch("scripts.run_eod.resolve_shadow_database_targets")
    @patch("scripts.run_eod.resolve_shadow_identity")
    def test_shadow_failure_has_distinct_post_publication_exit_code(
        self,
        resolve_identity,
        resolve_databases,
        validate_roles,
        observed,
        execute_shadow,
        validate_checkout,
    ):
        manifest = ROOT / "data/audit/vrp_hybrid_v2_eod/run/run_manifest.json"
        resolve_identity.return_value = ShadowIdentity("f" * 40, "operator")
        resolve_databases.return_value = ShadowDatabaseTargets("reference", "eod")
        observed.return_value = SimpleNamespace(
            return_code=0, manifest_paths=(manifest,)
        )
        execute_shadow.return_value = SimpleNamespace(
            success=False,
            error="snapshot failed",
            status_path=manifest.parent / "post_publish_shadow_status.json",
        )
        result = entrypoint.main(["--project-root", str(ROOT), "--shadow-write"])
        self.assertEqual(result, SHADOW_FAILURE_EXIT_CODE)

    @patch("scripts.run_eod.validate_shadow_checkout")
    @patch("scripts.run_eod.execute_post_publish_shadow")
    @patch("scripts.run_eod.run_eod_observed")
    @patch("scripts.run_eod.resolve_shadow_runtime_config")
    @patch("scripts.run_eod.validate_shadow_database_roles")
    @patch("scripts.run_eod.resolve_shadow_database_targets")
    @patch("scripts.run_eod.resolve_shadow_identity")
    def test_successful_file_publication_is_followed_by_exact_shadow_load(
        self,
        resolve_identity,
        resolve_databases,
        validate_roles,
        resolve_runtime,
        observed,
        execute_shadow,
        validate_checkout,
    ):
        events: list[str] = []
        manifest = ROOT / "data/audit/vrp_hybrid_v2_eod/run/run_manifest.json"
        identity = ShadowIdentity("1" * 40, "operator")
        databases = ShadowDatabaseTargets("reference", "eod")
        resolve_identity.return_value = identity
        resolve_databases.return_value = databases
        validate_roles.return_value = ("reference_user", "eod_user")
        resolve_runtime.return_value = ROOT / "config/runtime.json"
        validate_checkout.side_effect = lambda *args, **kwargs: events.append(
            "preflight"
        )
        observed.side_effect = lambda *args, **kwargs: (
            events.append("file"),
            SimpleNamespace(return_code=0, manifest_paths=(manifest,)),
        )[1]
        execute_shadow.side_effect = lambda **kwargs: (
            events.append("shadow"),
            SimpleNamespace(
                success=True,
                error=None,
                status_path=manifest.parent / "post_publish_shadow_status.json",
            ),
        )[1]
        result = entrypoint.main(["--project-root", str(ROOT), "--shadow-write"])
        self.assertEqual(result, 0)
        self.assertEqual(events, ["preflight", "file", "shadow"])
        shadow_args = execute_shadow.call_args.kwargs
        self.assertEqual(shadow_args["manifest_path"], manifest)
        self.assertEqual(
            shadow_args["validated_database_users"],
            ("reference_user", "eod_user"),
        )

    @patch("scripts.run_eod.run_eod_observed")
    @patch("scripts.run_eod.resolve_shadow_runtime_config")
    @patch("scripts.run_eod.validate_shadow_database_roles")
    @patch("scripts.run_eod.resolve_shadow_database_targets")
    @patch("scripts.run_eod.resolve_shadow_identity")
    def test_role_preflight_failure_occurs_before_canonical_execution(
        self,
        resolve_identity,
        resolve_databases,
        validate_roles,
        resolve_runtime,
        observed,
    ):
        resolve_identity.return_value = ShadowIdentity("2" * 40, "operator")
        resolve_databases.return_value = ShadowDatabaseTargets("reference", "eod")
        validate_roles.side_effect = ValueError("over-privileged")
        result = entrypoint.main(["--project-root", str(ROOT), "--shadow-write"])
        self.assertEqual(result, 2)
        observed.assert_not_called()


class StreamlitRoutingContractTests(unittest.TestCase):
    def test_dashboard_routes_through_stable_entrypoint_and_handles_shadow_failure(self):
        source = (
            ROOT / "notebooks/streamlit_vrp_hybrid_v2_eod.py"
        ).read_text(encoding="utf-8")
        self.assertIn('project_root / "scripts/run_eod.py"', source)
        self.assertNotIn(
            'with_name("vrp_hybrid_v2_eod_pipeline.py")',
            source,
        )
        self.assertIn("SHADOW_FAILURE_EXIT_CODE = 3", source)
        self.assertIn("File refresh published; PostgreSQL shadow recording failed.", source)
        self.assertIn("terminate_process_tree(process)", source)
        pipeline_source = (
            ROOT / "notebooks/vrp_hybrid_v2_eod_pipeline.py"
        ).read_text(encoding="utf-8")
        self.assertIn("except BaseException as exc:", pipeline_source)
        self.assertIn("restore_files(backup_map)", pipeline_source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
