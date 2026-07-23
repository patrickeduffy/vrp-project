from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load_script():
    name = "run_eod_postpass_test_module"
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts/run_eod.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_eod_script = load_script()


class StableEodPostpassTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.project_root = Path(self.temporary.name).resolve()
        (self.project_root / "notebooks").mkdir()
        (self.project_root / "notebooks/vrp_hybrid_v2_eod_pipeline.py").write_text(
            "# accepted runner\n",
            encoding="utf-8",
        )
        runtime_path = (
            self.project_root
            / "config/vrp_hybrid_v2_eod_runtime_config.json"
        )
        runtime_path.parent.mkdir(parents=True)
        runtime_path.write_text(
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

    def _args(self, **changes):
        values = {
            "approved_nav": 1_000_000.0,
            "code_version": None,
            "dry_run": False,
            "force_recalculate": False,
            "no_postgres_shadow": False,
            "no_publish": False,
            "postgres_artifact_root": None,
            "postgres_environment": "local",
            "project_root": self.project_root,
            "python_executable": Path("python.exe"),
            "recalc_start": None,
            "requested_by": "unit-test",
            "runtime_config": None,
            "skip_upstream": False,
            "target_date": "20260721",
        }
        values.update(changes)
        return argparse.Namespace(**values)

    def _success_patches(self, args):
        finalization = SimpleNamespace(
            exit_code=0,
            status="COMPLETED",
            status_path=self.run_dir / "postgres_finalization_status.json",
        )
        return (
            patch.object(run_eod_script, "parse_args", return_value=args),
            patch.object(run_eod_script, "run_eod", return_value=0),
            patch.object(
                run_eod_script,
                "resolve_clean_code_version",
                return_value="a" * 40,
            ),
            patch.object(
                run_eod_script,
                "load_completed_eod_handoff",
                return_value=SimpleNamespace(
                    run_dir=self.run_dir,
                    run_manifest_sha256="b" * 64,
                    source_bundle_sha256="c" * 64,
                ),
            ),
            patch.object(
                run_eod_script,
                "finalize_eod_postgres",
                return_value=finalization,
            ),
        )

    def test_normal_publish_requires_exact_handoff_and_database_finalization(self):
        patches = self._success_patches(self._args())
        with patch.dict(
            run_eod_script.os.environ,
            {"VRP_DATABASE_URL": "configured"},
        ), patches[0], patches[1] as file_eod, patches[2], patches[3] as handoff, patches[
            4
        ] as finalize:
            result = run_eod_script.main()

        self.assertEqual(result, 0)
        self.assertEqual(file_eod.call_count, 1)
        result_path = file_eod.call_args.kwargs["result_handoff"]
        self.assertEqual(result_path.name, "completed-eod.json")
        handoff.assert_called_once()
        finalize.assert_called_once()
        request = finalize.call_args.args[0]
        self.assertEqual(request.run_dir, self.run_dir)
        self.assertEqual(request.code_version, "a" * 40)
        self.assertEqual(request.run_manifest_sha256, "b" * 64)
        self.assertEqual(request.source_bundle_sha256, "c" * 64)

    def test_no_publish_and_skip_upstream_never_start_the_postpass(self):
        for changes, reason in (
            ({"no_publish": True}, "no-publish"),
            ({"skip_upstream": True, "no_publish": True}, "skip-upstream"),
            ({"no_postgres_shadow": True}, "explicit"),
        ):
            with self.subTest(reason=reason):
                patches = self._success_patches(self._args(**changes))
                with patches[0], patches[1] as file_eod, patches[2] as code, patches[
                    3
                ] as handoff, patches[4] as finalize:
                    result = run_eod_script.main()
                self.assertEqual(result, 0)
                file_eod.assert_called_once()
                self.assertNotIn("result_handoff", file_eod.call_args.kwargs)
                if reason == "explicit":
                    self.assertEqual(
                        file_eod.call_args.args[0].postgres_postpass_bypass_reason,
                        "explicit-no-postgres-shadow",
                    )
                code.assert_not_called()
                handoff.assert_not_called()
                finalize.assert_not_called()

    def test_published_skip_upstream_is_rejected_before_file_eod(self):
        patches = self._success_patches(self._args(skip_upstream=True))
        with patches[0], patches[1] as file_eod, patches[2], patches[3], patches[4]:
            result = run_eod_script.main()
        self.assertEqual(result, run_eod_script.EXIT_WRAPPER_CONFIGURATION_FAILED)
        file_eod.assert_not_called()

    def test_write_capable_run_rejects_redirected_runtime_contract(self):
        redirected = self.project_root / "config/redirected.json"
        redirected.write_text(
            json.dumps(
                {"outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"}}
            ),
            encoding="utf-8",
        )
        patches = self._success_patches(self._args(runtime_config=redirected))

        with patches[0], patches[1] as file_eod, patches[2], patches[3], patches[4]:
            result = run_eod_script.main()

        self.assertEqual(result, run_eod_script.EXIT_WRAPPER_CONFIGURATION_FAILED)
        file_eod.assert_not_called()

    def test_file_failure_short_circuits_handoff_and_database_work(self):
        patches = self._success_patches(self._args())
        with patch.dict(
            run_eod_script.os.environ,
            {"VRP_DATABASE_URL": "configured"},
        ), patches[0], patch.object(
            run_eod_script,
            "run_eod",
            return_value=7,
        ), patches[2], patches[3] as handoff, patches[4] as finalize:
            result = run_eod_script.main()
        self.assertEqual(result, 7)
        handoff.assert_not_called()
        finalize.assert_not_called()

    def test_invalid_handoff_and_finalizer_failure_are_nonzero_after_file_pass(self):
        patches = self._success_patches(self._args())
        with patch.dict(
            run_eod_script.os.environ,
            {"VRP_DATABASE_URL": "configured"},
        ), patches[0], patches[1], patches[2], patch.object(
            run_eod_script,
            "load_completed_eod_handoff",
            side_effect=ValueError("tampered"),
        ), patches[4] as finalize:
            result = run_eod_script.main()
        self.assertEqual(result, run_eod_script.EXIT_HANDOFF_INVALID)
        finalize.assert_not_called()

        failed = SimpleNamespace(
            exit_code=20,
            status="REFERENCE_SYNC_FAILED",
            status_path=self.run_dir / "postgres_finalization_status.json",
        )
        patches = self._success_patches(self._args())
        with patch.dict(
            run_eod_script.os.environ,
            {"VRP_DATABASE_URL": "configured"},
        ), patches[0], patches[1], patches[2], patches[3], patch.object(
            run_eod_script,
            "finalize_eod_postgres",
            return_value=failed,
        ):
            result = run_eod_script.main()
        self.assertEqual(result, 20)

    def test_missing_database_configuration_fails_before_file_eod(self):
        patches = self._success_patches(self._args())
        environment = dict(run_eod_script.os.environ)
        environment.pop("VRP_DATABASE_URL", None)
        with patch.dict(run_eod_script.os.environ, environment, clear=True), patches[
            0
        ], patches[1] as file_eod, patches[2], patches[3], patches[4]:
            result = run_eod_script.main()
        self.assertEqual(result, run_eod_script.EXIT_WRAPPER_CONFIGURATION_FAILED)
        file_eod.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
