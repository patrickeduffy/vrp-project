from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.orchestration.eod import (  # noqa: E402
    LEGACY_EOD_REL,
    EodRunRequest,
    build_eod_command,
    render_command,
    run_eod,
)


class StableEodEntrypointTests(unittest.TestCase):
    def test_invalid_approved_nav_is_rejected_before_command_execution(self):
        for value in (0.0, -1.0, float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "finite number greater than zero"):
                    EodRunRequest(project_root=ROOT, approved_nav=value)

    def test_default_command_delegates_without_changing_model_logic(self):
        request = EodRunRequest(project_root=ROOT)
        command = build_eod_command(request, python_executable="python.exe")
        self.assertEqual(command[0:2], ["python.exe", "-u"])
        self.assertEqual(Path(command[2]), ROOT / LEGACY_EOD_REL)
        self.assertEqual(command[3:5], ["--project-root", str(ROOT)])
        self.assertEqual(command[5:7], ["--approved-nav", "1000000"])
        self.assertNotIn("--no-publish", command)

    def test_all_diagnostic_options_are_forwarded_exactly_once(self):
        runtime = ROOT / "config/vrp_hybrid_v2_eod_runtime_config.json"
        request = EodRunRequest(
            project_root=ROOT,
            approved_nav=750_000.0,
            target_date="2026-07-21",
            runtime_config=runtime,
            recalc_start="2026-07-20",
            skip_upstream=True,
            force_recalculate=True,
            publish=False,
        )
        command = build_eod_command(request, python_executable="python.exe")
        for option in (
            "--runtime-config",
            "--target-date",
            "--recalc-start",
            "--skip-upstream",
            "--force-recalculate",
            "--no-publish",
        ):
            self.assertEqual(command.count(option), 1)
        self.assertIn("750000", command)

    def test_rendered_command_quotes_paths_with_spaces(self):
        rendered = render_command(["python.exe", r"C:\VRP Project\scripts\run.py"])
        self.assertIn('"C:\\VRP Project\\scripts\\run.py"', rendered)

    @patch("vrp.orchestration.eod.subprocess.run")
    def test_run_eod_delegates_in_project_root_and_merges_environment(self, run_process):
        run_process.return_value.returncode = 7
        request = EodRunRequest(project_root=ROOT, publish=False)
        result = run_eod(
            request,
            python_executable="python.exe",
            extra_environment={"VRP_TEST_MARKER": "present"},
        )
        self.assertEqual(result, 7)
        _, kwargs = run_process.call_args
        self.assertEqual(kwargs["cwd"], ROOT)
        self.assertFalse(kwargs["check"])
        self.assertEqual(kwargs["env"]["VRP_TEST_MARKER"], "present")
        self.assertIn("PATH", {key.upper(): value for key, value in kwargs["env"].items()})


if __name__ == "__main__":
    unittest.main(verbosity=2)
