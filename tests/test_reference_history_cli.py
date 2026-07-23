from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

import pandas as pd
from vrp.reference_history import LoadResult
from vrp.reference_history.sources import normalize_sofr_frame

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "load_reference_history_test_module",
    ROOT / "scripts/load_reference_history.py",
)
loader_cli = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(loader_cli)


class TrackingLoaderConnection:
    def __init__(self, events: list[str]):
        self.events = events
        self.rollbacks = 0
        self.closed = False

    def rollback(self):
        self.rollbacks += 1
        self.events.append("rollback")

    def close(self):
        self.closed = True
        self.events.append("close")


class ReferenceHistoryCliTests(unittest.TestCase):
    def test_mutating_load_rejects_redirected_runtime_before_connecting(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory).resolve()
            config_dir = project / "config"
            config_dir.mkdir()
            runtime = {
                "canonical": {
                    "sofr_cache": "data/sofr.csv",
                    "spy_eod": "data/spy.parquet",
                    "wilder_rsi": "data/rsi.parquet",
                    "rv21d": "data/rv.parquet",
                },
                "outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"},
            }
            (config_dir / "vrp_hybrid_v2_eod_runtime_config.json").write_text(
                json.dumps(runtime), encoding="utf-8"
            )
            redirected = config_dir / "redirected.json"
            redirected.write_text(json.dumps(runtime), encoding="utf-8")
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--runtime-config",
                    str(redirected),
                    "--code-version",
                    "a" * 40,
                ]
            )

            with patch.object(loader_cli, "connect_from_environment") as connect:
                with self.assertRaisesRegex(ValueError, "canonical production"):
                    loader_cli.run(args)

            connect.assert_not_called()

    def test_validate_only_reads_runtime_contract_without_writing_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            (project / "config").mkdir()
            (project / "data").mkdir()
            (project / "data/sofr.csv").write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n2018-04-04,1.74\n",
                encoding="utf-8",
            )
            runtime = {
                "canonical": {
                    "sofr_cache": "data/sofr.csv",
                    "spy_eod": "data/not-read.parquet",
                    "wilder_rsi": "data/not-read-rsi.parquet",
                    "rv21d": "data/not-read-rv.parquet",
                }
            }
            (project / "config/runtime.json").write_text(
                json.dumps(runtime), encoding="utf-8"
            )
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--runtime-config",
                    "config/runtime.json",
                    "--validate-only",
                ]
            )
            partial = normalize_sofr_frame(
                pd.DataFrame(
                    {
                        "observation_date": ["2018-04-03", "2018-04-04"],
                        "SOFR": ["1.83", "1.74"],
                    }
                ),
                enforce_production_coverage=False,
            )
            with patch.object(loader_cli, "_stable_validate", return_value=partial):
                result = loader_cli.run(args)
            self.assertEqual(result[0]["dataset_key"], "FRED_SOFR")
            self.assertIsNone(result[0]["definition_content_sha256"])
            self.assertEqual(result[0]["row_count"], 2)
            self.assertFalse((project / "data/reference_history").exists())

    def test_standalone_load_verifies_prior_database_continuity_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory).resolve()
            (project / "config").mkdir()
            (project / "data").mkdir()
            sofr_source = project / "data/sofr.csv"
            sofr_source.write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n",
                encoding="utf-8",
            )
            runtime = {
                "canonical": {
                    "sofr_cache": "data/sofr.csv",
                    "spy_eod": "data/spy.parquet",
                    "wilder_rsi": "data/rsi.parquet",
                    "rv21d": "data/rv.parquet",
                },
                "outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"},
            }
            (project / "config/vrp_hybrid_v2_eod_runtime_config.json").write_text(
                json.dumps(runtime), encoding="utf-8"
            )
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--artifact-root",
                    str(project / "artifacts"),
                    "--environment",
                    "test",
                    "--code-version",
                    "a" * 40,
                    "--requested-by",
                    "unit-test",
                ]
            )
            events: list[str] = []
            connection = TrackingLoaderConnection(events)
            prior_evidence = (object(),)
            frozen = SimpleNamespace(
                inputs={
                    "sofr": SimpleNamespace(
                        path=sofr_source,
                        content_sha256="1" * 64,
                    )
                }
            )
            store = SimpleNamespace(
                freeze_inputs=lambda sources: frozen,
                verify_frozen_inputs=lambda candidate: None,
            )
            prepared = object()

            @contextmanager
            def writer_lock(project_root):
                events.append("writer_lock_enter")
                yield
                events.append("writer_lock_exit")

            @contextmanager
            def standalone_lease(candidate):
                self.assertIs(candidate, connection)
                events.append("standalone_lease_enter")
                yield
                events.append("standalone_lease_exit")

            def completed_evidence(*args, **kwargs):
                self.assertEqual(kwargs["environment"], "test")
                events.append("prior_evidence")
                return prior_evidence

            def verify_continuity(candidate, evidence):
                self.assertIs(candidate, connection)
                self.assertIs(evidence, prior_evidence)
                self.assertTrue(evidence)
                events.append("continuity")

            def execute(candidate, exact_prepared, **kwargs):
                self.assertIs(candidate, connection)
                self.assertIs(exact_prepared, prepared)
                events.append("mutation")
                return LoadResult(
                    pipeline_run_id=UUID("00000000-0000-0000-0000-000000000001"),
                    reference_data_release_id=UUID(
                        "00000000-0000-0000-0000-000000000002"
                    ),
                    dataset_key="FRED_SOFR",
                    content_sha256="2" * 64,
                    source_row_count=1,
                    persisted_row_count=1,
                    new_count=1,
                    correction_count=0,
                    unchanged_count=0,
                    no_op=False,
                )

            with (
                patch.object(
                    loader_cli,
                    "ContentAddressedArtifactStore",
                    return_value=store,
                ),
                patch.object(loader_cli, "_normalize", return_value=object()),
                patch.object(loader_cli, "prepare_history", return_value=prepared),
                patch.object(loader_cli, "exclusive_eod_writer_lock", writer_lock),
                patch.object(loader_cli, "assert_no_unresolved_eod_finalizations"),
                patch.object(
                    loader_cli,
                    "completed_eod_finalization_evidence",
                    side_effect=completed_evidence,
                ),
                patch.object(
                    loader_cli,
                    "verify_database_finalization_continuity",
                    side_effect=verify_continuity,
                ),
                patch.object(loader_cli, "standalone_finalization_lease", standalone_lease),
                patch.object(loader_cli, "connect_from_environment", return_value=connection),
                patch.object(
                    loader_cli,
                    "execute_reference_history_load",
                    side_effect=execute,
                ),
            ):
                output = loader_cli.run(args)

            self.assertEqual(output[0]["dataset_key"], "FRED_SOFR")
            self.assertEqual(connection.rollbacks, 1)
            self.assertTrue(connection.closed)
            self.assertLess(
                events.index("standalone_lease_enter"), events.index("continuity")
            )
            self.assertLess(events.index("continuity"), events.index("rollback"))
            self.assertLess(events.index("rollback"), events.index("mutation"))
            self.assertLess(events.index("mutation"), events.index("standalone_lease_exit"))
            self.assertLess(events.index("standalone_lease_exit"), events.index("close"))

    def test_standalone_load_continuity_failure_prevents_mutation_and_closes(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory).resolve()
            (project / "config").mkdir()
            runtime = {
                "canonical": {
                    "sofr_cache": "data/sofr.csv",
                    "spy_eod": "data/spy.parquet",
                    "wilder_rsi": "data/rsi.parquet",
                    "rv21d": "data/rv.parquet",
                },
                "outputs": {"audit_dir": "data/audit/vrp_hybrid_v2_eod"},
            }
            (project / "config/vrp_hybrid_v2_eod_runtime_config.json").write_text(
                json.dumps(runtime), encoding="utf-8"
            )
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--environment",
                    "test",
                    "--code-version",
                    "a" * 40,
                    "--requested-by",
                    "unit-test",
                ]
            )
            events: list[str] = []
            connection = TrackingLoaderConnection(events)
            prior_evidence = (object(),)

            @contextmanager
            def writer_lock(project_root):
                yield

            @contextmanager
            def standalone_lease(candidate):
                self.assertIs(candidate, connection)
                events.append("standalone_lease_enter")
                try:
                    yield
                finally:
                    events.append("standalone_lease_exit")

            def reject_continuity(candidate, evidence):
                self.assertIs(candidate, connection)
                self.assertIs(evidence, prior_evidence)
                events.append("continuity")
                raise RuntimeError("prior PostgreSQL projection is incomplete")

            with (
                patch.object(loader_cli, "exclusive_eod_writer_lock", writer_lock),
                patch.object(loader_cli, "assert_no_unresolved_eod_finalizations"),
                patch.object(
                    loader_cli,
                    "completed_eod_finalization_evidence",
                    return_value=prior_evidence,
                ),
                patch.object(
                    loader_cli,
                    "verify_database_finalization_continuity",
                    side_effect=reject_continuity,
                ),
                patch.object(loader_cli, "standalone_finalization_lease", standalone_lease),
                patch.object(loader_cli, "connect_from_environment", return_value=connection),
                patch.object(loader_cli, "execute_reference_history_load") as execute,
            ):
                with self.assertRaisesRegex(RuntimeError, "projection is incomplete"):
                    loader_cli.run(args)

            execute.assert_not_called()
            self.assertEqual(connection.rollbacks, 0)
            self.assertTrue(connection.closed)
            self.assertEqual(
                events,
                [
                    "standalone_lease_enter",
                    "continuity",
                    "standalone_lease_exit",
                    "close",
                ],
            )

    def test_generation_cannot_be_negative(self):
        args = loader_cli.parse_args(["sofr", "--generation", "-1", "--validate-only"])
        with self.assertRaisesRegex(ValueError, "non-negative"):
            loader_cli.run(args)

    def test_exact_sofr_override_is_pinned_and_replaces_mutable_canonical(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            (project / "config").mkdir()
            (project / "data").mkdir()
            canonical = project / "data/current-sofr.csv"
            exact = project / "data/frozen-sofr.csv"
            canonical.write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n2018-04-04,1.74\n",
                encoding="utf-8",
            )
            exact.write_text(
                "observation_date,SOFR\n2018-04-03,1.83\n",
                encoding="utf-8",
            )
            runtime = {
                "canonical": {
                    "sofr_cache": str(canonical),
                    "spy_eod": "data/not-read.parquet",
                    "wilder_rsi": "data/not-read-rsi.parquet",
                    "rv21d": "data/not-read-rv.parquet",
                }
            }
            runtime_path = project / "config/runtime.json"
            runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
            digest = loader_cli.sha256_file(exact)
            args = loader_cli.parse_args(
                [
                    "sofr",
                    "--project-root",
                    str(project),
                    "--runtime-config",
                    str(runtime_path),
                    "--sofr-source",
                    str(exact),
                    "--expected-sofr-source-sha256",
                    digest,
                    "--validate-only",
                ]
            )
            partial = normalize_sofr_frame(
                pd.DataFrame(
                    {
                        "observation_date": ["2018-04-03"],
                        "SOFR": ["1.83"],
                    }
                ),
                enforce_production_coverage=False,
            )
            with patch.object(
                loader_cli,
                "_stable_validate",
                return_value=partial,
            ) as stable:
                result = loader_cli.run(args)

            self.assertEqual(result[0]["row_count"], 1)
            self.assertEqual(stable.call_args.args[1]["sofr"], exact.resolve())

            args.expected_sofr_source_sha256 = "0" * 64
            with self.assertRaisesRegex(ValueError, "pinned SHA-256"):
                loader_cli.run(args)


if __name__ == "__main__":
    unittest.main()
