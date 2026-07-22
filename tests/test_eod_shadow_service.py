from __future__ import annotations

import hashlib
import importlib.util
import tempfile
import unittest
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

from vrp.eod_shadow.models import (
    ArtifactMetadata,
    EodSnapshot,
    ForecastVarianceRecord,
    GoldenVerificationEvidence,
    ImpliedVarianceRecord,
    MarketSnapshotRecord,
    SelectedSignalRecord,
    SignalEvaluationRecord,
    SignalFeatureRecord,
    TARGET_TENORS,
    VersionedDocument,
)
from vrp.eod_shadow import service
from vrp.eod_shadow.sofr_evidence import SofrUpdaterEvidence
from vrp.storage.eod_postgres import SofrReference, SpyDailyFeatureReference
from vrp.storage.postgres import InsertResult


CODE_VERSION = "a" * 40
ROOT = Path(__file__).resolve().parents[1]
CLI_SPEC = importlib.util.spec_from_file_location(
    "load_eod_snapshot_test_module", ROOT / "scripts/load_eod_snapshot.py"
)
loader_cli = importlib.util.module_from_spec(CLI_SPEC)
assert CLI_SPEC.loader is not None
CLI_SPEC.loader.exec_module(loader_cli)


class FakeCursorContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakeConnection:
    autocommit = False

    def __init__(self):
        self.info = SimpleNamespace(transaction_status=SimpleNamespace(name="IDLE"))
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursorContext()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


@dataclass
class FakeRunState:
    pipeline_run_id: UUID
    environment: str
    idempotency_key: str
    valuation_date: date
    snapshot_at: datetime
    data_cutoff_at: datetime
    model_version_id: UUID
    configuration_version_id: UUID
    code_version: str
    orchestrator_version: str
    requested_by: str
    invocation: dict
    metadata: dict
    inserted: bool
    status: str = "RUNNING"
    qa_status: str = "PENDING"

    @property
    def is_completed_pass(self):
        return self.status == "COMPLETED" and self.qa_status == "PASS"


class FakeRepository:
    def __init__(self):
        self.events: list[str] = []
        self.state = None
        self.assets = {}
        self.links = []
        self.stage = None
        self.qa = []
        self.projection_override = None
        self.sofr = SofrReference(
            UUID("00000000-0000-0000-0000-000000000101"),
            UUID("00000000-0000-0000-0000-000000000102"),
            date(2026, 7, 20),
            Decimal("0.0364"),
            "1" * 64,
            "8" * 64,
        )
        self.spy = SpyDailyFeatureReference(
            UUID("00000000-0000-0000-0000-000000000201"),
            UUID("00000000-0000-0000-0000-000000000202"),
            UUID("00000000-0000-0000-0000-000000000203"),
            date(2026, 7, 21),
            Decimal("100.00000000"),
            50.0,
            0.01,
            10.0,
            "AVAILABLE",
            "PASS",
            "2" * 64,
        )

    def acquire_logical_run_lock(self, **kwargs):
        self.events.append("lock")

    def resolve_sofr_before(self, valuation_date, **kwargs):
        self.events.append("sofr")
        self.sofr_digest = kwargs["normalized_content_sha256"]
        return self.sofr

    def resolve_spy_feature(self, **kwargs):
        self.events.append("spy")
        self.definition_digest = kwargs["definition_content_sha256"]
        return self.spy

    def find_run(self, **kwargs):
        return self.state

    def register_model_version(self, **kwargs):
        self.events.append("model")
        return kwargs["model_version_id"]

    def register_configuration_version(self, **kwargs):
        self.events.append("configuration")
        return kwargs["configuration_version_id"]

    def begin_run(self, **kwargs):
        self.events.append("begin")
        self.state = FakeRunState(inserted=True, **kwargs)
        return self.state

    def start_shadow_import_stage(self, **kwargs):
        self.events.append("stage_start")
        self.stage = {
            "stage_name": kwargs["stage_name"],
            "status": "RUNNING",
            "input_fingerprint": kwargs["input_fingerprint"],
            "output_fingerprint": None,
            "metrics": {},
        }

    def register_asset(self, asset):
        self.assets[asset.data_asset_id] = asset
        return InsertResult(record_id=asset.data_asset_id, inserted=True)

    def link_asset(self, **kwargs):
        self.links.append(kwargs)

    def insert_market_snapshot(self, row):
        self.events.append("market")
        self.market = row

    def insert_implied_variances(self, rows):
        self.implied = tuple(rows)

    def insert_forecast_variances(self, rows):
        self.forecast = tuple(rows)

    def insert_signal_features(self, rows):
        self.features = tuple(rows)

    def insert_signal_evaluations(self, rows):
        self.evaluations = tuple(rows)

    def insert_selected_signal(self, row):
        self.selected = row

    def record_qa(self, row):
        self.qa.append(row)

    def _asset_projection(self):
        rows = []
        for link in self.links:
            asset = self.assets[link["data_asset_id"]]
            rows.append(
                {
                    "data_asset_id": str(asset.data_asset_id),
                    "dataset_name": asset.dataset_name,
                    "asset_class": asset.asset_class,
                    "asset_format": asset.asset_format,
                    "content_sha256": asset.content_sha256,
                    "storage_uri": asset.storage_uri,
                    "schema_version": asset.schema_version,
                    "source_system": asset.source_system,
                    "observation_start_at": asset.observation_start_at,
                    "observation_end_at": asset.observation_end_at,
                    "trade_date_start": asset.trade_date_start,
                    "trade_date_end": asset.trade_date_end,
                    "row_count": asset.row_count,
                    "byte_size": asset.byte_size,
                    "is_immutable": True,
                    "metadata": asset.metadata,
                    "usage_role": link["usage_role"],
                    "logical_name": link["logical_name"],
                    "stage_name": link["stage_name"],
                    "is_required": True,
                    "lineage": link["lineage"],
                }
            )
        return sorted(rows, key=lambda row: (row["usage_role"], row["logical_name"], row["data_asset_id"]))

    def fetch_run_projection(self, pipeline_run_id):
        if self.projection_override is not None:
            return self.projection_override
        return {"assets": self._asset_projection()}

    def complete_shadow_import_stage(self, **kwargs):
        self.events.append("stage_complete")
        self.stage.update(
            status="COMPLETED",
            output_fingerprint=kwargs["output_fingerprint"],
            metrics=kwargs["metrics"],
        )

    def finalize_run(self, pipeline_run_id):
        self.events.append("finalize")
        self.state.status = "COMPLETED"
        self.state.qa_status = "PASS"
        self.state.inserted = False

    def fetch_reconciliation_evidence(self, pipeline_run_id):
        return {
            "stages": [dict(self.stage)],
            "signal_publication_count": 0,
            "qa_results": [
                {
                    "check_code": row.check_code,
                    "scope_key": row.scope_key,
                    "outcome": row.outcome,
                    "is_hard_gate": row.is_hard_gate,
                    "evidence": row.evidence,
                }
                for row in self.qa
            ],
        }


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def make_snapshot(root: Path) -> EodSnapshot:
    staged = root / "staging"
    staged.mkdir()
    signal_path = staged / "signal.parquet"
    fixture_path = root / "fixture.json"
    model_path = root / "model.json"
    config_path = root / "config.json"
    production_config_path = root / "production-config.json"
    sofr_manifest_path = root / "sofr-update-manifest.json"
    sofr_snapshot_path = root / "sofr-refreshed-snapshot.csv"
    for path, content in (
        (signal_path, b"staged-signal"),
        (fixture_path, b'{"captured_at_utc":"2026-07-22T00:00:00+00:00"}'),
        (model_path, b"{}"),
        (config_path, b"{}"),
        (production_config_path, b'{"lock_id":"lock-v1"}'),
        (sofr_manifest_path, b"{}"),
        (sofr_snapshot_path, b"observation_date,SOFR\n2026-07-20,3.64\n"),
    ):
        path.write_bytes(content)
    valuation = date(2026, 7, 21)
    snapshot_at = datetime(2026, 7, 21, 20, tzinfo=timezone.utc)
    implied = tuple(
        ImpliedVarianceRecord(
            tenor,
            valuation,
            float(tenor),
            0.04 + tenor / 10000,
            20.0,
        )
        for tenor in TARGET_TENORS
    )
    forecast = tuple(
        ForecastVarianceRecord(tenor, valuation, -3.2, 0.04, 20.0)
        for tenor in TARGET_TENORS
    )
    features = tuple(
        SignalFeatureRecord(
            tenor_days=tenor,
            tenor_bucket="FRONT" if tenor <= 18 else "MIDDLE" if tenor <= 24 else "BACK",
            vrp_log=0.1,
            vrp_3m_prior_mean=0.0,
            vrp_3m_prior_sample_std=1.0,
            vrp_1y_prior_mean=0.0,
            vrp_1y_prior_sample_std=1.0,
            zscore_3m=0.1,
            zscore_1y=0.1,
            rsi14=50.0,
            rv21d_variance=0.01,
            rv21d_volatility_pct=10.0,
            zscore_3m_sample_count=63,
            zscore_1y_sample_count=252,
            history_through_date=date(2026, 7, 20),
        )
        for tenor in TARGET_TENORS
    )
    evaluations = tuple(
        SignalEvaluationRecord(
            evaluation_key=f"{tenor}:{layer}",
            tenor_days=tenor,
            tenor_bucket="FRONT" if tenor <= 18 else "MIDDLE" if tenor <= 24 else "BACK",
            signal_layer=layer,
            evaluation_status="INACTIVE",
            qualifies=False,
            vrp_pass=None,
            zscore_3m_pass=None,
            zscore_1y_pass=None,
            rsi14_pass=None,
            rv21d_pass=None,
            threshold_values={},
            comparison_results={},
            failed_checks=("RULE_INACTIVE",),
            rank_position=None,
            rank_score=None,
            target_size_pct_nav=None,
        )
        for tenor in TARGET_TENORS
        for layer in ("CORE", "SECONDARY")
    )
    return EodSnapshot(
        run_dir=root,
        valuation_date=valuation,
        lock_id="lock-v1",
        approved_nav=1_000_000.0,
        run_manifest={
            "status": "PASS",
            "finished_at": "2026-07-21T21:00:00+00:00",
        },
        publish_manifest={"lock_config": str(production_config_path)},
        model_identity=VersionedDocument(
            "model", "lock-v1", model_path, _sha(model_path), {"lock_date": "2026-07-11"}
        ),
        configuration_identity=VersionedDocument(
            "config",
            "lock-v1:content",
            config_path,
            _sha(config_path),
            {
                "value": 1,
                "runtime_configuration_sha256": _sha(config_path),
                "production_configuration_sha256": _sha(production_config_path),
            },
        ),
        sofr_evidence=SofrUpdaterEvidence(
            updater_manifest_path=sofr_manifest_path,
            updater_manifest_sha256=_sha(sofr_manifest_path),
            refreshed_snapshot_path=sofr_snapshot_path,
            refreshed_snapshot_sha256=_sha(sofr_snapshot_path),
            normalized_content_sha256="8" * 64,
            start_date=date(2018, 4, 3),
            end_date=date(2026, 7, 20),
            row_count=2071,
            observation_date=date(2026, 7, 20),
            rate_decimal=Decimal("0.0364"),
            row_sha256="1" * 64,
        ),
        market_snapshot=MarketSnapshotRecord(
            valuation,
            snapshot_at,
            snapshot_at,
            "EOD_OFFICIAL",
            "CLOSED",
            "FRESH",
            100.0,
            {"calendar": "XNYS"},
        ),
        implied_variance=implied,
        forecast_variance=forecast,
        signal_features=features,
        signal_evaluations=evaluations,
        selected_signal=SelectedSignalRecord(
            "NO_TRADE",
            "EOD_OFFICIAL",
            "rule-v1",
            None,
            "No Core or Secondary signal qualified.",
            1_000_000.0,
            None,
            {"qualified_candidates": []},
        ),
        artifacts=(
            ArtifactMetadata(
                "signal_history",
                signal_path,
                "PARQUET",
                _sha(signal_path),
                signal_path.stat().st_size,
                9,
                "staging/signal.parquet",
                True,
                trade_date_start=date(2025, 1, 2),
                trade_date_end=valuation,
            ),
        ),
        golden_evidence=GoldenVerificationEvidence(
            "PASS",
            "3" * 64,
            fixture_path,
            _sha(fixture_path),
            _sha(signal_path),
            "4" * 64,
            {},
        ),
        output_fingerprint="5" * 64,
    )


class EodShadowServiceTests(unittest.TestCase):
    def test_preflight_integrity_guard_covers_all_file_backed_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for label, path_for_snapshot in (
                ("snapshot artifact signal_history", lambda item: item.artifacts[0].path),
                ("model-lock document", lambda item: item.model_identity.path),
                ("golden EOD fixture", lambda item: item.golden_evidence.fixture_path),
                ("runtime configuration", lambda item: item.configuration_identity.path),
                (
                    "production configuration",
                    lambda item: Path(item.publish_manifest["lock_config"]),
                ),
            ):
                with self.subTest(label=label):
                    case_root = root / label.replace(" ", "-")
                    case_root.mkdir()
                    snapshot = make_snapshot(case_root)
                    path = path_for_snapshot(snapshot)
                    original = path.read_bytes()
                    path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
                    connection = FakeConnection()
                    repository_factory_called = False

                    def repository_factory(cursor):
                        nonlocal repository_factory_called
                        repository_factory_called = True
                        return FakeRepository()

                    with self.assertRaisesRegex(RuntimeError, label):
                        service.execute_eod_shadow_load(
                            connection,
                            snapshot,
                            environment="test",
                            code_version=CODE_VERSION,
                            requested_by="unit-test",
                            repository_factory=repository_factory,
                        )
                    self.assertFalse(repository_factory_called)
                    self.assertEqual(connection.commits, 0)
                    self.assertEqual(connection.rollbacks, 0)

    def test_late_artifact_mutation_rolls_back_instead_of_committing(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            signal_path = snapshot.artifacts[0].path

            class LateMutatingRepository(FakeRepository):
                def fetch_reconciliation_evidence(self, pipeline_run_id):
                    evidence = super().fetch_reconciliation_evidence(pipeline_run_id)
                    signal_path.write_bytes(b"late-mutated-signal")
                    return evidence

            repository = LateMutatingRepository()
            connection = FakeConnection()
            with patch.object(service, "_assert_projection"):
                with self.assertRaisesRegex(
                    RuntimeError, "snapshot artifact signal_history"
                ):
                    service.execute_eod_shadow_load(
                        connection,
                        snapshot,
                        environment="test",
                        code_version=CODE_VERSION,
                        requested_by="unit-test",
                        repository_factory=lambda cursor: repository,
                    )
            self.assertIn("finalize", repository.events)
            self.assertEqual(connection.commits, 0)
            self.assertEqual(connection.rollbacks, 1)

    def test_asset_coverage_and_noop_evidence_cover_every_semantic_field(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            inputs = service._asset_inputs(snapshot)
            signal_input = next(
                item for item in inputs if item.logical_name == "signal_history"
            )
            signal_record = service._asset_record(signal_input, snapshot)
            self.assertEqual(signal_record.trade_date_start, date(2025, 1, 2))
            self.assertEqual(signal_record.trade_date_end, snapshot.valuation_date)

            projections = [
                service._asset_projection(
                    record=service._asset_record(item, snapshot),
                    usage_role=item.usage_role,
                    logical_name=item.logical_name,
                    lineage=service._asset_lineage(item),
                )
                for item in inputs
            ]
            expected = service._expected_asset_evidence(inputs, snapshot)
            self.assertEqual(
                expected,
                service._asset_evidence_contract(projections),
            )
            signal_evidence = next(
                item for item in expected if item["logical_name"] == "signal_history"
            )
            self.assertEqual(
                set(signal_evidence), set(service._ASSET_EVIDENCE_FIELDS)
            )
            self.assertEqual(
                signal_evidence["metadata"]["relative_path"],
                "staging/signal.parquet",
            )
            self.assertEqual(
                signal_evidence["lineage"]["relative_path"],
                "staging/signal.parquet",
            )

            with_capture_timestamp = deepcopy(projections)
            for item in with_capture_timestamp:
                item["captured_at"] = "2099-01-01T00:00:00+00:00"
            self.assertEqual(
                expected,
                service._asset_evidence_contract(with_capture_timestamp),
            )

            for field in service._ASSET_EVIDENCE_FIELDS:
                changed = deepcopy(projections)
                signal = next(
                    item for item in changed if item["logical_name"] == "signal_history"
                )
                if field == "lineage":
                    signal[field]["relative_path"] = "changed/signal.parquet"
                elif field == "metadata":
                    signal[field]["relative_path"] = "changed/signal.parquet"
                elif isinstance(signal[field], bool):
                    signal[field] = not signal[field]
                elif isinstance(signal[field], int):
                    signal[field] += 1
                elif signal[field] is None:
                    signal[field] = "changed"
                else:
                    signal[field] = f"{signal[field]}-changed"
                observed = service._asset_evidence_contract(changed)
                self.assertTrue(
                    service.projection_mismatches(expected, observed),
                    field,
                )

    def test_cli_validate_only_never_opens_postgresql(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            snapshot = make_snapshot(root)
            args = loader_cli.parse_args(
                [
                    "--project-root",
                    str(root),
                    "--run-dir",
                    str(root),
                    "--validate-only",
                ]
            )
            with (
                patch.object(loader_cli, "load_staged_eod_snapshot", return_value=snapshot),
                patch.object(
                    loader_cli,
                    "connect_from_environment",
                    side_effect=AssertionError("database must not be opened"),
                ),
            ):
                output = loader_cli.run(args)
            self.assertEqual(output["status"], "VALID")
            self.assertEqual(output["signal_evaluation_count"], 18)
            self.assertEqual(output["sofr_normalized_content_sha256"], "8" * 64)
            self.assertEqual(output["sofr_observation_date"], "2026-07-20")
            self.assertEqual(output["sofr_observation_rate_decimal"], "0.0364")

    def test_cli_requires_caller_provided_load_identity_before_connecting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            snapshot = make_snapshot(root)
            args = loader_cli.parse_args(
                ["--project-root", str(root), "--run-dir", str(root)]
            )
            with (
                patch.object(loader_cli, "load_staged_eod_snapshot", return_value=snapshot),
                patch.object(loader_cli, "connect_from_environment") as connect,
            ):
                with self.assertRaisesRegex(ValueError, "environment"):
                    loader_cli.run(args)
            connect.assert_not_called()

    def test_atomic_load_then_retry_is_a_revalidated_no_op(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            repository = FakeRepository()
            connection = FakeConnection()
            with patch.object(service, "_assert_projection"):
                first = service.execute_eod_shadow_load(
                    connection,
                    snapshot,
                    environment="test",
                    code_version=CODE_VERSION,
                    requested_by="unit-test",
                    repository_factory=lambda cursor: repository,
                )
                second = service.execute_eod_shadow_load(
                    connection,
                    snapshot,
                    environment="test",
                    code_version=CODE_VERSION,
                    requested_by="unit-test",
                    repository_factory=lambda cursor: repository,
                )
            self.assertFalse(first.no_op)
            self.assertTrue(second.no_op)
            self.assertEqual(first.pipeline_run_id, second.pipeline_run_id)
            self.assertEqual(connection.commits, 2)
            self.assertEqual(connection.rollbacks, 0)
            self.assertEqual(repository.events.count("begin"), 1)
            self.assertEqual(repository.events.count("finalize"), 1)
            self.assertEqual(len(repository.evaluations), 18)
            self.assertEqual(
                repository.definition_digest,
                service.SPY_DAILY_DEFINITION_CONTRACT["content_sha256"],
            )

    def test_readback_mismatch_rolls_back_without_finalizing(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            repository = FakeRepository()
            connection = FakeConnection()
            repository.projection_override = {"assets": [], "corrupt": True}
            with self.assertRaisesRegex(RuntimeError, "did not reconcile"):
                service.execute_eod_shadow_load(
                    connection,
                    snapshot,
                    environment="test",
                    code_version=CODE_VERSION,
                    requested_by="unit-test",
                    repository_factory=lambda cursor: repository,
                )
            self.assertEqual(connection.commits, 0)
            self.assertEqual(connection.rollbacks, 1)
            self.assertNotIn("finalize", repository.events)

    def test_reference_mismatch_rolls_back_before_run_write(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            repository = FakeRepository()
            repository.spy = dataclass_replace(repository.spy, spy_close=Decimal("101"))
            connection = FakeConnection()
            with self.assertRaisesRegex(RuntimeError, "SPY close"):
                service.execute_eod_shadow_load(
                    connection,
                    snapshot,
                    environment="test",
                    code_version=CODE_VERSION,
                    requested_by="unit-test",
                    repository_factory=lambda cursor: repository,
                )
            self.assertEqual(connection.rollbacks, 1)
            self.assertNotIn("begin", repository.events)

    def test_projection_comparison_is_structural_and_tolerance_aware(self):
        self.assertEqual(
            service.projection_mismatches(
                {"rows": [{"value": 1.0}]},
                {"rows": [{"value": 1.0 + 1e-13}]},
            ),
            [],
        )

    def test_reference_revision_changes_the_logical_run_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = make_snapshot(Path(directory).resolve())
            repository = FakeRepository()
            first_digest, first_key = service._effective_input(
                snapshot,
                environment="test",
                code_version=CODE_VERSION,
                sofr=repository.sofr,
                spy=repository.spy,
            )
            revised_spy = dataclass_replace(
                repository.spy,
                daily_market_feature_id=UUID(
                    "00000000-0000-0000-0000-000000000299"
                ),
                row_sha256="9" * 64,
            )
            second_digest, second_key = service._effective_input(
                snapshot,
                environment="test",
                code_version=CODE_VERSION,
                sofr=repository.sofr,
                spy=revised_spy,
            )
            self.assertNotEqual(first_digest, second_digest)
            self.assertNotEqual(first_key, second_key)
        self.assertTrue(
            service.projection_mismatches(
                {"rows": [{"value": 1.0}]},
                {"rows": [{"other": 1.0}]},
            )
        )
        self.assertTrue(service.projection_mismatches({"pass": True}, {"pass": 1}))

    def test_code_version_requires_full_lowercase_git_sha(self):
        for value in ("abc", "A" * 40, "g" * 40):
            with self.assertRaisesRegex(ValueError, "40-character lowercase"):
                service.validate_code_version(value)
        self.assertEqual(service.validate_code_version(CODE_VERSION), CODE_VERSION)


def dataclass_replace(value, **changes):
    values = dict(value.__dict__)
    values.update(changes)
    return type(value)(**values)


if __name__ == "__main__":
    unittest.main()
