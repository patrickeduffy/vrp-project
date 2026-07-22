from __future__ import annotations

import sys
import unittest
from collections import deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vrp.storage.eod_postgres import (
    EXPECTED_EOD_TENORS,
    EodDataAsset,
    EodRepositoryConflict,
    EodRunConflict,
    ForecastVarianceRow,
    ImpliedVarianceRow,
    MarketSnapshotRow,
    PostgresEodRepository,
    QaResultRow,
    SelectedSignalRow,
    SignalEvaluationRow,
    SignalFeatureRow,
)
from vrp.storage.postgres import DatabaseConfigurationError


NOW = datetime(2026, 7, 21, 21, 0, tzinfo=timezone.utc)
VALUATION_DATE = date(2026, 7, 21)


class RecordingConnection:
    def __init__(self, *, autocommit: bool = False):
        self.autocommit = autocommit
        self.commit_count = 0
        self.rollback_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class RecordingCursor:
    def __init__(self, responses=(), *, autocommit: bool = False):
        self.responses = deque(responses)
        self.calls = []
        self.current = None
        self.connection = RecordingConnection(autocommit=autocommit)

    def execute(self, query, params=None):
        self.calls.append((" ".join(query.split()), params))
        self.current = self.responses.popleft() if self.responses else None
        return self

    def fetchone(self):
        result = self.current
        self.current = None
        return result


def run_row(
    *,
    pipeline_run_id=None,
    status="COMPLETED",
    qa_status="PASS",
    invocation=None,
    metadata=None,
):
    return (
        pipeline_run_id or uuid4(),
        "local",
        "eod/2026-07-21/source-digest",
        VALUATION_DATE,
        NOW,
        NOW,
        uuid4(),
        uuid4(),
        "commit-1",
        "shadow-v1",
        status,
        qa_status,
        invocation or {"mode": "shadow"},
        metadata or {"publication": False},
    )


def implied_rows(run_id, snapshot_id):
    return tuple(
        ImpliedVarianceRow(
            implied_variance_id=uuid4(),
            pipeline_run_id=run_id,
            market_snapshot_id=snapshot_id,
            tenor_days=tenor,
            target_expiration=date(2026, 8, 21),
            effective_dte=float(tenor),
            annualized_variance=0.02 + tenor / 10000,
            annualized_volatility_pct=15.0,
            calculation_status="AVAILABLE",
            quality_status="PASS",
            source_quote_at=NOW,
            quality_details={"tenor": tenor},
        )
        for tenor in EXPECTED_EOD_TENORS
    )


def forecast_rows(run_id, snapshot_id):
    return tuple(
        ForecastVarianceRow(
            forecast_variance_id=uuid4(),
            pipeline_run_id=run_id,
            market_snapshot_id=snapshot_id,
            tenor_days=tenor,
            forecast_as_of_date=VALUATION_DATE,
            predicted_log_variance=-4.0,
            annualized_variance=0.018,
            annualized_volatility_pct=13.4,
            calculation_status="AVAILABLE",
            quality_status="PASS",
            quality_details={"tenor": tenor},
        )
        for tenor in EXPECTED_EOD_TENORS
    )


def feature_rows(run_id, snapshot_id, daily_id, implied, forecast):
    return tuple(
        SignalFeatureRow(
            signal_feature_id=uuid4(),
            pipeline_run_id=run_id,
            market_snapshot_id=snapshot_id,
            tenor_days=tenor,
            tenor_bucket="FRONT" if tenor <= 18 else "MIDDLE" if tenor <= 24 else "BACK",
            implied_variance_id=implied[index].implied_variance_id,
            forecast_variance_id=forecast[index].forecast_variance_id,
            vrp_log=0.4,
            vrp_3m_prior_mean=0.3,
            vrp_3m_prior_sample_std=0.1,
            vrp_1y_prior_mean=0.25,
            vrp_1y_prior_sample_std=0.2,
            zscore_3m=1.0,
            zscore_1y=0.75,
            rsi14=52.4,
            rv21d_variance=0.015,
            rv21d_volatility_pct=12.2,
            zscore_3m_sample_count=63,
            zscore_1y_sample_count=252,
            history_through_date=date(2026, 7, 20),
            is_complete=True,
            daily_market_feature_id=daily_id,
            details={"tenor": tenor},
        )
        for index, tenor in enumerate(EXPECTED_EOD_TENORS)
    )


class EodPostgresRepositoryTests(unittest.TestCase):
    def test_logical_run_lock_is_bound_and_requires_outer_transaction(self):
        cursor = RecordingCursor()
        PostgresEodRepository(cursor).acquire_logical_run_lock(
            environment=" local ", idempotency_key=" run-identity "
        )
        sql, params = cursor.calls[0]
        self.assertIn("pg_advisory_xact_lock", sql)
        self.assertNotIn("run-identity", sql)
        self.assertEqual(params, ("vrp/eod/local/run-identity",))
        self.assertEqual(cursor.connection.commit_count, 0)
        self.assertEqual(cursor.connection.rollback_count, 0)

        with self.assertRaisesRegex(DatabaseConfigurationError, "non-autocommit"):
            PostgresEodRepository(
                RecordingCursor(autocommit=True)
            ).acquire_logical_run_lock(environment="local", idempotency_key="run")

    def test_model_registration_is_idempotent_and_detects_content_conflict(self):
        model_id = uuid4()
        manifest = {"model": "locked"}
        cursor = RecordingCursor(
            responses=[None, (model_id, "a" * 64, manifest, True, NOW)]
        )
        observed_id = PostgresEodRepository(cursor).register_model_version(
            model_version_id=uuid4(),
            model_key="unified_fds_no_min_return",
            version_label="locked-v1",
            content_sha256="a" * 64,
            manifest=manifest,
            locked_at=NOW,
        )
        self.assertEqual(observed_id, model_id)
        self.assertIn("ON CONFLICT DO NOTHING", cursor.calls[0][0])
        self.assertNotIn("locked-v1", cursor.calls[0][0])
        self.assertIn("locked-v1", cursor.calls[0][1])

        conflict = RecordingCursor(
            responses=[None, (model_id, "b" * 64, manifest, True, NOW)]
        )
        with self.assertRaises(EodRepositoryConflict):
            PostgresEodRepository(conflict).register_model_version(
                model_version_id=uuid4(),
                model_key="unified_fds_no_min_return",
                version_label="locked-v1",
                content_sha256="a" * 64,
                manifest=manifest,
                locked_at=NOW,
            )

    def test_configuration_registration_checks_full_json_contract(self):
        configuration_id = uuid4()
        cursor = RecordingCursor(
            responses=[None, (configuration_id, "c" * 64, {"threshold": 1})]
        )
        result = PostgresEodRepository(cursor).register_configuration_version(
            configuration_version_id=uuid4(),
            configuration_key="put_signal",
            version_label="locked-v1",
            content_sha256="c" * 64,
            configuration={"threshold": 1},
        )
        self.assertEqual(result, configuration_id)

        conflict = RecordingCursor(
            responses=[None, (configuration_id, "c" * 64, {"threshold": 2})]
        )
        with self.assertRaises(EodRepositoryConflict):
            PostgresEodRepository(conflict).register_configuration_version(
                configuration_version_id=uuid4(),
                configuration_key="put_signal",
                version_label="locked-v1",
                content_sha256="c" * 64,
                configuration={"threshold": 1},
            )

    def test_completed_matching_run_is_an_idempotent_noop(self):
        existing = run_row()
        cursor = RecordingCursor(responses=[None, existing])
        state = PostgresEodRepository(cursor).begin_run(
            pipeline_run_id=uuid4(),
            environment=existing[1],
            idempotency_key=existing[2],
            valuation_date=existing[3],
            snapshot_at=existing[4],
            data_cutoff_at=existing[5],
            model_version_id=existing[6],
            configuration_version_id=existing[7],
            code_version=existing[8],
            orchestrator_version=existing[9],
            requested_by="test",
            invocation=existing[12],
            metadata=existing[13],
        )
        self.assertFalse(state.inserted)
        self.assertTrue(state.is_completed_pass)
        self.assertEqual(state.pipeline_run_id, existing[0])

    def test_run_idempotency_key_rejects_changed_contract_or_active_run(self):
        changed = run_row()
        cursor = RecordingCursor(responses=[None, changed])
        with self.assertRaisesRegex(EodRunConflict, "immutable contract"):
            PostgresEodRepository(cursor).begin_run(
                pipeline_run_id=uuid4(),
                environment=changed[1],
                idempotency_key=changed[2],
                valuation_date=changed[3],
                snapshot_at=changed[4],
                data_cutoff_at=changed[5],
                model_version_id=changed[6],
                configuration_version_id=changed[7],
                code_version="different-commit",
                orchestrator_version=changed[9],
                requested_by=None,
                invocation=changed[12],
                metadata=changed[13],
            )

        running = run_row(status="RUNNING", qa_status="PENDING")
        cursor = RecordingCursor(responses=[None, running])
        with self.assertRaisesRegex(EodRunConflict, "explicit recovery"):
            PostgresEodRepository(cursor).begin_run(
                pipeline_run_id=uuid4(),
                environment=running[1],
                idempotency_key=running[2],
                valuation_date=running[3],
                snapshot_at=running[4],
                data_cutoff_at=running[5],
                model_version_id=running[6],
                configuration_version_id=running[7],
                code_version=running[8],
                orchestrator_version=running[9],
                requested_by=None,
                invocation=running[12],
                metadata=running[13],
            )

    def test_asset_retry_is_noop_only_for_matching_immutable_contract(self):
        asset_id = uuid4()
        asset = EodDataAsset(
            data_asset_id=uuid4(),
            dataset_name="eod_signal_history",
            asset_class="STANDARDIZED",
            asset_format="PARQUET",
            storage_uri="file:///content/" + "d" * 64 + ".parquet",
            content_sha256="d" * 64,
            captured_at=NOW,
            schema_version="v1",
            source_system="locked-eod",
            trade_date_start=VALUATION_DATE,
            trade_date_end=VALUATION_DATE,
            row_count=9,
            byte_size=1234,
            metadata={"role": "input"},
        )
        existing = (
            asset_id,
            asset.asset_class,
            asset.asset_format,
            asset.schema_version,
            asset.source_system,
            None,
            None,
            asset.trade_date_start,
            asset.trade_date_end,
            asset.row_count,
            asset.byte_size,
            True,
            asset.metadata,
        )
        cursor = RecordingCursor(responses=[None, existing])
        result = PostgresEodRepository(cursor).register_asset(asset)
        self.assertFalse(result.inserted)
        self.assertEqual(result.record_id, asset_id)
        self.assertNotIn("captured_at", cursor.calls[1][0])

        touched = EodDataAsset(
            **{
                **asset.__dict__,
                "captured_at": asset.captured_at + timedelta(hours=1),
            }
        )
        cursor = RecordingCursor(responses=[None, existing])
        touched_result = PostgresEodRepository(cursor).register_asset(touched)
        self.assertFalse(touched_result.inserted)
        self.assertEqual(touched_result.record_id, asset_id)

        mismatched = (*existing[:-3], 999, True, existing[-1])
        cursor = RecordingCursor(responses=[None, mismatched])
        with self.assertRaises(EodRepositoryConflict):
            PostgresEodRepository(cursor).register_asset(asset)

    def test_reference_reads_pin_strict_dates_and_definition_digest(self):
        observation_id = uuid4()
        release_id = uuid4()
        feature_id = uuid4()
        definition_id = uuid4()
        cursor = RecordingCursor(
            responses=[
                (
                    observation_id,
                    release_id,
                    date(2026, 7, 20),
                    Decimal("0.0357"),
                    "e" * 64,
                    "b" * 64,
                ),
                (
                    feature_id,
                    definition_id,
                    release_id,
                    VALUATION_DATE,
                    Decimal("748.32"),
                    52.4,
                    0.015,
                    12.2,
                    "AVAILABLE",
                    "PASS",
                    "f" * 64,
                ),
            ]
        )
        repository = PostgresEodRepository(cursor)
        sofr = repository.resolve_sofr_before(
            VALUATION_DATE,
            normalized_content_sha256="b" * 64,
        )
        spy = repository.resolve_spy_feature(
            valuation_date=VALUATION_DATE,
            definition_key="SPY_SIGNAL_FEATURES",
            version_label="v1-71854988797d",
            definition_content_sha256="a" * 64,
        )
        self.assertEqual(sofr.observation_date, date(2026, 7, 20))
        self.assertEqual(sofr.normalized_content_sha256, "b" * 64)
        self.assertEqual(spy.daily_market_feature_id, feature_id)
        self.assertIn("observation.observation_date < %s", cursor.calls[0][0])
        self.assertEqual(cursor.calls[0][1], ("SOFR", VALUATION_DATE, "b" * 64))
        self.assertNotIn("a" * 64, cursor.calls[1][0])
        self.assertEqual(cursor.calls[1][1][2:4], ("SPY_SIGNAL_FEATURES", "v1-71854988797d"))
        self.assertEqual(cursor.calls[1][1][-1], "a" * 64)

    def test_nine_tenor_writes_are_parameterized_and_never_publish(self):
        run_id = uuid4()
        snapshot_id = uuid4()
        daily_id = uuid4()
        cursor = RecordingCursor()
        repository = PostgresEodRepository(cursor)
        implied = implied_rows(run_id, snapshot_id)
        forecast = forecast_rows(run_id, snapshot_id)
        features = feature_rows(run_id, snapshot_id, daily_id, implied, forecast)

        repository.insert_market_snapshot(
            MarketSnapshotRow(
                market_snapshot_id=snapshot_id,
                pipeline_run_id=run_id,
                valuation_date=VALUATION_DATE,
                snapshot_at=NOW,
                source_latest_at=NOW,
                freshness_status="FRESH",
                spx_spot=6300.25,
                spy_price=748.32,
                sofr_rate=Decimal("0.0357"),
                sofr_observation_date=date(2026, 7, 20),
                sofr_observation_id=uuid4(),
                daily_market_feature_id=daily_id,
                details={"source": "shadow"},
            )
        )
        repository.insert_implied_variances(implied)
        repository.insert_forecast_variances(forecast)
        repository.insert_signal_features(features)
        evaluations = tuple(
            SignalEvaluationRow(
                signal_evaluation_id=uuid4(),
                pipeline_run_id=run_id,
                market_snapshot_id=snapshot_id,
                signal_feature_id=features[index].signal_feature_id,
                tenor_days=tenor,
                tenor_bucket=features[index].tenor_bucket,
                signal_layer=layer,
                evaluation_status="NOT_QUALIFIED",
                qualifies=False,
                vrp_pass=False,
                zscore_3m_pass=False,
                zscore_1y_pass=False,
                rsi14_pass=True,
                rv21d_pass=True,
                threshold_values={"vrp": 0.5},
                comparison_results={"vrp": False},
                failed_checks=("vrp",),
                rank_position=None,
                rank_score=None,
                target_size_pct_nav=0.02,
            )
            for index, tenor in enumerate(EXPECTED_EOD_TENORS)
            for layer in ("CORE", "SECONDARY")
        )
        repository.insert_signal_evaluations(evaluations)
        repository.insert_selected_signal(
            SelectedSignalRow(
                selected_signal_id=uuid4(),
                pipeline_run_id=run_id,
                market_snapshot_id=snapshot_id,
                selected_evaluation_id=None,
                decision="NO_TRADE",
                signal_state="EOD_OFFICIAL",
                selection_rule_id="locked-selection",
                no_trade_reason="no qualifying tenor",
                approved_nav_dollars=1_000_000,
                target_max_risk_dollars=None,
                first_observed_at=None,
                consecutive_snapshots=None,
                selection_trace={"selected": None},
            )
        )
        repository.record_qa(
            QaResultRow(
                qa_result_id=uuid4(),
                pipeline_run_id=run_id,
                stage_name="EOD_SHADOW_IMPORT",
                check_code="readback_reconciliation",
                scope_key="run",
                severity="ERROR",
                outcome="PASS",
                is_hard_gate=True,
                message="Projection matches source snapshot",
                observed_value={"sha256": "1" * 64},
                expected_value={"sha256": "1" * 64},
            )
        )

        sql_text = " ".join(sql for sql, _ in cursor.calls).lower()
        self.assertNotIn("insert into vrp.signal_publications", sql_text)
        self.assertNotIn("6300.25", sql_text)
        self.assertNotIn("no qualifying tenor", sql_text)
        self.assertEqual(sql_text.count("insert into vrp.implied_variance_term_structure"), 9)
        self.assertEqual(sql_text.count("insert into vrp.forecast_variance_term_structure"), 9)
        self.assertEqual(sql_text.count("insert into vrp.signal_features"), 9)
        self.assertEqual(sql_text.count("insert into vrp.signal_evaluations"), 18)
        self.assertEqual(cursor.connection.commit_count, 0)
        self.assertEqual(cursor.connection.rollback_count, 0)

    def test_tenor_batch_rejects_missing_or_duplicate_rows_before_sql(self):
        cursor = RecordingCursor()
        rows = list(implied_rows(uuid4(), uuid4()))
        with self.assertRaisesRegex(ValueError, "exactly one row"):
            PostgresEodRepository(cursor).insert_implied_variances(rows[:-1])
        self.assertEqual(cursor.calls, [])

        rows[-1] = rows[0]
        with self.assertRaises(ValueError):
            PostgresEodRepository(cursor).insert_implied_variances(rows)
        self.assertEqual(cursor.calls, [])

    def test_projection_and_finalize_are_bound_and_transaction_neutral(self):
        run_id = uuid4()
        projection = {"run": {"pipeline_run_id": str(run_id)}, "signal_features": []}
        cursor = RecordingCursor(responses=[(projection,), (run_id,)])
        repository = PostgresEodRepository(cursor)
        self.assertEqual(repository.fetch_run_projection(run_id), projection)
        repository.finalize_run(run_id)
        self.assertEqual(cursor.calls[0][1], (run_id,))
        self.assertEqual(cursor.calls[1][1], (run_id,))
        self.assertIn("COUNT(*)", cursor.calls[1][0])
        self.assertNotIn(str(run_id), cursor.calls[0][0])
        self.assertEqual(cursor.connection.commit_count, 0)
        self.assertEqual(cursor.connection.rollback_count, 0)

    def test_completed_reconciliation_evidence_is_stable_and_bound(self):
        run_id = uuid4()
        evidence = {
            "stages": [
                {
                    "stage_name": "EOD_SHADOW_IMPORT",
                    "status": "COMPLETED",
                    "metrics": {"signal_feature_count": 9},
                }
            ],
            "qa_results": [
                {
                    "check_code": "postgres_projection_reconciliation",
                    "scope_key": "run",
                    "outcome": "PASS",
                    "is_hard_gate": True,
                }
            ],
            "signal_publication_count": 0,
        }
        cursor = RecordingCursor(responses=[(evidence,)])
        observed = PostgresEodRepository(cursor).fetch_reconciliation_evidence(run_id)
        self.assertEqual(observed, evidence)
        sql, params = cursor.calls[0]
        self.assertEqual(params, (run_id,))
        self.assertIn("run.status = 'COMPLETED'", sql)
        self.assertIn("run.qa_status = 'PASS'", sql)
        self.assertNotIn("started_at", sql)
        self.assertNotIn("finished_at", sql)
        self.assertNotIn("checked_at", sql)
        self.assertIn("vrp.signal_publications", sql)

    def test_repository_source_has_no_transaction_or_publication_ownership(self):
        source = (ROOT / "src" / "vrp" / "storage" / "eod_postgres.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn(".commit(", source)
        self.assertNotIn(".rollback(", source)
        self.assertNotIn("INSERT INTO vrp.signal_publications", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
