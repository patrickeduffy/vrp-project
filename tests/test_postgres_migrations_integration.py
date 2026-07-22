from __future__ import annotations

import os
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
TEST_DSN = os.environ.get("VRP_TEST_DATABASE_URL")

if TEST_DSN:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - this must fail, not skip, in CI
        raise RuntimeError(
            "VRP_TEST_DATABASE_URL is configured but Psycopg cannot be imported"
        ) from exc
else:  # pragma: no cover - ordinary local run without PostgreSQL
    psycopg = None

from vrp.storage.postgres import PostgresReferenceDataRepository
from vrp.storage.reference_data import (
    DailyMarketFeature,
    DailyMarketFeatureDefinition,
    ReferenceDataRelease,
    ReferenceRateObservation,
    StoredDailyMarketFeature,
    StoredReferenceRateObservation,
    daily_market_feature_definition_sha256,
)


@unittest.skipUnless(TEST_DSN, "VRP test PostgreSQL is not configured")
class PostgresMigrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = psycopg.connect(TEST_DSN, autocommit=True)
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute("SELECT to_regnamespace('vrp')")
                if cursor.fetchone()[0] is not None:
                    raise RuntimeError(
                        "VRP_TEST_DATABASE_URL must point to a fresh disposable test database"
                    )
                for migration in (
                    ROOT / "migrations/0001_operational_schema.sql",
                    ROOT / "migrations/0002_reference_data.sql",
                ):
                    cursor.execute(migration.read_text(encoding="utf-8"), prepare=False)
            cls.repository_connection = psycopg.connect(TEST_DSN)
        except Exception:
            cls.connection.close()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.repository_connection.rollback()
        cls.repository_connection.close()
        cls.connection.close()

    def _insert_asset(self, dataset_name: str, digest_character: str):
        asset_id = uuid4()
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO vrp.data_assets (
                    data_asset_id, dataset_name, asset_class, asset_format,
                    storage_uri, content_sha256, captured_at
                ) VALUES (%s, %s, 'STANDARDIZED', 'PARQUET', %s, %s, clock_timestamp())
                """,
                (
                    asset_id,
                    dataset_name,
                    f"test://{dataset_name}/{asset_id}",
                    digest_character * 64,
                ),
            )
        return asset_id

    def _insert_pipeline_run(self, valuation_date: date):
        model_id = uuid4()
        configuration_id = uuid4()
        run_id = uuid4()
        snapshot_at = datetime.combine(
            valuation_date,
            datetime.min.time().replace(hour=20),
            tzinfo=timezone.utc,
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO vrp.model_versions (
                    model_version_id, model_key, version_label, content_sha256,
                    is_locked, locked_at
                ) VALUES (%s, %s, %s, %s, TRUE, clock_timestamp())
                """,
                (model_id, f"test-model-{model_id}", "v1", uuid4().hex * 2),
            )
            cursor.execute(
                """
                INSERT INTO vrp.configuration_versions (
                    configuration_version_id, configuration_key, version_label,
                    content_sha256, configuration
                ) VALUES (%s, %s, 'v1', %s, '{}'::jsonb)
                """,
                (
                    configuration_id,
                    f"test-configuration-{configuration_id}",
                    uuid4().hex * 2,
                ),
            )
            cursor.execute(
                """
                INSERT INTO vrp.pipeline_runs (
                    pipeline_run_id, environment, idempotency_key, run_kind,
                    valuation_date, snapshot_at, data_cutoff_at,
                    model_version_id, configuration_version_id, code_version,
                    orchestrator_version
                ) VALUES (
                    %s, 'test', %s, 'INTRADAY', %s, %s, %s,
                    %s, %s, 'test-code', 'test-orchestrator'
                )
                """,
                (
                    run_id,
                    f"test/{run_id}",
                    valuation_date,
                    snapshot_at,
                    snapshot_at,
                    model_id,
                    configuration_id,
                ),
            )
        return run_id, snapshot_at

    def test_migrations_apply_in_order(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT version FROM vrp.schema_migrations ORDER BY version")
            self.assertEqual([row[0] for row in cursor.fetchall()], ["0001", "0002"])

    def test_repository_revisions_and_operational_links(self):
        sofr_asset_id = self._insert_asset("test_sofr", "1")
        first_release = ReferenceDataRelease(
            release_id=uuid4(),
            dataset_key="FRED_SOFR_TEST",
            dataset_kind="REFERENCE_RATE",
            dataset_schema_version="v1",
            normalized_content_sha256="2" * 64,
            source_system="FRED",
            loader_version="test-v1",
            normalized_data_asset_id=sofr_asset_id,
            vintage_kind="LATEST_REVISED",
            retrieved_at=datetime(2026, 7, 21, 12, tzinfo=timezone.utc),
            observation_start_date=date(2026, 7, 20),
            observation_end_date=date(2026, 7, 20),
            source_row_count=1,
            persisted_row_count=1,
        )
        first_rate = StoredReferenceRateObservation(
            observation_id=uuid4(),
            release_id=first_release.release_id,
            value=ReferenceRateObservation(date(2026, 7, 20), Decimal("3.57")),
        )
        with self.repository_connection.transaction():
            with self.repository_connection.cursor() as cursor:
                repository = PostgresReferenceDataRepository(cursor)
                repository.acquire_dataset_lock(first_release.dataset_key)
                self.assertTrue(repository.register_release(first_release).inserted)
                repository.append_reference_rate(first_rate)

        second_release = ReferenceDataRelease(
            release_id=uuid4(),
            dataset_key="FRED_SOFR_TEST",
            dataset_kind="REFERENCE_RATE",
            dataset_schema_version="v1",
            normalized_content_sha256="3" * 64,
            source_system="FRED",
            loader_version="test-v1",
            normalized_data_asset_id=sofr_asset_id,
            vintage_kind="LATEST_REVISED",
            retrieved_at=datetime(2026, 7, 21, 13, tzinfo=timezone.utc),
            observation_start_date=date(2026, 7, 20),
            observation_end_date=date(2026, 7, 20),
            source_row_count=1,
            persisted_row_count=1,
        )
        second_rate = StoredReferenceRateObservation(
            observation_id=uuid4(),
            release_id=second_release.release_id,
            value=ReferenceRateObservation(date(2026, 7, 20), Decimal("3.58")),
            supersedes_observation_id=first_rate.observation_id,
        )
        with self.repository_connection.transaction():
            with self.repository_connection.cursor() as cursor:
                repository = PostgresReferenceDataRepository(cursor)
                repository.acquire_dataset_lock(second_release.dataset_key)
                repository.register_release(second_release)
                repository.append_reference_rate(second_rate)

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT reference_rate_observation_id, rate_percent, rate_decimal
                FROM vrp.current_reference_rate_observations
                WHERE observation_date = DATE '2026-07-20'
                """
            )
            current_id, rate_percent, rate_decimal = cursor.fetchone()
        self.assertEqual(current_id, second_rate.observation_id)
        self.assertEqual(str(rate_percent), "3.580000000000")
        self.assertEqual(str(rate_decimal), "0.03580000000000")

        with self.repository_connection.transaction():
            with self.repository_connection.cursor() as cursor:
                result = PostgresReferenceDataRepository(cursor).register_release(
                    second_release
                )
                self.assertFalse(result.inserted)
                self.assertEqual(result.record_id, second_release.release_id)

        sealed_row = StoredReferenceRateObservation(
            observation_id=uuid4(),
            release_id=second_release.release_id,
            value=ReferenceRateObservation(date(2026, 7, 21), Decimal("3.58")),
        )
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with self.repository_connection.transaction():
                with self.repository_connection.cursor() as cursor:
                    PostgresReferenceDataRepository(cursor).append_reference_rate(
                        sealed_row
                    )

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT load_transaction_id
                FROM vrp.reference_data_releases
                WHERE reference_data_release_id = %s
                """,
                (second_release.release_id,),
            )
            old_load_transaction_id = cursor.fetchone()[0]
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO vrp.reference_rate_observations (
                        reference_rate_observation_id,
                        reference_data_release_id,
                        load_transaction_id,
                        observation_date,
                        rate_percent,
                        row_sha256
                    ) VALUES (%s, %s, %s, DATE '2026-07-22', 3.58, %s)
                    """,
                    (
                        uuid4(),
                        second_release.release_id,
                        old_load_transaction_id,
                        "a" * 64,
                    ),
                )

        branch_release = ReferenceDataRelease(
            release_id=uuid4(),
            dataset_key="FRED_SOFR_TEST",
            dataset_kind="REFERENCE_RATE",
            dataset_schema_version="v1",
            normalized_content_sha256="4" * 64,
            source_system="FRED",
            loader_version="test-v1",
            normalized_data_asset_id=sofr_asset_id,
            vintage_kind="LATEST_REVISED",
            retrieved_at=datetime(2026, 7, 21, 14, tzinfo=timezone.utc),
            observation_start_date=date(2026, 7, 20),
            observation_end_date=date(2026, 7, 20),
            source_row_count=1,
            persisted_row_count=1,
        )
        with self.assertRaises(psycopg.errors.UniqueViolation):
            with self.repository_connection.transaction():
                with self.repository_connection.cursor() as cursor:
                    repository = PostgresReferenceDataRepository(cursor)
                    repository.register_release(branch_release)
                    repository.append_reference_rate(
                        StoredReferenceRateObservation(
                            observation_id=uuid4(),
                            release_id=branch_release.release_id,
                            value=ReferenceRateObservation(
                                date(2026, 7, 20), Decimal("3.59")
                            ),
                            supersedes_observation_id=first_rate.observation_id,
                        )
                    )

        with self.assertRaises(psycopg.errors.RaiseException):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE vrp.reference_rate_observations
                    SET rate_percent = 3.59
                    WHERE reference_rate_observation_id = %s
                    """,
                    (second_rate.observation_id,),
                )
        with self.assertRaises(psycopg.errors.RaiseException):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM vrp.reference_rate_observations
                    WHERE reference_rate_observation_id = %s
                    """,
                    (second_rate.observation_id,),
                )

        daily_asset_id = self._insert_asset("test_spy_daily", "5")
        definition_payload = {
            "price_source_field": "close",
            "adjustment_attested": False,
        }
        definition_digest = daily_market_feature_definition_sha256(
            definition_key="SPY_SIGNAL_FEATURES_TEST",
            price_adjustment="UNKNOWN",
            return_formula_version="canonical_spy_close_log_return_v1",
            rsi_formula_version="wilder_rsi14_spy_close_v3_clean_session_rebuild",
            rv_formula_version="sample_std_log_return_21d_ddof1_annualized_252_v1",
            definition=definition_payload,
        )
        definition = DailyMarketFeatureDefinition(
            definition_id=uuid4(),
            definition_key="SPY_SIGNAL_FEATURES_TEST",
            version_label="v1",
            content_sha256=definition_digest,
            price_adjustment="UNKNOWN",
            return_formula_version="canonical_spy_close_log_return_v1",
            rsi_formula_version="wilder_rsi14_spy_close_v3_clean_session_rebuild",
            rv_formula_version="sample_std_log_return_21d_ddof1_annualized_252_v1",
            definition=definition_payload,
        )
        daily_release = ReferenceDataRelease(
            release_id=uuid4(),
            dataset_key="SPY_SIGNAL_DAILY_TEST",
            dataset_kind="DAILY_MARKET_FEATURES",
            dataset_schema_version="v1",
            normalized_content_sha256="7" * 64,
            source_system="THETADATA_AND_DERIVED",
            loader_version="test-v1",
            normalized_data_asset_id=daily_asset_id,
            vintage_kind="LATEST_REVISED",
            retrieved_at=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
            observation_start_date=date(2026, 7, 21),
            observation_end_date=date(2026, 7, 21),
            source_row_count=1,
            persisted_row_count=1,
        )
        daily_value = DailyMarketFeature(
            trade_date=date(2026, 7, 21),
            prior_trade_date=date(2026, 7, 20),
            spy_close=Decimal("748.32"),
            spy_change=1.20,
            spy_log_return=0.001605,
            wilder_avg_gain_14=2.10,
            wilder_avg_loss_14=1.90,
            rsi14=52.487359683942614,
            rv21d_variance=0.013796901,
            rv21d_volatility_pct=11.746021142005407,
            calculation_status="AVAILABLE",
            quality_status="PASS",
        )
        daily_feature = StoredDailyMarketFeature(
            feature_id=uuid4(),
            definition_id=definition.definition_id,
            release_id=daily_release.release_id,
            value=daily_value,
        )
        with self.repository_connection.transaction():
            with self.repository_connection.cursor() as cursor:
                repository = PostgresReferenceDataRepository(cursor)
                repository.acquire_dataset_lock(daily_release.dataset_key)
                repository.register_feature_definition(definition)
                repository.register_release(daily_release)
                repository.append_daily_market_feature(daily_feature)

        run_id, snapshot_at = self._insert_pipeline_run(date(2026, 7, 22))
        snapshot_id = uuid4()
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO vrp.market_snapshots (
                        market_snapshot_id, pipeline_run_id, valuation_date,
                        snapshot_at, snapshot_kind, market_session,
                        freshness_status, sofr_observation_id
                    ) VALUES (
                        %s, %s, DATE '2026-07-22', %s, 'INTRADAY_PREVIEW',
                        'OPEN', 'FRESH', %s
                    )
                    """,
                    (uuid4(), run_id, snapshot_at, second_rate.observation_id),
                )

        same_day_run_id, same_day_snapshot_at = self._insert_pipeline_run(
            date(2026, 7, 20)
        )
        with self.assertRaises(psycopg.errors.CheckViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO vrp.market_snapshots (
                        market_snapshot_id, pipeline_run_id, valuation_date,
                        snapshot_at, snapshot_kind, market_session,
                        freshness_status, sofr_rate, sofr_observation_date
                    ) VALUES (
                        %s, %s, DATE '2026-07-20', %s, 'INTRADAY_PREVIEW',
                        'OPEN', 'FRESH', 0.0358, DATE '2026-07-20'
                    )
                    """,
                    (uuid4(), same_day_run_id, same_day_snapshot_at),
                )

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO vrp.market_snapshots (
                    market_snapshot_id, pipeline_run_id, valuation_date,
                    snapshot_at, snapshot_kind, market_session,
                    freshness_status, spy_price, sofr_rate,
                    sofr_observation_date, sofr_observation_id,
                    daily_market_feature_id
                ) VALUES (
                    %s, %s, DATE '2026-07-22', %s, 'INTRADAY_PREVIEW',
                    'OPEN', 'FRESH', 750.00, 0.0358, DATE '2026-07-20', %s, %s
                )
                """,
                (
                    snapshot_id,
                    run_id,
                    snapshot_at,
                    second_rate.observation_id,
                    daily_feature.feature_id,
                ),
            )

        implied_id = uuid4()
        forecast_id = uuid4()
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO vrp.implied_variance_term_structure (
                    implied_variance_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, target_expiration, effective_dte,
                    annualized_variance, annualized_volatility_pct,
                    calculation_status, quality_status
                ) VALUES (
                    %s, %s, %s, 21, DATE '2026-08-12', 21.0,
                    0.04, 20.0, 'AVAILABLE', 'PASS'
                )
                """,
                (implied_id, run_id, snapshot_id),
            )
            cursor.execute(
                """
                INSERT INTO vrp.forecast_variance_term_structure (
                    forecast_variance_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, forecast_as_of_date, predicted_log_variance,
                    annualized_variance, annualized_volatility_pct,
                    calculation_status, quality_status
                ) VALUES (
                    %s, %s, %s, 21, DATE '2026-07-21', -3.9,
                    0.02, 14.1421356237, 'AVAILABLE', 'PASS'
                )
                """,
                (forecast_id, run_id, snapshot_id),
            )
            cursor.execute(
                """
                INSERT INTO vrp.signal_features (
                    signal_feature_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, tenor_bucket, implied_variance_id,
                    forecast_variance_id, vrp_log, vrp_3m_prior_mean,
                    vrp_3m_prior_sample_std, vrp_1y_prior_mean,
                    vrp_1y_prior_sample_std, zscore_3m, zscore_1y, rsi14,
                    rv21d_variance, rv21d_volatility_pct,
                    zscore_3m_sample_count, zscore_1y_sample_count,
                    history_through_date, is_complete,
                    daily_market_feature_id, rsi14_source_kind
                ) VALUES (
                    %s, %s, %s, 21, 'MIDDLE', %s, %s, 0.69, 0.4,
                    0.2, 0.35, 0.25, 1.45, 1.36, 53.0, %s, %s,
                    63, 252, DATE '2026-07-21', TRUE, %s,
                    'INTRADAY_ESTIMATE'
                )
                """,
                (
                    uuid4(),
                    run_id,
                    snapshot_id,
                    implied_id,
                    forecast_id,
                    daily_value.rv21d_variance,
                    daily_value.rv21d_volatility_pct,
                    daily_feature.feature_id,
                ),
            )

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT snapshot.spy_price, daily.spy_close, feature.rsi14,
                       daily.rsi14, feature.rv21d_volatility_pct
                FROM vrp.market_snapshots AS snapshot
                JOIN vrp.daily_market_features AS daily
                  ON daily.daily_market_feature_id = snapshot.daily_market_feature_id
                JOIN vrp.signal_features AS feature
                  ON feature.market_snapshot_id = snapshot.market_snapshot_id
                WHERE snapshot.market_snapshot_id = %s
                """,
                (snapshot_id,),
            )
            live_price, official_close, intraday_rsi, official_rsi, rv21d = cursor.fetchone()
        self.assertEqual(str(live_price), "750.00000000")
        self.assertEqual(str(official_close), "748.32000000")
        self.assertNotEqual(intraday_rsi, official_rsi)
        self.assertAlmostEqual(rv21d, daily_value.rv21d_volatility_pct)

        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE vrp.signal_features
                    SET rv21d_volatility_pct = 99.0
                    WHERE market_snapshot_id = %s
                    """,
                    (snapshot_id,),
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
