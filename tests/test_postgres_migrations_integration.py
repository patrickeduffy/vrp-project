from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
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
from vrp.reference_history.artifacts import ContentAddressedArtifactStore, prepare_history
from vrp.reference_history.service import execute_reference_history_load
from vrp.reference_history.sources import normalize_sofr_csv, normalize_spy_daily_frames
from vrp.storage.reference_data import (
    DailyMarketFeature,
    DailyMarketFeatureDefinition,
    ReferenceDataRelease,
    ReferenceRateObservation,
    StoredDailyMarketFeature,
    StoredReferenceRateObservation,
    daily_market_feature_definition_sha256,
)
from tests.test_reference_history_normalization import synthetic_spy_frames


def local_path_from_file_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    path = Path(url2pathname(parsed.path))
    if parsed.netloc:
        return Path(f"//{parsed.netloc}{url2pathname(parsed.path)}")
    return path


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

    def test_historical_loader_is_idempotent_revision_safe_and_atomic(self):
        def prepared_sofr(store, source, rows):
            source.write_text(
                "observation_date,SOFR\n"
                + "".join(f"{day},{value}\n" for day, value in rows),
                encoding="utf-8",
            )
            frozen = store.freeze_inputs({"sofr": source})
            history = normalize_sofr_csv(
                frozen.inputs["sofr"].path,
                enforce_production_coverage=False,
            )
            return prepare_history(history, frozen_inputs=frozen, store=store)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ContentAddressedArtifactStore(root / "artifacts")
            source = root / "sofr.csv"
            first = prepared_sofr(
                store,
                source,
                (("2018-04-03", "1.83"), ("2018-04-04", "1.74")),
            )
            with psycopg.connect(TEST_DSN) as loader_connection:
                first_result = execute_reference_history_load(
                    loader_connection,
                    first,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v1",
                )
                first_rerun = execute_reference_history_load(
                    loader_connection,
                    first,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v1",
                )
                self.assertTrue(first_rerun.no_op)
                self.assertEqual(
                    first_result.reference_data_release_id,
                    first_rerun.reference_data_release_id,
                )

                second = prepared_sofr(
                    store,
                    source,
                    (
                        ("2018-04-03", "1.83"),
                        ("2018-04-04", "1.75"),
                        ("2018-04-05", "1.75"),
                    ),
                )
                second_result = execute_reference_history_load(
                    loader_connection,
                    second,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v1",
                )
                self.assertEqual(second_result.new_count, 1)
                self.assertEqual(second_result.correction_count, 1)

                old_after_advance = execute_reference_history_load(
                    loader_connection,
                    first,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v1",
                )
                self.assertTrue(old_after_advance.no_op)

                reverified_old_release = execute_reference_history_load(
                    loader_connection,
                    first,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v2",
                )
                self.assertTrue(reverified_old_release.no_op)
                self.assertNotEqual(
                    reverified_old_release.pipeline_run_id,
                    first_result.pipeline_run_id,
                )
                self.assertEqual(
                    reverified_old_release.reference_data_release_id,
                    first_result.reference_data_release_id,
                )

                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT asset.storage_uri
                        FROM vrp.pipeline_run_data_assets AS link
                        JOIN vrp.data_assets AS asset
                          ON asset.data_asset_id = link.data_asset_id
                        WHERE link.pipeline_run_id = %s
                          AND link.usage_role = 'QA_EVIDENCE'
                          AND link.logical_name = 'reference_history_qa_manifest'
                        """,
                        (reverified_old_release.pipeline_run_id,),
                    )
                    reverified_qa_path = local_path_from_file_uri(cursor.fetchone()[0])
                reverified_qa_bytes = reverified_qa_path.read_bytes()
                try:
                    reverified_qa_path.write_bytes(reverified_qa_bytes + b"\n")
                    with self.assertRaisesRegex(RuntimeError, "persisted artifact size"):
                        execute_reference_history_load(
                            loader_connection,
                            first,
                            artifact_store=store,
                            environment="integration-test",
                            code_version="integration-test-v2",
                        )
                finally:
                    reverified_qa_path.write_bytes(reverified_qa_bytes)

                equivalent_source = prepared_sofr(
                    store,
                    source,
                    (("2018-04-03", "1.830"), ("2018-04-04", "1.740")),
                )
                self.assertEqual(
                    equivalent_source.normalized.content_sha256,
                    first.normalized.content_sha256,
                )
                equivalent_result = execute_reference_history_load(
                    loader_connection,
                    equivalent_source,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v3",
                )
                self.assertEqual(
                    equivalent_result.reference_data_release_id,
                    first_result.reference_data_release_id,
                )
                original_input_path = first.source_artifacts[0].path
                original_input_bytes = original_input_path.read_bytes()
                try:
                    original_input_path.write_bytes(original_input_bytes + b"\n")
                    with self.assertRaisesRegex(RuntimeError, "persisted artifact size"):
                        execute_reference_history_load(
                            loader_connection,
                            equivalent_source,
                            artifact_store=store,
                            environment="integration-test",
                            code_version="integration-test-v3",
                        )
                finally:
                    original_input_path.write_bytes(original_input_bytes)

                third = prepared_sofr(
                    store,
                    source,
                    (
                        ("2018-04-03", "1.83"),
                        ("2018-04-04", "1.75"),
                        ("2018-04-05", "1.75"),
                        ("2018-04-06", "1.76"),
                    ),
                )

                def fail_after_rows(stage):
                    if stage == "after_release_rows":
                        raise RuntimeError("intentional rollback test")

                with self.assertRaisesRegex(RuntimeError, "intentional rollback"):
                    execute_reference_history_load(
                        loader_connection,
                        third,
                        artifact_store=store,
                        environment="integration-test",
                        code_version="integration-test-v1",
                        failure_injector=fail_after_rows,
                    )

                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM vrp.reference_data_releases
                        WHERE normalized_content_sha256 = %s
                        """,
                        (third.normalized.content_sha256,),
                    )
                    self.assertEqual(cursor.fetchone()[0], 0)
                    cursor.execute(
                        """
                        SELECT run.pipeline_run_id, run.status, run.qa_status,
                               stage.status, qa.outcome,
                               (
                                   SELECT COUNT(*)
                                   FROM vrp.pipeline_run_data_assets AS link
                                   WHERE link.pipeline_run_id = run.pipeline_run_id
                               )
                        FROM vrp.pipeline_runs AS run
                        JOIN vrp.pipeline_run_stages AS stage
                          ON stage.pipeline_run_id = run.pipeline_run_id
                        JOIN vrp.qa_results AS qa
                          ON qa.pipeline_run_id = run.pipeline_run_id
                        WHERE run.environment = 'integration-test'
                          AND run.invocation->>'normalized_content_sha256' = %s
                        """,
                        (third.normalized.content_sha256,),
                    )
                    failed_run = cursor.fetchone()
                    self.assertEqual(failed_run[1:], ("FAILED", "FAIL", "FAILED", "FAIL", 0))
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM vrp.data_assets
                        WHERE dataset_name = 'FRED_SOFR_NORMALIZED'
                          AND content_sha256 = %s
                        """,
                        (third.normalized.content_sha256,),
                    )
                    self.assertEqual(cursor.fetchone()[0], 0)

                third_result = execute_reference_history_load(
                    loader_connection,
                    third,
                    artifact_store=store,
                    environment="integration-test",
                    code_version="integration-test-v1",
                )
                self.assertFalse(third_result.no_op)
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT run.status, run.qa_status, stage.status,
                               stage.attempt_count, qa.outcome
                        FROM vrp.pipeline_runs AS run
                        JOIN vrp.pipeline_run_stages AS stage
                          ON stage.pipeline_run_id = run.pipeline_run_id
                        JOIN vrp.qa_results AS qa
                          ON qa.pipeline_run_id = run.pipeline_run_id
                        WHERE run.pipeline_run_id = %s
                        """,
                        (third_result.pipeline_run_id,),
                    )
                    self.assertEqual(
                        cursor.fetchone(),
                        ("COMPLETED", "PASS", "COMPLETED", 2, "PASS"),
                    )

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT run.status, run.qa_status, qa.outcome
                    FROM vrp.pipeline_runs AS run
                    JOIN vrp.qa_results AS qa
                      ON qa.pipeline_run_id = run.pipeline_run_id
                    WHERE run.pipeline_run_id = %s
                    """,
                    (first_result.pipeline_run_id,),
                )
                self.assertEqual(cursor.fetchone(), ("COMPLETED", "PASS", "PASS"))
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM vrp.current_reference_rate_observations
                    WHERE series_key = 'SOFR'
                    """
                )
                self.assertEqual(cursor.fetchone()[0], 4)
                cursor.execute(
                    """
                    SELECT asset.storage_uri
                    FROM vrp.reference_data_releases AS release
                    JOIN vrp.data_assets AS asset
                      ON asset.data_asset_id = release.qa_manifest_data_asset_id
                    WHERE release.reference_data_release_id = %s
                    """,
                    (third_result.reference_data_release_id,),
                )
                qa_uri = cursor.fetchone()[0]
            qa_path = local_path_from_file_uri(qa_uri)
            qa_text = qa_path.read_text(encoding="utf-8")
            self.assertNotIn("PENDING", qa_text)
            self.assertIn('"current_rows_match_candidate_at_commit":"PASS"', qa_text)

    def test_historical_loader_persists_daily_feature_definition_and_rows(self):
        import pandas as pd

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ContentAddressedArtifactStore(root / "artifacts")
            prices, rsi, realized = synthetic_spy_frames()
            sources = {
                "spy_eod": root / "spy_eod.csv",
                "wilder_rsi": root / "wilder_rsi.csv",
                "rv21d": root / "rv21d.csv",
            }
            prices.to_csv(sources["spy_eod"], index=False)
            rsi.to_csv(sources["wilder_rsi"], index=False)
            realized.to_csv(sources["rv21d"], index=False)
            frozen = store.freeze_inputs(sources)
            history = normalize_spy_daily_frames(
                pd.read_csv(frozen.inputs["spy_eod"].path),
                pd.read_csv(frozen.inputs["wilder_rsi"].path),
                pd.read_csv(frozen.inputs["rv21d"].path),
                enforce_production_coverage=False,
            )
            prepared = prepare_history(history, frozen_inputs=frozen, store=store)
            with psycopg.connect(TEST_DSN) as loader_connection:
                result = execute_reference_history_load(
                    loader_connection,
                    prepared,
                    artifact_store=store,
                    environment="integration-test-daily",
                    code_version="integration-test-v1",
                )
            self.assertEqual(result.persisted_row_count, 25)
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT daily_market_feature_definition_id)
                    FROM vrp.daily_market_features
                    WHERE reference_data_release_id = %s
                    """,
                    (result.reference_data_release_id,),
                )
                self.assertEqual(cursor.fetchone(), (25, 1))

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
