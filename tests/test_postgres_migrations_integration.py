from __future__ import annotations

import hashlib
import math
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
TEST_DSN = os.environ.get("VRP_TEST_DATABASE_URL")
EOD_TEST_CODE_VERSION = "1" * 40
EOD_ROLLBACK_CODE_VERSION = "2" * 40

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
from vrp.eod_shadow.sofr_evidence import SofrUpdaterEvidence
from vrp.eod_shadow.service import execute_eod_shadow_load
from vrp.storage.eod_postgres import PostgresEodRepository


def local_path_from_file_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    path = Path(url2pathname(parsed.path))
    if parsed.netloc:
        return Path(f"//{parsed.netloc}{url2pathname(parsed.path)}")
    return path


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_version_document(path: Path, payload: bytes) -> str:
    path.write_bytes(payload)
    return _sha256_bytes(payload)


def synthetic_eod_snapshot(root: Path) -> EodSnapshot:
    """Build a complete validated no-trade snapshot without running calculators."""

    valuation_date = date(2030, 1, 4)
    run_dir = root / "completed-eod-run"
    staging = run_dir / "staging"
    staging.mkdir(parents=True)

    model_path = root / "model-lock.json"
    model_bytes = b'{"lock_date":"2029-12-01","model":"locked-corsi"}'
    model_sha256 = _write_version_document(model_path, model_bytes)
    configuration_path = root / "signal-configuration.json"
    configuration_bytes = b'{"selection":"locked","thresholds":"production"}'
    configuration_sha256 = _write_version_document(
        configuration_path, configuration_bytes
    )

    signal_path = staging / "signal-history.parquet"
    signal_bytes = b"synthetic-validated-eod-signal-history-v1"
    signal_sha256 = _write_version_document(signal_path, signal_bytes)
    fixture_path = root / "golden-eod-fixture.json"
    fixture_bytes = (
        b'{"captured_at_utc":"2029-12-01T00:00:00+00:00",'
        b'"fixture":"accepted-synthetic-eod"}'
    )
    fixture_sha256 = _write_version_document(fixture_path, fixture_bytes)
    sofr_manifest_path = root / "sofr_update_manifest.json"
    sofr_manifest_bytes = b'{"status":"PUBLISHED","source":"FRED_SOFR"}'
    sofr_manifest_sha256 = _write_version_document(
        sofr_manifest_path, sofr_manifest_bytes
    )
    sofr_snapshot_path = root / "sofr_refreshed_snapshot.csv"
    sofr_snapshot_bytes = b"observation_date,SOFR\n2030-01-03,3.57\n"
    sofr_snapshot_sha256 = _write_version_document(
        sofr_snapshot_path, sofr_snapshot_bytes
    )

    implied = tuple(
        ImpliedVarianceRecord(
            tenor_days=tenor,
            target_expiration=valuation_date + timedelta(days=tenor),
            effective_dte=float(tenor),
            annualized_variance=0.022 + tenor / 100_000,
            annualized_volatility_pct=math.sqrt(0.022 + tenor / 100_000) * 100,
            quality_details={"source": "synthetic-integration"},
        )
        for tenor in TARGET_TENORS
    )
    forecast = tuple(
        ForecastVarianceRecord(
            tenor_days=tenor,
            forecast_as_of_date=valuation_date,
            predicted_log_variance=math.log(0.017 + tenor / 100_000),
            annualized_variance=0.017 + tenor / 100_000,
            annualized_volatility_pct=math.sqrt(0.017 + tenor / 100_000) * 100,
            quality_details={"model": "synthetic-locked"},
        )
        for tenor in TARGET_TENORS
    )
    features = tuple(
        SignalFeatureRecord(
            tenor_days=tenor,
            tenor_bucket=(
                "FRONT" if tenor <= 18 else "MIDDLE" if tenor <= 24 else "BACK"
            ),
            vrp_log=math.log(
                implied[index].annualized_variance
                / forecast[index].annualized_variance
            ),
            vrp_3m_prior_mean=0.20,
            vrp_3m_prior_sample_std=0.10,
            vrp_1y_prior_mean=0.15,
            vrp_1y_prior_sample_std=0.20,
            zscore_3m=0.25,
            zscore_1y=0.30,
            rsi14=52.0,
            rv21d_variance=0.0144,
            rv21d_volatility_pct=12.0,
            zscore_3m_sample_count=63,
            zscore_1y_sample_count=252,
            history_through_date=valuation_date - timedelta(days=1),
            details={"source": "synthetic-integration"},
        )
        for index, tenor in enumerate(TARGET_TENORS)
    )
    evaluations = tuple(
        SignalEvaluationRecord(
            evaluation_key=f"{layer}:{tenor}",
            tenor_days=tenor,
            tenor_bucket=features[index].tenor_bucket,
            signal_layer=layer,
            evaluation_status="INACTIVE",
            qualifies=False,
            vrp_pass=None,
            zscore_3m_pass=None,
            zscore_1y_pass=None,
            rsi14_pass=None,
            rv21d_pass=None,
            threshold_values={},
            comparison_results={"rule_exists": False},
            failed_checks=("RULE_INACTIVE",),
            rank_position=None,
            rank_score=None,
            target_size_pct_nav=None,
            details={"source": "synthetic-integration"},
        )
        for index, tenor in enumerate(TARGET_TENORS)
        for layer in ("CORE", "SECONDARY")
    )

    return EodSnapshot(
        run_dir=run_dir.resolve(),
        valuation_date=valuation_date,
        lock_id="synthetic-eod-lock-v1",
        approved_nav=1_000_000.0,
        run_manifest={
            "status": "PASS",
            "finished_at": "2030-01-04T21:05:00+00:00",
        },
        publish_manifest={"status": "PASS", "authoritative": False},
        model_identity=VersionedDocument(
            key="unified_fds_no_min_return",
            version_label="synthetic-locked-v1",
            path=model_path.resolve(),
            sha256=model_sha256,
            payload={"lock_date": "2029-12-01", "model": "locked-corsi"},
        ),
        configuration_identity=VersionedDocument(
            key="put_signal_configuration",
            version_label="synthetic-locked-v1",
            path=configuration_path.resolve(),
            sha256=configuration_sha256,
            payload={"selection": "locked", "thresholds": "production"},
        ),
        sofr_evidence=SofrUpdaterEvidence(
            updater_manifest_path=sofr_manifest_path.resolve(),
            updater_manifest_sha256=sofr_manifest_sha256,
            refreshed_snapshot_path=sofr_snapshot_path.resolve(),
            refreshed_snapshot_sha256=sofr_snapshot_sha256,
            normalized_content_sha256="8" * 64,
            start_date=date(2030, 1, 3),
            end_date=date(2030, 1, 3),
            row_count=1,
            observation_date=date(2030, 1, 3),
            rate_decimal=Decimal("0.0357"),
            row_sha256="d" * 64,
        ),
        market_snapshot=MarketSnapshotRecord(
            valuation_date=valuation_date,
            snapshot_at=datetime(2030, 1, 4, 21, tzinfo=timezone.utc),
            data_cutoff_at=datetime(2030, 1, 4, 21, tzinfo=timezone.utc),
            snapshot_kind="EOD_OFFICIAL",
            market_session="CLOSED",
            freshness_status="FRESH",
            spy_price=748.32,
            details={"source": "synthetic-integration"},
        ),
        implied_variance=implied,
        forecast_variance=forecast,
        signal_features=features,
        signal_evaluations=evaluations,
        selected_signal=SelectedSignalRecord(
            decision="NO_TRADE",
            signal_state="EOD_OFFICIAL",
            selection_rule_id="synthetic-locked-selection-v1",
            selected_evaluation_key=None,
            no_trade_reason="NO_CORE_OR_SECONDARY_SIGNAL_QUALIFIED",
            approved_nav_dollars=1_000_000.0,
            target_max_risk_dollars=None,
            selection_trace={"qualified": []},
        ),
        artifacts=(
            ArtifactMetadata(
                logical_name="signal_history",
                path=signal_path.resolve(),
                asset_format="PARQUET",
                sha256=signal_sha256,
                byte_size=len(signal_bytes),
                row_count=9,
                relative_path="staging/signal-history.parquet",
                identity_input=True,
                metadata={"synthetic": True},
            ),
            ArtifactMetadata(
                logical_name="sofr_update_manifest",
                path=sofr_manifest_path.resolve(),
                asset_format="JSON",
                sha256=sofr_manifest_sha256,
                byte_size=len(sofr_manifest_bytes),
                relative_path="reference/sofr_update_manifest.json",
                identity_input=True,
                metadata={"synthetic": True},
            ),
            ArtifactMetadata(
                logical_name="sofr_refreshed_snapshot",
                path=sofr_snapshot_path.resolve(),
                asset_format="CSV",
                sha256=sofr_snapshot_sha256,
                byte_size=len(sofr_snapshot_bytes),
                row_count=1,
                relative_path="reference/sofr_refreshed_snapshot.csv",
                identity_input=True,
                metadata={"synthetic": True},
                trade_date_start=date(2030, 1, 3),
                trade_date_end=date(2030, 1, 3),
            ),
        ),
        golden_evidence=GoldenVerificationEvidence(
            status="PASS",
            verification_id="a" * 64,
            fixture_path=fixture_path.resolve(),
            fixture_sha256=fixture_sha256,
            signal_history_sha256=signal_sha256,
            selected_decisions_sha256="b" * 64,
            manifest={"status": "PASS"},
        ),
        output_fingerprint="c" * 64,
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

    def _seed_eod_reference_pins(self, valuation_date: date) -> None:
        """Insert one accepted prior SOFR row and one exact-date SPY feature."""

        sofr_asset_id = self._insert_asset("eod_shadow_sofr_pin", "8")
        spy_asset_id = self._insert_asset("eod_shadow_spy_pin", "9")
        sofr_release_id = uuid4()
        spy_release_id = uuid4()
        sofr_observation_id = uuid4()
        daily_feature_id = uuid4()
        definition_id = uuid4()
        definition_sha256 = (
            "71854988797daedd685fa8d9a140fdcbd8dddaa8c2d7cd1c59c23e7f97a0371d"
        )
        prior_date = valuation_date - timedelta(days=1)

        with self.repository_connection.transaction():
            with self.repository_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO vrp.daily_market_feature_definitions (
                        daily_market_feature_definition_id, definition_key,
                        version_label, content_sha256, price_adjustment,
                        return_formula_version, rsi_formula_version,
                        rv_formula_version, definition
                    ) VALUES (
                        %s, %s, %s, %s, 'UNKNOWN', %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING daily_market_feature_definition_id
                    """,
                    (
                        definition_id,
                        "SPY_SIGNAL_FEATURES",
                        "v1-71854988797d",
                        definition_sha256,
                        "canonical_spy_close_log_return_v1",
                        "wilder_rsi14_spy_close_v3_clean_session_rebuild",
                        "sample_std_log_return_21d_ddof1_annualized_252_v1",
                        '{"source":"synthetic-integration"}',
                    ),
                )
                inserted_definition = cursor.fetchone()
                if inserted_definition is None:
                    cursor.execute(
                        """
                        SELECT daily_market_feature_definition_id
                        FROM vrp.daily_market_feature_definitions
                        WHERE content_sha256 = %s
                        """,
                        (definition_sha256,),
                    )
                    definition_id = cursor.fetchone()[0]
                else:
                    definition_id = inserted_definition[0]

                cursor.execute(
                    """
                    INSERT INTO vrp.reference_data_releases (
                        reference_data_release_id, dataset_key, dataset_kind,
                        dataset_schema_version, normalized_content_sha256,
                        source_system, loader_version, normalized_data_asset_id,
                        vintage_kind, retrieved_at, observation_start_date,
                        observation_end_date, source_row_count, persisted_row_count
                    ) VALUES (
                        %s, %s, 'REFERENCE_RATE', 'v1', %s, 'FRED', 'test-v1',
                        %s, 'LATEST_REVISED', %s, %s, %s, 1, 1
                    )
                    """,
                    (
                        sofr_release_id,
                        "FRED_SOFR_EOD_SHADOW_INTEGRATION",
                        "8" * 64,
                        sofr_asset_id,
                        datetime(2030, 1, 4, 12, tzinfo=timezone.utc),
                        prior_date,
                        prior_date,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vrp.reference_rate_observations (
                        reference_rate_observation_id, reference_data_release_id,
                        series_key, observation_date, rate_percent, row_sha256
                    ) VALUES (%s, %s, 'SOFR', %s, 3.57, %s)
                    """,
                    (
                        sofr_observation_id,
                        sofr_release_id,
                        prior_date,
                        "d" * 64,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vrp.reference_data_releases (
                        reference_data_release_id, dataset_key, dataset_kind,
                        dataset_schema_version, normalized_content_sha256,
                        source_system, loader_version, normalized_data_asset_id,
                        vintage_kind, retrieved_at, observation_start_date,
                        observation_end_date, source_row_count, persisted_row_count
                    ) VALUES (
                        %s, %s, 'DAILY_MARKET_FEATURES', 'v1', %s,
                        'THETADATA_AND_DERIVED', 'test-v1', %s,
                        'LATEST_REVISED', %s, %s, %s, 1, 1
                    )
                    """,
                    (
                        spy_release_id,
                        "SPY_SIGNAL_DAILY_EOD_SHADOW_INTEGRATION",
                        "9" * 64,
                        spy_asset_id,
                        datetime(2030, 1, 4, 22, tzinfo=timezone.utc),
                        valuation_date,
                        valuation_date,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vrp.daily_market_features (
                        daily_market_feature_id,
                        daily_market_feature_definition_id,
                        reference_data_release_id, symbol, trade_date,
                        prior_trade_date, spy_close, spy_change, spy_log_return,
                        wilder_avg_gain_14, wilder_avg_loss_14, rsi14,
                        rv21d_variance, rv21d_volatility_pct,
                        calculation_status, quality_status, row_sha256, details
                    ) VALUES (
                        %s, %s, %s, 'SPY', %s, %s, 748.32, 1.20, 0.001605,
                        2.10, 1.90, 52.0, 0.0144, 12.0,
                        'AVAILABLE', 'PASS', %s, %s::jsonb
                    )
                    """,
                    (
                        daily_feature_id,
                        definition_id,
                        spy_release_id,
                        valuation_date,
                        prior_date,
                        "e" * 64,
                        '{"source":"synthetic-integration"}',
                    ),
                )

    def test_eod_shadow_service_is_atomic_idempotent_and_non_publishing(self):
        class InjectedReconciliationFailureRepository(PostgresEodRepository):
            def fetch_run_projection(self, pipeline_run_id):
                raise RuntimeError("injected EOD reconciliation failure")

        with tempfile.TemporaryDirectory() as directory:
            snapshot = synthetic_eod_snapshot(Path(directory))
            self._seed_eod_reference_pins(snapshot.valuation_date)

            with psycopg.connect(TEST_DSN) as eod_connection:
                first = execute_eod_shadow_load(
                    eod_connection,
                    snapshot,
                    environment="integration-eod-shadow",
                    code_version=EOD_TEST_CODE_VERSION,
                    requested_by="integration-test",
                )
                self.assertFalse(first.no_op)
                self.assertEqual(first.implied_variance_count, 9)
                self.assertEqual(first.forecast_variance_count, 9)
                self.assertEqual(first.signal_feature_count, 9)
                self.assertEqual(first.signal_evaluation_count, 18)

                with self.connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                            run.status,
                            run.qa_status,
                            (SELECT COUNT(*) FROM vrp.market_snapshots AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*)
                             FROM vrp.implied_variance_term_structure AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*)
                             FROM vrp.forecast_variance_term_structure AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.signal_features AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.signal_evaluations AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.selected_signals AS item
                             WHERE item.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.qa_results AS qa
                             WHERE qa.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.qa_results AS qa
                             WHERE qa.pipeline_run_id = run.pipeline_run_id
                               AND qa.is_hard_gate AND qa.outcome = 'PASS'),
                            (SELECT array_agg(qa.check_code ORDER BY qa.check_code)
                             FROM vrp.qa_results AS qa
                             WHERE qa.pipeline_run_id = run.pipeline_run_id),
                            (SELECT COUNT(*) FROM vrp.pipeline_run_stages AS stage
                             WHERE stage.pipeline_run_id = run.pipeline_run_id
                               AND stage.status = 'COMPLETED'),
                            (SELECT COUNT(*) FROM vrp.signal_publications AS publication
                             WHERE publication.pipeline_run_id = run.pipeline_run_id)
                        FROM vrp.pipeline_runs AS run
                        WHERE run.pipeline_run_id = %s
                        """,
                        (first.pipeline_run_id,),
                    )
                    self.assertEqual(
                        cursor.fetchone(),
                        (
                            "COMPLETED",
                            "PASS",
                            1,
                            9,
                            9,
                            9,
                            18,
                            1,
                            2,
                            2,
                            [
                                "golden_eod_contract",
                                "postgres_projection_reconciliation",
                            ],
                            1,
                            0,
                        ),
                    )

                repeated = execute_eod_shadow_load(
                    eod_connection,
                    snapshot,
                    environment="integration-eod-shadow",
                    code_version=EOD_TEST_CODE_VERSION,
                    requested_by="integration-test",
                )
                self.assertTrue(repeated.no_op)
                self.assertEqual(repeated.pipeline_run_id, first.pipeline_run_id)
                self.assertEqual(repeated.market_snapshot_id, first.market_snapshot_id)
                self.assertEqual(repeated.selected_signal_id, first.selected_signal_id)

                with self.assertRaisesRegex(
                    RuntimeError, "injected EOD reconciliation failure"
                ):
                    execute_eod_shadow_load(
                        eod_connection,
                        snapshot,
                        environment="integration-eod-shadow",
                        code_version=EOD_ROLLBACK_CODE_VERSION,
                        requested_by="integration-test",
                        repository_factory=InjectedReconciliationFailureRepository,
                    )

            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*),
                        (SELECT COUNT(*) FROM vrp.market_snapshots AS snapshot
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = snapshot.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.implied_variance_term_structure AS item
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = item.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.forecast_variance_term_structure AS item
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = item.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.signal_features AS item
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = item.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.signal_evaluations AS item
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = item.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.selected_signals AS item
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = item.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s),
                        (SELECT COUNT(*) FROM vrp.signal_publications AS publication
                         JOIN vrp.pipeline_runs AS run
                           ON run.pipeline_run_id = publication.pipeline_run_id
                         WHERE run.run_kind = 'EOD'
                           AND run.valuation_date = %s)
                    FROM vrp.pipeline_runs AS run
                    WHERE run.run_kind = 'EOD'
                      AND run.valuation_date = %s
                    """,
                    (snapshot.valuation_date,) * 8,
                )
                self.assertEqual(cursor.fetchone(), (1, 1, 9, 9, 9, 18, 1, 0))
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM vrp.pipeline_runs
                    WHERE run_kind = 'EOD'
                      AND valuation_date = %s
                      AND code_version = %s
                    """,
                    (snapshot.valuation_date, EOD_ROLLBACK_CODE_VERSION),
                )
                self.assertEqual(cursor.fetchone()[0], 0)

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
                    FROM vrp.current_reference_rate_observations AS observation
                    JOIN vrp.reference_data_releases AS release
                      ON release.reference_data_release_id =
                         observation.reference_data_release_id
                    WHERE observation.series_key = 'SOFR'
                      AND release.dataset_key = 'FRED_SOFR'
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
