"""Transaction-neutral PostgreSQL persistence for deterministic EOD shadow imports.

The repository receives a Psycopg-compatible cursor.  It deliberately never
commits or rolls back: the EOD service owns the outer transaction that covers
the advisory lock, immutable registrations, imported output, reconciliation
evidence, stage completion, and final run transition.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import UUID

from .postgres import DatabaseConfigurationError, InsertResult
from .reference_data import render_json_object


EXPECTED_EOD_TENORS = (9, 12, 15, 18, 21, 24, 27, 30, 33)
_EXPECTED_EOD_TENOR_SET = frozenset(EXPECTED_EOD_TENORS)
_EXPECTED_EVALUATION_IDENTITIES = frozenset(
    (tenor, layer)
    for tenor in EXPECTED_EOD_TENORS
    for layer in ("CORE", "SECONDARY")
)


class EodRepositoryConflict(RuntimeError):
    """Raised when an immutable EOD identity is reused inconsistently."""


class EodRunConflict(EodRepositoryConflict):
    """Raised when a logical EOD run cannot be started or transitioned safely."""


class EodReferenceDataMissing(EodRepositoryConflict):
    """Raised when an exact accepted reference-data pin cannot be resolved."""


@dataclass(frozen=True)
class EodRunState:
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
    status: str
    qa_status: str
    invocation: Mapping[str, Any]
    metadata: Mapping[str, Any]
    inserted: bool = False

    @property
    def is_completed_pass(self) -> bool:
        return self.status == "COMPLETED" and self.qa_status == "PASS"


@dataclass(frozen=True)
class EodDataAsset:
    data_asset_id: UUID
    dataset_name: str
    asset_class: str
    asset_format: str
    storage_uri: str
    content_sha256: str
    captured_at: datetime
    schema_version: str | None = None
    source_system: str | None = None
    observation_start_at: datetime | None = None
    observation_end_at: datetime | None = None
    trade_date_start: date | None = None
    trade_date_end: date | None = None
    row_count: int | None = None
    byte_size: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SofrReference:
    observation_id: UUID
    reference_data_release_id: UUID
    observation_date: date
    rate_decimal: Decimal
    row_sha256: str
    normalized_content_sha256: str


@dataclass(frozen=True)
class SpyDailyFeatureReference:
    daily_market_feature_id: UUID
    daily_market_feature_definition_id: UUID
    reference_data_release_id: UUID
    trade_date: date
    spy_close: Decimal
    rsi14: float | None
    rv21d_variance: float | None
    rv21d_volatility_pct: float | None
    calculation_status: str
    quality_status: str
    row_sha256: str


@dataclass(frozen=True)
class MarketSnapshotRow:
    market_snapshot_id: UUID
    pipeline_run_id: UUID
    valuation_date: date
    snapshot_at: datetime
    source_latest_at: datetime | None
    freshness_status: str
    spx_spot: Decimal | float | None
    spy_price: Decimal | float | None
    sofr_rate: Decimal
    sofr_observation_date: date
    sofr_observation_id: UUID
    daily_market_feature_id: UUID
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ImpliedVarianceRow:
    implied_variance_id: UUID
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    tenor_days: int
    target_expiration: date | None
    effective_dte: float | None
    annualized_variance: float | None
    annualized_volatility_pct: float | None
    calculation_status: str
    quality_status: str
    source_quote_at: datetime | None
    quality_details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ForecastVarianceRow:
    forecast_variance_id: UUID
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    tenor_days: int
    forecast_as_of_date: date
    predicted_log_variance: float | None
    annualized_variance: float | None
    annualized_volatility_pct: float | None
    calculation_status: str
    quality_status: str
    quality_details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignalFeatureRow:
    signal_feature_id: UUID
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    tenor_days: int
    tenor_bucket: str
    implied_variance_id: UUID
    forecast_variance_id: UUID
    vrp_log: float | None
    vrp_3m_prior_mean: float | None
    vrp_3m_prior_sample_std: float | None
    vrp_1y_prior_mean: float | None
    vrp_1y_prior_sample_std: float | None
    zscore_3m: float | None
    zscore_1y: float | None
    rsi14: float | None
    rv21d_variance: float | None
    rv21d_volatility_pct: float | None
    zscore_3m_sample_count: int | None
    zscore_1y_sample_count: int | None
    history_through_date: date | None
    is_complete: bool
    daily_market_feature_id: UUID
    rsi14_source_kind: str = "DAILY_OFFICIAL"
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignalEvaluationRow:
    signal_evaluation_id: UUID
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    signal_feature_id: UUID
    tenor_days: int
    tenor_bucket: str
    signal_layer: str
    evaluation_status: str
    qualifies: bool
    vrp_pass: bool | None
    zscore_3m_pass: bool | None
    zscore_1y_pass: bool | None
    rsi14_pass: bool | None
    rv21d_pass: bool | None
    threshold_values: Mapping[str, Any]
    comparison_results: Mapping[str, Any]
    failed_checks: Sequence[str]
    rank_position: int | None
    rank_score: float | None
    target_size_pct_nav: Decimal | float | None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SelectedSignalRow:
    selected_signal_id: UUID
    pipeline_run_id: UUID
    market_snapshot_id: UUID
    selected_evaluation_id: UUID | None
    decision: str
    signal_state: str
    selection_rule_id: str
    no_trade_reason: str | None
    approved_nav_dollars: Decimal | float | None
    target_max_risk_dollars: Decimal | float | None
    first_observed_at: datetime | None
    consecutive_snapshots: int | None
    selection_trace: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QaResultRow:
    qa_result_id: UUID
    pipeline_run_id: UUID
    stage_name: str | None
    check_code: str
    scope_key: str
    severity: str
    outcome: str
    is_hard_gate: bool
    message: str
    observed_value: Mapping[str, Any] | None = None
    expected_value: Mapping[str, Any] | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EodRepositoryConflict("PostgreSQL returned a non-object JSON contract")
    return dict(value)


def _same_json(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return render_json_object(left) == render_json_object(right)


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _required_sha256(value: str, name: str) -> str:
    candidate = _required_text(value, name)
    if re.fullmatch(r"[0-9a-f]{64}", candidate) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return candidate


def _nine_tenor_rows(rows: Sequence[Any], kind: str) -> tuple[Any, ...]:
    materialized = tuple(rows)
    tenors = [row.tenor_days for row in materialized]
    if len(materialized) != 9 or frozenset(tenors) != _EXPECTED_EOD_TENOR_SET:
        raise ValueError(f"{kind} must contain exactly one row for each EOD tenor")
    if len(set(tenors)) != 9:
        raise ValueError(f"{kind} contains duplicate tenors")
    return tuple(sorted(materialized, key=lambda row: row.tenor_days))


class PostgresEodRepository:
    """Parameter-bound SQL adapter for one atomic EOD shadow-import transaction."""

    def __init__(self, cursor: Any):
        self.cursor = cursor

    def acquire_logical_run_lock(self, *, environment: str, idempotency_key: str) -> None:
        environment = _required_text(environment, "environment")
        idempotency_key = _required_text(idempotency_key, "idempotency_key")
        if not hasattr(self.cursor, "connection") or self.cursor.connection.autocommit:
            raise DatabaseConfigurationError(
                "EOD logical-run locks require a non-autocommit outer transaction"
            )
        self.cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"vrp/eod/{environment}/{idempotency_key}",),
        )

    def register_model_version(
        self,
        *,
        model_version_id: UUID,
        model_key: str,
        version_label: str,
        content_sha256: str,
        manifest: Mapping[str, Any],
        locked_at: datetime,
    ) -> UUID:
        model_key = _required_text(model_key, "model_key")
        version_label = _required_text(version_label, "version_label")
        rendered_manifest = render_json_object(manifest)
        self.cursor.execute(
            """
            INSERT INTO vrp.model_versions (
                model_version_id, model_key, version_label, content_sha256,
                manifest, is_locked, locked_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, TRUE, %s)
            ON CONFLICT DO NOTHING
            RETURNING model_version_id
            """,
            (
                model_version_id,
                model_key,
                version_label,
                content_sha256,
                rendered_manifest,
                locked_at,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return inserted[0]
        self.cursor.execute(
            """
            SELECT model_version_id, content_sha256, manifest, is_locked, locked_at
            FROM vrp.model_versions
            WHERE model_key = %s AND version_label = %s
            """,
            (model_key, version_label),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            raise EodRepositoryConflict(
                "model content is registered under a conflicting immutable identity"
            )
        observed_manifest = _mapping(existing[2])
        if (
            existing[1] != content_sha256
            or not _same_json(observed_manifest, manifest)
            or existing[3] is not True
            or existing[4] != locked_at
        ):
            raise EodRepositoryConflict(
                "model key/version already exists with different immutable content"
            )
        return existing[0]

    def register_configuration_version(
        self,
        *,
        configuration_version_id: UUID,
        configuration_key: str,
        version_label: str,
        content_sha256: str,
        configuration: Mapping[str, Any],
    ) -> UUID:
        configuration_key = _required_text(configuration_key, "configuration_key")
        version_label = _required_text(version_label, "version_label")
        rendered_configuration = render_json_object(configuration)
        self.cursor.execute(
            """
            INSERT INTO vrp.configuration_versions (
                configuration_version_id, configuration_key, version_label,
                content_sha256, configuration
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING configuration_version_id
            """,
            (
                configuration_version_id,
                configuration_key,
                version_label,
                content_sha256,
                rendered_configuration,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return inserted[0]
        self.cursor.execute(
            """
            SELECT configuration_version_id, content_sha256, configuration
            FROM vrp.configuration_versions
            WHERE configuration_key = %s AND version_label = %s
            """,
            (configuration_key, version_label),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            raise EodRepositoryConflict(
                "configuration content is registered under a conflicting immutable identity"
            )
        if existing[1] != content_sha256 or not _same_json(
            _mapping(existing[2]), configuration
        ):
            raise EodRepositoryConflict(
                "configuration key/version already exists with different immutable content"
            )
        return existing[0]

    @staticmethod
    def _run_state(row: Sequence[Any], *, inserted: bool) -> EodRunState:
        return EodRunState(
            pipeline_run_id=row[0],
            environment=row[1],
            idempotency_key=row[2],
            valuation_date=row[3],
            snapshot_at=row[4],
            data_cutoff_at=row[5],
            model_version_id=row[6],
            configuration_version_id=row[7],
            code_version=row[8],
            orchestrator_version=row[9],
            status=row[10],
            qa_status=row[11],
            invocation=_mapping(row[12]),
            metadata=_mapping(row[13]),
            inserted=inserted,
        )

    def find_run(self, *, environment: str, idempotency_key: str) -> EodRunState | None:
        self.cursor.execute(
            """
            SELECT
                pipeline_run_id, environment, idempotency_key, valuation_date,
                snapshot_at, data_cutoff_at, model_version_id,
                configuration_version_id, code_version, orchestrator_version,
                status, qa_status, invocation, metadata
            FROM vrp.pipeline_runs
            WHERE environment = %s
              AND idempotency_key = %s
              AND run_kind = 'EOD'
            """,
            (environment, idempotency_key),
        )
        row = self.cursor.fetchone()
        return None if row is None else self._run_state(row, inserted=False)

    def begin_run(
        self,
        *,
        pipeline_run_id: UUID,
        environment: str,
        idempotency_key: str,
        valuation_date: date,
        snapshot_at: datetime,
        data_cutoff_at: datetime,
        model_version_id: UUID,
        configuration_version_id: UUID,
        code_version: str,
        orchestrator_version: str,
        requested_by: str | None,
        invocation: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> EodRunState:
        rendered_invocation = render_json_object(invocation)
        rendered_metadata = render_json_object(metadata)
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_runs (
                pipeline_run_id, environment, idempotency_key, run_kind,
                valuation_date, snapshot_at, data_cutoff_at, model_version_id,
                configuration_version_id, code_version, orchestrator_version,
                status, qa_status, started_at, requested_by, invocation, metadata
            ) VALUES (
                %s, %s, %s, 'EOD', %s, %s, %s, %s, %s, %s, %s,
                'RUNNING', 'PENDING', CURRENT_TIMESTAMP, %s, %s::jsonb, %s::jsonb
            )
            ON CONFLICT (environment, idempotency_key) DO NOTHING
            RETURNING
                pipeline_run_id, environment, idempotency_key, valuation_date,
                snapshot_at, data_cutoff_at, model_version_id,
                configuration_version_id, code_version, orchestrator_version,
                status, qa_status, invocation, metadata
            """,
            (
                pipeline_run_id,
                environment,
                idempotency_key,
                valuation_date,
                snapshot_at,
                data_cutoff_at,
                model_version_id,
                configuration_version_id,
                code_version,
                orchestrator_version,
                requested_by,
                rendered_invocation,
                rendered_metadata,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return self._run_state(inserted, inserted=True)

        existing = self.find_run(environment=environment, idempotency_key=idempotency_key)
        if existing is None:
            raise EodRunConflict("logical EOD run identity conflicted without an EOD row")
        expected_contract = (
            valuation_date,
            snapshot_at,
            data_cutoff_at,
            model_version_id,
            configuration_version_id,
            code_version,
            orchestrator_version,
        )
        observed_contract = (
            existing.valuation_date,
            existing.snapshot_at,
            existing.data_cutoff_at,
            existing.model_version_id,
            existing.configuration_version_id,
            existing.code_version,
            existing.orchestrator_version,
        )
        if (
            observed_contract != expected_contract
            or not _same_json(existing.invocation, invocation)
            or not _same_json(existing.metadata, metadata)
        ):
            raise EodRunConflict("idempotent EOD run has a different immutable contract")
        if not existing.is_completed_pass:
            raise EodRunConflict(
                "existing EOD run is not COMPLETED/PASS; explicit recovery is required"
            )
        return existing

    def start_shadow_import_stage(
        self,
        *,
        pipeline_stage_id: UUID,
        pipeline_run_id: UUID,
        stage_name: str,
        input_fingerprint: str,
        stage_order: int = 0,
    ) -> UUID:
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_run_stages (
                pipeline_stage_id, pipeline_run_id, stage_name, stage_order,
                is_required, status, attempt_count, input_fingerprint, started_at
            ) VALUES (%s, %s, %s, %s, TRUE, 'RUNNING', 1, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING
            RETURNING pipeline_stage_id
            """,
            (
                pipeline_stage_id,
                pipeline_run_id,
                stage_name,
                stage_order,
                input_fingerprint,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is None:
            raise EodRunConflict("shadow-import stage identity is already in use")
        return inserted[0]

    def complete_shadow_import_stage(
        self,
        *,
        pipeline_run_id: UUID,
        stage_name: str,
        output_fingerprint: str,
        metrics: Mapping[str, Any],
    ) -> UUID:
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_run_stages
            SET status = 'COMPLETED',
                output_fingerprint = %s,
                finished_at = CURRENT_TIMESTAMP,
                metrics = %s::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s
              AND stage_name = %s
              AND status = 'RUNNING'
            RETURNING pipeline_stage_id
            """,
            (
                output_fingerprint,
                render_json_object(metrics),
                pipeline_run_id,
                stage_name,
            ),
        )
        completed = self.cursor.fetchone()
        if completed is None:
            raise EodRunConflict("shadow-import stage did not transition to COMPLETED")
        return completed[0]

    def register_asset(self, asset: EodDataAsset) -> InsertResult:
        rendered_metadata = render_json_object(asset.metadata)
        self.cursor.execute(
            """
            INSERT INTO vrp.data_assets (
                data_asset_id, dataset_name, asset_class, asset_format,
                storage_uri, content_sha256, schema_version, source_system,
                captured_at, observation_start_at, observation_end_at,
                trade_date_start, trade_date_end, row_count, byte_size,
                is_immutable, metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, TRUE, %s::jsonb
            )
            ON CONFLICT (dataset_name, storage_uri, content_sha256) DO NOTHING
            RETURNING data_asset_id
            """,
            (
                asset.data_asset_id,
                asset.dataset_name,
                asset.asset_class,
                asset.asset_format,
                asset.storage_uri,
                asset.content_sha256,
                asset.schema_version,
                asset.source_system,
                asset.captured_at,
                asset.observation_start_at,
                asset.observation_end_at,
                asset.trade_date_start,
                asset.trade_date_end,
                asset.row_count,
                asset.byte_size,
                rendered_metadata,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return InsertResult(record_id=inserted[0], inserted=True)
        self.cursor.execute(
            """
            SELECT
                data_asset_id, asset_class, asset_format, schema_version,
                source_system, observation_start_at, observation_end_at,
                trade_date_start, trade_date_end, row_count, byte_size,
                is_immutable, metadata
            FROM vrp.data_assets
            WHERE dataset_name = %s AND storage_uri = %s AND content_sha256 = %s
            """,
            (asset.dataset_name, asset.storage_uri, asset.content_sha256),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            raise EodRepositoryConflict("asset digest conflicted without a matching row")
        observed = (
            *existing[1:-1],
            render_json_object(_mapping(existing[-1])),
        )
        expected = (
            asset.asset_class,
            asset.asset_format,
            asset.schema_version,
            asset.source_system,
            asset.observation_start_at,
            asset.observation_end_at,
            asset.trade_date_start,
            asset.trade_date_end,
            asset.row_count,
            asset.byte_size,
            True,
            rendered_metadata,
        )
        if observed != expected:
            raise EodRepositoryConflict(
                "matching asset digest has a different immutable contract"
            )
        return InsertResult(record_id=existing[0], inserted=False)

    def link_asset(
        self,
        *,
        pipeline_run_id: UUID,
        data_asset_id: UUID,
        usage_role: str,
        logical_name: str,
        stage_name: str,
        lineage: Mapping[str, Any],
        is_required: bool = True,
    ) -> None:
        rendered_lineage = render_json_object(lineage)
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_run_data_assets (
                pipeline_run_id, data_asset_id, usage_role, logical_name,
                stage_name, is_required, lineage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING data_asset_id
            """,
            (
                pipeline_run_id,
                data_asset_id,
                usage_role,
                logical_name,
                stage_name,
                is_required,
                rendered_lineage,
            ),
        )
        if self.cursor.fetchone() is not None:
            return
        self.cursor.execute(
            """
            SELECT stage_name, is_required, lineage
            FROM vrp.pipeline_run_data_assets
            WHERE pipeline_run_id = %s
              AND data_asset_id = %s
              AND usage_role = %s
              AND logical_name = %s
            """,
            (pipeline_run_id, data_asset_id, usage_role, logical_name),
        )
        existing = self.cursor.fetchone()
        if (
            existing is None
            or existing[0] != stage_name
            or existing[1] is not is_required
            or render_json_object(_mapping(existing[2])) != rendered_lineage
        ):
            raise EodRepositoryConflict("asset link has a different lineage contract")

    def resolve_sofr_before(
        self,
        valuation_date: date,
        *,
        normalized_content_sha256: str,
    ) -> SofrReference:
        normalized_content_sha256 = _required_sha256(
            normalized_content_sha256,
            "normalized_content_sha256",
        )
        self.cursor.execute(
            """
            SELECT
                observation.reference_rate_observation_id,
                observation.reference_data_release_id,
                observation.observation_date,
                observation.rate_decimal,
                observation.row_sha256,
                release.normalized_content_sha256
            FROM vrp.current_reference_rate_observations AS observation
            JOIN vrp.reference_data_releases AS release
              ON release.reference_data_release_id =
                 observation.reference_data_release_id
            WHERE observation.series_key = %s
              AND observation.observation_date < %s
              AND release.source_system = 'FRED'
              AND release.normalized_content_sha256 = %s
            ORDER BY observation.observation_date DESC
            LIMIT 1
            """,
            ("SOFR", valuation_date, normalized_content_sha256),
        )
        row = self.cursor.fetchone()
        if row is None:
            raise EodReferenceDataMissing(
                "no current SOFR observation exists strictly before the valuation date"
            )
        return SofrReference(row[0], row[1], row[2], row[3], row[4], row[5])

    def resolve_spy_feature(
        self,
        *,
        valuation_date: date,
        definition_key: str,
        version_label: str,
        definition_content_sha256: str,
    ) -> SpyDailyFeatureReference:
        definition_key = _required_text(definition_key, "definition_key")
        version_label = _required_text(version_label, "version_label")
        definition_content_sha256 = _required_sha256(
            definition_content_sha256,
            "definition_content_sha256",
        )
        self.cursor.execute(
            """
            SELECT
                feature.daily_market_feature_id,
                feature.daily_market_feature_definition_id,
                feature.reference_data_release_id,
                feature.trade_date,
                feature.spy_close,
                feature.rsi14,
                feature.rv21d_variance,
                feature.rv21d_volatility_pct,
                feature.calculation_status,
                feature.quality_status,
                feature.row_sha256
            FROM vrp.current_daily_market_features AS feature
            JOIN vrp.daily_market_feature_definitions AS definition
              ON definition.daily_market_feature_definition_id =
                 feature.daily_market_feature_definition_id
            WHERE feature.symbol = %s
              AND feature.trade_date = %s
              AND definition.definition_key = %s
              AND definition.version_label = %s
              AND definition.content_sha256 = %s
            """,
            (
                "SPY",
                valuation_date,
                definition_key,
                version_label,
                definition_content_sha256,
            ),
        )
        row = self.cursor.fetchone()
        if row is None:
            raise EodReferenceDataMissing(
                "no exact current SPY daily feature exists for the valuation date"
            )
        return SpyDailyFeatureReference(*row)

    def insert_market_snapshot(self, row: MarketSnapshotRow) -> None:
        self.cursor.execute(
            """
            INSERT INTO vrp.market_snapshots (
                market_snapshot_id, pipeline_run_id, valuation_date, snapshot_at,
                snapshot_kind, market_session, source_latest_at, freshness_status,
                spx_spot, spy_price, sofr_rate, sofr_observation_date,
                sofr_observation_id, daily_market_feature_id, details
            ) VALUES (
                %s, %s, %s, %s, 'EOD_OFFICIAL', 'CLOSED', %s, %s, %s, %s,
                %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                row.market_snapshot_id,
                row.pipeline_run_id,
                row.valuation_date,
                row.snapshot_at,
                row.source_latest_at,
                row.freshness_status,
                row.spx_spot,
                row.spy_price,
                row.sofr_rate,
                row.sofr_observation_date,
                row.sofr_observation_id,
                row.daily_market_feature_id,
                render_json_object(row.details),
            ),
        )

    def insert_implied_variances(self, rows: Sequence[ImpliedVarianceRow]) -> None:
        for row in _nine_tenor_rows(rows, "implied variances"):
            self.cursor.execute(
                """
                INSERT INTO vrp.implied_variance_term_structure (
                    implied_variance_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, target_expiration, effective_dte,
                    annualized_variance, annualized_volatility_pct,
                    calculation_status, quality_status, source_quote_at,
                    quality_details
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                """,
                (
                    row.implied_variance_id,
                    row.pipeline_run_id,
                    row.market_snapshot_id,
                    row.tenor_days,
                    row.target_expiration,
                    row.effective_dte,
                    row.annualized_variance,
                    row.annualized_volatility_pct,
                    row.calculation_status,
                    row.quality_status,
                    row.source_quote_at,
                    render_json_object(row.quality_details),
                ),
            )

    def insert_forecast_variances(self, rows: Sequence[ForecastVarianceRow]) -> None:
        for row in _nine_tenor_rows(rows, "forecast variances"):
            self.cursor.execute(
                """
                INSERT INTO vrp.forecast_variance_term_structure (
                    forecast_variance_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, forecast_as_of_date, predicted_log_variance,
                    annualized_variance, annualized_volatility_pct,
                    calculation_status, quality_status, quality_details
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                """,
                (
                    row.forecast_variance_id,
                    row.pipeline_run_id,
                    row.market_snapshot_id,
                    row.tenor_days,
                    row.forecast_as_of_date,
                    row.predicted_log_variance,
                    row.annualized_variance,
                    row.annualized_volatility_pct,
                    row.calculation_status,
                    row.quality_status,
                    render_json_object(row.quality_details),
                ),
            )

    def insert_signal_features(self, rows: Sequence[SignalFeatureRow]) -> None:
        for row in _nine_tenor_rows(rows, "signal features"):
            self.cursor.execute(
                """
                INSERT INTO vrp.signal_features (
                    signal_feature_id, pipeline_run_id, market_snapshot_id,
                    tenor_days, tenor_bucket, implied_variance_id,
                    forecast_variance_id, vrp_log, vrp_3m_prior_mean,
                    vrp_3m_prior_sample_std, vrp_1y_prior_mean,
                    vrp_1y_prior_sample_std, zscore_3m, zscore_1y, rsi14,
                    rv21d_variance, rv21d_volatility_pct,
                    zscore_3m_sample_count, zscore_1y_sample_count,
                    history_through_date, is_complete, daily_market_feature_id,
                    rsi14_source_kind, details
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                """,
                (
                    row.signal_feature_id,
                    row.pipeline_run_id,
                    row.market_snapshot_id,
                    row.tenor_days,
                    row.tenor_bucket,
                    row.implied_variance_id,
                    row.forecast_variance_id,
                    row.vrp_log,
                    row.vrp_3m_prior_mean,
                    row.vrp_3m_prior_sample_std,
                    row.vrp_1y_prior_mean,
                    row.vrp_1y_prior_sample_std,
                    row.zscore_3m,
                    row.zscore_1y,
                    row.rsi14,
                    row.rv21d_variance,
                    row.rv21d_volatility_pct,
                    row.zscore_3m_sample_count,
                    row.zscore_1y_sample_count,
                    row.history_through_date,
                    row.is_complete,
                    row.daily_market_feature_id,
                    row.rsi14_source_kind,
                    render_json_object(row.details),
                ),
            )

    def insert_signal_evaluations(
        self, rows: Sequence[SignalEvaluationRow]
    ) -> None:
        materialized = tuple(rows)
        if any(row.signal_layer not in {"CORE", "SECONDARY"} for row in materialized):
            raise ValueError("shadow import accepts only CORE and SECONDARY evaluations")
        identities = {(row.tenor_days, row.signal_layer) for row in materialized}
        if (
            len(materialized) != 18
            or len(identities) != 18
            or frozenset(identities) != _EXPECTED_EVALUATION_IDENTITIES
        ):
            raise ValueError(
                "signal evaluations must contain every EOD tenor for CORE and SECONDARY"
            )
        for row in sorted(materialized, key=lambda item: (item.tenor_days, item.signal_layer)):
            self.cursor.execute(
                """
                INSERT INTO vrp.signal_evaluations (
                    signal_evaluation_id, pipeline_run_id, market_snapshot_id,
                    signal_feature_id, tenor_days, tenor_bucket, signal_layer,
                    evaluation_status, qualifies, vrp_pass, zscore_3m_pass,
                    zscore_1y_pass, rsi14_pass, rv21d_pass, threshold_values,
                    comparison_results, failed_checks, rank_position, rank_score,
                    target_size_pct_nav, details
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s::jsonb
                )
                """,
                (
                    row.signal_evaluation_id,
                    row.pipeline_run_id,
                    row.market_snapshot_id,
                    row.signal_feature_id,
                    row.tenor_days,
                    row.tenor_bucket,
                    row.signal_layer,
                    row.evaluation_status,
                    row.qualifies,
                    row.vrp_pass,
                    row.zscore_3m_pass,
                    row.zscore_1y_pass,
                    row.rsi14_pass,
                    row.rv21d_pass,
                    render_json_object(row.threshold_values),
                    render_json_object(row.comparison_results),
                    list(row.failed_checks),
                    row.rank_position,
                    row.rank_score,
                    row.target_size_pct_nav,
                    render_json_object(row.details),
                ),
            )

    def insert_selected_signal(self, row: SelectedSignalRow) -> None:
        self.cursor.execute(
            """
            INSERT INTO vrp.selected_signals (
                selected_signal_id, pipeline_run_id, market_snapshot_id,
                selected_evaluation_id, decision, signal_state,
                selection_rule_id, no_trade_reason, approved_nav_dollars,
                target_max_risk_dollars, first_observed_at,
                consecutive_snapshots, selection_trace
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                row.selected_signal_id,
                row.pipeline_run_id,
                row.market_snapshot_id,
                row.selected_evaluation_id,
                row.decision,
                row.signal_state,
                row.selection_rule_id,
                row.no_trade_reason,
                row.approved_nav_dollars,
                row.target_max_risk_dollars,
                row.first_observed_at,
                row.consecutive_snapshots,
                render_json_object(row.selection_trace),
            ),
        )

    def record_qa(self, row: QaResultRow) -> None:
        self.cursor.execute(
            """
            INSERT INTO vrp.qa_results (
                qa_result_id, pipeline_run_id, stage_name, check_code,
                scope_key, severity, outcome, is_hard_gate, message,
                observed_value, expected_value, evidence
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            (
                row.qa_result_id,
                row.pipeline_run_id,
                row.stage_name,
                row.check_code,
                row.scope_key,
                row.severity,
                row.outcome,
                row.is_hard_gate,
                row.message,
                None
                if row.observed_value is None
                else render_json_object(row.observed_value),
                None
                if row.expected_value is None
                else render_json_object(row.expected_value),
                render_json_object(row.evidence),
            ),
        )

    def fetch_run_projection(self, pipeline_run_id: UUID) -> Mapping[str, Any]:
        """Return one stable, ordered JSON projection for read-back reconciliation."""

        # PostgreSQL renders timestamptz values inside JSON using the session's
        # TimeZone.  Keep the projection byte-stable across developer machines
        # and deployed environments without changing the session after the
        # caller-owned transaction ends.
        self.cursor.execute("SET LOCAL TIME ZONE 'UTC'")
        self.cursor.execute(
            """
            SELECT jsonb_build_object(
                'run', jsonb_build_object(
                    'pipeline_run_id', run.pipeline_run_id,
                    'environment', run.environment,
                    'idempotency_key', run.idempotency_key,
                    'run_kind', run.run_kind,
                    'valuation_date', run.valuation_date,
                    'snapshot_at', run.snapshot_at,
                    'data_cutoff_at', run.data_cutoff_at,
                    'model_version_id', run.model_version_id,
                    'configuration_version_id', run.configuration_version_id,
                    'code_version', run.code_version,
                    'orchestrator_version', run.orchestrator_version,
                    'invocation', run.invocation,
                    'metadata', run.metadata
                ),
                'assets', COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'data_asset_id', asset.data_asset_id,
                            'dataset_name', asset.dataset_name,
                            'asset_class', asset.asset_class,
                            'asset_format', asset.asset_format,
                            'content_sha256', asset.content_sha256,
                            'storage_uri', asset.storage_uri,
                            'schema_version', asset.schema_version,
                            'source_system', asset.source_system,
                            'observation_start_at', asset.observation_start_at,
                            'observation_end_at', asset.observation_end_at,
                            'trade_date_start', asset.trade_date_start,
                            'trade_date_end', asset.trade_date_end,
                            'row_count', asset.row_count,
                            'byte_size', asset.byte_size,
                            'is_immutable', asset.is_immutable,
                            'metadata', asset.metadata,
                            'usage_role', link.usage_role,
                            'logical_name', link.logical_name,
                            'stage_name', link.stage_name,
                            'is_required', link.is_required,
                            'lineage', link.lineage
                        ) ORDER BY link.usage_role, link.logical_name, asset.data_asset_id
                    )
                    FROM vrp.pipeline_run_data_assets AS link
                    JOIN vrp.data_assets AS asset
                      ON asset.data_asset_id = link.data_asset_id
                    WHERE link.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'market_snapshot', (
                    SELECT to_jsonb(snapshot) - 'created_at'
                    FROM vrp.market_snapshots AS snapshot
                    WHERE snapshot.pipeline_run_id = run.pipeline_run_id
                ),
                'implied_variances', COALESCE((
                    SELECT jsonb_agg(to_jsonb(item) - 'computed_at' ORDER BY item.tenor_days)
                    FROM vrp.implied_variance_term_structure AS item
                    WHERE item.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'forecast_variances', COALESCE((
                    SELECT jsonb_agg(to_jsonb(item) - 'computed_at' ORDER BY item.tenor_days)
                    FROM vrp.forecast_variance_term_structure AS item
                    WHERE item.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'signal_features', COALESCE((
                    SELECT jsonb_agg(to_jsonb(item) - 'computed_at' ORDER BY item.tenor_days)
                    FROM vrp.signal_features AS item
                    WHERE item.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'signal_evaluations', COALESCE((
                    SELECT jsonb_agg(to_jsonb(item) - 'evaluated_at'
                                     ORDER BY item.tenor_days, item.signal_layer)
                    FROM vrp.signal_evaluations AS item
                    WHERE item.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'selected_signal', (
                    SELECT to_jsonb(item) - 'decided_at'
                    FROM vrp.selected_signals AS item
                    WHERE item.pipeline_run_id = run.pipeline_run_id
                )
            )
            FROM vrp.pipeline_runs AS run
            WHERE run.pipeline_run_id = %s
              AND run.run_kind = 'EOD'
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone()
        if row is None:
            raise EodRunConflict("EOD run does not exist for read-back reconciliation")
        projection = row[0]
        if isinstance(projection, str):
            projection = json.loads(projection)
        return _mapping(projection)

    def fetch_reconciliation_evidence(
        self, pipeline_run_id: UUID
    ) -> Mapping[str, Any]:
        """Return stable completed stage and QA evidence for idempotent no-ops."""

        self.cursor.execute(
            """
            SELECT jsonb_build_object(
                'stages', COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'pipeline_stage_id', stage.pipeline_stage_id,
                            'stage_name', stage.stage_name,
                            'stage_order', stage.stage_order,
                            'is_required', stage.is_required,
                            'status', stage.status,
                            'attempt_count', stage.attempt_count,
                            'input_fingerprint', stage.input_fingerprint,
                            'output_fingerprint', stage.output_fingerprint,
                            'last_error', stage.last_error,
                            'metrics', stage.metrics
                        ) ORDER BY stage.stage_order, stage.stage_name
                    )
                    FROM vrp.pipeline_run_stages AS stage
                    WHERE stage.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'qa_results', COALESCE((
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'qa_result_id', qa.qa_result_id,
                            'stage_name', qa.stage_name,
                            'check_code', qa.check_code,
                            'scope_key', qa.scope_key,
                            'severity', qa.severity,
                            'outcome', qa.outcome,
                            'is_hard_gate', qa.is_hard_gate,
                            'message', qa.message,
                            'observed_value', qa.observed_value,
                            'expected_value', qa.expected_value,
                            'evidence', qa.evidence
                        ) ORDER BY qa.check_code, qa.scope_key
                    )
                    FROM vrp.qa_results AS qa
                    WHERE qa.pipeline_run_id = run.pipeline_run_id
                ), '[]'::jsonb),
                'signal_publication_count', (
                    SELECT COUNT(*)
                    FROM vrp.signal_publications AS publication
                    WHERE publication.pipeline_run_id = run.pipeline_run_id
                )
            )
            FROM vrp.pipeline_runs AS run
            WHERE run.pipeline_run_id = %s
              AND run.run_kind = 'EOD'
              AND run.status = 'COMPLETED'
              AND run.qa_status = 'PASS'
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone()
        if row is None:
            raise EodRunConflict(
                "EOD run does not have COMPLETED/PASS reconciliation evidence"
            )
        evidence = row[0]
        if isinstance(evidence, str):
            evidence = json.loads(evidence)
        return _mapping(evidence)

    def finalize_run(self, pipeline_run_id: UUID) -> None:
        """Transition a fully reconciled shadow import to COMPLETED/PASS."""

        self.cursor.execute(
            """
            UPDATE vrp.pipeline_runs AS run
            SET status = 'COMPLETED',
                qa_status = 'PASS',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE run.pipeline_run_id = %s
              AND run.run_kind = 'EOD'
              AND run.status = 'RUNNING'
              AND NOT EXISTS (
                  SELECT 1 FROM vrp.pipeline_run_stages AS stage
                  WHERE stage.pipeline_run_id = run.pipeline_run_id
                    AND stage.is_required
                    AND stage.status <> 'COMPLETED'
              )
              AND (SELECT COUNT(*) FROM vrp.market_snapshots AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id) = 1
              AND (SELECT COUNT(*) FROM vrp.implied_variance_term_structure AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id) = 9
              AND (SELECT COUNT(*) FROM vrp.forecast_variance_term_structure AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id) = 9
              AND (SELECT COUNT(*) FROM vrp.signal_features AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id) = 9
              AND (SELECT COUNT(*) FROM vrp.signal_evaluations AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id
                     AND item.signal_layer IN ('CORE', 'SECONDARY')) = 18
              AND NOT EXISTS (
                  SELECT 1 FROM vrp.signal_evaluations AS item
                  WHERE item.pipeline_run_id = run.pipeline_run_id
                    AND item.signal_layer NOT IN ('CORE', 'SECONDARY')
              )
              AND (SELECT COUNT(*) FROM vrp.selected_signals AS item
                   WHERE item.pipeline_run_id = run.pipeline_run_id) = 1
              AND EXISTS (
                  SELECT 1 FROM vrp.qa_results AS qa
                  WHERE qa.pipeline_run_id = run.pipeline_run_id
                    AND qa.is_hard_gate
                    AND qa.outcome = 'PASS'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM vrp.qa_results AS qa
                  WHERE qa.pipeline_run_id = run.pipeline_run_id
                    AND qa.is_hard_gate
                    AND qa.outcome <> 'PASS'
              )
            RETURNING pipeline_run_id
            """,
            (pipeline_run_id,),
        )
        if self.cursor.fetchone() is None:
            raise EodRunConflict(
                "EOD run is incomplete, unreconciled, failed QA, or already finalized"
            )
