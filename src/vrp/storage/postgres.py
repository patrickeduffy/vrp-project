"""Thin Psycopg-compatible adapter for append-only compact reference data.

The repository deliberately receives a cursor and never commits or rolls back.
The EOD orchestrator owns the transaction containing assets, lineage, reference
rows, QA, and stage completion.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol, Sequence
from uuid import UUID

from .reference_data import (
    DailyMarketFeatureDefinition,
    ReferenceDataRelease,
    StoredDailyMarketFeature,
    StoredReferenceRateObservation,
    render_json_object,
)


class DatabaseConfigurationError(RuntimeError):
    """Raised when database access is requested without safe configuration."""


class ReferenceDataConflict(RuntimeError):
    """Raised when an immutable identity is reused with different content."""


class Connection(Protocol):
    autocommit: bool


class Cursor(Protocol):
    connection: Connection

    def execute(self, query: str, params: Sequence[Any] | None = None) -> Any: ...

    def fetchone(self) -> Sequence[Any] | None: ...


@dataclass(frozen=True)
class InsertResult:
    record_id: UUID
    inserted: bool


class PostgresReferenceDataRepository:
    """Static, parameter-bound writes against migration 0002."""

    def __init__(self, cursor: Cursor):
        self._cursor = cursor

    def acquire_dataset_lock(self, dataset_key: str) -> None:
        """Serialize correction chains for one dataset inside the caller's transaction."""

        if not isinstance(dataset_key, str) or not dataset_key.strip():
            raise ValueError("dataset_key must be a non-empty string")
        if not hasattr(self._cursor, "connection") or self._cursor.connection.autocommit:
            raise DatabaseConfigurationError(
                "dataset locks require a non-autocommit outer transaction"
            )
        dataset_key = dataset_key.strip()
        self._cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"vrp/reference-data/{dataset_key}",),
        )

    def register_release(self, release: ReferenceDataRelease) -> InsertResult:
        self._cursor.execute(
            """
            INSERT INTO vrp.reference_data_releases (
                reference_data_release_id,
                dataset_key,
                dataset_kind,
                dataset_schema_version,
                normalized_content_sha256,
                source_system,
                loader_version,
                normalized_data_asset_id,
                qa_manifest_data_asset_id,
                loaded_by_pipeline_run_id,
                vintage_kind,
                source_published_at,
                retrieved_at,
                observation_start_date,
                observation_end_date,
                source_row_count,
                persisted_row_count,
                metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            ON CONFLICT (
                dataset_key,
                dataset_schema_version,
                normalized_content_sha256
            ) DO NOTHING
            RETURNING reference_data_release_id
            """,
            (
                release.release_id,
                release.dataset_key,
                release.dataset_kind,
                release.dataset_schema_version,
                release.normalized_content_sha256,
                release.source_system,
                release.loader_version,
                release.normalized_data_asset_id,
                release.qa_manifest_data_asset_id,
                release.loaded_by_pipeline_run_id,
                release.vintage_kind,
                release.source_published_at,
                release.retrieved_at,
                release.observation_start_date,
                release.observation_end_date,
                release.source_row_count,
                release.persisted_row_count,
                render_json_object(release.metadata),
            ),
        )
        inserted = self._cursor.fetchone()
        if inserted is not None:
            return InsertResult(record_id=inserted[0], inserted=True)
        self._cursor.execute(
            """
            SELECT
                reference_data_release_id,
                dataset_kind,
                source_system,
                vintage_kind,
                observation_start_date,
                observation_end_date,
                source_row_count,
                persisted_row_count
            FROM vrp.reference_data_releases
            WHERE dataset_key = %s
              AND dataset_schema_version = %s
              AND normalized_content_sha256 = %s
            """,
            (
                release.dataset_key,
                release.dataset_schema_version,
                release.normalized_content_sha256,
            ),
        )
        existing = self._cursor.fetchone()
        if existing is None:
            raise ReferenceDataConflict("release identity conflicted without a matching digest")
        expected_contract = (
            release.dataset_kind,
            release.source_system,
            release.vintage_kind,
            release.observation_start_date,
            release.observation_end_date,
            release.source_row_count,
            release.persisted_row_count,
        )
        if tuple(existing[1:]) != expected_contract:
            raise ReferenceDataConflict(
                "matching release digest has a different immutable data contract"
            )
        return InsertResult(record_id=existing[0], inserted=False)

    def register_feature_definition(
        self,
        definition: DailyMarketFeatureDefinition,
    ) -> InsertResult:
        self._cursor.execute(
            """
            INSERT INTO vrp.daily_market_feature_definitions (
                daily_market_feature_definition_id,
                definition_key,
                version_label,
                content_sha256,
                price_adjustment,
                return_formula_version,
                rsi_formula_version,
                rv_formula_version,
                definition
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING daily_market_feature_definition_id
            """,
            (
                definition.definition_id,
                definition.definition_key,
                definition.version_label,
                definition.content_sha256,
                definition.price_adjustment,
                definition.return_formula_version,
                definition.rsi_formula_version,
                definition.rv_formula_version,
                render_json_object(definition.definition),
            ),
        )
        inserted = self._cursor.fetchone()
        if inserted is not None:
            return InsertResult(record_id=inserted[0], inserted=True)
        self._cursor.execute(
            """
            SELECT daily_market_feature_definition_id, content_sha256
            FROM vrp.daily_market_feature_definitions
            WHERE definition_key = %s AND version_label = %s
            """,
            (definition.definition_key, definition.version_label),
        )
        existing = self._cursor.fetchone()
        if existing is None:
            raise ReferenceDataConflict(
                "feature definition content is already registered under another identity"
            )
        if existing[1] != definition.content_sha256:
            raise ReferenceDataConflict(
                "feature definition label already exists with different content"
            )
        return InsertResult(record_id=existing[0], inserted=False)

    def append_reference_rate(self, row: StoredReferenceRateObservation) -> None:
        value = row.value
        self._cursor.execute(
            """
            INSERT INTO vrp.reference_rate_observations (
                reference_rate_observation_id,
                reference_data_release_id,
                series_key,
                observation_date,
                rate_percent,
                supersedes_observation_id,
                row_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row.observation_id,
                row.release_id,
                value.series_key,
                value.observation_date,
                value.rate_percent,
                row.supersedes_observation_id,
                value.row_sha256,
            ),
        )

    def append_daily_market_feature(self, row: StoredDailyMarketFeature) -> None:
        value = row.value
        self._cursor.execute(
            """
            INSERT INTO vrp.daily_market_features (
                daily_market_feature_id,
                daily_market_feature_definition_id,
                reference_data_release_id,
                symbol,
                trade_date,
                prior_trade_date,
                spy_close,
                spy_change,
                spy_log_return,
                wilder_avg_gain_14,
                wilder_avg_loss_14,
                rsi14,
                rv21d_variance,
                rv21d_volatility_pct,
                calculation_status,
                quality_status,
                supersedes_daily_market_feature_id,
                row_sha256,
                details
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                row.feature_id,
                row.definition_id,
                row.release_id,
                value.symbol,
                value.trade_date,
                value.prior_trade_date,
                value.spy_close,
                value.spy_change,
                value.spy_log_return,
                value.wilder_avg_gain_14,
                value.wilder_avg_loss_14,
                value.rsi14,
                value.rv21d_variance,
                value.rv21d_volatility_pct,
                value.calculation_status,
                value.quality_status,
                row.supersedes_feature_id,
                value.row_sha256,
                render_json_object(value.details),
            ),
        )


def connect_from_environment(variable_name: str = "VRP_DATABASE_URL") -> Any:
    """Create a Psycopg connection without placing credentials in arguments or logs."""

    dsn = os.environ.get(variable_name)
    if not dsn:
        raise DatabaseConfigurationError(f"{variable_name} is not configured")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is installed in production/CI
        raise DatabaseConfigurationError(
            "Psycopg is not installed; install the project's pinned dependencies"
        ) from exc
    return psycopg.connect(dsn)
