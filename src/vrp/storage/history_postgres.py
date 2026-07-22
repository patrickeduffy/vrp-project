"""PostgreSQL reads and operational ledger writes for reference-history loads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping
from uuid import UUID

from vrp.reference_history.models import CurrentRow, DataAsset

from .postgres import InsertResult, ReferenceDataConflict
from .reference_data import render_json_object


class ReferenceHistoryRunConflict(RuntimeError):
    """Raised when a deterministic loader run cannot be resumed safely."""


@dataclass(frozen=True)
class ExistingRelease:
    release_id: UUID
    dataset_key: str
    dataset_kind: str
    schema_version: str
    normalized_content_sha256: str
    source_system: str
    loader_version: str
    normalized_data_asset_id: UUID
    normalized_asset_sha256: str
    normalized_asset_storage_uri: str
    normalized_asset_byte_size: int
    qa_manifest_data_asset_id: UUID | None
    qa_asset_sha256: str | None
    qa_asset_storage_uri: str | None
    qa_asset_byte_size: int | None
    loaded_by_pipeline_run_id: UUID | None
    loaded_run_status: str | None
    loaded_run_qa_status: str | None
    vintage_kind: str
    retrieved_at: datetime
    source_row_count: int
    persisted_row_count: int
    observation_start_date: date | None
    observation_end_date: date | None
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class RunState:
    pipeline_run_id: UUID
    status: str
    qa_status: str
    inserted: bool


@dataclass(frozen=True)
class LinkedRunAsset:
    data_asset_id: UUID
    usage_role: str
    logical_name: str
    stage_name: str | None
    is_required: bool
    storage_uri: str
    content_sha256: str
    byte_size: int | None


@dataclass(frozen=True)
class CompletedRunEvidence:
    pipeline_run_id: UUID
    qa_data_asset_id: UUID
    assets: tuple[LinkedRunAsset, ...]


class PostgresReferenceHistoryRepository:
    def __init__(self, cursor: Any):
        self.cursor = cursor

    def find_release(
        self,
        dataset_key: str,
        schema_version: str,
        content_sha256: str,
    ) -> ExistingRelease | None:
        self.cursor.execute(
            """
            SELECT
                release.reference_data_release_id,
                release.dataset_key,
                release.dataset_kind,
                release.dataset_schema_version,
                release.normalized_content_sha256,
                release.source_system,
                release.loader_version,
                release.normalized_data_asset_id,
                normalized.content_sha256,
                normalized.storage_uri,
                normalized.byte_size,
                release.qa_manifest_data_asset_id,
                qa.content_sha256,
                qa.storage_uri,
                qa.byte_size,
                release.loaded_by_pipeline_run_id,
                run.status,
                run.qa_status,
                release.vintage_kind,
                release.retrieved_at,
                release.source_row_count,
                release.persisted_row_count,
                release.observation_start_date,
                release.observation_end_date,
                release.metadata
            FROM vrp.reference_data_releases AS release
            JOIN vrp.data_assets AS normalized
              ON normalized.data_asset_id = release.normalized_data_asset_id
            LEFT JOIN vrp.data_assets AS qa
              ON qa.data_asset_id = release.qa_manifest_data_asset_id
            LEFT JOIN vrp.pipeline_runs AS run
              ON run.pipeline_run_id = release.loaded_by_pipeline_run_id
            WHERE release.dataset_key = %s
              AND release.dataset_schema_version = %s
              AND release.normalized_content_sha256 = %s
            """,
            (dataset_key, schema_version, content_sha256),
        )
        row = self.cursor.fetchone()
        if row is None:
            return None
        return ExistingRelease(
            release_id=row[0],
            dataset_key=row[1],
            dataset_kind=row[2],
            schema_version=row[3],
            normalized_content_sha256=row[4],
            source_system=row[5],
            loader_version=row[6],
            normalized_data_asset_id=row[7],
            normalized_asset_sha256=row[8],
            normalized_asset_storage_uri=row[9],
            normalized_asset_byte_size=int(row[10]),
            qa_manifest_data_asset_id=row[11],
            qa_asset_sha256=row[12],
            qa_asset_storage_uri=row[13],
            qa_asset_byte_size=None if row[14] is None else int(row[14]),
            loaded_by_pipeline_run_id=row[15],
            loaded_run_status=row[16],
            loaded_run_qa_status=row[17],
            vintage_kind=row[18],
            retrieved_at=row[19],
            source_row_count=int(row[20]),
            persisted_row_count=int(row[21]),
            observation_start_date=row[22],
            observation_end_date=row[23],
            metadata=dict(row[24]),
        )

    def fetch_completed_run_evidence(self, pipeline_run_id: UUID) -> CompletedRunEvidence:
        self.cursor.execute(
            """
            SELECT
                run.status,
                run.qa_status,
                stage.status,
                qa.outcome,
                qa.is_hard_gate,
                qa.evidence->>'qa_data_asset_id'
            FROM vrp.pipeline_runs AS run
            JOIN vrp.pipeline_run_stages AS stage
              ON stage.pipeline_run_id = run.pipeline_run_id
             AND stage.stage_name = 'LOAD_REFERENCE_HISTORY'
            JOIN vrp.qa_results AS qa
              ON qa.pipeline_run_id = run.pipeline_run_id
             AND qa.check_code = 'reference_history_load'
             AND qa.scope_key = 'run'
            WHERE run.pipeline_run_id = %s
            """,
            (pipeline_run_id,),
        )
        row = self.cursor.fetchone()
        if row is None or row[:5] != ("COMPLETED", "PASS", "COMPLETED", "PASS", True):
            raise ReferenceDataConflict("loader run does not have completed PASS evidence")
        try:
            qa_data_asset_id = UUID(str(row[5]))
        except (TypeError, ValueError) as exc:
            raise ReferenceDataConflict("loader run QA result does not identify its evidence asset") from exc

        self.cursor.execute(
            """
            SELECT
                link.data_asset_id,
                link.usage_role,
                link.logical_name,
                link.stage_name,
                link.is_required,
                asset.storage_uri,
                asset.content_sha256,
                asset.byte_size
            FROM vrp.pipeline_run_data_assets AS link
            JOIN vrp.data_assets AS asset
              ON asset.data_asset_id = link.data_asset_id
            WHERE link.pipeline_run_id = %s
            ORDER BY link.usage_role, link.logical_name, link.data_asset_id
            """,
            (pipeline_run_id,),
        )
        assets = tuple(
            LinkedRunAsset(
                data_asset_id=item[0],
                usage_role=item[1],
                logical_name=item[2],
                stage_name=item[3],
                is_required=item[4],
                storage_uri=item[5],
                content_sha256=item[6],
                byte_size=None if item[7] is None else int(item[7]),
            )
            for item in self.cursor.fetchall()
        )
        if not assets:
            raise ReferenceDataConflict("loader run has no linked data assets")
        return CompletedRunEvidence(
            pipeline_run_id=pipeline_run_id,
            qa_data_asset_id=qa_data_asset_id,
            assets=assets,
        )

    def find_latest_release(
        self,
        dataset_key: str,
        schema_version: str,
    ) -> ExistingRelease | None:
        self.cursor.execute(
            """
            SELECT normalized_content_sha256
            FROM vrp.reference_data_releases
            WHERE dataset_key = %s AND dataset_schema_version = %s
            ORDER BY retrieved_at DESC, accepted_at DESC
            LIMIT 1
            """,
            (dataset_key, schema_version),
        )
        row = self.cursor.fetchone()
        if row is None:
            return None
        return self.find_release(dataset_key, schema_version, row[0])

    def fetch_current_rates(self) -> tuple[CurrentRow, ...]:
        self.cursor.execute(
            """
            SELECT reference_rate_observation_id, observation_date, row_sha256
            FROM vrp.current_reference_rate_observations
            WHERE series_key = 'SOFR'
            ORDER BY observation_date
            """
        )
        return tuple(
            CurrentRow(natural_key=row[1], record_id=row[0], row_sha256=row[2])
            for row in self.cursor.fetchall()
        )

    def fetch_current_daily(self, definition_id: UUID) -> tuple[CurrentRow, ...]:
        self.cursor.execute(
            """
            SELECT daily_market_feature_id, trade_date, row_sha256
            FROM vrp.current_daily_market_features
            WHERE daily_market_feature_definition_id = %s
              AND symbol = 'SPY'
            ORDER BY trade_date
            """,
            (definition_id,),
        )
        return tuple(
            CurrentRow(natural_key=row[1], record_id=row[0], row_sha256=row[2])
            for row in self.cursor.fetchall()
        )

    def find_feature_definition_id(
        self,
        definition_key: str,
        content_sha256: str,
    ) -> UUID | None:
        self.cursor.execute(
            """
            SELECT daily_market_feature_definition_id
            FROM vrp.daily_market_feature_definitions
            WHERE definition_key = %s AND content_sha256 = %s
            """,
            (definition_key, content_sha256),
        )
        row = self.cursor.fetchone()
        return None if row is None else row[0]

    def register_asset(self, asset: DataAsset) -> InsertResult:
        self.cursor.execute(
            """
            INSERT INTO vrp.data_assets (
                data_asset_id,
                dataset_name,
                asset_class,
                asset_format,
                storage_uri,
                content_sha256,
                schema_version,
                source_system,
                captured_at,
                trade_date_start,
                trade_date_end,
                row_count,
                byte_size,
                is_immutable,
                metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, TRUE, %s::jsonb
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
                asset.trade_date_start,
                asset.trade_date_end,
                asset.row_count,
                asset.byte_size,
                render_json_object(asset.metadata),
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return InsertResult(record_id=inserted[0], inserted=True)
        self.cursor.execute(
            """
            SELECT
                data_asset_id,
                asset_class,
                asset_format,
                schema_version,
                source_system,
                trade_date_start,
                trade_date_end,
                row_count,
                byte_size,
                is_immutable,
                captured_at,
                metadata
            FROM vrp.data_assets
            WHERE dataset_name = %s AND storage_uri = %s AND content_sha256 = %s
            """,
            (asset.dataset_name, asset.storage_uri, asset.content_sha256),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            raise ReferenceDataConflict("asset identity conflicted without a matching row")
        expected = (
            asset.asset_class,
            asset.asset_format,
            asset.schema_version,
            asset.source_system,
            asset.trade_date_start,
            asset.trade_date_end,
            asset.row_count,
            asset.byte_size,
            True,
            asset.captured_at,
            dict(asset.metadata),
        )
        observed = (*existing[1:-1], dict(existing[-1]))
        if observed != expected:
            raise ReferenceDataConflict("matching asset digest has a different immutable contract")
        return InsertResult(record_id=existing[0], inserted=False)

    def link_asset(
        self,
        *,
        pipeline_run_id: UUID,
        data_asset_id: UUID,
        usage_role: str,
        logical_name: str,
        stage_name: str,
        lineage: Mapping[str, Any] | None = None,
    ) -> None:
        rendered_lineage = render_json_object(lineage or {})
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_run_data_assets (
                pipeline_run_id,
                data_asset_id,
                usage_role,
                logical_name,
                stage_name,
                lineage
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING data_asset_id
            """,
            (
                pipeline_run_id,
                data_asset_id,
                usage_role,
                logical_name,
                stage_name,
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
        if existing is None or existing[0] != stage_name or existing[1] is not True:
            raise ReferenceDataConflict("asset lineage identity has a different contract")
        if render_json_object(dict(existing[2])) != rendered_lineage:
            raise ReferenceDataConflict("asset lineage has conflicting evidence")

    def register_model_version(
        self,
        *,
        model_version_id: UUID,
        version_label: str,
        content_sha256: str,
        manifest: Mapping[str, Any],
        locked_at: datetime,
    ) -> UUID:
        self.cursor.execute(
            """
            INSERT INTO vrp.model_versions (
                model_version_id, model_key, version_label, content_sha256,
                manifest, is_locked, locked_at
            ) VALUES (%s, 'reference_history_normalizer', %s, %s, %s::jsonb, TRUE, %s)
            ON CONFLICT DO NOTHING
            RETURNING model_version_id
            """,
            (
                model_version_id,
                version_label,
                content_sha256,
                render_json_object(manifest),
                locked_at,
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return inserted[0]
        self.cursor.execute(
            """
            SELECT model_version_id, content_sha256
            FROM vrp.model_versions
            WHERE model_key = 'reference_history_normalizer' AND version_label = %s
            """,
            (version_label,),
        )
        existing = self.cursor.fetchone()
        if existing is None or existing[1] != content_sha256:
            raise ReferenceDataConflict("normalizer model version has conflicting content")
        return existing[0]

    def register_configuration_version(
        self,
        *,
        configuration_version_id: UUID,
        version_label: str,
        content_sha256: str,
        configuration: Mapping[str, Any],
    ) -> UUID:
        self.cursor.execute(
            """
            INSERT INTO vrp.configuration_versions (
                configuration_version_id,
                configuration_key,
                version_label,
                content_sha256,
                configuration
            ) VALUES (%s, 'reference_history_loader', %s, %s, %s::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING configuration_version_id
            """,
            (
                configuration_version_id,
                version_label,
                content_sha256,
                render_json_object(configuration),
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return inserted[0]
        self.cursor.execute(
            """
            SELECT configuration_version_id, content_sha256
            FROM vrp.configuration_versions
            WHERE configuration_key = 'reference_history_loader' AND version_label = %s
            """,
            (version_label,),
        )
        existing = self.cursor.fetchone()
        if existing is None or existing[1] != content_sha256:
            raise ReferenceDataConflict("reference loader configuration has conflicting content")
        return existing[0]

    def begin_run(
        self,
        *,
        pipeline_run_id: UUID,
        environment: str,
        idempotency_key: str,
        valuation_date: date,
        snapshot_at: datetime,
        model_version_id: UUID,
        configuration_version_id: UUID,
        code_version: str,
        orchestrator_version: str,
        requested_by: str | None,
        invocation: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> RunState:
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_runs (
                pipeline_run_id,
                environment,
                idempotency_key,
                run_kind,
                valuation_date,
                snapshot_at,
                data_cutoff_at,
                model_version_id,
                configuration_version_id,
                code_version,
                orchestrator_version,
                status,
                qa_status,
                started_at,
                requested_by,
                invocation,
                metadata
            ) VALUES (
                %s, %s, %s, 'BACKFILL', %s, %s, %s, %s, %s, %s,
                %s, 'RUNNING', 'PENDING', CURRENT_TIMESTAMP, %s, %s::jsonb, %s::jsonb
            )
            ON CONFLICT (environment, idempotency_key) DO NOTHING
            RETURNING pipeline_run_id, status, qa_status
            """,
            (
                pipeline_run_id,
                environment,
                idempotency_key,
                valuation_date,
                snapshot_at,
                snapshot_at,
                model_version_id,
                configuration_version_id,
                code_version,
                orchestrator_version,
                requested_by,
                render_json_object(invocation),
                render_json_object(metadata),
            ),
        )
        inserted = self.cursor.fetchone()
        if inserted is not None:
            return RunState(inserted[0], inserted[1], inserted[2], True)
        self.cursor.execute(
            """
            SELECT
                pipeline_run_id,
                status,
                qa_status,
                valuation_date,
                snapshot_at,
                model_version_id,
                configuration_version_id,
                code_version,
                orchestrator_version
            FROM vrp.pipeline_runs
            WHERE environment = %s AND idempotency_key = %s
            """,
            (environment, idempotency_key),
        )
        existing = self.cursor.fetchone()
        if existing is None:
            raise ReferenceHistoryRunConflict("run conflict did not resolve to an existing row")
        expected = (
            valuation_date,
            snapshot_at,
            model_version_id,
            configuration_version_id,
            code_version,
            orchestrator_version,
        )
        if tuple(existing[3:]) != expected:
            raise ReferenceHistoryRunConflict("idempotent run has a different immutable contract")
        if existing[1] == "COMPLETED" and existing[2] == "PASS":
            return RunState(existing[0], existing[1], existing[2], False)
        if existing[1] == "RUNNING":
            raise ReferenceHistoryRunConflict(
                "the deterministic loader run is already RUNNING; explicit stale-run recovery is required"
            )
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_runs
            SET status = 'RUNNING',
                qa_status = 'PENDING',
                started_at = CURRENT_TIMESTAMP,
                completed_at = NULL,
                error_summary = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s
              AND status IN ('FAILED', 'DEGRADED', 'CANCELLED')
            RETURNING status, qa_status
            """,
            (existing[0],),
        )
        restarted = self.cursor.fetchone()
        if restarted is None:
            raise ReferenceHistoryRunConflict("existing loader run cannot be resumed safely")
        return RunState(existing[0], restarted[0], restarted[1], False)

    def start_stage(
        self,
        *,
        pipeline_stage_id: UUID,
        pipeline_run_id: UUID,
        stage_name: str,
        input_fingerprint: str,
    ) -> None:
        self.cursor.execute(
            """
            INSERT INTO vrp.pipeline_run_stages (
                pipeline_stage_id,
                pipeline_run_id,
                stage_name,
                stage_order,
                status,
                attempt_count,
                input_fingerprint,
                started_at
            ) VALUES (%s, %s, %s, 0, 'RUNNING', 1, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (pipeline_run_id, stage_name) DO UPDATE
            SET status = 'RUNNING',
                attempt_count = vrp.pipeline_run_stages.attempt_count + 1,
                input_fingerprint = EXCLUDED.input_fingerprint,
                output_fingerprint = NULL,
                started_at = CURRENT_TIMESTAMP,
                finished_at = NULL,
                last_error = '{}'::jsonb,
                metrics = '{}'::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE vrp.pipeline_run_stages.status = 'FAILED'
            RETURNING pipeline_stage_id
            """,
            (
                pipeline_stage_id,
                pipeline_run_id,
                stage_name,
                input_fingerprint,
            ),
        )
        if self.cursor.fetchone() is None:
            raise ReferenceHistoryRunConflict("loader stage is already active or completed")

    def record_qa(
        self,
        *,
        qa_result_id: UUID,
        pipeline_run_id: UUID,
        stage_name: str,
        check_code: str,
        outcome: str,
        is_hard_gate: bool,
        message: str,
        observed_value: Mapping[str, Any] | None = None,
        expected_value: Mapping[str, Any] | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        self.cursor.execute(
            """
            INSERT INTO vrp.qa_results (
                qa_result_id,
                pipeline_run_id,
                stage_name,
                check_code,
                scope_key,
                severity,
                outcome,
                is_hard_gate,
                message,
                observed_value,
                expected_value,
                evidence
            ) VALUES (
                %s, %s, %s, %s, 'run', %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            ON CONFLICT (pipeline_run_id, check_code, scope_key) DO UPDATE
            SET outcome = EXCLUDED.outcome,
                severity = EXCLUDED.severity,
                is_hard_gate = EXCLUDED.is_hard_gate,
                message = EXCLUDED.message,
                observed_value = EXCLUDED.observed_value,
                expected_value = EXCLUDED.expected_value,
                evidence = EXCLUDED.evidence,
                checked_at = CURRENT_TIMESTAMP
            """,
            (
                qa_result_id,
                pipeline_run_id,
                stage_name,
                check_code,
                "ERROR" if is_hard_gate else "INFO",
                outcome,
                is_hard_gate,
                message,
                None if observed_value is None else render_json_object(observed_value),
                None if expected_value is None else render_json_object(expected_value),
                render_json_object(evidence or {}),
            ),
        )

    def complete_run(
        self,
        *,
        pipeline_run_id: UUID,
        stage_name: str,
        output_fingerprint: str,
        metrics: Mapping[str, Any],
    ) -> None:
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_run_stages
            SET status = 'COMPLETED',
                output_fingerprint = %s,
                finished_at = CURRENT_TIMESTAMP,
                metrics = %s::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s AND stage_name = %s AND status = 'RUNNING'
            RETURNING pipeline_stage_id
            """,
            (
                output_fingerprint,
                render_json_object(metrics),
                pipeline_run_id,
                stage_name,
            ),
        )
        if self.cursor.fetchone() is None:
            raise ReferenceHistoryRunConflict("loader stage did not transition to COMPLETED")
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_runs
            SET status = 'COMPLETED',
                qa_status = 'PASS',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s AND status = 'RUNNING'
            RETURNING pipeline_run_id
            """,
            (pipeline_run_id,),
        )
        if self.cursor.fetchone() is None:
            raise ReferenceHistoryRunConflict("loader run did not transition to COMPLETED")

    def fail_run(
        self,
        *,
        pipeline_run_id: UUID,
        stage_name: str,
        error_summary: str,
    ) -> bool:
        sanitized = str(error_summary).strip()[:2000] or "reference-history load failed"
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_run_stages
            SET status = 'FAILED',
                finished_at = CURRENT_TIMESTAMP,
                last_error = %s::jsonb,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s AND stage_name = %s AND status = 'RUNNING'
            RETURNING pipeline_stage_id
            """,
            (
                render_json_object({"error": sanitized}),
                pipeline_run_id,
                stage_name,
            ),
        )
        stage_failed = self.cursor.fetchone() is not None
        self.cursor.execute(
            """
            UPDATE vrp.pipeline_runs
            SET status = 'FAILED',
                qa_status = 'FAIL',
                completed_at = CURRENT_TIMESTAMP,
                error_summary = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_run_id = %s AND status = 'RUNNING'
            RETURNING pipeline_run_id
            """,
            (sanitized, pipeline_run_id),
        )
        run_failed = self.cursor.fetchone() is not None
        if stage_failed != run_failed:
            raise ReferenceHistoryRunConflict("loader run and stage failure states diverged")
        return run_failed

    def count_release_children(self, release_id: UUID, dataset_kind: str) -> int:
        table = {
            "REFERENCE_RATE": "vrp.reference_rate_observations",
            "DAILY_MARKET_FEATURES": "vrp.daily_market_features",
        }.get(dataset_kind)
        if table is None:
            raise ValueError("unsupported dataset kind")
        self.cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE reference_data_release_id = %s",
            (release_id,),
        )
        return int(self.cursor.fetchone()[0])
