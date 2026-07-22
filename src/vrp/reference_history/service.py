"""Transactional historical loader for normalized reference data."""

from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any, Callable, Mapping

from vrp.storage.history_postgres import ExistingRelease, PostgresReferenceHistoryRepository
from vrp.storage.postgres import PostgresReferenceDataRepository
from vrp.storage.reference_data import (
    ReferenceDataRelease,
    StoredDailyMarketFeature,
    StoredReferenceRateObservation,
)

from .artifacts import ContentAddressedArtifactStore
from .canonical import canonical_json_bytes, sha256_bytes
from .ids import (
    configuration_version_id,
    daily_row_id,
    model_version_id,
    qa_result_id,
    rate_row_id,
    release_id,
    run_id,
    stage_id,
)
from .models import FileArtifact, LoadResult, PreparedHistory
from .planning import assert_current_matches, plan_changes

ORCHESTRATOR_VERSION = "reference-history-loader-v1"
STAGE_NAME = "LOAD_REFERENCE_HISTORY"
NORMALIZER_VERSION_LABEL = "v1"
LOADER_CONFIGURATION_VERSION_LABEL = "v1"

NORMALIZER_MANIFEST = {
    "identity": "reference_history_normalizer",
    "revision": 1,
    "semantics": [
        "full-history-normalization",
        "date-level-append-only-corrections",
        "canonical-json-sha256",
        "strict-source-formula-reconciliation",
    ],
}
LOADER_CONFIGURATION = {
    "allow_deletions": False,
    "asset_policy": "content-addressed-exact-byte-snapshots-before-normalization",
    "correction_policy": "successor-row-only",
    "dataset_lock": "postgresql-transaction-advisory-lock",
    "publication_transaction": "run-assets-release-rows-qa-completion-atomic",
    "reference_history_schema": 1,
}


def _contract_digest(value: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json_bytes(value))


NORMALIZER_CONTENT_SHA256 = _contract_digest(NORMALIZER_MANIFEST)
LOADER_CONFIGURATION_SHA256 = _contract_digest(LOADER_CONFIGURATION)


def _effective_input_digest(
    prepared: PreparedHistory,
    *,
    code_version: str,
    generation: int,
) -> str:
    return _contract_digest(
        {
            "code_version": code_version,
            "generation": generation,
            "loader_configuration_sha256": LOADER_CONFIGURATION_SHA256,
            "normalized_content_sha256": prepared.normalized.content_sha256,
            "normalizer_content_sha256": NORMALIZER_CONTENT_SHA256,
            "source_assets": [
                {
                    "content_sha256": artifact.asset.content_sha256,
                    "logical_name": artifact.logical_name,
                }
                for artifact in sorted(
                    prepared.source_artifacts, key=lambda item: item.logical_name
                )
            ],
        }
    )


def _idempotency_key(prepared: PreparedHistory, effective_input_sha256: str) -> str:
    history = prepared.normalized
    return (
        f"reference-history/v2/{history.dataset_key}/{history.schema_version}/"
        f"{effective_input_sha256}"
    )


def _snapshot_at(prepared: PreparedHistory) -> datetime:
    return datetime.combine(prepared.normalized.end_date, time.max, tzinfo=timezone.utc)


def _require_idle_connection(connection: Any) -> None:
    if getattr(connection, "autocommit", False):
        raise ValueError("reference-history loading requires a non-autocommit connection")
    info = getattr(connection, "info", None)
    status = getattr(info, "transaction_status", None)
    if status is not None and getattr(status, "name", None) != "IDLE":
        raise ValueError("reference-history loading requires an idle dedicated connection")


def _definition_digest(prepared: PreparedHistory) -> str | None:
    definition = prepared.normalized.definition
    return None if definition is None else definition.content_sha256


def _validate_existing_release(
    existing: ExistingRelease,
    prepared: PreparedHistory,
) -> None:
    history = prepared.normalized
    expected_release_id = release_id(
        history.dataset_key,
        history.schema_version,
        history.content_sha256,
    )
    expected = (
        expected_release_id,
        history.dataset_key,
        history.dataset_kind,
        history.schema_version,
        history.content_sha256,
        history.source_system,
        ORCHESTRATOR_VERSION,
        history.content_sha256,
        history.vintage_kind,
        len(history.rows),
        history.start_date,
        history.end_date,
        _definition_digest(prepared),
    )
    observed = (
        existing.release_id,
        existing.dataset_key,
        existing.dataset_kind,
        existing.schema_version,
        existing.normalized_content_sha256,
        existing.source_system,
        existing.loader_version,
        existing.normalized_asset_sha256,
        existing.vintage_kind,
        existing.source_row_count,
        existing.observation_start_date,
        existing.observation_end_date,
        existing.metadata.get("definition_content_sha256"),
    )
    if observed != expected:
        raise RuntimeError(
            "existing release digest has an inconsistent immutable contract: "
            f"observed={observed} expected={expected}"
        )
    if existing.qa_manifest_data_asset_id is None:
        raise RuntimeError("existing release is missing immutable QA evidence")
    if (
        existing.qa_asset_sha256 is None
        or existing.qa_asset_storage_uri is None
        or existing.qa_asset_byte_size is None
    ):
        raise RuntimeError("existing release QA asset contract is incomplete")
    if existing.loaded_by_pipeline_run_id is None:
        raise RuntimeError("existing release is missing its loader run identity")
    if (existing.loaded_run_status, existing.loaded_run_qa_status) != ("COMPLETED", "PASS"):
        raise RuntimeError("existing release is not owned by a completed PASS loader run")


def _verify_existing_release(
    repository: PostgresReferenceHistoryRepository,
    artifact_store: ContentAddressedArtifactStore,
    existing: ExistingRelease,
    prepared: PreparedHistory,
) -> None:
    _validate_existing_release(existing, prepared)
    source_ids = existing.metadata.get("source_data_asset_ids")
    if not isinstance(source_ids, list) or not source_ids:
        raise RuntimeError("existing release does not identify its immutable input assets")
    _verify_completed_run_evidence(
        repository,
        artifact_store,
        existing.loaded_by_pipeline_run_id,
        expected_input_asset_ids={str(value) for value in source_ids},
        expected_normalized_asset_id=existing.normalized_data_asset_id,
        expected_qa_asset_id=existing.qa_manifest_data_asset_id,
    )


def _artifact_contract(artifact: FileArtifact) -> tuple[Any, str, str, int | None]:
    return (
        artifact.asset.data_asset_id,
        artifact.asset.storage_uri,
        artifact.asset.content_sha256,
        artifact.asset.byte_size,
    )


def _linked_asset_contract(asset: Any) -> tuple[Any, str, str, int | None]:
    return (
        asset.data_asset_id,
        asset.storage_uri,
        asset.content_sha256,
        asset.byte_size,
    )


def _verify_completed_run_evidence(
    repository: PostgresReferenceHistoryRepository,
    artifact_store: ContentAddressedArtifactStore,
    pipeline_run_id: Any,
    *,
    expected_input_artifacts: tuple[FileArtifact, ...] | None = None,
    expected_input_asset_ids: set[str] | None = None,
    expected_normalized_artifact: FileArtifact | None = None,
    expected_normalized_asset_id: Any | None = None,
    expected_qa_asset_id: Any | None = None,
) -> None:
    if (expected_input_artifacts is None) == (expected_input_asset_ids is None):
        raise ValueError("exactly one expected input-evidence contract is required")
    if (expected_normalized_artifact is None) == (expected_normalized_asset_id is None):
        raise ValueError("exactly one expected normalized-evidence contract is required")

    evidence = repository.fetch_completed_run_evidence(pipeline_run_id)
    slots: dict[tuple[str, str], Any] = {}
    for asset in evidence.assets:
        if not asset.is_required or asset.stage_name != STAGE_NAME:
            raise RuntimeError("loader run contains non-required or cross-stage evidence")
        slot = (asset.usage_role, asset.logical_name)
        if slot in slots:
            raise RuntimeError(f"loader run contains duplicate evidence for {slot}")
        if asset.byte_size is None:
            raise RuntimeError(f"loader run evidence is missing byte size for {slot}")
        slots[slot] = asset

    inputs = {
        logical_name: asset
        for (usage_role, logical_name), asset in slots.items()
        if usage_role == "INPUT"
    }
    normalized = slots.get(("OUTPUT", "normalized_history"))
    qa = slots.get(("QA_EVIDENCE", "reference_history_qa_manifest"))
    expected_slots = {
        *(('INPUT', logical_name) for logical_name in inputs),
        ("OUTPUT", "normalized_history"),
        ("QA_EVIDENCE", "reference_history_qa_manifest"),
    }
    if not inputs or normalized is None or qa is None or set(slots) != expected_slots:
        raise RuntimeError("loader run evidence links are incomplete or unexpected")
    if qa.data_asset_id != evidence.qa_data_asset_id:
        raise RuntimeError("loader run QA result and QA evidence link disagree")
    if expected_qa_asset_id is not None and qa.data_asset_id != expected_qa_asset_id:
        raise RuntimeError("loader run does not link the expected immutable QA asset")

    if expected_input_artifacts is not None:
        expected_inputs = {item.logical_name: item for item in expected_input_artifacts}
        if set(inputs) != set(expected_inputs):
            raise RuntimeError("loader run does not link the expected input logical names")
        for logical_name, expected in expected_inputs.items():
            if _linked_asset_contract(inputs[logical_name]) != _artifact_contract(expected):
                raise RuntimeError(f"loader run input evidence changed for {logical_name}")
    else:
        observed_input_ids = {str(item.data_asset_id) for item in inputs.values()}
        if observed_input_ids != expected_input_asset_ids:
            raise RuntimeError("release-owning run input evidence does not match release metadata")

    if expected_normalized_artifact is not None:
        if _linked_asset_contract(normalized) != _artifact_contract(expected_normalized_artifact):
            raise RuntimeError("loader run normalized evidence changed")
    elif normalized.data_asset_id != expected_normalized_asset_id:
        raise RuntimeError("release-owning run does not link the release normalized asset")

    for asset in evidence.assets:
        artifact_store.verify_storage_asset(
            storage_uri=asset.storage_uri,
            content_sha256=asset.content_sha256,
            byte_size=asset.byte_size,
        )


def _fetch_current(
    repository: PostgresReferenceHistoryRepository,
    prepared: PreparedHistory,
    definition_id_value,
):
    if prepared.normalized.dataset_kind == "REFERENCE_RATE":
        return repository.fetch_current_rates()
    if definition_id_value is None:
        raise RuntimeError("daily history requires an accepted feature definition")
    return repository.fetch_current_daily(definition_id_value)


def _register_versions(
    repository: PostgresReferenceHistoryRepository,
) -> tuple[Any, Any]:
    model_id = repository.register_model_version(
        model_version_id=model_version_id(NORMALIZER_CONTENT_SHA256),
        version_label=NORMALIZER_VERSION_LABEL,
        content_sha256=NORMALIZER_CONTENT_SHA256,
        manifest=NORMALIZER_MANIFEST,
        locked_at=datetime.now(timezone.utc),
    )
    configuration_id = repository.register_configuration_version(
        configuration_version_id=configuration_version_id(LOADER_CONFIGURATION_SHA256),
        version_label=LOADER_CONFIGURATION_VERSION_LABEL,
        content_sha256=LOADER_CONFIGURATION_SHA256,
        configuration=LOADER_CONFIGURATION,
    )
    return model_id, configuration_id


def _begin_run(
    repository: PostgresReferenceHistoryRepository,
    prepared: PreparedHistory,
    *,
    pipeline_run_id,
    idempotency_key: str,
    effective_input_sha256: str,
    environment: str,
    code_version: str,
    generation: int,
    requested_by: str | None,
):
    model_id, configuration_id = _register_versions(repository)
    history = prepared.normalized
    return repository.begin_run(
        pipeline_run_id=pipeline_run_id,
        environment=environment,
        idempotency_key=idempotency_key,
        valuation_date=history.end_date,
        snapshot_at=_snapshot_at(prepared),
        model_version_id=model_id,
        configuration_version_id=configuration_id,
        code_version=code_version,
        orchestrator_version=ORCHESTRATOR_VERSION,
        requested_by=requested_by,
        invocation={
            "dataset_key": history.dataset_key,
            "effective_input_sha256": effective_input_sha256,
            "generation": generation,
            "normalized_content_sha256": history.content_sha256,
            "source_storage_uris": [
                artifact.asset.storage_uri for artifact in prepared.source_artifacts
            ],
        },
        metadata={"publishes_signal": False},
    )


def _qa_payload(
    prepared: PreparedHistory,
    *,
    action: str,
    generation: int,
    effective_input_sha256: str,
    metrics: Mapping[str, int],
) -> dict[str, Any]:
    history = prepared.normalized
    checks = {
        "artifact_bytes_verified": "PASS",
        "immutable_release_contract": "PASS",
        "source_formulas_reconciled": "PASS",
    }
    if action == "NEW_RELEASE":
        checks.update(
            {
                "candidate_coverage_nonshrinking": "PASS",
                "current_rows_match_candidate_at_commit": "PASS",
                "release_and_children_same_transaction": "PASS",
            }
        )
    else:
        checks["existing_release_child_count"] = "PASS"
    return {
        "checks": checks,
        "dataset_key": history.dataset_key,
        "definition_content_sha256": _definition_digest(prepared),
        "effective_input_sha256": effective_input_sha256,
        "generation": generation,
        "manifest_schema": "reference-history-qa-v1",
        "normalized_content_sha256": history.content_sha256,
        "plan": dict(metrics),
        "publication_action": action,
        "source_assets": [
            {
                "content_sha256": artifact.asset.content_sha256,
                "logical_name": artifact.logical_name,
            }
            for artifact in prepared.source_artifacts
        ],
    }


def _record_failed_attempt(
    connection: Any,
    prepared: PreparedHistory,
    *,
    pipeline_run_id,
    pipeline_stage_id,
    idempotency_key: str,
    effective_input_sha256: str,
    environment: str,
    code_version: str,
    generation: int,
    requested_by: str | None,
    error: Exception,
) -> None:
    """Persist failure state only when the deterministic run is not already complete."""

    try:
        with connection.cursor() as cursor:
            repository = PostgresReferenceHistoryRepository(cursor)
            PostgresReferenceDataRepository(cursor).acquire_dataset_lock(
                prepared.normalized.dataset_key
            )
            state = _begin_run(
                repository,
                prepared,
                pipeline_run_id=pipeline_run_id,
                idempotency_key=idempotency_key,
                effective_input_sha256=effective_input_sha256,
                environment=environment,
                code_version=code_version,
                generation=generation,
                requested_by=requested_by,
            )
            if state.status == "COMPLETED" and state.qa_status == "PASS":
                connection.commit()
                return
            repository.start_stage(
                pipeline_stage_id=pipeline_stage_id,
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                input_fingerprint=effective_input_sha256,
            )
            transitioned = repository.fail_run(
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                error_summary=f"{type(error).__name__}: {error}",
            )
            if not transitioned:
                raise RuntimeError("failed loader attempt did not transition its run state")
            repository.record_qa(
                qa_result_id=qa_result_id(
                    pipeline_run_id, "reference_history_load", "run"
                ),
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                check_code="reference_history_load",
                outcome="FAIL",
                is_hard_gate=True,
                message="Reference-history load failed; all data writes were rolled back.",
                observed_value={"exception_type": type(error).__name__},
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def execute_reference_history_load(
    connection: Any,
    prepared: PreparedHistory,
    *,
    artifact_store: ContentAddressedArtifactStore,
    environment: str,
    code_version: str,
    generation: int = 0,
    requested_by: str | None = None,
    failure_injector: Callable[[str], None] | None = None,
) -> LoadResult:
    """Load one complete dataset; data and successful run state commit atomically."""

    _require_idle_connection(connection)
    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("environment must be a non-empty string")
    if not isinstance(code_version, str) or not code_version.strip():
        raise ValueError("code_version must be a non-empty string")
    if type(generation) is not int or generation < 0:
        raise ValueError("generation must be a non-negative integer")

    environment = environment.strip()
    code_version = code_version.strip()
    history = prepared.normalized
    effective_input_sha256 = _effective_input_digest(
        prepared,
        code_version=code_version,
        generation=generation,
    )
    idempotency_key = _idempotency_key(prepared, effective_input_sha256)
    pipeline_run_id = run_id(environment, idempotency_key)
    pipeline_stage_id = stage_id(pipeline_run_id, STAGE_NAME)
    accepted_release_id = release_id(
        history.dataset_key,
        history.schema_version,
        history.content_sha256,
    )

    try:
        with connection.cursor() as cursor:
            repository = PostgresReferenceHistoryRepository(cursor)
            data_repository = PostgresReferenceDataRepository(cursor)
            data_repository.acquire_dataset_lock(history.dataset_key)
            artifact_store.verify_prepared(prepared)

            run_state = _begin_run(
                repository,
                prepared,
                pipeline_run_id=pipeline_run_id,
                idempotency_key=idempotency_key,
                effective_input_sha256=effective_input_sha256,
                environment=environment,
                code_version=code_version,
                generation=generation,
                requested_by=requested_by,
            )
            existing = repository.find_release(
                history.dataset_key,
                history.schema_version,
                history.content_sha256,
            )
            if run_state.status == "COMPLETED" and run_state.qa_status == "PASS":
                if existing is None:
                    raise RuntimeError("completed loader run is missing its immutable release")
                _verify_existing_release(repository, artifact_store, existing, prepared)
                _verify_completed_run_evidence(
                    repository,
                    artifact_store,
                    pipeline_run_id,
                    expected_input_artifacts=prepared.source_artifacts,
                    expected_normalized_artifact=prepared.normalized_artifact,
                )
                if repository.count_release_children(
                    existing.release_id, history.dataset_kind
                ) != existing.persisted_row_count:
                    raise RuntimeError("existing release child count is inconsistent")
                connection.commit()
                return LoadResult(
                    pipeline_run_id=pipeline_run_id,
                    reference_data_release_id=existing.release_id,
                    dataset_key=history.dataset_key,
                    content_sha256=history.content_sha256,
                    source_row_count=len(history.rows),
                    persisted_row_count=0,
                    new_count=0,
                    correction_count=0,
                    unchanged_count=len(history.rows),
                    no_op=True,
                )

            repository.start_stage(
                pipeline_stage_id=pipeline_stage_id,
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                input_fingerprint=effective_input_sha256,
            )
            definition_id_value = None
            if history.definition is not None:
                definition_id_value = data_repository.register_feature_definition(
                    history.definition
                ).record_id

            if existing is not None:
                _verify_existing_release(repository, artifact_store, existing, prepared)
                if repository.count_release_children(
                    existing.release_id, history.dataset_kind
                ) != existing.persisted_row_count:
                    raise RuntimeError("existing release child count is inconsistent")
                action = "REUSED_EXISTING_RELEASE"
                metrics = {
                    "correction_count": 0,
                    "new_count": 0,
                    "persisted_count": 0,
                    "source_row_count": len(history.rows),
                    "unchanged_count": len(history.rows),
                }
                plan = None
                accepted_release_id = existing.release_id
            else:
                latest = repository.find_latest_release(
                    history.dataset_key, history.schema_version
                )
                if latest is not None and prepared.retrieved_at <= latest.retrieved_at:
                    raise RuntimeError(
                        "candidate snapshot predates the latest accepted release; "
                        "refusing to reverse newer corrections"
                    )
                current = _fetch_current(repository, prepared, definition_id_value)
                plan = plan_changes(history, current)
                if not plan.persisted_count:
                    raise RuntimeError(
                        "a new normalized digest produced zero semantic row changes"
                    )
                action = "NEW_RELEASE"
                metrics = {
                    "correction_count": plan.correction_count,
                    "new_count": plan.new_count,
                    "persisted_count": plan.persisted_count,
                    "source_row_count": len(history.rows),
                    "unchanged_count": plan.unchanged_count,
                }

            qa_artifact = artifact_store.write_qa_manifest(
                history,
                _qa_payload(
                    prepared,
                    action=action,
                    generation=generation,
                    effective_input_sha256=effective_input_sha256,
                    metrics=metrics,
                ),
            )
            artifact_store.verify_artifact(qa_artifact)
            registered_assets: list[tuple[Any, str, str]] = []
            for artifact in (
                *prepared.source_artifacts,
                prepared.normalized_artifact,
                qa_artifact,
            ):
                artifact_store.verify_artifact(artifact)
                stored_asset = repository.register_asset(artifact.asset)
                registered_assets.append(
                    (stored_asset.record_id, artifact.usage_role, artifact.logical_name)
                )
                repository.link_asset(
                    pipeline_run_id=pipeline_run_id,
                    data_asset_id=stored_asset.record_id,
                    usage_role=artifact.usage_role,
                    logical_name=artifact.logical_name,
                    stage_name=STAGE_NAME,
                    lineage={
                        "content_sha256": artifact.asset.content_sha256,
                        **dict(artifact.lineage),
                    },
                )

            def _one_asset_id(role: str, logical_name: str):
                matches = [
                    item[0]
                    for item in registered_assets
                    if item[1] == role and item[2] == logical_name
                ]
                if len(matches) != 1:
                    raise RuntimeError(
                        f"expected one {role}/{logical_name} asset; found {len(matches)}"
                    )
                return matches[0]

            normalized_asset_id = _one_asset_id("OUTPUT", "normalized_history")
            qa_asset_id = _one_asset_id(
                "QA_EVIDENCE", "reference_history_qa_manifest"
            )

            if action == "NEW_RELEASE":
                release = ReferenceDataRelease(
                    release_id=accepted_release_id,
                    dataset_key=history.dataset_key,
                    dataset_kind=history.dataset_kind,
                    dataset_schema_version=history.schema_version,
                    normalized_content_sha256=history.content_sha256,
                    source_system=history.source_system,
                    loader_version=ORCHESTRATOR_VERSION,
                    normalized_data_asset_id=normalized_asset_id,
                    qa_manifest_data_asset_id=qa_asset_id,
                    loaded_by_pipeline_run_id=pipeline_run_id,
                    vintage_kind=history.vintage_kind,
                    retrieved_at=prepared.retrieved_at,
                    source_row_count=len(history.rows),
                    persisted_row_count=plan.persisted_count,
                    observation_start_date=history.start_date,
                    observation_end_date=history.end_date,
                    metadata={
                        "definition_content_sha256": _definition_digest(prepared),
                        "effective_input_sha256": effective_input_sha256,
                        "source_data_asset_ids": [
                            str(item[0])
                            for item in registered_assets
                            if item[1] == "INPUT"
                        ],
                    },
                )
                release_result = data_repository.register_release(release)
                if not release_result.inserted:
                    raise RuntimeError("new release unexpectedly resolved as an existing row")

                for change in plan.changes:
                    value = change.row
                    if history.dataset_kind == "REFERENCE_RATE":
                        data_repository.append_reference_rate(
                            StoredReferenceRateObservation(
                                observation_id=rate_row_id(
                                    accepted_release_id,
                                    value.series_key,
                                    value.observation_date,
                                    value.row_sha256,
                                ),
                                release_id=accepted_release_id,
                                value=value,
                                supersedes_observation_id=change.supersedes_record_id,
                            )
                        )
                    else:
                        data_repository.append_daily_market_feature(
                            StoredDailyMarketFeature(
                                feature_id=daily_row_id(
                                    accepted_release_id,
                                    definition_id_value,
                                    value.symbol,
                                    value.trade_date,
                                    value.row_sha256,
                                ),
                                definition_id=definition_id_value,
                                release_id=accepted_release_id,
                                value=value,
                                supersedes_feature_id=change.supersedes_record_id,
                            )
                        )
                if failure_injector is not None:
                    failure_injector("after_release_rows")

                current_after = _fetch_current(repository, prepared, definition_id_value)
                assert_current_matches(history, current_after)
                child_count = repository.count_release_children(
                    accepted_release_id, history.dataset_kind
                )
                if child_count != plan.persisted_count:
                    raise RuntimeError(
                        "release child count does not match persisted_row_count: "
                        f"children={child_count} expected={plan.persisted_count}"
                    )

            repository.record_qa(
                qa_result_id=qa_result_id(
                    pipeline_run_id, "reference_history_load", "run"
                ),
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                check_code="reference_history_load",
                outcome="PASS",
                is_hard_gate=True,
                message=(
                    "Normalized history was committed and current leaves reconcile exactly."
                    if action == "NEW_RELEASE"
                    else "Existing immutable normalized release was verified without changing current leaves."
                ),
                observed_value=metrics,
                expected_value={"source_row_count": len(history.rows)},
                evidence={"qa_data_asset_id": str(qa_asset_id)},
            )
            output_fingerprint = _contract_digest(
                {
                    "effective_input_sha256": effective_input_sha256,
                    "qa_content_sha256": qa_artifact.asset.content_sha256,
                    "reference_data_release_id": str(accepted_release_id),
                }
            )
            repository.complete_run(
                pipeline_run_id=pipeline_run_id,
                stage_name=STAGE_NAME,
                output_fingerprint=output_fingerprint,
                metrics=metrics,
            )
        connection.commit()
        return LoadResult(
            pipeline_run_id=pipeline_run_id,
            reference_data_release_id=accepted_release_id,
            dataset_key=history.dataset_key,
            content_sha256=history.content_sha256,
            source_row_count=len(history.rows),
            persisted_row_count=metrics["persisted_count"],
            new_count=metrics["new_count"],
            correction_count=metrics["correction_count"],
            unchanged_count=metrics["unchanged_count"],
            no_op=action != "NEW_RELEASE",
        )
    except Exception as exc:
        connection.rollback()
        try:
            _record_failed_attempt(
                connection,
                prepared,
                pipeline_run_id=pipeline_run_id,
                pipeline_stage_id=pipeline_stage_id,
                idempotency_key=idempotency_key,
                effective_input_sha256=effective_input_sha256,
                environment=environment,
                code_version=code_version,
                generation=generation,
                requested_by=requested_by,
                error=exc,
            )
        except Exception as audit_exc:
            raise ExceptionGroup(
                "reference-history load failed and its failure ledger could not be written",
                [exc, audit_exc],
            ) from None
        raise
