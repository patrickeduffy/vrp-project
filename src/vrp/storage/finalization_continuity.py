"""Verify that file-complete EOD history still exists in the active database."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from vrp.eod_shadow.service import database_readback_fingerprint
from vrp.orchestration.eod_finalization_gate import (
    CompletedEodFinalizationEvidence,
)
from vrp.storage.eod_postgres import PostgresEodRepository


class DatabaseFinalizationContinuityError(RuntimeError):
    """The connected PostgreSQL target is missing exact prior EOD evidence."""


_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_STAGE_NAME = "RECORD_EOD_SHADOW"
_EXPECTED_TENORS = (9, 12, 15, 18, 21, 24, 27, 30, 33)
_EXPECTED_EVALUATIONS = tuple(
    (tenor, layer)
    for tenor in _EXPECTED_TENORS
    for layer in ("CORE", "SECONDARY")
)
_EXPECTED_METRICS = {
    "forecast_variance_count": 9,
    "implied_variance_count": 9,
    "selected_signal_count": 1,
    "signal_evaluation_count": 18,
    "signal_feature_count": 9,
}
_EXPECTED_READBACK_TOLERANCES = {
    "absolute_tolerance": 1e-12,
    "relative_tolerance": 1e-10,
}


# Return the complete continuity contract as one JSON object.  Ordered row
# identities catch partial restores.  Stage and QA evidence are outside the
# database projection, so they are checked explicitly against the independently
# pinned expected-projection fingerprint as well.
_CONTINUITY_QUERY = """
SELECT jsonb_build_object(
    'run', jsonb_build_object(
        'environment', run.environment,
        'code_version', run.code_version,
        'run_kind', run.run_kind,
        'orchestrator_version', run.orchestrator_version,
        'status', run.status,
        'qa_status', run.qa_status,
        'snapshot_content_sha256', run.invocation -> 'snapshot_content_sha256',
        'effective_input_sha256', run.invocation -> 'effective_input_sha256',
        'golden_verification_id', run.invocation -> 'golden_verification_id',
        'output_fingerprint', run.invocation -> 'output_fingerprint',
        'artifact_digests', run.invocation -> 'artifact_digests',
        'authoritative', run.metadata -> 'authoritative',
        'shadow_recorder', run.metadata -> 'shadow_recorder',
        'publishes_signal', run.metadata -> 'publishes_signal'
    ),
    'market_snapshot_ids', COALESCE((
        SELECT jsonb_agg(item.market_snapshot_id ORDER BY item.market_snapshot_id)
        FROM vrp.market_snapshots AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'implied_tenors', COALESCE((
        SELECT jsonb_agg(item.tenor_days ORDER BY item.tenor_days)
        FROM vrp.implied_variance_term_structure AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'forecast_tenors', COALESCE((
        SELECT jsonb_agg(item.tenor_days ORDER BY item.tenor_days)
        FROM vrp.forecast_variance_term_structure AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'feature_tenors', COALESCE((
        SELECT jsonb_agg(item.tenor_days ORDER BY item.tenor_days)
        FROM vrp.signal_features AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'evaluation_identities', COALESCE((
        SELECT jsonb_agg(
            jsonb_build_array(item.tenor_days, item.signal_layer)
            ORDER BY item.tenor_days, item.signal_layer
        )
        FROM vrp.signal_evaluations AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'selected_signal_ids', COALESCE((
        SELECT jsonb_agg(item.selected_signal_id ORDER BY item.selected_signal_id)
        FROM vrp.selected_signals AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'selected_market_snapshot_ids', COALESCE((
        SELECT jsonb_agg(item.market_snapshot_id ORDER BY item.market_snapshot_id)
        FROM vrp.selected_signals AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'stages', COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'stage_name', item.stage_name,
                'stage_order', item.stage_order,
                'is_required', item.is_required,
                'status', item.status,
                'attempt_count', item.attempt_count,
                'input_fingerprint', item.input_fingerprint,
                'output_fingerprint', item.output_fingerprint,
                'metrics', item.metrics,
                'last_error', item.last_error
            ) ORDER BY item.stage_order, item.stage_name
        )
        FROM vrp.pipeline_run_stages AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'qa_results', COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'stage_name', item.stage_name,
                'check_code', item.check_code,
                'scope_key', item.scope_key,
                'severity', item.severity,
                'outcome', item.outcome,
                'is_hard_gate', item.is_hard_gate,
                'observed_value', item.observed_value,
                'expected_value', item.expected_value,
                'evidence', item.evidence
            ) ORDER BY item.check_code, item.scope_key
        )
        FROM vrp.qa_results AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    ), '[]'::jsonb),
    'assets', COALESCE((
        SELECT jsonb_agg(
            jsonb_build_object(
                'data_asset_id', asset.data_asset_id,
                'dataset_name', asset.dataset_name,
                'asset_class', asset.asset_class,
                'asset_format', asset.asset_format,
                'storage_uri', asset.storage_uri,
                'content_sha256', asset.content_sha256,
                'schema_version', asset.schema_version,
                'source_system', asset.source_system,
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
    'signal_publication_count', (
        SELECT COUNT(*)
        FROM vrp.signal_publications AS item
        WHERE item.pipeline_run_id = run.pipeline_run_id
    )
)
FROM vrp.pipeline_runs AS run
WHERE run.pipeline_run_id = %s::uuid
"""


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DatabaseFinalizationContinuityError(
            f"prior EOD database evidence has invalid {label}"
        )
    return value


def _sequence(value: Any, label: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise DatabaseFinalizationContinuityError(
            f"prior EOD database evidence has invalid {label}"
        )
    return value


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_HEX.fullmatch(value) is not None


def _asset_contract_matches(
    assets_value: Any,
    *,
    artifact_digests_value: Any,
    golden_qa: Mapping[str, Any],
    run_manifest_sha256: str,
) -> bool:
    try:
        assets = _sequence(assets_value, "asset evidence")
        artifact_digests = _sequence(
            artifact_digests_value,
            "invocation artifact digests",
        )
    except DatabaseFinalizationContinuityError:
        return False
    if not assets or not artifact_digests:
        return False

    digests: dict[str, str] = {}
    for raw_digest in artifact_digests:
        if not isinstance(raw_digest, Mapping) or set(raw_digest) != {
            "logical_name",
            "sha256",
        }:
            return False
        logical_name = raw_digest.get("logical_name")
        sha256 = raw_digest.get("sha256")
        if (
            not isinstance(logical_name, str)
            or not logical_name
            or logical_name in digests
            or not _is_sha256(sha256)
        ):
            return False
        digests[logical_name] = sha256

    assets_by_name: dict[str, Mapping[str, Any]] = {}
    identity_assets: dict[str, str] = {}
    for raw_asset in assets:
        if not isinstance(raw_asset, Mapping):
            return False
        logical_name = raw_asset.get("logical_name")
        content_sha256 = raw_asset.get("content_sha256")
        lineage = raw_asset.get("lineage")
        metadata = raw_asset.get("metadata")
        if (
            not isinstance(logical_name, str)
            or not logical_name
            or logical_name in assets_by_name
            or not _is_sha256(content_sha256)
            or not isinstance(lineage, Mapping)
            or not isinstance(metadata, Mapping)
        ):
            return False
        identity_input = lineage.get("identity_input")
        relative_path = lineage.get("relative_path")
        if (
            not isinstance(identity_input, bool)
            or not isinstance(relative_path, str)
            or not relative_path
            or lineage.get("content_sha256") != content_sha256
            or metadata.get("identity_input") is not identity_input
            or metadata.get("logical_name") != logical_name
            or metadata.get("relative_path") != relative_path
            or raw_asset.get("dataset_name") != f"vrp_eod_shadow/{logical_name}"
            or raw_asset.get("schema_version") != "eod-shadow-v1"
            or raw_asset.get("source_system") != "VRP_HYBRID_V2_EOD"
            or raw_asset.get("stage_name") != _STAGE_NAME
            or raw_asset.get("is_required") is not True
            or raw_asset.get("is_immutable") is not True
            or not isinstance(raw_asset.get("storage_uri"), str)
            or not raw_asset.get("storage_uri")
            or not isinstance(raw_asset.get("byte_size"), int)
            or raw_asset.get("byte_size") < 0
        ):
            return False
        row_count = raw_asset.get("row_count")
        if row_count is not None and (
            not isinstance(row_count, int)
            or isinstance(row_count, bool)
            or row_count < 0
        ):
            return False

        expected_role = (
            "QA_EVIDENCE"
            if logical_name == "golden_eod_fixture"
            else "MANIFEST"
            if logical_name in {"run_manifest", "publish_manifest"}
            else "INPUT"
        )
        expected_class = (
            "REPORT"
            if logical_name == "golden_eod_fixture"
            else "MANIFEST"
            if logical_name in {"run_manifest", "publish_manifest"}
            else "DERIVED"
        )
        if (
            raw_asset.get("usage_role") != expected_role
            or raw_asset.get("asset_class") != expected_class
        ):
            return False
        if identity_input:
            identity_assets[logical_name] = content_sha256
        elif logical_name not in {"run_manifest", "publish_manifest"}:
            return False
        assets_by_name[logical_name] = raw_asset

    golden_evidence = golden_qa.get("evidence")
    if not isinstance(golden_evidence, Mapping):
        return False
    golden_asset = assets_by_name.get("golden_eod_fixture")
    if (
        golden_asset is None
        or golden_asset.get("content_sha256") != golden_evidence.get("fixture_sha256")
    ):
        return False
    expected_identity_assets = {
        **digests,
        "golden_eod_fixture": str(golden_evidence.get("fixture_sha256")),
    }
    if identity_assets != expected_identity_assets:
        return False

    for logical_name, qa_field in (
        ("signal_history", "signal_history_sha256"),
        ("selected_decisions", "selected_decisions_sha256"),
    ):
        asset = assets_by_name.get(logical_name)
        if asset is not None and asset.get("content_sha256") != golden_evidence.get(
            qa_field
        ):
            return False
    run_manifest = assets_by_name.get("run_manifest")
    expected_asset_names = set(digests) | {
        "golden_eod_fixture",
        "publish_manifest",
        "run_manifest",
    }
    if set(assets_by_name) != expected_asset_names:
        return False
    if (
        run_manifest is None
        or run_manifest.get("content_sha256") != run_manifest_sha256
        or not _is_sha256(
            assets_by_name["publish_manifest"].get("content_sha256")
        )
    ):
        return False
    return True


def _continuity_contract_matches(
    payload: Mapping[str, Any],
    item: CompletedEodFinalizationEvidence,
) -> bool:
    run = payload.get("run")
    if not isinstance(run, Mapping):
        return False
    database_projection_sha256 = getattr(
        item,
        "database_projection_sha256",
        None,
    )
    if not _is_sha256(database_projection_sha256):
        return False
    if run != {
        "environment": item.environment,
        "code_version": item.code_version,
        "run_kind": "EOD",
        "orchestrator_version": "eod-postgres-shadow-v1",
        "status": "COMPLETED",
        "qa_status": "PASS",
        "snapshot_content_sha256": item.snapshot_content_sha256,
        "effective_input_sha256": run.get("effective_input_sha256"),
        "golden_verification_id": run.get("golden_verification_id"),
        "output_fingerprint": run.get("output_fingerprint"),
        "artifact_digests": run.get("artifact_digests"),
        "authoritative": False,
        "shadow_recorder": True,
        "publishes_signal": False,
    }:
        return False
    if not all(
        _is_sha256(run.get(field))
        for field in (
            "effective_input_sha256",
            "golden_verification_id",
            "output_fingerprint",
        )
    ):
        return False
    if list(payload.get("market_snapshot_ids", [])) != [item.market_snapshot_id]:
        return False
    expected_tenors = list(_EXPECTED_TENORS)
    if any(
        list(payload.get(key, [])) != expected_tenors
        for key in ("implied_tenors", "forecast_tenors", "feature_tenors")
    ):
        return False
    if [tuple(value) for value in payload.get("evaluation_identities", [])] != list(
        _EXPECTED_EVALUATIONS
    ):
        return False
    if list(payload.get("selected_signal_ids", [])) != [item.selected_signal_id]:
        return False
    if list(payload.get("selected_market_snapshot_ids", [])) != [
        item.market_snapshot_id
    ]:
        return False

    stages = payload.get("stages")
    if not isinstance(stages, Sequence) or isinstance(stages, (str, bytes)):
        return False
    if len(stages) != 1 or not isinstance(stages[0], Mapping):
        return False
    stage = stages[0]
    if stage != {
        "stage_name": _STAGE_NAME,
        "stage_order": 0,
        "is_required": True,
        "status": "COMPLETED",
        "attempt_count": 1,
        "input_fingerprint": run["effective_input_sha256"],
        "output_fingerprint": database_projection_sha256,
        "metrics": _EXPECTED_METRICS,
        "last_error": {},
    }:
        return False

    qa_results = payload.get("qa_results")
    if not isinstance(qa_results, Sequence) or isinstance(qa_results, (str, bytes)):
        return False
    if len(qa_results) != 2 or not all(
        isinstance(value, Mapping) for value in qa_results
    ):
        return False
    by_code = {value.get("check_code"): value for value in qa_results}
    if set(by_code) != {
        "golden_eod_contract",
        "postgres_projection_reconciliation",
    }:
        return False
    golden = by_code["golden_eod_contract"]
    readback = by_code["postgres_projection_reconciliation"]
    common = {
        "stage_name": _STAGE_NAME,
        "scope_key": "run",
        "severity": "ERROR",
        "outcome": "PASS",
        "is_hard_gate": True,
    }
    if any(golden.get(key) != value for key, value in common.items()):
        return False
    if golden.get("observed_value") != {"status": "PASS"} or golden.get(
        "expected_value"
    ) != {"status": "PASS"}:
        return False
    golden_evidence = golden.get("evidence")
    if not isinstance(golden_evidence, Mapping) or set(golden_evidence) != {
        "fixture_sha256",
        "selected_decisions_sha256",
        "signal_history_sha256",
        "verification_id",
    }:
        return False
    if not all(_is_sha256(value) for value in golden_evidence.values()) or (
        golden_evidence.get("verification_id") != run["golden_verification_id"]
    ):
        return False

    if any(readback.get(key) != value for key, value in common.items()):
        return False
    expected_readback = {
        "output_fingerprint": database_projection_sha256,
        **_EXPECTED_METRICS,
    }
    if (
        readback.get("observed_value") != expected_readback
        or readback.get("expected_value") != expected_readback
        or readback.get("evidence") != _EXPECTED_READBACK_TOLERANCES
    ):
        return False
    if not _asset_contract_matches(
        payload.get("assets"),
        artifact_digests_value=run.get("artifact_digests"),
        golden_qa=golden,
        run_manifest_sha256=item.run_manifest_sha256,
    ):
        return False
    return payload.get("signal_publication_count") == 0


def _verify_database_finalization_continuity_in_transaction(
    connection,
    evidence: Sequence[CompletedEodFinalizationEvidence],
) -> None:
    for item in evidence:
        with connection.cursor() as cursor:
            cursor.execute(_CONTINUITY_QUERY, (item.pipeline_run_id,))
            row = cursor.fetchone()
        if row is None:
            raise DatabaseFinalizationContinuityError(
                "connected PostgreSQL target is missing prior EOD pipeline run "
                f"{item.pipeline_run_id} for {item.run_dir}"
            )
        payload = row[0]
        if not isinstance(payload, Mapping) or not _continuity_contract_matches(
            payload,
            item,
        ):
            raise DatabaseFinalizationContinuityError(
                "connected PostgreSQL target does not contain the exact prior "
                f"EOD shadow projection for {item.run_dir}"
            )
        try:
            with connection.cursor() as cursor:
                current_projection = PostgresEodRepository(
                    cursor
                ).fetch_run_projection(UUID(item.pipeline_run_id))
            current_readback_sha256 = database_readback_fingerprint(
                current_projection
            )
        except Exception as exc:
            raise DatabaseFinalizationContinuityError(
                "connected PostgreSQL target cannot re-read the exact prior "
                f"EOD shadow projection for {item.run_dir}"
            ) from exc
        if current_readback_sha256 != item.database_readback_sha256:
            raise DatabaseFinalizationContinuityError(
                "connected PostgreSQL target has changed since the prior EOD "
                f"shadow projection was finalized for {item.run_dir}"
            )


def verify_database_finalization_continuity(
    connection,
    evidence: Sequence[CompletedEodFinalizationEvidence],
) -> None:
    """Fail closed unless each prior sidecar has its exact DB shadow projection."""

    if not evidence:
        return
    if not getattr(connection, "autocommit", False):
        _verify_database_finalization_continuity_in_transaction(
            connection,
            evidence,
        )
        return
    transaction = getattr(connection, "transaction", None)
    if not callable(transaction):
        raise DatabaseFinalizationContinuityError(
            "autocommit database continuity verification requires a transaction context"
        )
    try:
        with transaction():
            _verify_database_finalization_continuity_in_transaction(
                connection,
                evidence,
            )
    except DatabaseFinalizationContinuityError:
        raise
    except Exception as exc:
        raise DatabaseFinalizationContinuityError(
            "database continuity verification transaction failed"
        ) from exc


__all__ = [
    "DatabaseFinalizationContinuityError",
    "verify_database_finalization_continuity",
]
