"""Stable UUIDv5 identities for retry-safe reference-history writes."""

from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5

REFERENCE_HISTORY_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://github.com/patrickeduffy/vrp-project/reference-history/v1",
)


def stable_id(kind: str, *parts: object) -> UUID:
    """Return a versioned UUIDv5 for a normalized identity tuple."""

    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("kind must be a non-empty string")
    separator = "\x1f"
    if separator in kind:
        raise ValueError("stable identity values cannot contain the unit separator")
    rendered: list[str] = [f"kind:{kind.strip()}"]
    for part in parts:
        if part is None:
            rendered.append("none:")
        elif isinstance(part, UUID):
            rendered.append(f"uuid:{part}")
        else:
            text = str(part).strip()
            if not text:
                raise ValueError("stable identity parts cannot be empty")
            if separator in text:
                raise ValueError("stable identity values cannot contain the unit separator")
            type_name = f"{type(part).__module__}.{type(part).__qualname__}"
            rendered.append(f"{type_name}:{text}")
    return uuid5(REFERENCE_HISTORY_NAMESPACE, separator.join(rendered))


def release_id(dataset_key: str, schema_version: str, content_sha256: str) -> UUID:
    return stable_id("release", dataset_key, schema_version, content_sha256)


def definition_id(definition_key: str, version_label: str, content_sha256: str) -> UUID:
    return stable_id("definition", definition_key, version_label, content_sha256)


def asset_id(dataset_name: str, storage_uri: str, content_sha256: str) -> UUID:
    return stable_id("asset", dataset_name, storage_uri, content_sha256)


def run_id(environment: str, idempotency_key: str) -> UUID:
    return stable_id("run", environment, idempotency_key)


def stage_id(pipeline_run_id: UUID, stage_name: str) -> UUID:
    return stable_id("stage", pipeline_run_id, stage_name)


def qa_result_id(pipeline_run_id: UUID, check_code: str, scope_key: str) -> UUID:
    return stable_id("qa", pipeline_run_id, check_code, scope_key)


def rate_row_id(
    reference_data_release_id: UUID,
    series_key: str,
    observation_date: object,
    row_sha256: str,
) -> UUID:
    return stable_id(
        "rate-row",
        reference_data_release_id,
        series_key,
        observation_date,
        row_sha256,
    )


def daily_row_id(
    reference_data_release_id: UUID,
    daily_definition_id: UUID,
    symbol: str,
    trade_date: object,
    row_sha256: str,
) -> UUID:
    return stable_id(
        "daily-row",
        reference_data_release_id,
        daily_definition_id,
        symbol,
        trade_date,
        row_sha256,
    )


def model_version_id(content_sha256: str) -> UUID:
    return stable_id("model-version", "reference_history_normalizer", content_sha256)


def configuration_version_id(content_sha256: str) -> UUID:
    return stable_id("configuration-version", "reference_history_loader", content_sha256)
