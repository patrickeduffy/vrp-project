"""Canonical bytes and hashes for normalized compact history."""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from vrp.storage.reference_data import DailyMarketFeature, ReferenceRateObservation

from .models import HistoryRow, NormalizedHistory


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON cannot contain NaN or infinity")
        return value
    if isinstance(value, Decimal):
        return decimal_text(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    raise ValueError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        _json_safe(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def decimal_text(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def float_text(value: float | None) -> str | None:
    if value is None:
        return None
    if not math.isfinite(value):
        raise ValueError("canonical floats must be finite")
    return "0" if value == 0 else format(value, ".17g")


def canonical_reference_rate(row: ReferenceRateObservation) -> dict[str, Any]:
    return {
        "observation_date": row.observation_date.isoformat(),
        "rate_decimal": decimal_text(row.rate_decimal),
        "rate_percent": decimal_text(row.rate_percent),
        "row_sha256": row.row_sha256,
        "series_key": row.series_key,
        "source_unit": "ANNUAL_PERCENTAGE_POINTS",
    }


def canonical_daily_feature(row: DailyMarketFeature) -> dict[str, Any]:
    return {
        "calculation_status": row.calculation_status,
        "details": row.details,
        "prior_trade_date": (
            None if row.prior_trade_date is None else row.prior_trade_date.isoformat()
        ),
        "quality_status": row.quality_status,
        "rsi14": float_text(row.rsi14),
        "rv21d_variance": float_text(row.rv21d_variance),
        "rv21d_volatility_pct": float_text(row.rv21d_volatility_pct),
        "row_sha256": row.row_sha256,
        "spy_change": float_text(row.spy_change),
        "spy_close": decimal_text(row.spy_close),
        "spy_log_return": float_text(row.spy_log_return),
        "symbol": row.symbol,
        "trade_date": row.trade_date.isoformat(),
        "wilder_avg_gain_14": float_text(row.wilder_avg_gain_14),
        "wilder_avg_loss_14": float_text(row.wilder_avg_loss_14),
    }


def history_row_date(row: HistoryRow):
    return row.observation_date if isinstance(row, ReferenceRateObservation) else row.trade_date


def make_normalized_history(
    *,
    dataset_key: str,
    dataset_kind: str,
    schema_version: str,
    source_system: str,
    vintage_kind: str,
    rows: Iterable[HistoryRow],
    definition=None,
    source_row_counts: Mapping[str, int] | None = None,
    contract: Mapping[str, Any] | None = None,
) -> NormalizedHistory:
    ordered = tuple(sorted(rows, key=history_row_date))
    if not ordered:
        raise ValueError("normalized history cannot be empty")
    dates = [history_row_date(row) for row in ordered]
    if len(dates) != len(set(dates)):
        raise ValueError("normalized history must be unique by date")
    if dataset_kind == "REFERENCE_RATE":
        if not all(isinstance(row, ReferenceRateObservation) for row in ordered):
            raise ValueError("reference-rate history contains a non-rate row")
        canonical_rows = [canonical_reference_rate(row) for row in ordered]
    elif dataset_kind == "DAILY_MARKET_FEATURES":
        if not all(isinstance(row, DailyMarketFeature) for row in ordered):
            raise ValueError("daily-market history contains a non-daily row")
        if definition is None:
            raise ValueError("daily-market history requires a feature definition")
        canonical_rows = [canonical_daily_feature(row) for row in ordered]
    else:
        raise ValueError("unsupported dataset kind")

    payload = {
        "contract": dict(contract or {}),
        "dataset_key": dataset_key,
        "dataset_kind": dataset_kind,
        "definition_content_sha256": (
            None if definition is None else definition.content_sha256
        ),
        "end_date": dates[-1].isoformat(),
        "row_count": len(ordered),
        "rows": canonical_rows,
        "schema_version": schema_version,
        "source_system": source_system,
        "start_date": dates[0].isoformat(),
        "vintage_kind": vintage_kind,
    }
    encoded = canonical_json_bytes(payload)
    return NormalizedHistory(
        dataset_key=dataset_key,
        dataset_kind=dataset_kind,
        schema_version=schema_version,
        source_system=source_system,
        vintage_kind=vintage_kind,
        rows=ordered,
        canonical_bytes=encoded,
        content_sha256=sha256_bytes(encoded),
        definition=definition,
        source_row_counts=dict(source_row_counts or {}),
        metadata=dict(contract or {}),
    )
