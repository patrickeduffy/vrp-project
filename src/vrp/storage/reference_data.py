"""Validated, unit-explicit records for compact historical reference data."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
VINTAGE_KINDS = frozenset({"POINT_IN_TIME", "LATEST_REVISED", "UNKNOWN"})
DATASET_KINDS = frozenset({"REFERENCE_RATE", "DAILY_MARKET_FEATURES"})
PRICE_ADJUSTMENTS = frozenset(
    {"UNADJUSTED", "SPLIT_ADJUSTED", "TOTAL_RETURN_ADJUSTED", "UNKNOWN"}
)
CALCULATION_STATUSES = frozenset({"AVAILABLE", "WARMUP", "MISSING", "FAILED"})
QUALITY_STATUSES = frozenset({"PASS", "WARN", "FAIL"})


def _required_text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _uuid(value: UUID | None, name: str, *, optional: bool = False) -> UUID | None:
    if value is None and optional:
        return None
    if not isinstance(value, UUID):
        raise ValueError(f"{name} must be a UUID")
    return value


def _date_only(value: date | None, name: str, *, optional: bool = False) -> date | None:
    if value is None and optional:
        return None
    if type(value) is not date:
        raise ValueError(f"{name} must be a date without a time component")
    return value


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _decimal(value: Decimal | str | int | float, name: str) -> Decimal:
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal number") from exc
    if not normalized.is_finite():
        raise ValueError(f"{name} must be a finite decimal number")
    return normalized


def _decimal_at_scale(
    value: Decimal | str | int | float,
    name: str,
    *,
    scale: int,
    maximum_absolute: Decimal,
) -> Decimal:
    normalized = _decimal(value, name)
    if abs(normalized) >= maximum_absolute:
        raise ValueError(f"{name} exceeds the PostgreSQL precision contract")
    quantum = Decimal(1).scaleb(-scale)
    try:
        quantized = normalized.quantize(quantum)
    except InvalidOperation as exc:
        raise ValueError(f"{name} exceeds the PostgreSQL precision contract") from exc
    if quantized != normalized:
        raise ValueError(f"{name} cannot have more than {scale} fractional digits")
    return quantized


def _optional_float(value: float | int | None, name: str) -> float | None:
    if value is None:
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite or None") from exc
    if not math.isfinite(normalized):
        raise ValueError(f"{name} must be finite or None")
    return normalized


def _freeze_json(value: Any, name: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{name} must contain only finite JSON-compatible values")
        return value
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{name} JSON object keys must be strings")
            frozen[key] = _freeze_json(item, f"{name}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, f"{name}[]") for item in value)
    raise ValueError(f"{name} must contain only finite JSON-compatible values")


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _json_object(value: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    return _freeze_json(value, name)


def render_json_object(value: Mapping[str, Any]) -> str:
    """Render a validated immutable JSON object for a PostgreSQL JSONB parameter."""

    return json.dumps(
        _thaw_json(value),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _aware_datetime(value: datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return value


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    encoded = render_json_object(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_float(value: float | None) -> str | None:
    if value is None:
        return None
    return "0" if value == 0 else format(value, ".17g")


def _canonical_decimal(value: Decimal) -> str:
    return "0" if value == 0 else format(value.normalize(), "f")


def daily_market_feature_definition_sha256(
    *,
    definition_key: str,
    price_adjustment: str,
    return_formula_version: str,
    rsi_formula_version: str,
    rv_formula_version: str,
    definition: Mapping[str, Any],
    symbol: str = "SPY",
) -> str:
    """Hash every immutable field in the database feature-definition contract."""

    return _canonical_hash(
        {
            "annualization_sessions": 252,
            "close_unit": "USD_PER_SHARE",
            "definition": _json_object(definition, "definition"),
            "definition_key": _required_text(definition_key, "definition_key"),
            "exchange_calendar": "XNYS",
            "price_adjustment": price_adjustment,
            "return_formula_version": _required_text(
                return_formula_version,
                "return_formula_version",
            ),
            "return_unit": "DECIMAL_LOG_RETURN",
            "rsi_formula_version": _required_text(
                rsi_formula_version,
                "rsi_formula_version",
            ),
            "rsi_period_sessions": 14,
            "rsi_unit": "INDEX_0_100",
            "rv_formula_version": _required_text(
                rv_formula_version,
                "rv_formula_version",
            ),
            "rv_sample_ddof": 1,
            "rv_variance_unit": "ANNUALIZED_DECIMAL_VARIANCE",
            "rv_volatility_unit": "ANNUALIZED_PERCENTAGE_POINTS",
            "rv_window_sessions": 21,
            "symbol": symbol,
        }
    )


@dataclass(frozen=True)
class ReferenceDataRelease:
    """One immutable accepted normalization of a compact reference dataset."""

    release_id: UUID
    dataset_key: str
    dataset_kind: str
    dataset_schema_version: str
    normalized_content_sha256: str
    source_system: str
    loader_version: str
    normalized_data_asset_id: UUID
    vintage_kind: str
    retrieved_at: datetime
    source_row_count: int
    persisted_row_count: int
    qa_manifest_data_asset_id: UUID | None = None
    loaded_by_pipeline_run_id: UUID | None = None
    source_published_at: datetime | None = None
    observation_start_date: date | None = None
    observation_end_date: date | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _uuid(self.release_id, "release_id")
        _uuid(self.normalized_data_asset_id, "normalized_data_asset_id")
        _uuid(self.qa_manifest_data_asset_id, "qa_manifest_data_asset_id", optional=True)
        _uuid(self.loaded_by_pipeline_run_id, "loaded_by_pipeline_run_id", optional=True)
        object.__setattr__(self, "dataset_key", _required_text(self.dataset_key, "dataset_key"))
        if self.dataset_kind not in DATASET_KINDS:
            raise ValueError(f"dataset_kind must be one of {sorted(DATASET_KINDS)}")
        object.__setattr__(
            self,
            "dataset_schema_version",
            _required_text(self.dataset_schema_version, "dataset_schema_version"),
        )
        object.__setattr__(
            self,
            "normalized_content_sha256",
            _sha256(self.normalized_content_sha256, "normalized_content_sha256"),
        )
        object.__setattr__(self, "source_system", _required_text(self.source_system, "source_system"))
        object.__setattr__(self, "loader_version", _required_text(self.loader_version, "loader_version"))
        if self.vintage_kind not in VINTAGE_KINDS:
            raise ValueError(f"vintage_kind must be one of {sorted(VINTAGE_KINDS)}")
        _aware_datetime(self.retrieved_at, "retrieved_at")
        _aware_datetime(self.source_published_at, "source_published_at")
        _date_only(self.observation_start_date, "observation_start_date", optional=True)
        _date_only(self.observation_end_date, "observation_end_date", optional=True)
        if type(self.source_row_count) is not int or self.source_row_count < 0:
            raise ValueError("source_row_count must be a non-negative integer")
        if type(self.persisted_row_count) is not int or self.persisted_row_count < 0:
            raise ValueError("persisted_row_count must be a non-negative integer")
        if self.persisted_row_count > self.source_row_count:
            raise ValueError("persisted_row_count cannot exceed source_row_count")
        if (
            self.observation_start_date is not None
            and self.observation_end_date is not None
            and self.observation_end_date < self.observation_start_date
        ):
            raise ValueError("observation_end_date cannot precede observation_start_date")
        object.__setattr__(self, "metadata", _json_object(self.metadata, "metadata"))


@dataclass(frozen=True)
class ReferenceRateObservation:
    """A SOFR observation in FRED's source unit: annual percentage points."""

    observation_date: date
    rate_percent: Decimal
    series_key: str = "SOFR"

    def __post_init__(self) -> None:
        _date_only(self.observation_date, "observation_date")
        if self.series_key != "SOFR":
            raise ValueError("series_key must be SOFR")
        rate = _decimal_at_scale(
            self.rate_percent,
            "rate_percent",
            scale=12,
            maximum_absolute=Decimal("10000"),
        )
        if rate < Decimal("-5") or rate > Decimal("25"):
            raise ValueError("rate_percent must be between -5 and 25 percentage points")
        object.__setattr__(self, "rate_percent", rate)

    @property
    def rate_decimal(self) -> Decimal:
        return self.rate_percent / Decimal("100")

    @property
    def row_sha256(self) -> str:
        return _canonical_hash(
            {
                "observation_date": self.observation_date.isoformat(),
                "rate_percent": _canonical_decimal(self.rate_percent),
                "series_key": self.series_key,
                "source_unit": "ANNUAL_PERCENTAGE_POINTS",
            }
        )


@dataclass(frozen=True)
class StoredReferenceRateObservation:
    observation_id: UUID
    release_id: UUID
    value: ReferenceRateObservation
    supersedes_observation_id: UUID | None = None

    def __post_init__(self) -> None:
        _uuid(self.observation_id, "observation_id")
        _uuid(self.release_id, "release_id")
        _uuid(
            self.supersedes_observation_id,
            "supersedes_observation_id",
            optional=True,
        )
        if not isinstance(self.value, ReferenceRateObservation):
            raise ValueError("value must be a ReferenceRateObservation")
        if self.supersedes_observation_id == self.observation_id:
            raise ValueError("an observation cannot supersede itself")


@dataclass(frozen=True)
class DailyMarketFeatureDefinition:
    """Immutable formula and unit contract for the compact SPY daily panel."""

    definition_id: UUID
    definition_key: str
    version_label: str
    content_sha256: str
    price_adjustment: str
    return_formula_version: str
    rsi_formula_version: str
    rv_formula_version: str
    definition: Mapping[str, Any]
    symbol: str = "SPY"

    def __post_init__(self) -> None:
        _uuid(self.definition_id, "definition_id")
        object.__setattr__(self, "definition_key", _required_text(self.definition_key, "definition_key"))
        object.__setattr__(self, "version_label", _required_text(self.version_label, "version_label"))
        declared_sha256 = _sha256(self.content_sha256, "content_sha256")
        if self.symbol != "SPY":
            raise ValueError("symbol must be SPY")
        if self.price_adjustment not in PRICE_ADJUSTMENTS:
            raise ValueError(f"price_adjustment must be one of {sorted(PRICE_ADJUSTMENTS)}")
        object.__setattr__(
            self,
            "return_formula_version",
            _required_text(self.return_formula_version, "return_formula_version"),
        )
        object.__setattr__(
            self,
            "rsi_formula_version",
            _required_text(self.rsi_formula_version, "rsi_formula_version"),
        )
        object.__setattr__(
            self,
            "rv_formula_version",
            _required_text(self.rv_formula_version, "rv_formula_version"),
        )
        object.__setattr__(self, "definition", _json_object(self.definition, "definition"))
        calculated_sha256 = daily_market_feature_definition_sha256(
            definition_key=self.definition_key,
            price_adjustment=self.price_adjustment,
            return_formula_version=self.return_formula_version,
            rsi_formula_version=self.rsi_formula_version,
            rv_formula_version=self.rv_formula_version,
            definition=self.definition,
            symbol=self.symbol,
        )
        if declared_sha256 != calculated_sha256:
            raise ValueError(
                "content_sha256 does not match the immutable feature-definition contract"
            )
        object.__setattr__(self, "content_sha256", declared_sha256)


@dataclass(frozen=True)
class DailyMarketFeature:
    """One session of SPY close/return, Wilder RSI state, and signal RV21D."""

    trade_date: date
    spy_close: Decimal
    prior_trade_date: date | None
    spy_change: float | None
    spy_log_return: float | None
    wilder_avg_gain_14: float | None
    wilder_avg_loss_14: float | None
    rsi14: float | None
    rv21d_variance: float | None
    rv21d_volatility_pct: float | None
    calculation_status: str
    quality_status: str
    symbol: str = "SPY"
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _date_only(self.trade_date, "trade_date")
        _date_only(self.prior_trade_date, "prior_trade_date", optional=True)
        if self.symbol != "SPY":
            raise ValueError("symbol must be SPY")
        close = _decimal_at_scale(
            self.spy_close,
            "spy_close",
            scale=8,
            maximum_absolute=Decimal("1000000000000"),
        )
        if close <= 0:
            raise ValueError("spy_close must be greater than zero")
        object.__setattr__(self, "spy_close", close)
        if self.prior_trade_date is not None and self.prior_trade_date >= self.trade_date:
            raise ValueError("prior_trade_date must precede trade_date")
        for name in (
            "spy_change",
            "spy_log_return",
            "wilder_avg_gain_14",
            "wilder_avg_loss_14",
            "rsi14",
            "rv21d_variance",
            "rv21d_volatility_pct",
        ):
            object.__setattr__(self, name, _optional_float(getattr(self, name), name))
        for name in ("wilder_avg_gain_14", "wilder_avg_loss_14", "rv21d_variance", "rv21d_volatility_pct"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.rsi14 is not None and not 0 <= self.rsi14 <= 100:
            raise ValueError("rsi14 must be between 0 and 100")
        if self.calculation_status not in CALCULATION_STATUSES:
            raise ValueError(
                f"calculation_status must be one of {sorted(CALCULATION_STATUSES)}"
            )
        if self.quality_status not in QUALITY_STATUSES:
            raise ValueError(f"quality_status must be one of {sorted(QUALITY_STATUSES)}")
        if self.calculation_status == "AVAILABLE":
            required = (
                self.prior_trade_date,
                self.spy_change,
                self.spy_log_return,
                self.wilder_avg_gain_14,
                self.wilder_avg_loss_14,
                self.rsi14,
                self.rv21d_variance,
                self.rv21d_volatility_pct,
            )
            if any(value is None for value in required):
                raise ValueError("AVAILABLE daily features require complete return, RSI, and RV21D values")
        object.__setattr__(self, "details", _json_object(self.details, "details"))

    @property
    def row_sha256(self) -> str:
        return _canonical_hash(
            {
                "calculation_status": self.calculation_status,
                "details": self.details,
                "prior_trade_date": (
                    None if self.prior_trade_date is None else self.prior_trade_date.isoformat()
                ),
                "quality_status": self.quality_status,
                "rsi14": _canonical_float(self.rsi14),
                "rv21d_variance": _canonical_float(self.rv21d_variance),
                "rv21d_volatility_pct": _canonical_float(self.rv21d_volatility_pct),
                "spy_change": _canonical_float(self.spy_change),
                "spy_close": _canonical_decimal(self.spy_close),
                "spy_log_return": _canonical_float(self.spy_log_return),
                "symbol": self.symbol,
                "trade_date": self.trade_date.isoformat(),
                "wilder_avg_gain_14": _canonical_float(self.wilder_avg_gain_14),
                "wilder_avg_loss_14": _canonical_float(self.wilder_avg_loss_14),
            }
        )


@dataclass(frozen=True)
class StoredDailyMarketFeature:
    feature_id: UUID
    definition_id: UUID
    release_id: UUID
    value: DailyMarketFeature
    supersedes_feature_id: UUID | None = None

    def __post_init__(self) -> None:
        _uuid(self.feature_id, "feature_id")
        _uuid(self.definition_id, "definition_id")
        _uuid(self.release_id, "release_id")
        _uuid(self.supersedes_feature_id, "supersedes_feature_id", optional=True)
        if not isinstance(self.value, DailyMarketFeature):
            raise ValueError("value must be a DailyMarketFeature")
        if self.supersedes_feature_id == self.feature_id:
            raise ValueError("a daily feature cannot supersede itself")
