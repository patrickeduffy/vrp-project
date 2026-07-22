"""Fail-closed readers for the accepted SOFR and SPY daily artifacts."""

from __future__ import annotations

import math
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Iterable

import pandas as pd

from vrp.storage.reference_data import (
    DailyMarketFeature,
    DailyMarketFeatureDefinition,
    ReferenceRateObservation,
    daily_market_feature_definition_sha256,
)

from .canonical import canonical_json_bytes, float_text, make_normalized_history, sha256_bytes
from .ids import definition_id
from .models import NormalizedHistory

SOFR_SCHEMA_VERSION = "fred-sofr-history-v1"
DAILY_SCHEMA_VERSION = "spy-signal-daily-history-v1"
RSI_FORMULA_VERSION = "wilder_rsi14_spy_close_v3_clean_session_rebuild"
RETURN_FORMULA_VERSION = "canonical_spy_close_log_return_v1"
RV_FORMULA_VERSION = "sample_std_log_return_21d_ddof1_annualized_252_v1"
FLOAT_TOLERANCE = 1e-10
RETURN_TOLERANCE = 1e-14
SOFR_PRODUCTION_START = date(2018, 4, 3)
SPY_PRODUCTION_START = date(2018, 1, 2)
SOFR_ACCEPTED_BASELINE_END = date(2026, 7, 20)
SOFR_ACCEPTED_BASELINE_ROW_COUNT = 2071
SOFR_ACCEPTED_BASELINE_DATE_SHA256 = (
    "a6392b2fa3510a9e0c22e80d61612675278b3dcb857ab0fc5ceb3633f380508b"
)


def _require_columns(frame: pd.DataFrame, required: Iterable[str], dataset: str) -> None:
    missing = sorted(set(required).difference(str(column) for column in frame.columns))
    if missing:
        raise ValueError(f"{dataset} is missing required columns: {missing}")


def _normalized_dates(series: pd.Series, dataset: str) -> pd.Series:
    text = series.astype("string").str.strip()
    compact = text.str.fullmatch(r"\d{8}", na=False)
    if compact.all():
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        if pd.api.types.is_numeric_dtype(series.dtype):
            raise ValueError(f"{dataset} numeric dates must use YYYYMMDD")
        parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{dataset} contains invalid or missing dates")
    if getattr(parsed.dtype, "tz", None) is not None:
        raise ValueError(f"{dataset} dates must not contain a timezone")
    normalized = parsed.dt.normalize()
    if not parsed.eq(normalized).all():
        raise ValueError(f"{dataset} dates must not contain a time component")
    return normalized


def _numeric(series: pd.Series, name: str, *, allow_null: bool = False) -> pd.Series:
    original_null = series.isna()
    values = pd.to_numeric(series, errors="coerce")
    if (values.isna() & ~original_null).any():
        raise ValueError(f"{name} contains non-numeric text")
    if not allow_null and values.isna().any():
        raise ValueError(f"{name} contains missing or non-numeric values")
    non_null = values.dropna()
    if not all(math.isfinite(float(value)) for value in non_null):
        raise ValueError(f"{name} contains NaN or infinity")
    return values.astype(float)


def _unique_sorted(frame: pd.DataFrame, date_column: str, dataset: str) -> pd.DataFrame:
    out = frame.copy()
    out[date_column] = _normalized_dates(out[date_column], dataset)
    if out.duplicated(date_column).any():
        duplicates = out.loc[out.duplicated(date_column, keep=False), date_column]
        preview = sorted({str(value.date()) for value in duplicates})[:5]
        raise ValueError(f"{dataset} contains duplicate dates: {preview}")
    return out.sort_values(date_column).reset_index(drop=True)


def normalize_sofr_frame(
    frame: pd.DataFrame,
    *,
    enforce_production_coverage: bool = True,
) -> NormalizedHistory:
    """Normalize a FRED SOFR table without passing the rate through float."""

    _require_columns(frame, ("observation_date", "SOFR"), "SOFR")
    work = _unique_sorted(frame[["observation_date", "SOFR"]], "observation_date", "SOFR")
    if work["observation_date"].dt.dayofweek.isin([5, 6]).any():
        raise ValueError("SOFR history contains a weekend observation")
    if enforce_production_coverage:
        observed_start = pd.Timestamp(work.loc[0, "observation_date"]).date()
        if observed_start != SOFR_PRODUCTION_START:
            raise ValueError(
                "SOFR full-history source must begin on "
                f"{SOFR_PRODUCTION_START.isoformat()}; observed={observed_start}"
            )
        baseline = work.loc[
            work["observation_date"] <= pd.Timestamp(SOFR_ACCEPTED_BASELINE_END),
            "observation_date",
        ]
        baseline_dates = [pd.Timestamp(value).date().isoformat() for value in baseline]
        baseline_digest = sha256_bytes(
            canonical_json_bytes({"observation_dates": baseline_dates})
        )
        if (
            len(baseline_dates) != SOFR_ACCEPTED_BASELINE_ROW_COUNT
            or baseline_digest != SOFR_ACCEPTED_BASELINE_DATE_SHA256
        ):
            raise ValueError(
                "SOFR history does not contain the complete accepted baseline through "
                f"{SOFR_ACCEPTED_BASELINE_END.isoformat()}"
            )
    rows: list[ReferenceRateObservation] = []
    for item in work.itertuples(index=False):
        raw_rate = item.SOFR
        if pd.isna(raw_rate):
            raise ValueError("SOFR contains a missing rate")
        rows.append(
            ReferenceRateObservation(
                observation_date=pd.Timestamp(item.observation_date).date(),
                rate_percent=Decimal(str(raw_rate).strip()),
            )
        )
    return make_normalized_history(
        dataset_key="FRED_SOFR",
        dataset_kind="REFERENCE_RATE",
        schema_version=SOFR_SCHEMA_VERSION,
        source_system="FRED",
        vintage_kind="LATEST_REVISED",
        rows=rows,
        source_row_counts={"sofr": len(frame)},
        contract={
            "date_column": "observation_date",
            "rate_column": "SOFR",
            "selection_rule": "latest_observation_strictly_before_trade_date",
            "source_unit": "ANNUAL_PERCENTAGE_POINTS",
            "accepted_baseline_date_sha256": SOFR_ACCEPTED_BASELINE_DATE_SHA256,
        },
    )


def normalize_sofr_csv(
    path: Path,
    *,
    enforce_production_coverage: bool = True,
) -> NormalizedHistory:
    frame = pd.read_csv(
        path,
        dtype={"observation_date": "string", "SOFR": "string"},
        keep_default_na=True,
    )
    return normalize_sofr_frame(
        frame,
        enforce_production_coverage=enforce_production_coverage,
    )


def _same_dates(left: pd.Series, right: pd.Series, label: str) -> None:
    left_dates = tuple(pd.Timestamp(value).date() for value in left)
    right_dates = tuple(pd.Timestamp(value).date() for value in right)
    if left_dates != right_dates:
        left_set = set(left_dates)
        right_set = set(right_dates)
        raise ValueError(
            f"{label} dates do not match canonical SPY dates: "
            f"missing={sorted(left_set - right_set)[:5]} extra={sorted(right_set - left_set)[:5]}"
        )


def _assert_close(left: pd.Series, right: pd.Series, label: str) -> None:
    differences = (left.astype(float) - right.astype(float)).abs()
    if differences.max() > FLOAT_TOLERANCE:
        raise ValueError(
            f"{label} close differs from canonical SPY close; max_abs_diff={differences.max()}"
        )


def _price_decimal(value: float) -> Decimal:
    rounded = round(float(value), 8)
    if abs(float(value) - rounded) > FLOAT_TOLERANCE:
        raise ValueError("SPY close has more than eight material fractional digits")
    return Decimal(format(rounded, ".8f"))


def _assert_spy_identity(frame: pd.DataFrame, dataset: str) -> None:
    for column in ("symbol", "source_symbol"):
        if column in frame.columns:
            values = set(frame[column].dropna().astype(str).str.strip())
            if frame[column].isna().any() or values != {"SPY"}:
                raise ValueError(f"{dataset} {column} must identify only SPY")


def _require_provenance(frame: pd.DataFrame, dataset: str, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{dataset} is missing production provenance columns: {missing}")
    for column in columns:
        values = frame[column]
        if values.isna().any() or values.astype(str).str.strip().eq("").any():
            raise ValueError(f"{dataset} {column} contains missing provenance")


def _validate_xnys_sessions(dates: pd.Series) -> None:
    try:
        import pandas_market_calendars as market_calendars
    except ImportError as exc:  # pragma: no cover - pinned production dependency
        raise RuntimeError("pandas-market-calendars is required for XNYS validation") from exc
    observed = tuple(pd.Timestamp(value).date() for value in dates)
    schedule = market_calendars.get_calendar("XNYS").schedule(
        start_date=observed[0],
        end_date=observed[-1],
    )
    expected = tuple(pd.Timestamp(value).date() for value in schedule.index)
    if observed != expected:
        observed_set = set(observed)
        expected_set = set(expected)
        raise ValueError(
            "canonical SPY dates do not exactly match XNYS sessions: "
            f"missing={sorted(expected_set - observed_set)[:5]} "
            f"extra={sorted(observed_set - expected_set)[:5]}"
        )


def _rsi_value(avg_gain: float, avg_loss: float) -> float:
    if math.isclose(avg_loss, 0.0, abs_tol=1e-15):
        return 50.0 if math.isclose(avg_gain, 0.0, abs_tol=1e-15) else 100.0
    ratio = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + ratio)


def _validate_recursive_rsi(work: pd.DataFrame) -> None:
    for index in range(1, len(work)):
        change = float(work.loc[index, "spy_change"])
        expected_gain = (
            float(work.loc[index - 1, "wilder_avg_gain_14"]) * 13.0
            + max(change, 0.0)
        ) / 14.0
        expected_loss = (
            float(work.loc[index - 1, "wilder_avg_loss_14"]) * 13.0
            + max(-change, 0.0)
        ) / 14.0
        expected_rsi = _rsi_value(expected_gain, expected_loss)
        observed = (
            float(work.loc[index, "wilder_avg_gain_14"]),
            float(work.loc[index, "wilder_avg_loss_14"]),
            float(work.loc[index, "spy_wilder_rsi14"]),
        )
        expected = (expected_gain, expected_loss, expected_rsi)
        if any(abs(actual - wanted) > FLOAT_TOLERANCE for actual, wanted in zip(observed, expected)):
            trade_date = pd.Timestamp(work.loc[index, "trade_date"]).date()
            raise ValueError(f"Wilder RSI recursive state is inconsistent on {trade_date}")


def _build_definition(rsi: pd.DataFrame) -> DailyMarketFeatureDefinition:
    seed = rsi.iloc[0]
    seed_payload = {
        "rsi14": float_text(float(seed["spy_wilder_rsi14"])),
        "spy_change": float_text(float(seed["spy_change"])),
        "spy_close": format(Decimal(str(seed["spy_close"])).normalize(), "f"),
        "trade_date": pd.Timestamp(seed["trade_date"]).date().isoformat(),
        "wilder_avg_gain_14": float_text(float(seed["wilder_avg_gain_14"])),
        "wilder_avg_loss_14": float_text(float(seed["wilder_avg_loss_14"])),
    }
    seed_sha256 = sha256_bytes(canonical_json_bytes(seed_payload))
    definition_payload = {
        "adjustment_attested": False,
        "price_source_field": "close",
        "rsi_seed_state_sha256": seed_sha256,
        "rsi_seed_trade_date": seed_payload["trade_date"],
        "rsi_source_columns": [
            "spy_change",
            "wilder_avg_gain_14",
            "wilder_avg_loss_14",
            "spy_wilder_rsi14",
        ],
        "rv_source_column": "rv21d_vol_pct",
    }
    content_sha256 = daily_market_feature_definition_sha256(
        definition_key="SPY_SIGNAL_FEATURES",
        price_adjustment="UNKNOWN",
        return_formula_version=RETURN_FORMULA_VERSION,
        rsi_formula_version=RSI_FORMULA_VERSION,
        rv_formula_version=RV_FORMULA_VERSION,
        definition=definition_payload,
    )
    version_label = f"v1-{content_sha256[:12]}"
    return DailyMarketFeatureDefinition(
        definition_id=definition_id(
            "SPY_SIGNAL_FEATURES",
            version_label,
            content_sha256,
        ),
        definition_key="SPY_SIGNAL_FEATURES",
        version_label=version_label,
        content_sha256=content_sha256,
        price_adjustment="UNKNOWN",
        return_formula_version=RETURN_FORMULA_VERSION,
        rsi_formula_version=RSI_FORMULA_VERSION,
        rv_formula_version=RV_FORMULA_VERSION,
        definition=definition_payload,
    )


def normalize_spy_daily_frames(
    prices: pd.DataFrame,
    rsi: pd.DataFrame,
    realized_vol: pd.DataFrame,
    *,
    enforce_production_coverage: bool = True,
) -> NormalizedHistory:
    """Copy and reconcile the three accepted SPY daily histories."""

    _require_columns(prices, ("trade_date", "spy_close"), "SPY EOD")
    _require_columns(
        rsi,
        (
            "trade_date",
            "spy_close",
            "spy_change",
            "wilder_avg_gain_14",
            "wilder_avg_loss_14",
            "spy_wilder_rsi14",
            "rsi_formula_version",
        ),
        "Wilder RSI",
    )
    _assert_spy_identity(prices, "SPY EOD")
    _assert_spy_identity(rsi, "Wilder RSI")
    _assert_spy_identity(realized_vol, "SPY realized volatility")
    if enforce_production_coverage:
        _require_provenance(prices, "SPY EOD", ("source_symbol",))
        _require_provenance(rsi, "Wilder RSI", ("source_name",))
        _require_provenance(
            realized_vol,
            "SPY realized volatility",
            ("source_symbol", "rv21d_source"),
        )
    _require_columns(
        realized_vol,
        ("trade_date", "spy_close", "spy_log_return", "rv21d_vol_pct"),
        "SPY realized volatility",
    )

    price_work = _unique_sorted(
        prices[["trade_date", "spy_close"]], "trade_date", "SPY EOD"
    )
    rsi_work = _unique_sorted(
        rsi[
            [
                "trade_date",
                "spy_close",
                "spy_change",
                "wilder_avg_gain_14",
                "wilder_avg_loss_14",
                "spy_wilder_rsi14",
                "rsi_formula_version",
            ]
        ],
        "trade_date",
        "Wilder RSI",
    )
    rv_work = _unique_sorted(
        realized_vol[["trade_date", "spy_close", "spy_log_return", "rv21d_vol_pct"]],
        "trade_date",
        "SPY realized volatility",
    )
    _same_dates(price_work["trade_date"], rsi_work["trade_date"], "Wilder RSI")
    _same_dates(price_work["trade_date"], rv_work["trade_date"], "SPY realized volatility")
    if price_work["trade_date"].dt.dayofweek.isin([5, 6]).any():
        raise ValueError("canonical SPY history contains a weekend date")
    if enforce_production_coverage:
        observed_start = pd.Timestamp(price_work.loc[0, "trade_date"]).date()
        if observed_start != SPY_PRODUCTION_START:
            raise ValueError(
                "SPY full-history source must begin on "
                f"{SPY_PRODUCTION_START.isoformat()}; observed={observed_start}"
            )
        _validate_xnys_sessions(price_work["trade_date"])

    price_work["spy_close"] = _numeric(price_work["spy_close"], "SPY EOD spy_close")
    rsi_work["spy_close"] = _numeric(rsi_work["spy_close"], "Wilder RSI spy_close")
    rv_work["spy_close"] = _numeric(rv_work["spy_close"], "realized-vol spy_close")
    if not price_work["spy_close"].gt(0).all():
        raise ValueError("SPY EOD contains a non-positive close")
    _assert_close(price_work["spy_close"], rsi_work["spy_close"], "Wilder RSI")
    _assert_close(price_work["spy_close"], rv_work["spy_close"], "realized-vol")

    for column in (
        "spy_change",
        "wilder_avg_gain_14",
        "wilder_avg_loss_14",
        "spy_wilder_rsi14",
    ):
        rsi_work[column] = _numeric(rsi_work[column], f"Wilder RSI {column}")
    if not rsi_work["wilder_avg_gain_14"].ge(0).all() or not rsi_work[
        "wilder_avg_loss_14"
    ].ge(0).all():
        raise ValueError("Wilder average gain/loss cannot be negative")
    if not rsi_work["spy_wilder_rsi14"].between(0, 100).all():
        raise ValueError("Wilder RSI values must be within [0, 100]")
    if rsi_work["rsi_formula_version"].isna().any():
        raise ValueError("Wilder RSI formula version cannot be missing")
    observed_versions = set(rsi_work["rsi_formula_version"].astype(str))
    if observed_versions != {RSI_FORMULA_VERSION}:
        raise ValueError(
            f"Wilder RSI formula version mismatch: observed={sorted(observed_versions)}"
        )

    expected_changes = price_work["spy_close"].diff()
    change_differences = (rsi_work["spy_change"] - expected_changes).abs().iloc[1:]
    if len(change_differences) and change_differences.max() > FLOAT_TOLERANCE:
        raise ValueError("Wilder RSI spy_change does not match canonical SPY close changes")
    _validate_recursive_rsi(rsi_work)

    rv_work["spy_log_return"] = _numeric(
        rv_work["spy_log_return"], "realized-vol spy_log_return", allow_null=True
    )
    rv_work["rv21d_vol_pct"] = _numeric(
        rv_work["rv21d_vol_pct"], "realized-vol rv21d_vol_pct", allow_null=True
    )
    if not pd.isna(rv_work.loc[0, "spy_log_return"]):
        raise ValueError("the first SPY log return must be null")
    expected_returns = price_work["spy_close"].map(math.log).diff()
    return_differences = (rv_work["spy_log_return"] - expected_returns).abs().iloc[1:]
    if return_differences.isna().any() or (
        len(return_differences) and return_differences.max() > RETURN_TOLERANCE
    ):
        raise ValueError("stored SPY log returns do not match canonical closes")

    rv_available = rv_work["rv21d_vol_pct"].notna()
    if not rv_available.any():
        raise ValueError("RV21D has no available observations")
    first_available = int(rv_available[rv_available].index[0])
    if rv_available.iloc[:first_available].any() or not rv_available.iloc[first_available:].all():
        raise ValueError("RV21D may contain only a leading warm-up null block")
    if not rv_work.loc[rv_available, "rv21d_vol_pct"].ge(0).all():
        raise ValueError("RV21D volatility cannot be negative")
    expected_rv21d = (
        rv_work["spy_log_return"].rolling(21).std(ddof=1) * math.sqrt(252.0) * 100.0
    )
    if not expected_rv21d.isna().eq(rv_work["rv21d_vol_pct"].isna()).all():
        raise ValueError("RV21D warm-up mask does not match the locked 21-session formula")
    rv_differences = (rv_work["rv21d_vol_pct"] - expected_rv21d).abs().dropna()
    if len(rv_differences) and rv_differences.max() > FLOAT_TOLERANCE:
        raise ValueError("RV21D values do not match sample std(log return, 21, ddof=1)")

    definition = _build_definition(rsi_work)
    rows: list[DailyMarketFeature] = []
    for index in range(len(price_work)):
        trade_date = pd.Timestamp(price_work.loc[index, "trade_date"]).date()
        prior_trade_date: date | None = None
        if index:
            prior_trade_date = pd.Timestamp(price_work.loc[index - 1, "trade_date"]).date()
        rv_pct = rv_work.loc[index, "rv21d_vol_pct"]
        rv_pct_value = None if pd.isna(rv_pct) else float(rv_pct)
        rows.append(
            DailyMarketFeature(
                trade_date=trade_date,
                spy_close=_price_decimal(float(price_work.loc[index, "spy_close"])),
                prior_trade_date=prior_trade_date,
                spy_change=float(rsi_work.loc[index, "spy_change"]),
                spy_log_return=(
                    None
                    if pd.isna(rv_work.loc[index, "spy_log_return"])
                    else float(rv_work.loc[index, "spy_log_return"])
                ),
                wilder_avg_gain_14=float(rsi_work.loc[index, "wilder_avg_gain_14"]),
                wilder_avg_loss_14=float(rsi_work.loc[index, "wilder_avg_loss_14"]),
                rsi14=float(rsi_work.loc[index, "spy_wilder_rsi14"]),
                rv21d_variance=(
                    None if rv_pct_value is None else (rv_pct_value / 100.0) ** 2
                ),
                rv21d_volatility_pct=rv_pct_value,
                calculation_status=("AVAILABLE" if index >= first_available else "WARMUP"),
                quality_status="PASS",
            )
        )

    return make_normalized_history(
        dataset_key="SPY_SIGNAL_DAILY_FEATURES",
        dataset_kind="DAILY_MARKET_FEATURES",
        schema_version=DAILY_SCHEMA_VERSION,
        source_system="VRP_ACCEPTED_SPY_FEATURE_ARTIFACTS",
        vintage_kind="LATEST_REVISED",
        rows=rows,
        definition=definition,
        source_row_counts={
            "spy_eod": len(prices),
            "wilder_rsi": len(rsi),
            "rv21d": len(realized_vol),
        },
        contract={
            "return_formula_version": RETURN_FORMULA_VERSION,
            "rsi_formula_version": RSI_FORMULA_VERSION,
            "rv_formula_version": RV_FORMULA_VERSION,
            "rv_source_column": "rv21d_vol_pct",
        },
    )


def normalize_spy_daily_files(
    spy_eod_path: Path,
    rsi_path: Path,
    realized_vol_path: Path,
    *,
    enforce_production_coverage: bool = True,
) -> NormalizedHistory:
    return normalize_spy_daily_frames(
        pd.read_parquet(spy_eod_path),
        pd.read_parquet(rsi_path),
        pd.read_parquet(realized_vol_path),
        enforce_production_coverage=enforce_production_coverage,
    )
