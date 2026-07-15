#!/usr/bin/env python3
"""Tenor-isolated parameter sweep using repaired Wilder RSI.

Approved scope
--------------
* Signal source: repaired Wilder-RSI signal base.
* Outcome source: existing naked-ATM-put tenor sizing trade panel.
* Join key: trade_date x tenor.
* Tenors: 12, 15, 18, 21, 24, 27, 30, 33.
* Core and Secondary are evaluated independently.
* No one-trade-per-day suppression, no sizing, no production overwrite.
* Parameter grids are centered on the approved starting parameters.

The script resolves established source fields by explicit aliases and fails if
an outcome return cannot be identified unambiguously. It never selects a P&L
field merely because its name contains a generic substring.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCRIPT_NAME = "vrp_wilder_rsi_tenor_isolated_parameter_sweep_v1.py"
SCRIPT_VERSION = "1.0.0"

DEFAULT_SIGNAL_REL = Path(
    "data/processed/vrp_repaired_wilder_rsi_signal/"
    "vrp_repaired_wilder_rsi_signal_base_v1.parquet"
)
DEFAULT_OUTCOME_REL = Path(
    "data/processed/naked_atm_put_tenor_sizing_trades_v0_1.parquet"
)
DEFAULT_OUTPUT_DIR_REL = Path("data/processed/vrp_wilder_rsi_parameter_sweep")
DEFAULT_AUDIT_DIR_REL = Path(
    "data/audit/wilder_rsi_tenor_isolated_parameter_sweep"
)

BASE_OUTPUT_NAME = "vrp_wilder_rsi_tenor_isolated_parameter_sweep_results_v1.parquet"
LEADERBOARD_OUTPUT_NAME = "vrp_wilder_rsi_tenor_isolated_parameter_leaderboard_v1.parquet"

TARGET_TENORS = (12, 15, 18, 21, 24, 27, 30, 33)
MIN_RANKING_TRADES = 30
TOP_PER_SLEEVE = 25
TAIL_FRACTION = 0.05
WORST_FRACTION = 0.01

# Exact, approved starting values. Core Front and 9D are intentionally absent.
BASELINES: dict[tuple[str, int], dict[str, float]] = {
    ("Core", 21): {"vrp_log": 0.65, "z": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    ("Core", 24): {"vrp_log": 0.65, "z": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    ("Core", 27): {"vrp_log": 0.70, "z": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    ("Core", 30): {"vrp_log": 0.70, "z": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    ("Core", 33): {"vrp_log": 0.70, "z": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    ("Secondary", 12): {"vrp_log": 0.65, "z": 0.20, "rsi_cap": 75.0, "rv_floor": 7.0},
    ("Secondary", 15): {"vrp_log": 0.65, "z": 0.20, "rsi_cap": 75.0, "rv_floor": 7.0},
    ("Secondary", 18): {"vrp_log": 0.65, "z": 0.20, "rsi_cap": 75.0, "rv_floor": 7.0},
    ("Secondary", 21): {"vrp_log": 0.65, "z": 0.20, "rsi_cap": 76.0, "rv_floor": 7.0},
    ("Secondary", 24): {"vrp_log": 0.65, "z": 0.20, "rsi_cap": 76.0, "rv_floor": 7.0},
    ("Secondary", 27): {"vrp_log": 0.65, "z": 0.00, "rsi_cap": 77.0, "rv_floor": 6.5},
    ("Secondary", 30): {"vrp_log": 0.65, "z": 0.00, "rsi_cap": 77.0, "rv_floor": 6.5},
    ("Secondary", 33): {"vrp_log": 0.65, "z": 0.00, "rsi_cap": 77.0, "rv_floor": 6.5},
}

SIGNAL_ALIASES: dict[str, tuple[str, ...]] = {
    "trade_date": ("trade_date", "date", "signal_date"),
    "tenor": ("tenor", "dte", "target_days"),
    "vrp": ("model_vrp_log_final", "model_vrp_log"),
    "z3": ("z_3m_final", "model_vrp_z_3m", "z_3m"),
    "z1": ("z_1y_final", "model_vrp_z_1y", "z_1y"),
    "rsi": ("rsi14_final", "spy_wilder_rsi14"),
    "rv": ("rv21d_vol_pct_final", "rv21d_vol_pct"),
}

OUTCOME_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "trade_date": ("trade_date", "date", "signal_date", "entry_date"),
    "tenor": ("tenor", "dte", "target_days", "selected_tenor"),
}

# Direct normalized return fields, strongest/most explicit first. These are not
# generic substring matches; only exact normalized aliases are accepted.
DIRECT_RETURN_ALIASES: tuple[str, ...] = (
    "return_on_max_loss",
    "trade_return_on_max_loss",
    "pnl_on_max_loss",
    "pnl_pct_max_loss",
    "pnl_pct_of_max_loss",
    "return_on_max_risk",
    "trade_return_on_max_risk",
    "pnl_on_max_risk",
    "pnl_pct_max_risk",
    "pnl_pct_of_max_risk",
    "return_on_risk",
    "romr",
    "trade_return",
    "realized_return",
    "expiration_return",
    "expiry_return",
    "holding_period_return",
    "net_return",
    "return_pct",
    "return_percent",
    "pnl_pct",
    "pnl_percent",
)

RAW_PNL_ALIASES: tuple[str, ...] = (
    "trade_pnl",
    "realized_pnl",
    "expiration_pnl",
    "expiry_pnl",
    "net_pnl",
    "pnl",
    "trade_payoff",
    "payoff",
)

RISK_DENOMINATOR_ALIASES: tuple[str, ...] = (
    "max_loss",
    "max_loss_amount",
    "max_risk",
    "max_risk_amount",
    "capital_at_risk",
    "risk_amount",
    "initial_max_loss",
    "inception_max_loss",
)

EXPLICIT_PERCENT_FIELDS = {
    "return_pct",
    "return_percent",
    "pnl_pct",
    "pnl_percent",
}


@dataclass(frozen=True)
class OutcomeResolution:
    method: str
    return_field: str
    pnl_field: str | None
    risk_field: str | None
    scale_factor: float
    scale_reason: str


class ScopeError(RuntimeError):
    """Raised when an approved hard check fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_REL))
    parser.add_argument("--outcome-path", default=str(DEFAULT_OUTCOME_REL))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR_REL))
    parser.add_argument("--audit-dir", default=str(DEFAULT_AUDIT_DIR_REL))
    parser.add_argument("--min-ranking-trades", type=int, default=MIN_RANKING_TRADES)
    parser.add_argument("--top-per-sleeve", type=int, default=TOP_PER_SLEEVE)
    return parser.parse_args()


def normalized_path(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def resolve_path(project_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return normalized_path(path if path.is_absolute() else project_root / path)


def print_section(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def normalize_column_name(value: Any) -> str:
    text = str(value).strip().lower()
    chars = [ch if ch.isalnum() else "_" for ch in text]
    normalized = "".join(chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def normalized_column_map(columns: Iterable[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for column in columns:
        result.setdefault(normalize_column_name(column), []).append(column)
    return result


def resolve_exact_alias(
    columns: Sequence[str], aliases: Sequence[str], role: str, required: bool = True
) -> str | None:
    mapping = normalized_column_map(columns)
    matches: list[str] = []
    matched_aliases: list[str] = []
    for alias in aliases:
        normalized_alias = normalize_column_name(alias)
        for actual in mapping.get(normalized_alias, []):
            if actual not in matches:
                matches.append(actual)
                matched_aliases.append(alias)
    if not matches:
        if required:
            raise ScopeError(
                f"Could not resolve required {role}. Accepted aliases: {list(aliases)}; "
                f"available columns: {list(columns)}"
            )
        return None
    # Alias order is explicit precedence. Multiple physical columns matching the
    # same strongest alias are ambiguous and must fail.
    strongest_normalized = normalize_column_name(matched_aliases[0])
    strongest = [
        actual
        for actual in matches
        if normalize_column_name(actual) == strongest_normalized
    ]
    if len(strongest) > 1:
        raise ScopeError(f"Ambiguous {role}; equally strong columns found: {strongest}")
    return matches[0]


def parse_trade_date(series: pd.Series, role: str) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed = pd.to_datetime(series, errors="coerce")
    else:
        text = series.astype("string").str.strip()
        compact_mask = text.str.fullmatch(r"\d{8}", na=False)
        parsed = pd.to_datetime(text, errors="coerce")
        if compact_mask.any():
            parsed.loc[compact_mask] = pd.to_datetime(
                text.loc[compact_mask], format="%Y%m%d", errors="coerce"
            )
    parsed = parsed.dt.normalize()
    bad = int(parsed.isna().sum())
    if bad:
        raise ScopeError(f"{role} contains {bad:,} unparseable dates.")
    return parsed


def parse_tenor(series: pd.Series, role: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        extracted = series.astype("string").str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
        numeric = numeric.fillna(pd.to_numeric(extracted, errors="coerce"))
    bad = int(numeric.isna().sum())
    if bad:
        raise ScopeError(f"{role} contains {bad:,} unparseable tenor values.")
    rounded = numeric.round().astype(int)
    if not np.allclose(numeric.to_numpy(dtype=float), rounded.to_numpy(dtype=float), atol=1e-9):
        raise ScopeError(f"{role} contains non-integer tenor values.")
    return rounded


def coerce_numeric(series: pd.Series, role: str) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    inf_count = int(np.isinf(numeric.to_numpy(dtype=float, na_value=np.nan)).sum())
    if inf_count:
        raise ScopeError(f"{role} contains {inf_count:,} infinite values.")
    return numeric.astype(float)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, np.generic):
        return json_safe(value.item())
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if pd.isna(value):
        return None
    return str(value)


def candidate_field_matches(columns: Sequence[str], aliases: Sequence[str]) -> list[str]:
    mapping = normalized_column_map(columns)
    result: list[str] = []
    for alias in aliases:
        for actual in mapping.get(normalize_column_name(alias), []):
            if actual not in result:
                result.append(actual)
    return result


def resolve_outcome(frame: pd.DataFrame) -> tuple[pd.Series, OutcomeResolution]:
    columns = list(frame.columns)
    direct = candidate_field_matches(columns, DIRECT_RETURN_ALIASES)
    if direct:
        chosen = direct[0]
        # If two aliases of equal priority normalize to different physical columns,
        # exact precedence still picks the documented first alias. However, if the
        # chosen field is entirely null, move to the next explicit field.
        usable = []
        for field in direct:
            numeric = pd.to_numeric(frame[field], errors="coerce")
            if int(numeric.notna().sum()) > 0:
                usable.append(field)
        if not usable:
            raise ScopeError(f"Direct outcome fields were found but all are null: {direct}")
        chosen = usable[0]
        raw = coerce_numeric(frame[chosen], f"outcome field {chosen}")
        normalized = normalize_column_name(chosen)
        scale_factor = 1.0
        scale_reason = "source field used as stored"
        if normalized.endswith("_bps") or normalized.endswith("_basis_points"):
            scale_factor = 0.0001
            scale_reason = "explicit basis-point suffix"
        elif normalized in EXPLICIT_PERCENT_FIELDS:
            nonnull = raw.dropna()
            if nonnull.empty:
                raise ScopeError(f"Outcome field {chosen} has no non-null values.")
            # Explicit *_pct fields are only converted when their observed scale
            # clearly looks like whole percentage points. Otherwise they remain raw.
            q99_abs = float(nonnull.abs().quantile(0.99))
            if q99_abs > 2.0:
                scale_factor = 0.01
                scale_reason = f"explicit percent field with 99th abs percentile={q99_abs:.6g}"
            else:
                scale_reason = f"explicit percent field already fraction-scaled; 99th abs percentile={q99_abs:.6g}"
        resolved = raw * scale_factor
        return resolved, OutcomeResolution(
            method="direct_return_field",
            return_field=chosen,
            pnl_field=None,
            risk_field=None,
            scale_factor=scale_factor,
            scale_reason=scale_reason,
        )

    pnl_candidates = candidate_field_matches(columns, RAW_PNL_ALIASES)
    risk_candidates = candidate_field_matches(columns, RISK_DENOMINATOR_ALIASES)
    usable_pairs: list[tuple[str, str, pd.Series]] = []
    for pnl_field in pnl_candidates:
        pnl = pd.to_numeric(frame[pnl_field], errors="coerce")
        for risk_field in risk_candidates:
            risk = pd.to_numeric(frame[risk_field], errors="coerce").abs()
            valid = pnl.notna() & risk.notna() & (risk > 0)
            if valid.any():
                ratio = pd.Series(np.nan, index=frame.index, dtype=float)
                ratio.loc[valid] = pnl.loc[valid] / risk.loc[valid]
                usable_pairs.append((pnl_field, risk_field, ratio))
    if len(usable_pairs) == 1:
        pnl_field, risk_field, ratio = usable_pairs[0]
        return ratio, OutcomeResolution(
            method="raw_pnl_divided_by_absolute_risk",
            return_field=f"{pnl_field}/{risk_field}",
            pnl_field=pnl_field,
            risk_field=risk_field,
            scale_factor=1.0,
            scale_reason="deterministic P&L / abs(max-risk) construction",
        )
    if len(usable_pairs) > 1:
        pairs = [(pnl, risk) for pnl, risk, _ in usable_pairs]
        raise ScopeError(
            "No direct normalized return field was found and multiple usable P&L/risk "
            f"pairs exist, so the return mapping is ambiguous: {pairs}"
        )
    raise ScopeError(
        "Could not resolve a realized return. No accepted direct return field and no unique "
        f"P&L/risk pair were found. Direct aliases={list(DIRECT_RETURN_ALIASES)}; "
        f"P&L aliases={list(RAW_PNL_ALIASES)}; risk aliases={list(RISK_DENOMINATOR_ALIASES)}"
    )


def collapse_identical_outcome_duplicates(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    key = ["trade_date", "tenor"]
    duplicate_mask = frame.duplicated(key, keep=False)
    if not duplicate_mask.any():
        return frame, 0
    duplicate_groups = frame.loc[duplicate_mask].groupby(key, dropna=False, sort=False)
    conflicts: list[dict[str, Any]] = []
    for (trade_date, tenor), group in duplicate_groups:
        values = group["trade_return"].dropna().to_numpy(dtype=float)
        if len(values) == 0:
            continue
        if not np.allclose(values, values[0], rtol=0.0, atol=1e-12):
            conflicts.append(
                {
                    "trade_date": trade_date,
                    "tenor": int(tenor),
                    "rows": len(group),
                    "returns": values[:10].tolist(),
                }
            )
            if len(conflicts) >= 10:
                break
    if conflicts:
        raise ScopeError(
            "Outcome source has conflicting returns for trade_date x tenor. "
            f"Examples: {json.dumps(json_safe(conflicts))}"
        )
    before = len(frame)
    collapsed = (
        frame.sort_values(key)
        .drop_duplicates(key, keep="first")
        .reset_index(drop=True)
    )
    return collapsed, before - len(collapsed)


def grid_values(center: float, offsets: Sequence[float], decimals: int = 10) -> list[float]:
    return sorted({round(center + offset, decimals) for offset in offsets})


def build_parameter_grid(baseline: Mapping[str, float]) -> list[dict[str, float]]:
    vrp_values = grid_values(baseline["vrp_log"], (-0.10, -0.05, 0.0, 0.05, 0.10))
    z_values = grid_values(baseline["z"], (-0.20, -0.10, 0.0, 0.10, 0.20))
    rsi_values = grid_values(baseline["rsi_cap"], tuple(float(x) for x in range(-5, 6)))
    rv_values = grid_values(baseline["rv_floor"], (-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5))
    return [
        {"vrp_log": vrp, "z": z, "rsi_cap": rsi, "rv_floor": rv}
        for vrp, z, rsi, rv in itertools.product(vrp_values, z_values, rsi_values, rv_values)
    ]


def worst_expected_shortfall(values: np.ndarray, fraction: float) -> float:
    if len(values) == 0:
        return np.nan
    count = max(1, int(math.ceil(len(values) * fraction)))
    # np.partition avoids a complete sort for this metric.
    worst = np.partition(values, count - 1)[:count]
    return float(np.mean(worst))


def max_drawdown_additive(values: np.ndarray) -> float:
    if len(values) == 0:
        return np.nan
    cumulative = np.cumsum(values, dtype=float)
    wealth = np.concatenate(([0.0], cumulative))
    running_peak = np.maximum.accumulate(wealth)
    drawdowns = wealth - running_peak
    return float(np.min(drawdowns))


def largest_consecutive_losing_cluster(values: np.ndarray) -> tuple[float, int]:
    worst_sum = 0.0
    worst_count = 0
    current_sum = 0.0
    current_count = 0
    for value in values:
        if value < 0:
            current_sum += float(value)
            current_count += 1
            if current_sum < worst_sum:
                worst_sum = current_sum
                worst_count = current_count
        else:
            current_sum = 0.0
            current_count = 0
    return float(worst_sum), int(worst_count)


def calculate_metrics(returns: np.ndarray, eligible_rows: int) -> dict[str, Any]:
    trade_count = int(len(returns))
    if trade_count == 0:
        return {
            "trade_count": 0,
            "frequency": 0.0 if eligible_rows else np.nan,
            "win_rate": np.nan,
            "avg_return": np.nan,
            "avg_win": np.nan,
            "avg_loss": np.nan,
            "max_loss": np.nan,
            "tail_loss": np.nan,
            "worst_1pct_loss": np.nan,
            "profit_factor": np.nan,
            "max_drawdown": np.nan,
            "largest_losing_cluster": np.nan,
            "largest_losing_cluster_count": 0,
        }
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    gross_profit = float(np.sum(wins)) if len(wins) else 0.0
    gross_loss = float(-np.sum(losses)) if len(losses) else 0.0
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = np.inf
    else:
        profit_factor = np.nan
    cluster_sum, cluster_count = largest_consecutive_losing_cluster(returns)
    return {
        "trade_count": trade_count,
        "frequency": float(trade_count / eligible_rows) if eligible_rows else np.nan,
        "win_rate": float(np.mean(returns > 0)),
        "avg_return": float(np.mean(returns)),
        "avg_win": float(np.mean(wins)) if len(wins) else np.nan,
        "avg_loss": float(np.mean(losses)) if len(losses) else np.nan,
        "max_loss": float(np.min(returns)),
        "tail_loss": worst_expected_shortfall(returns, TAIL_FRACTION),
        "worst_1pct_loss": worst_expected_shortfall(returns, WORST_FRACTION),
        "profit_factor": float(profit_factor),
        "max_drawdown": max_drawdown_additive(returns),
        "largest_losing_cluster": cluster_sum,
        "largest_losing_cluster_count": cluster_count,
    }


def percentile_rank_higher_is_better(series: pd.Series, eligible: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=series.index, dtype=float)
    adjusted = series.loc[eligible].copy()
    if adjusted.empty:
        return result
    finite = adjusted.replace([np.inf, -np.inf], np.nan).dropna()
    if finite.empty:
        # This can occur for profit factor when every eligible set has no losses.
        # Positive infinity is best, negative infinity is worst, and NaN remains unranked.
        adjusted = adjusted.replace(np.inf, 1.0).replace(-np.inf, -1.0)
    else:
        finite_max = float(finite.max())
        finite_min = float(finite.min())
        adjusted = adjusted.replace(np.inf, finite_max + max(1.0, abs(finite_max) * 0.01))
        adjusted = adjusted.replace(-np.inf, finite_min - max(1.0, abs(finite_min) * 0.01))
    result.loc[eligible] = adjusted.rank(method="average", pct=True, ascending=True)
    return result


def assign_ranks(results: pd.DataFrame, min_trades: int) -> pd.DataFrame:
    metric_columns = [
        "win_rate",
        "avg_return",
        "profit_factor",
        "worst_1pct_loss",
        "max_drawdown",
        "largest_losing_cluster",
    ]
    ranked_parts: list[pd.DataFrame] = []
    for (_, _), group in results.groupby(["layer", "tenor"], sort=False):
        group = group.copy()
        eligible = group["trade_count"] >= min_trades
        percentile_columns = []
        for metric in metric_columns:
            pct_col = f"rank_pct_{metric}"
            group[pct_col] = percentile_rank_higher_is_better(group[metric], eligible)
            percentile_columns.append(pct_col)
        group["ranking_eligible"] = eligible
        group["parameter_quality_score"] = group[percentile_columns].mean(axis=1, skipna=False)
        group["parameter_set_rank"] = np.nan
        if eligible.any():
            ordered = group.loc[eligible].sort_values(
                [
                    "parameter_quality_score",
                    "win_rate",
                    "avg_return",
                    "worst_1pct_loss",
                    "trade_count",
                    "parameter_set_id",
                ],
                ascending=[False, False, False, False, False, True],
                kind="mergesort",
            )
            group.loc[ordered.index, "parameter_set_rank"] = np.arange(1, len(ordered) + 1)
        ranked_parts.append(group)
    ranked = pd.concat(ranked_parts, ignore_index=True)
    ranked["parameter_set_rank"] = ranked["parameter_set_rank"].astype("Int64")
    return ranked


def build_leaderboard(results: pd.DataFrame, top_per_sleeve: int) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for (_, _), group in results.groupby(["layer", "tenor"], sort=False):
        top = group.loc[group["ranking_eligible"]].nsmallest(
            top_per_sleeve, "parameter_set_rank"
        )
        baseline = group.loc[group["is_baseline"]]
        combined = pd.concat([top, baseline], ignore_index=False)
        combined = combined.loc[~combined.index.duplicated(keep="first")]
        pieces.append(combined)
    leaderboard = pd.concat(pieces, ignore_index=True)
    return leaderboard.sort_values(
        ["layer_sort", "tenor", "is_baseline", "parameter_set_rank"],
        ascending=[True, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)


def ensure_production_paths_not_targeted(project_root: Path, outputs: Sequence[Path]) -> None:
    production_dir = normalized_path(project_root / "data/processed/vrp_final_signal")
    for output in outputs:
        normalized = normalized_path(output)
        try:
            normalized.relative_to(production_dir)
        except ValueError:
            continue
        raise ScopeError(f"Refusing to write parameter-sweep output into production directory: {normalized}")


def main() -> int:
    args = parse_args()
    project_root = normalized_path(Path(args.project_root))
    signal_path = resolve_path(project_root, args.signal_path)
    outcome_path = resolve_path(project_root, args.outcome_path)
    output_dir = resolve_path(project_root, args.output_dir)
    audit_dir = resolve_path(project_root, args.audit_dir)
    results_output = output_dir / BASE_OUTPUT_NAME
    leaderboard_output = output_dir / LEADERBOARD_OUTPUT_NAME
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_output = audit_dir / f"parameter_sweep_manifest_{timestamp}.json"
    baseline_output = audit_dir / f"parameter_sweep_baseline_results_{timestamp}.csv"
    top_output = audit_dir / f"parameter_sweep_top_candidates_{timestamp}.csv"
    join_output = audit_dir / f"parameter_sweep_join_coverage_{timestamp}.csv"

    print_section("Tenor-isolated repaired-Wilder-RSI parameter sweep")
    print(f"Script:             {SCRIPT_NAME} v{SCRIPT_VERSION}")
    print(f"Project root:       {project_root}")
    print(f"Signal input:       {signal_path}")
    print(f"Outcome input:      {outcome_path}")
    print(f"Results output:     {results_output}")
    print(f"Leaderboard output: {leaderboard_output}")
    print(f"Audit directory:    {audit_dir}")
    print(f"Target tenors:      {list(TARGET_TENORS)}")
    print("Selection rule:     none; every qualifying tenor is counted independently")
    print("Core/Secondary:     evaluated separately")
    print(f"Minimum rank trades:{args.min_ranking_trades}")

    if not project_root.exists():
        raise ScopeError(f"Project root does not exist: {project_root}")
    if not signal_path.exists():
        raise ScopeError(f"Signal input does not exist: {signal_path}")
    if not outcome_path.exists():
        raise ScopeError(f"Outcome input does not exist: {outcome_path}")
    ensure_production_paths_not_targeted(project_root, [results_output, leaderboard_output])

    print_section("Loading and resolving inputs")
    signal_raw = pd.read_parquet(signal_path)
    outcome_raw = pd.read_parquet(outcome_path)
    print(f"Signal rows/columns:  {len(signal_raw):,} / {len(signal_raw.columns):,}")
    print(f"Outcome rows/columns: {len(outcome_raw):,} / {len(outcome_raw.columns):,}")

    signal_cols = {
        role: resolve_exact_alias(signal_raw.columns, aliases, f"signal {role}")
        for role, aliases in SIGNAL_ALIASES.items()
    }
    outcome_date_col = resolve_exact_alias(
        outcome_raw.columns, OUTCOME_KEY_ALIASES["trade_date"], "outcome trade_date"
    )
    outcome_tenor_col = resolve_exact_alias(
        outcome_raw.columns, OUTCOME_KEY_ALIASES["tenor"], "outcome tenor"
    )
    outcome_return, outcome_resolution = resolve_outcome(outcome_raw)

    print("Resolved signal columns:")
    for role, column in signal_cols.items():
        print(f"  {role:<12} -> {column}")
    print("Resolved outcome columns:")
    print(f"  trade_date   -> {outcome_date_col}")
    print(f"  tenor        -> {outcome_tenor_col}")
    print(f"  return       -> {outcome_resolution.return_field}")
    print(f"  method       -> {outcome_resolution.method}")
    print(f"  scale        -> {outcome_resolution.scale_factor} ({outcome_resolution.scale_reason})")

    signal = pd.DataFrame(
        {
            "trade_date": parse_trade_date(signal_raw[signal_cols["trade_date"]], "signal trade_date"),
            "tenor": parse_tenor(signal_raw[signal_cols["tenor"]], "signal tenor"),
            "model_vrp_log": coerce_numeric(signal_raw[signal_cols["vrp"]], "signal model_vrp_log"),
            "z_3m": coerce_numeric(signal_raw[signal_cols["z3"]], "signal z_3m"),
            "z_1y": coerce_numeric(signal_raw[signal_cols["z1"]], "signal z_1y"),
            "rsi14": coerce_numeric(signal_raw[signal_cols["rsi"]], "signal rsi14"),
            "rv21d_vol_pct": coerce_numeric(signal_raw[signal_cols["rv"]], "signal rv21d_vol_pct"),
        }
    )
    outcome = pd.DataFrame(
        {
            "trade_date": parse_trade_date(outcome_raw[outcome_date_col], "outcome trade_date"),
            "tenor": parse_tenor(outcome_raw[outcome_tenor_col], "outcome tenor"),
            "trade_return": outcome_return,
        }
    )

    signal = signal.loc[signal["tenor"].isin(TARGET_TENORS)].copy()
    outcome = outcome.loc[outcome["tenor"].isin(TARGET_TENORS)].copy()

    if signal.duplicated(["trade_date", "tenor"]).any():
        examples = signal.loc[
            signal.duplicated(["trade_date", "tenor"], keep=False), ["trade_date", "tenor"]
        ].head(20)
        raise ScopeError(
            "Signal source is not unique on trade_date x tenor. Examples:\n"
            + examples.to_string(index=False)
        )

    outcome, duplicate_rows_collapsed = collapse_identical_outcome_duplicates(outcome)
    if outcome.duplicated(["trade_date", "tenor"]).any():
        raise ScopeError("Outcome source remains non-unique on trade_date x tenor after identical-value collapse.")

    signal_tenors = sorted(signal["tenor"].unique().tolist())
    outcome_tenors = sorted(outcome["tenor"].unique().tolist())
    missing_signal_tenors = sorted(set(TARGET_TENORS) - set(signal_tenors))
    missing_outcome_tenors = sorted(set(TARGET_TENORS) - set(outcome_tenors))
    if missing_signal_tenors:
        raise ScopeError(f"Signal input is missing target tenors: {missing_signal_tenors}")
    if missing_outcome_tenors:
        raise ScopeError(f"Outcome input is missing target tenors: {missing_outcome_tenors}")

    max_outcome_tenors_per_date = int(outcome.groupby("trade_date")["tenor"].nunique().max())
    if max_outcome_tenors_per_date <= 1:
        raise ScopeError(
            "Outcome source does not preserve multiple tenors per date; it appears to be one-trade-per-day filtered."
        )

    # Outcome rows without a realized return are allowed only as incomplete/unmatured
    # rows; they are excluded from the sweep and fully documented by tenor/date.
    outcome_valid = outcome.loc[outcome["trade_return"].notna()].copy()
    if outcome_valid.empty:
        raise ScopeError("Outcome source contains no non-null realized returns for target tenors.")

    # Null outcome rows are acceptable only after the latest realized date for that tenor
    # (unmatured tail). Historical holes would bias a parameter sweep and therefore fail.
    historical_null_examples: list[dict[str, Any]] = []
    for tenor in TARGET_TENORS:
        tenor_outcome = outcome.loc[outcome["tenor"] == tenor].sort_values("trade_date")
        nonnull = tenor_outcome.loc[tenor_outcome["trade_return"].notna()]
        if nonnull.empty:
            continue
        last_realized = nonnull["trade_date"].max()
        historical_nulls = tenor_outcome.loc[
            tenor_outcome["trade_return"].isna() & (tenor_outcome["trade_date"] <= last_realized)
        ]
        for row in historical_nulls.head(10).itertuples(index=False):
            historical_null_examples.append(
                {"trade_date": row.trade_date, "tenor": int(row.tenor)}
            )
    if historical_null_examples:
        raise ScopeError(
            "Outcome source contains missing realized returns inside the usable historical period. "
            f"Examples: {json.dumps(json_safe(historical_null_examples[:20]))}"
        )

    joined = signal.merge(
        outcome_valid,
        on=["trade_date", "tenor"],
        how="inner",
        validate="one_to_one",
    )
    joined = joined.sort_values(["tenor", "trade_date"], kind="mergesort").reset_index(drop=True)
    if joined.empty:
        raise ScopeError("Signal and outcome inputs have no overlapping trade_date x tenor rows.")

    # A realized outcome row must have all signal inputs needed by the rule. Expected
    # z-score warmups occur before the usable sweep period and are excluded explicitly.
    signal_required = ["model_vrp_log", "z_3m", "z_1y", "rsi14", "rv21d_vol_pct"]
    joined["signal_inputs_complete"] = joined[signal_required].notna().all(axis=1)
    joined_usable = joined.loc[joined["signal_inputs_complete"]].copy()
    if joined_usable.empty:
        raise ScopeError("No joined rows have complete signal inputs and a realized trade outcome.")

    joined_tenors = sorted(joined_usable["tenor"].unique().tolist())
    missing_joined_tenors = sorted(set(TARGET_TENORS) - set(joined_tenors))
    if missing_joined_tenors:
        raise ScopeError(f"No usable joined outcomes for target tenors: {missing_joined_tenors}")

    # For each tenor, every complete signal row from the first through the last realized
    # outcome date must have an outcome. Otherwise looser parameter combinations could
    # qualify on dates whose performance is absent, creating selection bias.
    missing_coverage_examples: list[dict[str, Any]] = []
    for tenor in TARGET_TENORS:
        valid_outcome_t = outcome_valid.loc[outcome_valid["tenor"] == tenor]
        first_realized = valid_outcome_t["trade_date"].min()
        last_realized = valid_outcome_t["trade_date"].max()
        signal_t = signal.loc[
            (signal["tenor"] == tenor)
            & (signal["trade_date"] >= first_realized)
            & (signal["trade_date"] <= last_realized)
        ].copy()
        signal_t = signal_t.loc[signal_t[signal_required].notna().all(axis=1)]
        expected_keys = signal_t[["trade_date", "tenor"]]
        actual_keys = valid_outcome_t[["trade_date", "tenor"]]
        missing_keys = expected_keys.merge(
            actual_keys, on=["trade_date", "tenor"], how="left", indicator=True
        )
        missing_keys = missing_keys.loc[missing_keys["_merge"] == "left_only"]
        for row in missing_keys.head(10).itertuples(index=False):
            missing_coverage_examples.append(
                {"trade_date": row.trade_date, "tenor": int(row.tenor)}
            )
    if missing_coverage_examples:
        raise ScopeError(
            "Outcome source does not cover every complete signal date x tenor inside its realized period. "
            "This would bias looser parameter sets. "
            f"Examples: {json.dumps(json_safe(missing_coverage_examples[:20]))}"
        )

    join_rows: list[dict[str, Any]] = []
    for tenor in TARGET_TENORS:
        sig = signal.loc[signal["tenor"] == tenor]
        out_all = outcome.loc[outcome["tenor"] == tenor]
        out_valid = outcome_valid.loc[outcome_valid["tenor"] == tenor]
        joined_all_t = joined.loc[joined["tenor"] == tenor]
        usable = joined_usable.loc[joined_usable["tenor"] == tenor]
        join_rows.append(
            {
                "tenor": tenor,
                "signal_rows": len(sig),
                "signal_first_date": sig["trade_date"].min(),
                "signal_last_date": sig["trade_date"].max(),
                "outcome_rows": len(out_all),
                "outcome_nonnull_rows": len(out_valid),
                "outcome_null_rows": int(out_all["trade_return"].isna().sum()),
                "outcome_first_date": out_valid["trade_date"].min(),
                "outcome_last_date": out_valid["trade_date"].max(),
                "joined_rows": len(joined_all_t),
                "joined_complete_signal_rows": len(usable),
                "joined_incomplete_signal_rows": int((~joined_all_t["signal_inputs_complete"]).sum()),
                "join_coverage_vs_nonnull_outcomes": (
                    len(joined_all_t) / len(out_valid) if len(out_valid) else np.nan
                ),
            }
        )
    join_coverage = pd.DataFrame(join_rows)

    print(f"Signal target-tenor rows:       {len(signal):,}")
    print(f"Outcome target-tenor rows:      {len(outcome):,}")
    print(f"Outcome rows with return:       {len(outcome_valid):,}")
    print(f"Identical duplicate rows folded:{duplicate_rows_collapsed:,}")
    print(f"Max outcome tenors per date:    {max_outcome_tenors_per_date}")
    print(f"Joined realized rows:           {len(joined):,}")
    print(f"Joined usable rows:             {len(joined_usable):,}")
    print("\nJoin coverage by tenor:")
    print(join_coverage.to_string(index=False))

    print_section("Running isolated parameter grids")
    result_rows: list[dict[str, Any]] = []
    expected_grid_size = 5 * 5 * 11 * 7
    total_sleeves = len(BASELINES)
    for sleeve_number, ((layer, tenor), baseline) in enumerate(BASELINES.items(), start=1):
        tenor_frame = joined_usable.loc[joined_usable["tenor"] == tenor].sort_values("trade_date")
        eligible_rows = len(tenor_frame)
        vrp = tenor_frame["model_vrp_log"].to_numpy(dtype=float)
        z3 = tenor_frame["z_3m"].to_numpy(dtype=float)
        z1 = tenor_frame["z_1y"].to_numpy(dtype=float)
        rsi = tenor_frame["rsi14"].to_numpy(dtype=float)
        rv = tenor_frame["rv21d_vol_pct"].to_numpy(dtype=float)
        returns = tenor_frame["trade_return"].to_numpy(dtype=float)
        dates = tenor_frame["trade_date"].to_numpy()
        grid = build_parameter_grid(baseline)
        if len(grid) != expected_grid_size:
            raise ScopeError(
                f"Unexpected parameter-grid size for {layer} {tenor}D: {len(grid)} != {expected_grid_size}"
            )
        baseline_hits = 0
        for parameter_number, params in enumerate(grid, start=1):
            is_baseline = all(
                math.isclose(params[key], baseline[key], rel_tol=0.0, abs_tol=1e-12)
                for key in ("vrp_log", "z", "rsi_cap", "rv_floor")
            )
            baseline_hits += int(is_baseline)
            mask = (
                (vrp > params["vrp_log"])
                & (z3 > params["z"])
                & (z1 > params["z"])
                & (rsi < params["rsi_cap"])
                & (rv > params["rv_floor"])
            )
            selected_returns = returns[mask]
            selected_dates = dates[mask]
            metrics = calculate_metrics(selected_returns, eligible_rows)
            result_rows.append(
                {
                    "layer": layer,
                    "layer_sort": 0 if layer == "Core" else 1,
                    "tenor": tenor,
                    "parameter_set_id": (
                        f"{layer.lower()}_{tenor:02d}d_"
                        f"vrp{params['vrp_log']:.2f}_z{params['z']:.2f}_"
                        f"rsi{params['rsi_cap']:.0f}_rv{params['rv_floor']:.1f}"
                    ),
                    "vrp_log_threshold": params["vrp_log"],
                    "z_3m_threshold": params["z"],
                    "z_1y_threshold": params["z"],
                    "rsi_cap": params["rsi_cap"],
                    "rv21d_floor": params["rv_floor"],
                    "is_baseline": is_baseline,
                    "eligible_outcome_rows": eligible_rows,
                    "first_qualifying_date": pd.Timestamp(selected_dates.min()) if len(selected_dates) else pd.NaT,
                    "last_qualifying_date": pd.Timestamp(selected_dates.max()) if len(selected_dates) else pd.NaT,
                    **metrics,
                }
            )
        if baseline_hits != 1:
            raise ScopeError(
                f"Baseline parameter set was not included exactly once for {layer} {tenor}D; hits={baseline_hits}"
            )
        print(
            f"[{sleeve_number:02d}/{total_sleeves}] {layer:<9} {tenor:>2}D | "
            f"eligible rows={eligible_rows:,} | parameter sets={len(grid):,}"
        )

    results = pd.DataFrame(result_rows)
    if len(results) != expected_grid_size * total_sleeves:
        raise ScopeError(
            f"Unexpected total result rows: {len(results):,} != {expected_grid_size * total_sleeves:,}"
        )
    results = assign_ranks(results, args.min_ranking_trades)
    results = results.sort_values(
        ["layer_sort", "tenor", "parameter_set_rank", "parameter_set_id"],
        na_position="last",
        kind="mergesort",
    ).reset_index(drop=True)
    leaderboard = build_leaderboard(results, args.top_per_sleeve)
    baseline_results = results.loc[results["is_baseline"]].copy()
    top_candidates = (
        results.loc[results["ranking_eligible"]]
        .sort_values(["layer_sort", "tenor", "parameter_set_rank"], kind="mergesort")
        .groupby(["layer", "tenor"], sort=False, as_index=False)
        .head(args.top_per_sleeve)
        .reset_index(drop=True)
    )

    baseline_counts = baseline_results.groupby(["layer", "tenor"]).size()
    if len(baseline_results) != total_sleeves or not (baseline_counts == 1).all():
        raise ScopeError("Baseline results are missing or duplicated for one or more sleeves.")

    print_section("Writing non-production outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    results.to_parquet(results_output, index=False)
    leaderboard.to_parquet(leaderboard_output, index=False)
    baseline_results.to_csv(baseline_output, index=False)
    top_candidates.to_csv(top_output, index=False)
    join_coverage.to_csv(join_output, index=False)

    # Reopen and verify the primary outputs.
    results_check = pd.read_parquet(results_output)
    leaderboard_check = pd.read_parquet(leaderboard_output)
    if len(results_check) != len(results):
        raise ScopeError("Written results parquet failed row-count verification.")
    if results_check["parameter_set_id"].duplicated().any():
        raise ScopeError("Written results parquet has duplicate parameter_set_id values.")
    if len(leaderboard_check) != len(leaderboard):
        raise ScopeError("Written leaderboard parquet failed row-count verification.")

    manifest = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "run_timestamp": timestamp,
        "project_root": project_root,
        "inputs": {
            "signal_path": signal_path,
            "signal_sha256": file_sha256(signal_path),
            "signal_rows_raw": len(signal_raw),
            "outcome_path": outcome_path,
            "outcome_sha256": file_sha256(outcome_path),
            "outcome_rows_raw": len(outcome_raw),
        },
        "resolved_columns": {
            "signal": signal_cols,
            "outcome_trade_date": outcome_date_col,
            "outcome_tenor": outcome_tenor_col,
            "outcome_resolution": outcome_resolution.__dict__,
        },
        "scope": {
            "target_tenors": TARGET_TENORS,
            "nine_day_active": False,
            "core_front_active": False,
            "one_trade_per_day_applied": False,
            "core_secondary_separate": True,
            "strict_comparisons": {
                "vrp": ">",
                "z_3m": ">",
                "z_1y": ">",
                "rsi": "<",
                "rv21d": ">",
            },
        },
        "parameter_grid": {
            "vrp_offsets": [-0.10, -0.05, 0.0, 0.05, 0.10],
            "z_offsets": [-0.20, -0.10, 0.0, 0.10, 0.20],
            "rsi_offsets": list(range(-5, 6)),
            "rv21d_offsets": [-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5],
            "parameter_sets_per_sleeve": expected_grid_size,
            "baseline_parameters": {
                f"{layer}_{tenor}D": values for (layer, tenor), values in BASELINES.items()
            },
        },
        "metric_definitions": {
            "frequency": "qualifying trades / joined complete realized-outcome rows for that tenor",
            "win": "trade_return > 0",
            "tail_loss": f"mean of worst {TAIL_FRACTION:.0%} of qualifying trade returns",
            "worst_1pct_loss": f"mean of worst {WORST_FRACTION:.0%} of qualifying trade returns",
            "profit_factor": "sum positive returns / absolute sum negative returns",
            "max_drawdown": "most-negative drawdown of additive cumulative trade returns in trade-date order",
            "largest_losing_cluster": "most-negative sum across a consecutive run of losing trades",
            "ranking": (
                "equal-weight mean of within-sleeve percentile ranks for win_rate, avg_return, "
                "profit_factor, worst_1pct_loss, max_drawdown, and largest_losing_cluster; "
                "all higher-is-better"
            ),
            "minimum_ranking_trades": args.min_ranking_trades,
        },
        "join_summary": {
            "signal_target_rows": len(signal),
            "outcome_target_rows": len(outcome),
            "outcome_nonnull_rows": len(outcome_valid),
            "identical_outcome_duplicates_collapsed": duplicate_rows_collapsed,
            "max_outcome_tenors_per_date": max_outcome_tenors_per_date,
            "joined_rows": len(joined),
            "joined_complete_signal_rows": len(joined_usable),
            "coverage_by_tenor": join_rows,
        },
        "outputs": {
            "results_parquet": results_output,
            "leaderboard_parquet": leaderboard_output,
            "baseline_csv": baseline_output,
            "top_candidates_csv": top_output,
            "join_coverage_csv": join_output,
            "results_rows": len(results),
            "leaderboard_rows": len(leaderboard),
        },
        "production_files_modified": False,
        "status": "PASS",
    }
    manifest_output.write_text(
        json.dumps(json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )

    print(f"Wrote: {results_output}")
    print(f"Wrote: {leaderboard_output}")
    print(f"Wrote: {baseline_output}")
    print(f"Wrote: {top_output}")
    print(f"Wrote: {join_output}")
    print(f"Wrote: {manifest_output}")

    print_section("Baseline results")
    display_cols = [
        "layer",
        "tenor",
        "trade_count",
        "frequency",
        "win_rate",
        "avg_return",
        "avg_win",
        "avg_loss",
        "max_loss",
        "tail_loss",
        "worst_1pct_loss",
        "profit_factor",
        "max_drawdown",
        "largest_losing_cluster",
        "parameter_set_rank",
        "ranking_eligible",
    ]
    print(
        baseline_results.sort_values(["layer_sort", "tenor"])[display_cols]
        .to_string(index=False)
    )

    print_section("PASS")
    print("Tenor-isolated repaired-Wilder-RSI parameter sweep completed successfully.")
    print("Every qualifying tenor was counted independently; no one-trade-per-day rule was applied.")
    print("No sizing was run and canonical production files were not modified.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print_section("FAIL")
        print(str(exc))
        if not isinstance(exc, ScopeError):
            traceback.print_exc()
        sys.exit(1)
