#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_final_signal_panel_build_v1.py

Production builder for the final VRP Corsi/FDS signal panel.

This script intentionally uses the recomputed unified-FDS denominator signal fields:
    forecast_variance_final = forecast_variance_candidate
    forecast_vol_final      = sqrt(forecast_variance_candidate) * 100
    model_vrp_log_final     = log(implied_variance / forecast_variance_candidate)
    z_3m_final              = prior-only 63-row z-score by tenor
    z_1y_final              = prior-only 252-row z-score by tenor

It does not pass through stale/source model_vrp_log, model_vrp_z_3m, or model_vrp_z_1y
for signal evaluation. Those source fields are retained only for diagnostics.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# ======================================================================================
# Defaults
# ======================================================================================

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")

DEFAULT_FORECAST_PANEL_REL = Path(
    r"data\processed\vrp_front_middle_corsi_forecast_repair_v1"
    r"\07A_unified_fds_no_min_return_oos_forecast_panel_20200102_20260701_20260704_203242.parquet"
)

DEFAULT_SIGNAL_RULES_REL = Path(
    r"data\processed\vrp_core_secondary_tertiary_independent_sizing_research_v1"
    r"\09S_core_secondary_consistent_secondary_locked_rules_long_20260705_221829.parquet"
)

DEFAULT_SIZING_RULES_REL = Path(
    r"data\processed\vrp_core_secondary_tertiary_independent_sizing_research_v1"
    r"\15S_final_one_trade_per_day_naked_put_sizing_lock_rules_20260706_142800.parquet"
)

DEFAULT_SELECTION_RULE_REL = Path(
    r"data\audit\vrp_core_secondary_tertiary_independent_sizing_research_v1"
    r"\15S_final_one_trade_per_day_naked_put_selection_rule_20260706_142800.json"
)

DEFAULT_RV21D_REL = Path(r"data\processed\market_data\spy_realized_vol_history_v1.parquet")

DEFAULT_PROCESSED_OUT_REL = Path(r"data\processed\vrp_final_signal")
DEFAULT_AUDIT_OUT_REL = Path(r"data\audit\vrp_final_signal")

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
EXPECTED_ACTIVE_RULE_ROWS = 13
EXPECTED_ACTIVE_SIZING_ROWS = 13
DEFAULT_Z_3M_WINDOW = 63
DEFAULT_Z_1Y_WINDOW = 252
DEFAULT_Z_DDOF = 1

FINAL_SIGNAL_VERSION = "vrp_final_corsi_signal_panel_v1"
FINAL_SIGNAL_DECISION_ID = "recomputed_unified_fds_final_signal_001"


# ======================================================================================
# Config / helpers
# ======================================================================================

@dataclass
class Config:
    project_root: Path
    forecast_panel_path: Path
    signal_rules_path: Path
    sizing_rules_path: Path
    selection_rule_path: Path
    rv21d_path: Path
    processed_out_dir: Path
    audit_out_dir: Path
    z_3m_window: int
    z_1y_window: int
    z_ddof: int
    run_timestamp: str
    fail_on_16s_mismatch: bool
    optional_16s_path: Optional[Path]


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def resolve_path(value: Optional[str], project_root: Path, default_rel: Path) -> Path:
    if value is None:
        return project_root / default_rel
    return Path(value)


def ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")


def normalize_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return (
        series.astype("string")
        .str.strip()
        .str.lower()
        .map({"true": True, "1": True, "yes": True, "y": True, "false": False, "0": False, "no": False, "n": False})
        .fillna(False)
        .astype(bool)
    )


def prior_only_z(series: pd.Series, window: int, ddof: int = 1) -> pd.Series:
    prior = series.shift(1)
    mean = prior.rolling(window=window, min_periods=window).mean()
    std = prior.rolling(window=window, min_periods=window).std(ddof=ddof)
    return (series - mean) / std


def date_range_str(df: pd.DataFrame, col: str = "trade_date") -> str:
    if col not in df.columns or df.empty:
        return "NA"
    d = pd.to_datetime(df[col], errors="coerce")
    if not d.notna().any():
        return "NA"
    return f"{d.min().date()} to {d.max().date()}"


def safe_json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def add_validation(rows: List[Dict[str, str]], check: str, status: bool, detail: str) -> None:
    rows.append({"check": check, "status": "PASS" if bool(status) else "FAIL", "detail": detail})


def read_selection_rule(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ======================================================================================
# Core build steps
# ======================================================================================


def load_sources(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any], pd.DataFrame, Optional[pd.DataFrame]]:
    print_header("Loading source files")

    source_paths = {
        "forecast_panel": cfg.forecast_panel_path,
        "signal_rules": cfg.signal_rules_path,
        "sizing_rules": cfg.sizing_rules_path,
        "selection_rule": cfg.selection_rule_path,
        "rv21d": cfg.rv21d_path,
    }

    for label, path in source_paths.items():
        print(f"{label:20s} {path}")
        ensure_exists(path, label)

    if cfg.optional_16s_path is not None:
        print(f"{'optional_16s':20s} {cfg.optional_16s_path} exists={cfg.optional_16s_path.exists()}")

    forecast_raw = pd.read_parquet(cfg.forecast_panel_path)
    signal_rules_raw = pd.read_parquet(cfg.signal_rules_path)
    sizing_rules_raw = pd.read_parquet(cfg.sizing_rules_path)
    rv21d_raw = pd.read_parquet(cfg.rv21d_path)
    selection_rule = read_selection_rule(cfg.selection_rule_path)

    locked_16s = None
    if cfg.optional_16s_path is not None and cfg.optional_16s_path.exists():
        locked_16s = pd.read_parquet(cfg.optional_16s_path)

    return forecast_raw, signal_rules_raw, sizing_rules_raw, selection_rule, rv21d_raw, locked_16s


def normalize_sources(
    forecast_raw: pd.DataFrame,
    signal_rules_raw: pd.DataFrame,
    sizing_rules_raw: pd.DataFrame,
    rv21d_raw: pd.DataFrame,
    locked_16s: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
    forecast = forecast_raw.copy()
    signal_rules = signal_rules_raw.copy()
    sizing_rules = sizing_rules_raw.copy()
    rv21d = rv21d_raw.copy()
    locked = None if locked_16s is None else locked_16s.copy()

    forecast["trade_date"] = pd.to_datetime(forecast["trade_date"], errors="coerce")
    forecast["tenor"] = pd.to_numeric(forecast["tenor"], errors="coerce").astype("Int64")

    signal_rules["tenor"] = pd.to_numeric(signal_rules["tenor"], errors="coerce").astype("Int64")
    sizing_rules["tenor"] = pd.to_numeric(sizing_rules["tenor"], errors="coerce").astype("Int64")

    rv21d["trade_date"] = pd.to_datetime(rv21d["trade_date"], errors="coerce")

    if locked is not None and "trade_date" in locked.columns:
        locked["trade_date"] = pd.to_datetime(locked["trade_date"], errors="coerce")

    return forecast, signal_rules, sizing_rules, rv21d, locked


def build_unified_base(forecast: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    required = [
        "trade_date",
        "tenor",
        "model_spec",
        "model_source",
        "fit_status_candidate",
        "implied_variance",
        "vix_style_vol",
        "forecast_variance_candidate",
        "model_vrp_log",
        "model_vrp_z_3m",
        "model_vrp_z_1y",
        "RSI14",
        "tenor_bucket",
    ]
    missing = [c for c in required if c not in forecast.columns]
    if missing:
        raise RuntimeError(f"Forecast panel missing required columns: {missing}")

    base = forecast[
        (forecast["model_spec"].astype(str) == "unified_fds_no_min_return")
        & (forecast["model_source"].astype(str) == "unified_fds_no_min_return_oos_refit")
        & (forecast["fit_status_candidate"].astype(str) == "candidate_fit")
    ].copy()

    base = base.sort_values(["trade_date", "tenor"]).reset_index(drop=True)

    # Final recomputed fields. These are the only signal fields used downstream.
    base["final_signal_version"] = FINAL_SIGNAL_VERSION
    base["final_signal_decision_id"] = FINAL_SIGNAL_DECISION_ID
    base["implied_variance_final"] = base["implied_variance"].astype(float)
    base["vix_style_vol_final"] = base["vix_style_vol"].astype(float)
    base["forecast_variance_final"] = base["forecast_variance_candidate"].astype(float)
    base["forecast_vol_final"] = np.sqrt(base["forecast_variance_final"]) * 100.0
    base["model_vrp_log_final"] = np.log(base["implied_variance_final"] / base["forecast_variance_final"])
    base["rsi14_final"] = base["RSI14"].astype(float)

    # Source fields retained only for diagnostics.
    base["source_model_vrp_log"] = base["model_vrp_log"].astype(float)
    base["source_model_vrp_z_3m"] = base["model_vrp_z_3m"].astype(float)
    base["source_model_vrp_z_1y"] = base["model_vrp_z_1y"].astype(float)
    base["source_vs_final_model_vrp_log_diff"] = base["source_model_vrp_log"] - base["model_vrp_log_final"]

    # Prior-only z-scores from recomputed final VRP log.
    base = base.sort_values(["tenor", "trade_date"]).copy()
    base["z_3m_final"] = (
        base.groupby("tenor", group_keys=False)["model_vrp_log_final"]
        .apply(lambda s: prior_only_z(s, cfg.z_3m_window, cfg.z_ddof))
    )
    base["z_1y_final"] = (
        base.groupby("tenor", group_keys=False)["model_vrp_log_final"]
        .apply(lambda s: prior_only_z(s, cfg.z_1y_window, cfg.z_ddof))
    )

    base = base.sort_values(["trade_date", "tenor"]).reset_index(drop=True)
    return base


def join_rv21d(base: pd.DataFrame, rv21d: pd.DataFrame) -> pd.DataFrame:
    required = ["trade_date", "spy_close", "rv21d_vol_pct"]
    missing = [c for c in required if c not in rv21d.columns]
    if missing:
        raise RuntimeError(f"RV21D file missing required columns: {missing}")

    rv = rv21d[["trade_date", "spy_close", "rv21d_vol_pct"]].copy()
    rv["rv21d_vol_pct_final"] = rv["rv21d_vol_pct"].astype(float)

    out = base.merge(
        rv[["trade_date", "spy_close", "rv21d_vol_pct_final"]],
        on="trade_date",
        how="left",
        validate="many_to_one",
    )
    return out


def prepare_active_rules(signal_rules: pd.DataFrame) -> pd.DataFrame:
    required = [
        "include_tenor",
        "signal_layer",
        "signal_priority",
        "tenor_bucket",
        "tenor",
        "model_vrp_log_min",
        "model_vrp_z_3m_min",
        "model_vrp_z_1y_min",
        "RSI14_max",
        "rv21d_vol_pct_min",
        "comparison_operator_model_vrp_log",
        "comparison_operator_model_vrp_z_3m",
        "comparison_operator_model_vrp_z_1y",
        "comparison_operator_RSI14",
        "comparison_operator_rv21d_vol_pct",
    ]
    missing = [c for c in required if c not in signal_rules.columns]
    if missing:
        raise RuntimeError(f"Signal rules missing required columns: {missing}")

    rules = signal_rules.copy()
    rules["include_tenor"] = normalize_bool(rules["include_tenor"])

    active = rules[
        (rules["include_tenor"] == True)
        & (rules["signal_layer"].isin(["Core", "Secondary"]))
    ].copy()

    for col in ["model_vrp_log_min", "model_vrp_z_3m_min", "model_vrp_z_1y_min", "RSI14_max", "rv21d_vol_pct_min"]:
        active[col] = pd.to_numeric(active[col], errors="raise")

    return active


def prepare_active_sizing(sizing_rules: pd.DataFrame) -> pd.DataFrame:
    required = [
        "sleeve_id",
        "locked_size_pct",
        "signal_layer",
        "tenor_bucket",
        "tenor",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "signal_layer_order",
        "tenor_bucket_order",
        "display_order",
    ]
    missing = [c for c in required if c not in sizing_rules.columns]
    if missing:
        raise RuntimeError(f"Sizing rules missing required columns: {missing}")

    sizing = sizing_rules.copy()

    if "is_locked" in sizing.columns:
        sizing["is_locked"] = normalize_bool(sizing["is_locked"])
    else:
        sizing["is_locked"] = True

    if "is_final_sizing_lock" in sizing.columns:
        sizing["is_final_sizing_lock"] = normalize_bool(sizing["is_final_sizing_lock"])
    else:
        sizing["is_final_sizing_lock"] = True

    active = sizing[
        (sizing["signal_layer"].isin(["Core", "Secondary"]))
        & (sizing["is_locked"] == True)
        & (sizing["is_final_sizing_lock"] == True)
    ].copy()

    numeric_cols = [
        "locked_size_pct",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "signal_layer_order",
        "tenor_bucket_order",
        "display_order",
    ]
    for col in numeric_cols:
        active[col] = pd.to_numeric(active[col], errors="raise")

    return active


def build_candidates(base: pd.DataFrame, active_rules: pd.DataFrame, active_sizing: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "trade_date",
        "tenor",
        "tenor_bucket",
        "final_signal_version",
        "final_signal_decision_id",
        "implied_variance_final",
        "vix_style_vol_final",
        "forecast_variance_final",
        "forecast_vol_final",
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
        "spy_close",
        "model_spec",
        "model_source",
        "fit_status_candidate",
        "forecast_repair_scope",
    ]
    base_cols = [c for c in base_cols if c in base.columns]

    rule_cols = [
        "signal_layer",
        "signal_priority",
        "tenor",
        "tenor_bucket",
        "model_vrp_log_min",
        "model_vrp_z_3m_min",
        "model_vrp_z_1y_min",
        "RSI14_max",
        "rv21d_vol_pct_min",
        "comparison_operator_model_vrp_log",
        "comparison_operator_model_vrp_z_3m",
        "comparison_operator_model_vrp_z_1y",
        "comparison_operator_RSI14",
        "comparison_operator_rv21d_vol_pct",
        "stack_lock_version",
        "stack_lock_decision_id",
        "lock_version",
        "lock_decision_id",
    ]
    rule_cols = [c for c in rule_cols if c in active_rules.columns]

    candidates = base[base_cols].merge(
        active_rules[rule_cols],
        on=["tenor", "tenor_bucket"],
        how="inner",
        validate="many_to_many",
    )

    expected_ops = {
        "comparison_operator_model_vrp_log": [">"],
        "comparison_operator_model_vrp_z_3m": [">"],
        "comparison_operator_model_vrp_z_1y": [">"],
        "comparison_operator_RSI14": ["<"],
        "comparison_operator_rv21d_vol_pct": [">"],
    }
    for col, expected in expected_ops.items():
        if col in candidates.columns:
            actual = sorted(candidates[col].dropna().astype(str).unique().tolist())
            if actual != expected:
                raise RuntimeError(f"Unexpected operators in {col}: actual={actual}, expected={expected}")

    candidates["pass_model_vrp_log"] = candidates["model_vrp_log_final"] > candidates["model_vrp_log_min"]
    candidates["pass_z_3m"] = candidates["z_3m_final"] > candidates["model_vrp_z_3m_min"]
    candidates["pass_z_1y"] = candidates["z_1y_final"] > candidates["model_vrp_z_1y_min"]
    candidates["pass_rsi14"] = candidates["rsi14_final"] < candidates["RSI14_max"]
    candidates["pass_rv21d"] = candidates["rv21d_vol_pct_final"] > candidates["rv21d_vol_pct_min"]

    pass_cols = ["pass_model_vrp_log", "pass_z_3m", "pass_z_1y", "pass_rsi14", "pass_rv21d"]
    candidates["qualified"] = candidates[pass_cols].all(axis=1)

    sizing_cols = [
        "sleeve_id",
        "signal_layer",
        "tenor_bucket",
        "tenor",
        "locked_size_pct",
        "locked_size_label",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "signal_layer_order",
        "tenor_bucket_order",
        "display_order",
        "sizing_lock_version",
        "sizing_lock_decision_id",
        "program_type",
        "program_label",
        "pricing_basis_label",
        "not_defined_spread_sizing",
    ]
    sizing_cols = [c for c in sizing_cols if c in active_sizing.columns]

    candidates = candidates.merge(
        active_sizing[sizing_cols],
        on=["signal_layer", "tenor_bucket", "tenor"],
        how="left",
        validate="many_to_one",
    )

    return candidates


def select_one_trade_per_day(candidates: pd.DataFrame) -> pd.DataFrame:
    qualified = candidates[candidates["qualified"]].copy()

    sort_cols = [
        "trade_date",
        "locked_size_pct",
        "signal_layer_order",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "tenor",
        "display_order",
    ]
    missing = [c for c in sort_cols if c not in qualified.columns]
    if missing:
        raise RuntimeError(f"Qualified candidates missing selection sort columns: {missing}")

    qualified = qualified.sort_values(
        sort_cols,
        ascending=[
            True,
            False,  # highest locked size
            True,   # Core before Secondary if size tied
            False,  # higher sizing quality
            False,  # higher win rate
            True,   # lower positive expected loss
            False,  # longer tenor
            True,
        ],
    ).reset_index(drop=True)

    qualified["selection_rank"] = qualified.groupby("trade_date").cumcount() + 1
    selected = qualified[qualified["selection_rank"] == 1].copy()
    return selected


def build_panel(base: pd.DataFrame, candidates: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    panel = base.copy()

    core_pass = (
        candidates[(candidates["signal_layer"] == "Core") & (candidates["qualified"])]
        [["trade_date", "tenor"]]
        .drop_duplicates()
        .assign(core_pass=True)
    )
    secondary_pass = (
        candidates[(candidates["signal_layer"] == "Secondary") & (candidates["qualified"])]
        [["trade_date", "tenor"]]
        .drop_duplicates()
        .assign(secondary_pass=True)
    )

    panel = panel.merge(core_pass, on=["trade_date", "tenor"], how="left")
    panel = panel.merge(secondary_pass, on=["trade_date", "tenor"], how="left")
    panel["core_pass"] = panel["core_pass"].fillna(False).astype(bool)
    panel["secondary_pass"] = panel["secondary_pass"].fillna(False).astype(bool)

    selected_cols = [
        "trade_date",
        "tenor",
        "signal_layer",
        "tenor_bucket",
        "sleeve_id",
        "locked_size_pct",
        "locked_size_label",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "selection_rank",
    ]
    selected_cols = [c for c in selected_cols if c in selected.columns]

    selected_marker = selected[selected_cols].copy()
    selected_marker = selected_marker.rename(
        columns={
            "signal_layer": "selected_layer",
            "tenor_bucket": "selected_tenor_bucket",
            "sleeve_id": "selected_sleeve_id",
            "locked_size_pct": "selected_size_pct",
            "locked_size_label": "selected_size_label",
            "sizing_quality_score": "selected_sizing_quality_score",
            "win_rate": "selected_win_rate",
            "pnl_per_day_expected_loss_1pct_positive": "selected_1pct_expected_loss_positive",
        }
    )

    panel = panel.merge(selected_marker, on=["trade_date", "tenor"], how="left", validate="one_to_one")
    panel["selected"] = panel["selected_layer"].notna()
    panel["selected_tenor"] = np.where(panel["selected"], panel["tenor"].astype(float), np.nan)

    final_cols = [
        "trade_date",
        "tenor",
        "tenor_bucket",
        "final_signal_version",
        "final_signal_decision_id",
        "model_spec",
        "model_source",
        "fit_status_candidate",
        "forecast_repair_scope",
        "implied_variance_final",
        "vix_style_vol_final",
        "forecast_variance_final",
        "forecast_vol_final",
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
        "spy_close",
        "source_model_vrp_log",
        "source_model_vrp_z_3m",
        "source_model_vrp_z_1y",
        "source_vs_final_model_vrp_log_diff",
        "core_pass",
        "secondary_pass",
        "selected",
        "selected_layer",
        "selected_tenor_bucket",
        "selected_tenor",
        "selected_sleeve_id",
        "selected_size_pct",
        "selected_size_label",
        "selected_sizing_quality_score",
        "selected_win_rate",
        "selected_1pct_expected_loss_positive",
        "selection_rank",
    ]
    final_cols = [c for c in final_cols if c in panel.columns]
    return panel[final_cols].sort_values(["trade_date", "tenor"]).reset_index(drop=True)


def build_latest_snapshot(panel: pd.DataFrame) -> pd.DataFrame:
    latest_date = panel["trade_date"].max()
    latest = panel[panel["trade_date"] == latest_date].copy()
    return latest.sort_values("tenor").reset_index(drop=True)


def build_selected_output(selected: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "trade_date",
        "signal_layer",
        "tenor_bucket",
        "tenor",
        "sleeve_id",
        "locked_size_pct",
        "locked_size_label",
        "sizing_quality_score",
        "win_rate",
        "pnl_per_day_expected_loss_1pct_positive",
        "implied_variance_final",
        "vix_style_vol_final",
        "forecast_variance_final",
        "forecast_vol_final",
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
        "spy_close",
        "selection_rank",
    ]
    cols = [c for c in cols if c in selected.columns]
    return selected[cols].sort_values("trade_date").reset_index(drop=True)


def compare_to_16s(selected_output: pd.DataFrame, locked_16s: Optional[pd.DataFrame]) -> Tuple[Dict[str, Any], pd.DataFrame]:
    if locked_16s is None:
        return {"comparison_available": False}, pd.DataFrame()

    locked = locked_16s.copy()
    if "trade_date" in locked.columns:
        locked["trade_date"] = pd.to_datetime(locked["trade_date"], errors="coerce")

    layer_col = None
    for c in ["signal_layer", "selected_layer", "layer"]:
        if c in locked.columns:
            layer_col = c
            break

    tenor_col = None
    for c in ["tenor", "selected_tenor", "dte"]:
        if c in locked.columns:
            tenor_col = c
            break

    if layer_col is None or tenor_col is None:
        return {"comparison_available": False, "reason": "could_not_detect_layer_or_tenor_columns"}, pd.DataFrame()

    locked_cols = ["trade_date", layer_col, tenor_col]
    if "sleeve_id" in locked.columns:
        locked_cols.append("sleeve_id")
    if "locked_size_pct" in locked.columns:
        locked_cols.append("locked_size_pct")

    locked_compare = locked[locked_cols].copy().rename(
        columns={
            layer_col: "locked_signal_layer",
            tenor_col: "locked_tenor",
            "sleeve_id": "locked_sleeve_id",
            "locked_size_pct": "locked_size_pct",
        }
    )

    selected_compare = selected_output[["trade_date", "signal_layer", "tenor", "sleeve_id", "locked_size_pct"]].copy()
    selected_compare = selected_compare.rename(
        columns={
            "signal_layer": "prod_signal_layer",
            "tenor": "prod_tenor",
            "sleeve_id": "prod_sleeve_id",
            "locked_size_pct": "prod_locked_size_pct",
        }
    )

    locked_compare["locked_tenor"] = pd.to_numeric(locked_compare["locked_tenor"], errors="coerce")
    selected_compare["prod_tenor"] = pd.to_numeric(selected_compare["prod_tenor"], errors="coerce")

    detail = locked_compare.merge(selected_compare, on="trade_date", how="outer", indicator=True)
    detail["layer_match"] = detail["locked_signal_layer"].astype("string") == detail["prod_signal_layer"].astype("string")
    detail["tenor_match"] = detail["locked_tenor"].astype("float") == detail["prod_tenor"].astype("float")
    detail["exact_match"] = (detail["_merge"] == "both") & detail["layer_match"] & detail["tenor_match"]

    summary = {
        "comparison_available": True,
        "locked_rows": int(len(locked_compare)),
        "production_selected_rows": int(len(selected_compare)),
        "both_dates": int((detail["_merge"] == "both").sum()),
        "locked_only_dates": int((detail["_merge"] == "left_only").sum()),
        "production_only_dates": int((detail["_merge"] == "right_only").sum()),
        "exact_layer_tenor_matches": int(detail["exact_match"].sum()),
        "common_date_mismatches": int(((detail["_merge"] == "both") & (~detail["exact_match"])).sum()),
        "note": "Comparison is diagnostic only unless --fail-on-16s-mismatch is passed. Production uses recomputed unified-FDS final fields.",
    }
    return summary, detail.sort_values("trade_date").reset_index(drop=True)


def validate_outputs(
    cfg: Config,
    base: pd.DataFrame,
    active_rules: pd.DataFrame,
    active_sizing: pd.DataFrame,
    candidates: pd.DataFrame,
    selected_output: pd.DataFrame,
    panel: pd.DataFrame,
    latest_snapshot: pd.DataFrame,
    comparison_summary: Dict[str, Any],
) -> pd.DataFrame:
    rows: List[Dict[str, str]] = []

    add_validation(
        rows,
        "filtered_to_unified_fds_candidate_rows",
        len(base) > 0
        and set(base["model_spec"].astype(str).unique()) == {"unified_fds_no_min_return"}
        and set(base["model_source"].astype(str).unique()) == {"unified_fds_no_min_return_oos_refit"}
        and set(base["fit_status_candidate"].astype(str).unique()) == {"candidate_fit"},
        f"rows={len(base):,}",
    )

    dup_count = int(panel.duplicated(["trade_date", "tenor"], keep=False).sum())
    add_validation(rows, "one_row_per_trade_date_tenor", dup_count == 0, f"duplicate_rows={dup_count:,}")

    actual_tenors = sorted(panel["tenor"].dropna().astype(int).unique().tolist())
    add_validation(rows, "expected_tenor_grid", actual_tenors == EXPECTED_TENORS, f"actual_tenors={actual_tenors}")

    add_validation(
        rows,
        "positive_implied_and_forecast_variance",
        (panel["implied_variance_final"] > 0).all() and (panel["forecast_variance_final"] > 0).all(),
        "implied_variance_final and forecast_variance_final must be positive",
    )

    forecast_vol_diff = (panel["forecast_vol_final"] - np.sqrt(panel["forecast_variance_final"]) * 100.0).abs()
    add_validation(rows, "forecast_vol_reconstruction", (forecast_vol_diff <= 1e-10).all(), f"max_abs_diff={forecast_vol_diff.max()}")

    vrp_log_diff = (panel["model_vrp_log_final"] - np.log(panel["implied_variance_final"] / panel["forecast_variance_final"])).abs()
    add_validation(rows, "model_vrp_log_final_reconstruction", (vrp_log_diff <= 1e-10).all(), f"max_abs_diff={vrp_log_diff.max()}")

    missing_rv = int(panel["rv21d_vol_pct_final"].isna().sum())
    add_validation(rows, "rv21d_join_complete", missing_rv == 0, f"missing={missing_rv:,}")

    add_validation(rows, "active_signal_rule_count", len(active_rules) == EXPECTED_ACTIVE_RULE_ROWS, f"active_rules={len(active_rules):,}")
    add_validation(rows, "active_sizing_rule_count", len(active_sizing) == EXPECTED_ACTIVE_SIZING_ROWS, f"active_sizing={len(active_sizing):,}")

    missing_size_qualified = int(candidates.loc[candidates["qualified"], "locked_size_pct"].isna().sum())
    add_validation(rows, "qualified_candidates_have_sizing", missing_size_qualified == 0, f"missing_size_qualified={missing_size_qualified:,}")

    max_selected_per_date = int(selected_output.groupby("trade_date").size().max()) if len(selected_output) > 0 else 0
    add_validation(rows, "max_one_selected_trade_per_day", max_selected_per_date <= 1, f"max_selected_per_date={max_selected_per_date}")

    add_validation(
        rows,
        "latest_snapshot_has_9_tenors",
        len(latest_snapshot) == 9 and sorted(latest_snapshot["tenor"].astype(int).tolist()) == EXPECTED_TENORS,
        f"latest_rows={len(latest_snapshot):,}; latest_tenors={sorted(latest_snapshot['tenor'].astype(int).tolist()) if len(latest_snapshot) else []}",
    )

    if cfg.fail_on_16s_mismatch and comparison_summary.get("comparison_available"):
        mismatch = comparison_summary.get("common_date_mismatches", 0) + comparison_summary.get("production_only_dates", 0) + comparison_summary.get("locked_only_dates", 0)
        add_validation(rows, "optional_exact_16s_match", mismatch == 0, f"total_16s_mismatch_count={mismatch}")

    return pd.DataFrame(rows)


def write_outputs(
    cfg: Config,
    panel: pd.DataFrame,
    latest_snapshot: pd.DataFrame,
    selected_output: pd.DataFrame,
    candidates: pd.DataFrame,
    active_rules: pd.DataFrame,
    active_sizing: pd.DataFrame,
    validation: pd.DataFrame,
    selection_rule: Dict[str, Any],
    comparison_summary: Dict[str, Any],
    comparison_detail: pd.DataFrame,
) -> Dict[str, str]:
    cfg.processed_out_dir.mkdir(parents=True, exist_ok=True)
    cfg.audit_out_dir.mkdir(parents=True, exist_ok=True)

    # Production outputs: stable filenames for dashboard / downstream process.
    panel_path = cfg.processed_out_dir / "vrp_final_corsi_signal_base_panel_v1.parquet"
    latest_path = cfg.processed_out_dir / "vrp_final_corsi_latest_snapshot_v1.parquet"
    selected_path = cfg.processed_out_dir / "vrp_final_corsi_selected_trades_v1.parquet"

    panel.to_parquet(panel_path, index=False)
    latest_snapshot.to_parquet(latest_path, index=False)
    selected_output.to_parquet(selected_path, index=False)

    # Audit outputs: timestamped.
    validation_path = cfg.audit_out_dir / f"vrp_final_signal_validation_{cfg.run_timestamp}.csv"
    selected_distribution_path = cfg.audit_out_dir / f"vrp_final_signal_selected_distribution_{cfg.run_timestamp}.csv"
    source_vs_final_path = cfg.audit_out_dir / f"vrp_final_signal_source_vs_final_vrp_diagnostic_{cfg.run_timestamp}.csv"
    candidates_path = cfg.audit_out_dir / f"vrp_final_signal_candidates_{cfg.run_timestamp}.csv"
    active_rules_path = cfg.audit_out_dir / f"vrp_final_signal_active_rules_{cfg.run_timestamp}.csv"
    active_sizing_path = cfg.audit_out_dir / f"vrp_final_signal_active_sizing_{cfg.run_timestamp}.csv"
    comparison_path = cfg.audit_out_dir / f"vrp_final_signal_16S_comparison_{cfg.run_timestamp}.csv"
    manifest_path = cfg.audit_out_dir / f"vrp_final_signal_manifest_{cfg.run_timestamp}.json"

    validation.to_csv(validation_path, index=False)

    selected_distribution = (
        selected_output.groupby(["signal_layer", "tenor_bucket", "tenor", "sleeve_id", "locked_size_pct"])
        .size()
        .reset_index(name="selected_count")
        .sort_values(["signal_layer", "tenor"])
    )
    selected_distribution.to_csv(selected_distribution_path, index=False)

    source_diag = (
        panel.groupby("tenor")
        .agg(
            rows=("trade_date", "size"),
            min_date=("trade_date", "min"),
            max_date=("trade_date", "max"),
            source_vs_final_abs_mean=("source_vs_final_model_vrp_log_diff", lambda s: float(s.abs().mean())),
            source_vs_final_abs_median=("source_vs_final_model_vrp_log_diff", lambda s: float(s.abs().median())),
            source_vs_final_abs_p99=("source_vs_final_model_vrp_log_diff", lambda s: float(s.abs().quantile(0.99))),
            source_vs_final_abs_max=("source_vs_final_model_vrp_log_diff", lambda s: float(s.abs().max())),
        )
        .reset_index()
    )
    source_diag["min_date"] = pd.to_datetime(source_diag["min_date"]).dt.date.astype(str)
    source_diag["max_date"] = pd.to_datetime(source_diag["max_date"]).dt.date.astype(str)
    source_diag.to_csv(source_vs_final_path, index=False)

    candidates.to_csv(candidates_path, index=False)
    active_rules.to_csv(active_rules_path, index=False)
    active_sizing.to_csv(active_sizing_path, index=False)

    comparison_file_written = None
    if not comparison_detail.empty:
        comparison_detail.to_csv(comparison_path, index=False)
        comparison_file_written = str(comparison_path)

    manifest = {
        "run_timestamp": cfg.run_timestamp,
        "final_signal_version": FINAL_SIGNAL_VERSION,
        "final_signal_decision_id": FINAL_SIGNAL_DECISION_ID,
        "methodology_decision": "Use recomputed unified-FDS final signal fields; do not use stale source model_vrp_log/z-score columns for signal evaluation.",
        "config": {k: str(v) if isinstance(v, Path) else v for k, v in asdict(cfg).items()},
        "inputs": {
            "forecast_panel_path": str(cfg.forecast_panel_path),
            "signal_rules_path": str(cfg.signal_rules_path),
            "sizing_rules_path": str(cfg.sizing_rules_path),
            "selection_rule_path": str(cfg.selection_rule_path),
            "rv21d_path": str(cfg.rv21d_path),
        },
        "selection_rule_json": selection_rule,
        "row_counts": {
            "panel_rows": int(len(panel)),
            "latest_snapshot_rows": int(len(latest_snapshot)),
            "selected_rows": int(len(selected_output)),
            "candidate_rows": int(len(candidates)),
            "qualified_candidate_rows": int(candidates["qualified"].sum()),
            "active_rule_rows": int(len(active_rules)),
            "active_sizing_rows": int(len(active_sizing)),
        },
        "date_ranges": {
            "panel": date_range_str(panel),
            "selected": date_range_str(selected_output),
            "latest_snapshot_date": str(latest_snapshot["trade_date"].max().date()) if len(latest_snapshot) else None,
        },
        "comparison_to_locked_16s": comparison_summary,
        "validation": validation.to_dict(orient="records"),
        "outputs": {
            "panel": str(panel_path),
            "latest_snapshot": str(latest_path),
            "selected_trades": str(selected_path),
            "validation": str(validation_path),
            "selected_distribution": str(selected_distribution_path),
            "source_vs_final_vrp_diagnostic": str(source_vs_final_path),
            "candidates_audit": str(candidates_path),
            "active_rules_audit": str(active_rules_path),
            "active_sizing_audit": str(active_sizing_path),
            "comparison_16s": comparison_file_written,
            "manifest": str(manifest_path),
        },
    }

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=safe_json_default)

    return manifest["outputs"]


# ======================================================================================
# Main
# ======================================================================================


def parse_args(argv: Optional[Iterable[str]] = None) -> Config:
    parser = argparse.ArgumentParser(description="Build final VRP Corsi/FDS signal panel v1.")

    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT), help="Project root directory")
    parser.add_argument("--forecast-panel-path", default=None, help="Override 07A forecast panel path")
    parser.add_argument("--signal-rules-path", default=None, help="Override 09S signal rules path")
    parser.add_argument("--sizing-rules-path", default=None, help="Override 15S sizing rules path")
    parser.add_argument("--selection-rule-path", default=None, help="Override 15S selection rule JSON path")
    parser.add_argument("--rv21d-path", default=None, help="Override canonical RV21D path")
    parser.add_argument("--processed-out-dir", default=None, help="Override processed output directory")
    parser.add_argument("--audit-out-dir", default=None, help="Override audit output directory")
    parser.add_argument("--z-3m-window", type=int, default=DEFAULT_Z_3M_WINDOW, help="Prior-only 3m z-score window")
    parser.add_argument("--z-1y-window", type=int, default=DEFAULT_Z_1Y_WINDOW, help="Prior-only 1y z-score window")
    parser.add_argument("--z-ddof", type=int, default=DEFAULT_Z_DDOF, help="Rolling std ddof for z-score")
    parser.add_argument("--run-timestamp", default=None, help="Optional run timestamp override")
    parser.add_argument("--optional-16s-path", default=None, help="Optional locked 16S selected artifact for diagnostic comparison")
    parser.add_argument("--fail-on-16s-mismatch", action="store_true", help="Fail if optional 16S artifact differs from production selected trades")

    args = parser.parse_args(list(argv) if argv is not None else None)

    project_root = Path(args.project_root)
    run_timestamp = args.run_timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")

    processed_out_dir = Path(args.processed_out_dir) if args.processed_out_dir else project_root / DEFAULT_PROCESSED_OUT_REL
    audit_out_dir = Path(args.audit_out_dir) if args.audit_out_dir else project_root / DEFAULT_AUDIT_OUT_REL

    optional_16s_path = Path(args.optional_16s_path) if args.optional_16s_path else (
        project_root
        / r"data\processed\vrp_core_secondary_tertiary_independent_sizing_research_v1"
        / "16S_locked_cumulative_pnl_timeseries_20260706_143802.parquet"
    )

    return Config(
        project_root=project_root,
        forecast_panel_path=resolve_path(args.forecast_panel_path, project_root, DEFAULT_FORECAST_PANEL_REL),
        signal_rules_path=resolve_path(args.signal_rules_path, project_root, DEFAULT_SIGNAL_RULES_REL),
        sizing_rules_path=resolve_path(args.sizing_rules_path, project_root, DEFAULT_SIZING_RULES_REL),
        selection_rule_path=resolve_path(args.selection_rule_path, project_root, DEFAULT_SELECTION_RULE_REL),
        rv21d_path=resolve_path(args.rv21d_path, project_root, DEFAULT_RV21D_REL),
        processed_out_dir=processed_out_dir,
        audit_out_dir=audit_out_dir,
        z_3m_window=args.z_3m_window,
        z_1y_window=args.z_1y_window,
        z_ddof=args.z_ddof,
        run_timestamp=run_timestamp,
        fail_on_16s_mismatch=bool(args.fail_on_16s_mismatch),
        optional_16s_path=optional_16s_path,
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    cfg = parse_args(argv)

    print_header("VRP final signal panel build v1")
    print(f"Project root:       {cfg.project_root}")
    print(f"Run timestamp:      {cfg.run_timestamp}")
    print(f"Final version:      {FINAL_SIGNAL_VERSION}")
    print(f"Decision ID:        {FINAL_SIGNAL_DECISION_ID}")
    print("Methodology:        recomputed unified-FDS final VRP/z-score fields")

    forecast_raw, signal_rules_raw, sizing_rules_raw, selection_rule, rv21d_raw, locked_16s = load_sources(cfg)
    forecast, signal_rules, sizing_rules, rv21d, locked_16s = normalize_sources(
        forecast_raw, signal_rules_raw, sizing_rules_raw, rv21d_raw, locked_16s
    )

    print_header("Building unified-FDS candidate base")
    base = build_unified_base(forecast, cfg)
    print(f"Rows:               {len(base):,}")
    print(f"Date range:         {date_range_str(base)}")
    print(f"Unique dates:       {base['trade_date'].nunique():,}")
    print(f"Tenors:             {sorted(base['tenor'].dropna().astype(int).unique().tolist())}")
    print(f"Duplicate keys:     {base.duplicated(['trade_date', 'tenor'], keep=False).sum():,}")

    print_header("Joining RV21D")
    base = join_rv21d(base, rv21d)
    print(f"Rows after join:    {len(base):,}")
    print(f"Missing RV21D:      {base['rv21d_vol_pct_final'].isna().sum():,}")

    print_header("Preparing active rules and sizing")
    active_rules = prepare_active_rules(signal_rules)
    active_sizing = prepare_active_sizing(sizing_rules)
    print(f"Active rules:       {len(active_rules):,}")
    print(f"Active sizing rows: {len(active_sizing):,}")

    print_header("Applying signal thresholds")
    candidates = build_candidates(base, active_rules, active_sizing)
    qualified_count = int(candidates["qualified"].sum())
    print(f"Candidate rows:     {len(candidates):,}")
    print(f"Qualified rows:     {qualified_count:,}")

    print_header("Selecting one trade per day")
    selected = select_one_trade_per_day(candidates)
    selected_output = build_selected_output(selected)
    print(f"Selected trades:    {len(selected_output):,}")
    if len(selected_output):
        print(f"Selected range:     {date_range_str(selected_output)}")
        print(f"Max per date:       {selected_output.groupby('trade_date').size().max()}")

    print_header("Building final panel and latest snapshot")
    panel = build_panel(base, candidates, selected)
    latest_snapshot = build_latest_snapshot(panel)
    print(f"Panel rows:         {len(panel):,}")
    print(f"Latest date:        {latest_snapshot['trade_date'].max().date() if len(latest_snapshot) else 'NA'}")
    print(f"Latest rows:        {len(latest_snapshot):,}")

    print_header("Diagnostic comparison to locked 16S artifact")
    comparison_summary, comparison_detail = compare_to_16s(selected_output, locked_16s)
    print(json.dumps(comparison_summary, indent=2, default=safe_json_default))

    print_header("Hard validations")
    validation = validate_outputs(
        cfg=cfg,
        base=base,
        active_rules=active_rules,
        active_sizing=active_sizing,
        candidates=candidates,
        selected_output=selected_output,
        panel=panel,
        latest_snapshot=latest_snapshot,
        comparison_summary=comparison_summary,
    )
    print(validation.to_string(index=False))

    if (validation["status"] == "FAIL").any():
        print("\nValidation failure found. No production outputs will be written.")
        return 1

    print_header("Writing production and audit outputs")
    outputs = write_outputs(
        cfg=cfg,
        panel=panel,
        latest_snapshot=latest_snapshot,
        selected_output=selected_output,
        candidates=candidates,
        active_rules=active_rules,
        active_sizing=active_sizing,
        validation=validation,
        selection_rule=selection_rule,
        comparison_summary=comparison_summary,
        comparison_detail=comparison_detail,
    )

    for label, path in outputs.items():
        if path is not None:
            print(f"{label:35s} {path}")

    print("\nDONE — final VRP Corsi/FDS signal panel v1 built successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
