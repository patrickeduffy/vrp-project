#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_production_health_check_v1.py

Read-only production health / freshness checker for the VRP Corsi/FDS production stack.

Purpose
-------
This script does not rebuild data and does not modify production outputs. It reads the current
production artifacts and reports whether the EOD production stack is internally fresh and coherent.

It checks:
    - required file existence
    - file modified timestamps
    - latest dates across SPY EOD, RV21D, Corsi/HAR support, forecast source, final panel, latest snapshot
    - latest snapshot tenor completeness
    - duplicate trade_date x tenor rows in final panel
    - selected trades max one per date
    - optional expected-date freshness
    - optional modified-today freshness

Default usage
-------------
py vrp_production_health_check_v1.py --project-root "C:\\Users\\patri\\vrp_project"

With explicit expected official date:
py vrp_production_health_check_v1.py --project-root "C:\\Users\\patri\\vrp_project" --expected-date 2026-07-01

By default the script exits with code 1 if any hard check fails. Use --warn-only to always exit 0.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ======================================================================================
# Defaults
# ======================================================================================

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")

DEFAULT_SPY_EOD_REL = Path(r"data\processed\market_data\spy_eod_prices_v1.parquet")
DEFAULT_RV21D_REL = Path(r"data\processed\market_data\spy_realized_vol_history_v1.parquet")
DEFAULT_CORSI_SUPPORT_REL = Path(r"data\processed\market_data\spy_corsi_har_input_panel_v1.parquet")

DEFAULT_FORECAST_SOURCE_REL = Path(
    r"data\processed\vrp_front_middle_corsi_forecast_repair_v1"
    r"\07A_unified_fds_no_min_return_oos_forecast_panel_20200102_20260709_20260710_101156_20260710_100854_schema_repair.parquet"
)

DEFAULT_FINAL_PANEL_REL = Path(
    r"data\processed\vrp_final_signal\vrp_final_corsi_signal_base_panel_v1.parquet"
)
DEFAULT_LATEST_SNAPSHOT_REL = Path(
    r"data\processed\vrp_final_signal\vrp_final_corsi_latest_snapshot_v1.parquet"
)
DEFAULT_SELECTED_TRADES_REL = Path(
    r"data\processed\vrp_final_signal\vrp_final_corsi_selected_trades_v1.parquet"
)

DEFAULT_AUDIT_OUT_REL = Path(r"data\audit\production_health")

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
EXPECTED_LATEST_SNAPSHOT_ROWS = 9
EXPECTED_ACTIVE_FINAL_VERSION = "vrp_final_corsi_signal_panel_v1"

# Final production panel columns that should exist after vrp_final_signal_panel_build_v1.py
REQUIRED_FINAL_PANEL_COLS = [
    "trade_date",
    "tenor",
    "implied_variance_final",
    "vix_style_vol_final",
    "forecast_variance_final",
    "forecast_vol_final",
    "model_vrp_log_final",
    "z_3m_final",
    "z_1y_final",
    "rsi14_final",
    "rv21d_vol_pct_final",
    "core_pass",
    "secondary_pass",
    "selected",
]

REQUIRED_LATEST_SNAPSHOT_COLS = REQUIRED_FINAL_PANEL_COLS

REQUIRED_SELECTED_COLS = [
    "trade_date",
    "tenor",
    "signal_layer",
    "locked_size_pct",
]


# ======================================================================================
# Config / helper types
# ======================================================================================

@dataclass
class Config:
    project_root: Path
    spy_eod_path: Path
    rv21d_path: Path
    corsi_support_path: Path
    forecast_source_path: Path
    final_panel_path: Path
    latest_snapshot_path: Path
    selected_trades_path: Path
    audit_out_dir: Path
    expected_date: Optional[pd.Timestamp]
    require_modified_today: bool
    warn_only: bool
    run_timestamp: str


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_date_arg(value: Optional[str]) -> Optional[pd.Timestamp]:
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    try:
        if len(raw) == 8 and raw.isdigit():
            return pd.to_datetime(raw, format="%Y%m%d", errors="raise").normalize()
        return pd.to_datetime(raw, errors="raise").normalize()
    except Exception as exc:
        raise ValueError(f"expected-date must be YYYY-MM-DD or YYYYMMDD. Got {value!r}") from exc


def resolve_path(project_root: Path, explicit: Optional[str], default_rel: Path) -> Path:
    if explicit is None:
        return project_root / default_rel
    return Path(explicit)


def file_info(path: Path) -> Dict[str, Any]:
    exists = path.exists()
    out: Dict[str, Any] = {
        "path": str(path),
        "exists": exists,
        "size_bytes": None,
        "modified_timestamp": None,
        "modified_date": None,
    }
    if exists:
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime)
        out.update(
            {
                "size_bytes": int(stat.st_size),
                "modified_timestamp": modified.strftime("%Y-%m-%d %H:%M:%S"),
                "modified_date": modified.date().isoformat(),
            }
        )
    return out


def load_parquet(path: Path, label: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Failed to read {label}: {path}. Error: {type(exc).__name__}: {exc}") from exc


def normalize_trade_date(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if "trade_date" not in df.columns:
        raise RuntimeError(f"{label} missing required trade_date column")
    out = df.copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.normalize()
    if out["trade_date"].isna().all():
        raise RuntimeError(f"{label} trade_date column could not be parsed")
    return out


def latest_date(df: pd.DataFrame, label: str) -> Optional[pd.Timestamp]:
    if "trade_date" not in df.columns or len(df) == 0:
        return None
    d = pd.to_datetime(df["trade_date"], errors="coerce").dt.normalize()
    if d.notna().any():
        return pd.Timestamp(d.max()).normalize()
    return None


def date_to_str(ts: Optional[pd.Timestamp]) -> Optional[str]:
    if ts is None or pd.isna(ts):
        return None
    return pd.Timestamp(ts).date().isoformat()


def safe_cols_missing(df: pd.DataFrame, required: Sequence[str]) -> List[str]:
    return [c for c in required if c not in df.columns]


def add_check(rows: List[Dict[str, Any]], check: str, status: bool, detail: str, severity: str = "hard") -> None:
    rows.append(
        {
            "check": check,
            "status": "PASS" if bool(status) else "FAIL",
            "severity": severity,
            "detail": detail,
        }
    )


def summarize_panel(label: str, df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "label": label,
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "latest_date": None,
        "min_date": None,
        "unique_dates": None,
        "tenors": None,
        "unique_trade_date_tenor_keys": None,
        "duplicate_trade_date_tenor_rows": None,
    }
    if "trade_date" in df.columns:
        d = pd.to_datetime(df["trade_date"], errors="coerce").dt.normalize()
        if d.notna().any():
            out["min_date"] = d.min().date().isoformat()
            out["latest_date"] = d.max().date().isoformat()
            out["unique_dates"] = int(d.nunique())
    if "tenor" in df.columns:
        try:
            out["tenors"] = sorted(pd.Series(df["tenor"]).dropna().astype(int).unique().tolist())
        except Exception:
            out["tenors"] = sorted(pd.Series(df["tenor"]).dropna().astype(str).unique().tolist())
    if {"trade_date", "tenor"}.issubset(df.columns):
        out["unique_trade_date_tenor_keys"] = int(df[["trade_date", "tenor"]].drop_duplicates().shape[0])
        out["duplicate_trade_date_tenor_rows"] = int(df.duplicated(["trade_date", "tenor"], keep=False).sum())
    return out


def filter_unified_forecast_source(df: pd.DataFrame) -> pd.DataFrame:
    """Return the rows that represent the final unified-FDS candidate block when available."""
    required_filter_cols = {"model_spec", "model_source", "fit_status_candidate"}
    if required_filter_cols.issubset(df.columns):
        out = df[
            (df["model_spec"].astype(str) == "unified_fds_no_min_return")
            & (df["model_source"].astype(str) == "unified_fds_no_min_return_oos_refit")
            & (df["fit_status_candidate"].astype(str) == "candidate_fit")
        ].copy()
        if len(out) > 0:
            return out
    return df.copy()


def get_final_version_values(df: pd.DataFrame) -> List[str]:
    for col in ["final_signal_version", "signal_panel_version", "version"]:
        if col in df.columns:
            return sorted(df[col].dropna().astype(str).unique().tolist())
    return []


# ======================================================================================
# Main health logic
# ======================================================================================


def run_health_check(cfg: Config) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cfg.audit_out_dir.mkdir(parents=True, exist_ok=True)

    print_header("VRP production health check v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Expected date:             {date_to_str(cfg.expected_date)}")
    print(f"Require modified today:    {cfg.require_modified_today}")
    print(f"Warn only:                 {cfg.warn_only}")

    sources = {
        "spy_eod": cfg.spy_eod_path,
        "rv21d": cfg.rv21d_path,
        "corsi_support": cfg.corsi_support_path,
        "forecast_source": cfg.forecast_source_path,
        "final_panel": cfg.final_panel_path,
        "latest_snapshot": cfg.latest_snapshot_path,
        "selected_trades": cfg.selected_trades_path,
    }

    print_header("File existence")
    file_rows: List[Dict[str, Any]] = []
    checks: List[Dict[str, Any]] = []

    for label, path in sources.items():
        info = file_info(path)
        file_rows.append({"label": label, **info})
        exists = bool(info["exists"])
        add_check(checks, f"file_exists__{label}", exists, str(path), severity="hard")
        print(f"{label:18s} exists={exists!s:5s} path={path}")

    missing = [r["label"] for r in file_rows if not r["exists"]]
    if missing:
        # Save partial audit before raising.
        validation = pd.DataFrame(checks)
        manifest = {
            "run_timestamp": cfg.run_timestamp,
            "overall_status": "FAIL",
            "failure_stage": "file_existence",
            "missing_files": missing,
            "files": file_rows,
            "validation": validation.to_dict(orient="records"),
        }
        write_audits(cfg, validation, manifest, pd.DataFrame(file_rows), pd.DataFrame(), pd.DataFrame())
        raise FileNotFoundError(f"Missing required production files: {missing}")

    # Optional modified-today checks.
    today_str = date.today().isoformat()
    for row in file_rows:
        if cfg.require_modified_today:
            add_check(
                checks,
                f"modified_today__{row['label']}",
                row.get("modified_date") == today_str,
                f"modified_date={row.get('modified_date')} expected_today={today_str}; path={row.get('path')}",
                severity="hard",
            )
        else:
            add_check(
                checks,
                f"modified_timestamp_recorded__{row['label']}",
                row.get("modified_timestamp") is not None,
                f"modified_timestamp={row.get('modified_timestamp')}",
                severity="info",
            )

    # Load all panels.
    print_header("Loading production files")
    spy_eod = normalize_trade_date(load_parquet(cfg.spy_eod_path, "spy_eod"), "spy_eod")
    rv21d = normalize_trade_date(load_parquet(cfg.rv21d_path, "rv21d"), "rv21d")
    corsi_support = normalize_trade_date(load_parquet(cfg.corsi_support_path, "corsi_support"), "corsi_support")
    forecast_source_raw = normalize_trade_date(load_parquet(cfg.forecast_source_path, "forecast_source"), "forecast_source")
    final_panel = normalize_trade_date(load_parquet(cfg.final_panel_path, "final_panel"), "final_panel")
    latest_snapshot = normalize_trade_date(load_parquet(cfg.latest_snapshot_path, "latest_snapshot"), "latest_snapshot")
    selected_trades = normalize_trade_date(load_parquet(cfg.selected_trades_path, "selected_trades"), "selected_trades")

    if "tenor" in forecast_source_raw.columns:
        forecast_source_raw["tenor"] = pd.to_numeric(forecast_source_raw["tenor"], errors="coerce")
    if "tenor" in final_panel.columns:
        final_panel["tenor"] = pd.to_numeric(final_panel["tenor"], errors="coerce")
    if "tenor" in latest_snapshot.columns:
        latest_snapshot["tenor"] = pd.to_numeric(latest_snapshot["tenor"], errors="coerce")
    if "tenor" in selected_trades.columns:
        selected_trades["tenor"] = pd.to_numeric(selected_trades["tenor"], errors="coerce")

    forecast_source_unified = filter_unified_forecast_source(forecast_source_raw)

    panel_summaries = [
        summarize_panel("spy_eod", spy_eod),
        summarize_panel("rv21d", rv21d),
        summarize_panel("corsi_support", corsi_support),
        summarize_panel("forecast_source_raw", forecast_source_raw),
        summarize_panel("forecast_source_unified_candidate", forecast_source_unified),
        summarize_panel("final_panel", final_panel),
        summarize_panel("latest_snapshot", latest_snapshot),
        summarize_panel("selected_trades", selected_trades),
    ]

    summary_df = pd.DataFrame(panel_summaries)

    print_header("Latest dates")
    latest = {
        "spy_eod": latest_date(spy_eod, "spy_eod"),
        "rv21d": latest_date(rv21d, "rv21d"),
        "corsi_support": latest_date(corsi_support, "corsi_support"),
        "forecast_source_unified_candidate": latest_date(forecast_source_unified, "forecast_source_unified_candidate"),
        "final_panel": latest_date(final_panel, "final_panel"),
        "latest_snapshot": latest_date(latest_snapshot, "latest_snapshot"),
        "selected_trades_max_date": latest_date(selected_trades, "selected_trades"),
    }

    latest_rows = [
        {"component": k, "latest_date": date_to_str(v)} for k, v in latest.items()
    ]
    latest_df = pd.DataFrame(latest_rows)
    print(latest_df.to_string(index=False))

    # Reference date: explicit expected date if supplied, else latest SPY EOD date.
    reference_date = cfg.expected_date if cfg.expected_date is not None else latest["spy_eod"]

    # Existence / row checks.
    add_check(checks, "spy_eod_has_rows", len(spy_eod) > 0, f"rows={len(spy_eod):,}", severity="hard")
    add_check(checks, "rv21d_has_rows", len(rv21d) > 0, f"rows={len(rv21d):,}", severity="hard")
    add_check(checks, "corsi_support_has_rows", len(corsi_support) > 0, f"rows={len(corsi_support):,}", severity="hard")
    add_check(checks, "forecast_source_unified_has_rows", len(forecast_source_unified) > 0, f"rows={len(forecast_source_unified):,}", severity="hard")
    add_check(checks, "final_panel_has_rows", len(final_panel) > 0, f"rows={len(final_panel):,}", severity="hard")
    add_check(checks, "latest_snapshot_has_rows", len(latest_snapshot) > 0, f"rows={len(latest_snapshot):,}", severity="hard")

    # Required columns.
    final_missing_cols = safe_cols_missing(final_panel, REQUIRED_FINAL_PANEL_COLS)
    latest_missing_cols = safe_cols_missing(latest_snapshot, REQUIRED_LATEST_SNAPSHOT_COLS)
    selected_missing_cols = safe_cols_missing(selected_trades, REQUIRED_SELECTED_COLS) if len(selected_trades) > 0 else []

    add_check(
        checks,
        "final_panel_required_columns",
        len(final_missing_cols) == 0,
        f"missing={final_missing_cols}",
        severity="hard",
    )
    add_check(
        checks,
        "latest_snapshot_required_columns",
        len(latest_missing_cols) == 0,
        f"missing={latest_missing_cols}",
        severity="hard",
    )
    add_check(
        checks,
        "selected_trades_required_columns",
        len(selected_missing_cols) == 0,
        f"missing={selected_missing_cols}; selected_rows={len(selected_trades):,}",
        severity="hard",
    )

    # Date alignment checks.
    if reference_date is not None:
        for component in ["spy_eod", "rv21d", "corsi_support", "forecast_source_unified_candidate", "final_panel", "latest_snapshot"]:
            add_check(
                checks,
                f"latest_date_matches_reference__{component}",
                latest.get(component) == reference_date,
                f"latest={date_to_str(latest.get(component))}; reference={date_to_str(reference_date)}",
                severity="hard",
            )

    # Internal alignment even if no explicit expected date.
    add_check(
        checks,
        "final_panel_latest_matches_forecast_source_latest",
        latest["final_panel"] == latest["forecast_source_unified_candidate"],
        f"final_panel={date_to_str(latest['final_panel'])}; forecast_source_unified_candidate={date_to_str(latest['forecast_source_unified_candidate'])}",
        severity="hard",
    )
    add_check(
        checks,
        "final_panel_latest_matches_rv21d_latest",
        latest["final_panel"] == latest["rv21d"],
        f"final_panel={date_to_str(latest['final_panel'])}; rv21d={date_to_str(latest['rv21d'])}",
        severity="hard",
    )
    add_check(
        checks,
        "latest_snapshot_latest_matches_final_panel_latest",
        latest["latest_snapshot"] == latest["final_panel"],
        f"latest_snapshot={date_to_str(latest['latest_snapshot'])}; final_panel={date_to_str(latest['final_panel'])}",
        severity="hard",
    )

    # Duplicate and tenor-grid checks.
    final_dup_rows = int(final_panel.duplicated(["trade_date", "tenor"], keep=False).sum()) if {"trade_date", "tenor"}.issubset(final_panel.columns) else -1
    add_check(
        checks,
        "final_panel_no_duplicate_trade_date_tenor",
        final_dup_rows == 0,
        f"duplicate_rows={final_dup_rows:,}",
        severity="hard",
    )

    latest_tenors: List[int] = []
    if "tenor" in latest_snapshot.columns:
        latest_tenors = sorted(latest_snapshot["tenor"].dropna().astype(int).unique().tolist())

    add_check(
        checks,
        "latest_snapshot_has_9_rows",
        len(latest_snapshot) == EXPECTED_LATEST_SNAPSHOT_ROWS,
        f"latest_rows={len(latest_snapshot):,}; expected={EXPECTED_LATEST_SNAPSHOT_ROWS}",
        severity="hard",
    )
    add_check(
        checks,
        "latest_snapshot_expected_tenor_grid",
        latest_tenors == EXPECTED_TENORS,
        f"latest_tenors={latest_tenors}; expected={EXPECTED_TENORS}",
        severity="hard",
    )

    # Latest final panel should also have the full tenor grid.
    latest_panel_rows = final_panel[final_panel["trade_date"] == latest["final_panel"]].copy() if latest["final_panel"] is not None else final_panel.iloc[0:0].copy()
    latest_panel_tenors = sorted(latest_panel_rows["tenor"].dropna().astype(int).unique().tolist()) if "tenor" in latest_panel_rows.columns else []
    add_check(
        checks,
        "final_panel_latest_date_expected_tenor_grid",
        latest_panel_tenors == EXPECTED_TENORS,
        f"latest_panel_rows={len(latest_panel_rows):,}; latest_panel_tenors={latest_panel_tenors}",
        severity="hard",
    )

    # Selected max one per day.
    if len(selected_trades) > 0 and "trade_date" in selected_trades.columns:
        selected_per_day = selected_trades.groupby("trade_date").size()
        max_selected_per_day = int(selected_per_day.max()) if len(selected_per_day) > 0 else 0
    else:
        max_selected_per_day = 0
    add_check(
        checks,
        "selected_trades_max_one_per_date",
        max_selected_per_day <= 1,
        f"max_selected_per_day={max_selected_per_day}; selected_rows={len(selected_trades):,}",
        severity="hard",
    )

    # Recompute exact fields for latest and all final rows when columns exist.
    if {"forecast_vol_final", "forecast_variance_final"}.issubset(final_panel.columns):
        fv_diff = (final_panel["forecast_vol_final"].astype(float) - np.sqrt(final_panel["forecast_variance_final"].astype(float)) * 100).abs()
        add_check(
            checks,
            "final_panel_forecast_vol_reconstruction",
            bool((fv_diff <= 1e-10).all()),
            f"max_abs_diff={float(fv_diff.max()) if len(fv_diff) else None}",
            severity="hard",
        )
    else:
        add_check(checks, "final_panel_forecast_vol_reconstruction", False, "required columns missing", severity="hard")

    if {"model_vrp_log_final", "implied_variance_final", "forecast_variance_final"}.issubset(final_panel.columns):
        vrp_diff = (
            final_panel["model_vrp_log_final"].astype(float)
            - np.log(final_panel["implied_variance_final"].astype(float) / final_panel["forecast_variance_final"].astype(float))
        ).abs()
        add_check(
            checks,
            "final_panel_model_vrp_log_reconstruction",
            bool((vrp_diff <= 1e-10).all()),
            f"max_abs_diff={float(vrp_diff.max()) if len(vrp_diff) else None}",
            severity="hard",
        )
    else:
        add_check(checks, "final_panel_model_vrp_log_reconstruction", False, "required columns missing", severity="hard")

    # Check latest snapshot selected flags are sane.
    if "selected" in latest_snapshot.columns:
        latest_selected_count = int(latest_snapshot["selected"].fillna(False).astype(bool).sum())
    else:
        latest_selected_count = -1
    add_check(
        checks,
        "latest_snapshot_selected_count_lte_one",
        0 <= latest_selected_count <= 1,
        f"latest_selected_count={latest_selected_count}",
        severity="hard",
    )

    # Version check if present; info if absent.
    versions = get_final_version_values(final_panel)
    if versions:
        add_check(
            checks,
            "final_panel_version_expected_if_present",
            versions == [EXPECTED_ACTIVE_FINAL_VERSION],
            f"versions={versions}; expected={[EXPECTED_ACTIVE_FINAL_VERSION]}",
            severity="hard",
        )
    else:
        add_check(
            checks,
            "final_panel_version_absent",
            True,
            "No final_signal_version/signal_panel_version/version column found; not required in v1 file.",
            severity="info",
        )

    validation = pd.DataFrame(checks)

    hard_fail_count = int(((validation["severity"] == "hard") & (validation["status"] == "FAIL")).sum())
    hard_pass_count = int(((validation["severity"] == "hard") & (validation["status"] == "PASS")).sum())
    info_fail_count = int(((validation["severity"] == "info") & (validation["status"] == "FAIL")).sum())
    overall_status = "PASS" if hard_fail_count == 0 else "FAIL"

    # High-level component table for console.
    component_status_rows: List[Dict[str, Any]] = []
    for component in ["spy_eod", "rv21d", "corsi_support", "forecast_source_unified_candidate", "final_panel", "latest_snapshot"]:
        component_status_rows.append(
            {
                "component": component,
                "latest_date": date_to_str(latest[component]),
                "reference_date": date_to_str(reference_date),
                "date_status": "PASS" if latest[component] == reference_date else "FAIL",
            }
        )
    component_status = pd.DataFrame(component_status_rows)

    print_header("Production health summary")
    print(f"PRODUCTION HEALTH: {overall_status}")
    print(f"Reference official date: {date_to_str(reference_date)}")
    print(component_status.to_string(index=False))
    print(f"Hard checks passed: {hard_pass_count}")
    print(f"Hard checks failed: {hard_fail_count}")
    print(f"Info checks failed: {info_fail_count}")

    if hard_fail_count > 0:
        print("\nBlocking failures:")
        print(
            validation[(validation["severity"] == "hard") & (validation["status"] == "FAIL")][
                ["check", "detail"]
            ].to_string(index=False)
        )

    manifest = {
        "run_timestamp": cfg.run_timestamp,
        "script": "vrp_production_health_check_v1.py",
        "project_root": str(cfg.project_root),
        "overall_status": overall_status,
        "reference_official_date": date_to_str(reference_date),
        "expected_date_argument": date_to_str(cfg.expected_date),
        "require_modified_today": cfg.require_modified_today,
        "warn_only": cfg.warn_only,
        "latest_dates": {k: date_to_str(v) for k, v in latest.items()},
        "files": file_rows,
        "panel_summaries": panel_summaries,
        "hard_pass_count": hard_pass_count,
        "hard_fail_count": hard_fail_count,
        "info_fail_count": info_fail_count,
        "validation": validation.to_dict(orient="records"),
        "notes": [
            "This is a read-only health check. It does not rebuild production data.",
            "Forecast source freshness is checked against the unified_fds_no_min_return candidate block when those columns exist.",
            "Selected-trades max date can be earlier than the final panel latest date if there is no latest-date trade signal; this is not a failure.",
        ],
    }

    write_audits(cfg, validation, manifest, pd.DataFrame(file_rows), summary_df, component_status)
    return validation, manifest


def write_audits(
    cfg: Config,
    validation: pd.DataFrame,
    manifest: Dict[str, Any],
    files_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    component_status: pd.DataFrame,
) -> None:
    cfg.audit_out_dir.mkdir(parents=True, exist_ok=True)

    validation_path = cfg.audit_out_dir / f"vrp_production_health_check_{cfg.run_timestamp}.csv"
    manifest_path = cfg.audit_out_dir / f"vrp_production_health_manifest_{cfg.run_timestamp}.json"
    files_path = cfg.audit_out_dir / f"vrp_production_health_files_{cfg.run_timestamp}.csv"
    summary_path = cfg.audit_out_dir / f"vrp_production_health_panel_summary_{cfg.run_timestamp}.csv"
    component_path = cfg.audit_out_dir / f"vrp_production_health_component_status_{cfg.run_timestamp}.csv"

    validation.to_csv(validation_path, index=False)
    files_df.to_csv(files_path, index=False)
    if not summary_df.empty:
        summary_df.to_csv(summary_path, index=False)
    if not component_status.empty:
        component_status.to_csv(component_path, index=False)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print_header("Saved health-check audit outputs")
    print(f"validation          {validation_path}")
    print(f"manifest            {manifest_path}")
    print(f"files               {files_path}")
    if not summary_df.empty:
        print(f"panel_summary       {summary_path}")
    if not component_status.empty:
        print(f"component_status    {component_path}")


# ======================================================================================
# CLI
# ======================================================================================


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    parser = argparse.ArgumentParser(description="Read-only VRP production health/freshness check v1")

    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--spy-eod-path", default=None)
    parser.add_argument("--rv21d-path", default=None)
    parser.add_argument("--corsi-support-path", default=None)
    parser.add_argument("--forecast-source-path", default=None)
    parser.add_argument("--final-panel-path", default=None)
    parser.add_argument("--latest-snapshot-path", default=None)
    parser.add_argument("--selected-trades-path", default=None)
    parser.add_argument("--audit-out-dir", default=None)
    parser.add_argument("--expected-date", default=None, help="Optional expected official latest date: YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--require-modified-today", action="store_true", help="Fail if required files were not modified today")
    parser.add_argument("--warn-only", action="store_true", help="Always exit 0 even if hard health checks fail")

    args = parser.parse_args(argv)

    project_root = Path(args.project_root)
    run_timestamp = now_stamp()

    audit_out_dir = Path(args.audit_out_dir) if args.audit_out_dir else project_root / DEFAULT_AUDIT_OUT_REL

    return Config(
        project_root=project_root,
        spy_eod_path=resolve_path(project_root, args.spy_eod_path, DEFAULT_SPY_EOD_REL),
        rv21d_path=resolve_path(project_root, args.rv21d_path, DEFAULT_RV21D_REL),
        corsi_support_path=resolve_path(project_root, args.corsi_support_path, DEFAULT_CORSI_SUPPORT_REL),
        forecast_source_path=resolve_path(project_root, args.forecast_source_path, DEFAULT_FORECAST_SOURCE_REL),
        final_panel_path=resolve_path(project_root, args.final_panel_path, DEFAULT_FINAL_PANEL_REL),
        latest_snapshot_path=resolve_path(project_root, args.latest_snapshot_path, DEFAULT_LATEST_SNAPSHOT_REL),
        selected_trades_path=resolve_path(project_root, args.selected_trades_path, DEFAULT_SELECTED_TRADES_REL),
        audit_out_dir=audit_out_dir,
        expected_date=parse_date_arg(args.expected_date),
        require_modified_today=bool(args.require_modified_today),
        warn_only=bool(args.warn_only),
        run_timestamp=run_timestamp,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    validation, manifest = run_health_check(cfg)

    if manifest.get("overall_status") == "PASS":
        print("\nDONE — production health check passed.")
        return 0

    print("\nDONE — production health check failed.")
    if cfg.warn_only:
        print("warn-only enabled; exiting 0 despite failures.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
