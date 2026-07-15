#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_implied_variance_source_validation_v1.py

Read-only validator for choosing the canonical implied-variance source for VRP production.

Purpose
-------
Inspect the existing repaired VIX-style implied-variance term-structure file and compare it
against the implied-variance fields embedded in the current 07A forecast source and final
signal panel.

This script does NOT:
    - call ThetaData
    - rebuild option-chain implied variance
    - modify production data
    - recompute forecast variance, VRP, z-scores, signal rules, or sizing

It only reads existing parquet files and writes audit outputs.
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

DEFAULT_REPAIRED_IV_REL = Path(
    r"data\processed\vix_term_structure_history_v0_7_1_repaired_total_variance.parquet"
)

DEFAULT_FORECAST_SOURCE_REL = Path(
    r"data\processed\vrp_front_middle_corsi_forecast_repair_v1"
    r"\07A_unified_fds_no_min_return_oos_forecast_panel_20200102_20260701_20260704_203242.parquet"
)

DEFAULT_FINAL_PANEL_REL = Path(
    r"data\processed\vrp_final_signal\vrp_final_corsi_signal_base_panel_v1.parquet"
)

DEFAULT_AUDIT_OUT_REL = Path(r"data\audit\implied_variance_source_validation")

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

SCRIPT_VERSION = "vrp_implied_variance_source_validation_v1"
DECISION_CONTEXT = "validate_repaired_vix_term_structure_as_candidate_canonical_iv_source"


# ======================================================================================
# Config / helpers
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    repaired_iv_path: Path
    forecast_source_path: Path
    final_panel_path: Path
    audit_out_dir: Path
    variance_material_threshold: float
    vol_material_threshold: float
    reconstruction_tolerance: float
    fail_on_material_diff: bool
    run_timestamp: str


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def resolve_path(value: Optional[str], project_root: Path, default_rel: Path) -> Path:
    if value is None:
        return project_root / default_rel
    p = Path(value)
    return p if p.is_absolute() else project_root / p


def file_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "size_bytes": None,
            "size_mb": None,
            "modified_time": None,
        }
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "size_mb": round(stat.st_size / 1024 / 1024, 4),
        "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
    }


def add_check(
    checks: List[Dict[str, Any]],
    check: str,
    status: str,
    detail: str,
    severity: str = "HARD",
    component: str = "general",
) -> None:
    checks.append(
        {
            "component": component,
            "check": check,
            "severity": severity,
            "status": status,
            "detail": detail,
        }
    )


def require_columns(df: pd.DataFrame, required: Iterable[str], label: str) -> List[str]:
    return [c for c in required if c not in df.columns]


def parse_trade_date_series(series: pd.Series) -> pd.Series:
    """
    Robust production date parser.

    Handles:
      - pandas datetime
      - YYYY-MM-DD strings
      - YYYYMMDD strings
      - numeric YYYYMMDD values, which pandas otherwise misreads as nanoseconds after 1970
    """
    s = series.copy()

    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="raise").dt.normalize()

    non_null = s.dropna()
    if len(non_null) == 0:
        return pd.to_datetime(s, errors="coerce").dt.normalize()

    # Numeric YYYYMMDD, e.g. 20260625.
    if pd.api.types.is_numeric_dtype(non_null):
        vals = pd.to_numeric(s, errors="coerce")
        rounded = vals.round()

        numeric_non_null = vals.dropna()
        rounded_non_null = rounded.dropna()

        if len(numeric_non_null) == len(rounded_non_null):
            integer_like = ((numeric_non_null - rounded_non_null).abs() < 1e-6).all()
        else:
            integer_like = False

        if integer_like:
            as_str = rounded.astype("Int64").astype("string").str.zfill(8)
            valid_yyyymmdd = as_str.dropna().str.fullmatch(r"\d{8}").all()
            if valid_yyyymmdd:
                return pd.to_datetime(as_str, format="%Y%m%d", errors="raise").dt.normalize()

    # String YYYYMMDD.
    as_str = s.astype("string").str.strip()
    if as_str.dropna().str.fullmatch(r"\d{8}").all():
        return pd.to_datetime(as_str, format="%Y%m%d", errors="raise").dt.normalize()

    # General parser for ISO-style dates and timestamps.
    return pd.to_datetime(s, errors="raise").dt.normalize()


def normalize_trade_date(df: pd.DataFrame, date_col: str = "trade_date") -> pd.DataFrame:
    out = df.copy()
    out[date_col] = parse_trade_date_series(out[date_col])
    return out
def date_str(x: Any) -> Optional[str]:
    if pd.isna(x):
        return None
    return pd.Timestamp(x).strftime("%Y-%m-%d")


def safe_unique_sorted(series: pd.Series) -> List[Any]:
    vals = sorted(pd.Series(series).dropna().unique().tolist())
    clean: List[Any] = []
    for v in vals:
        if isinstance(v, (np.integer, int)):
            clean.append(int(v))
        elif isinstance(v, (np.floating, float)) and float(v).is_integer():
            clean.append(int(v))
        else:
            clean.append(v)
    return clean


def summarize_panel(df: pd.DataFrame, label: str, tenor_col: Optional[str]) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "component": label,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": ", ".join(map(str, df.columns.tolist())),
    }
    if "trade_date" in df.columns and len(df) > 0:
        dates = pd.to_datetime(df["trade_date"]).dt.normalize()
        row["date_min"] = date_str(dates.min())
        row["date_max"] = date_str(dates.max())
        row["unique_dates"] = int(dates.nunique())
    else:
        row["date_min"] = None
        row["date_max"] = None
        row["unique_dates"] = None

    if tenor_col and tenor_col in df.columns and len(df) > 0:
        row["tenor_column"] = tenor_col
        row["tenor_values"] = json.dumps(safe_unique_sorted(df[tenor_col]))
        row["unique_tenors"] = int(pd.Series(df[tenor_col]).nunique(dropna=True))
    else:
        row["tenor_column"] = tenor_col
        row["tenor_values"] = None
        row["unique_tenors"] = None

    return row


def check_no_duplicate_keys(df: pd.DataFrame, label: str, key_cols: List[str], checks: List[Dict[str, Any]]) -> None:
    if any(c not in df.columns for c in key_cols):
        add_check(
            checks,
            f"{label}_duplicate_keys",
            "FAIL",
            f"missing key cols={[c for c in key_cols if c not in df.columns]}",
            component=label,
        )
        return
    dup_count = int(df.duplicated(key_cols).sum())
    add_check(
        checks,
        f"{label}_duplicate_keys",
        "PASS" if dup_count == 0 else "FAIL",
        f"duplicate_rows={dup_count}; key_cols={key_cols}",
        component=label,
    )


def check_positive_and_vol_recon(
    df: pd.DataFrame,
    label: str,
    var_col: str,
    vol_col: str,
    tolerance: float,
    checks: List[Dict[str, Any]],
) -> None:
    missing = require_columns(df, [var_col, vol_col], label)
    if missing:
        add_check(
            checks,
            f"{label}_variance_vol_columns",
            "FAIL",
            f"missing={missing}",
            component=label,
        )
        return

    var = pd.to_numeric(df[var_col], errors="coerce")
    vol = pd.to_numeric(df[vol_col], errors="coerce")
    nonfinite_var = int((~np.isfinite(var)).sum())
    nonfinite_vol = int((~np.isfinite(vol)).sum())
    nonpositive_var = int((var <= 0).sum())
    status = "PASS" if nonfinite_var == 0 and nonfinite_vol == 0 and nonpositive_var == 0 else "FAIL"
    add_check(
        checks,
        f"{label}_positive_finite_variance_vol",
        status,
        f"nonfinite_var={nonfinite_var}; nonfinite_vol={nonfinite_vol}; nonpositive_var={nonpositive_var}",
        component=label,
    )

    recon = np.sqrt(var) * 100.0
    diff = (vol - recon).abs()
    max_abs_diff = float(diff.max()) if len(diff) else np.nan
    fail_count = int((diff > tolerance).sum())
    add_check(
        checks,
        f"{label}_vix_style_vol_reconstruction",
        "PASS" if fail_count == 0 else "FAIL",
        f"max_abs_diff={max_abs_diff}; tolerance={tolerance}; fail_count={fail_count}",
        component=label,
    )


def check_tenor_grid(
    df: pd.DataFrame,
    label: str,
    tenor_col: str,
    expected_tenors: List[int],
    checks: List[Dict[str, Any]],
) -> pd.DataFrame:
    if "trade_date" not in df.columns or tenor_col not in df.columns:
        add_check(
            checks,
            f"{label}_tenor_grid",
            "FAIL",
            f"missing trade_date or {tenor_col}",
            component=label,
        )
        return pd.DataFrame()

    work = df[["trade_date", tenor_col]].copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"]).dt.normalize()
    work[tenor_col] = pd.to_numeric(work[tenor_col], errors="coerce").astype("Int64")

    expected_set = set(expected_tenors)
    rows: List[Dict[str, Any]] = []
    bad_dates = 0
    for dt, g in work.groupby("trade_date"):
        tenors = set(int(x) for x in g[tenor_col].dropna().unique().tolist())
        missing = sorted(expected_set - tenors)
        extra = sorted(tenors - expected_set)
        duplicate_tenors = int(g.duplicated([tenor_col]).sum())
        ok = len(missing) == 0 and len(extra) == 0 and duplicate_tenors == 0
        if not ok:
            bad_dates += 1
        rows.append(
            {
                "component": label,
                "trade_date": date_str(dt),
                "tenor_count": len(tenors),
                "missing_tenors": json.dumps(missing),
                "extra_tenors": json.dumps(extra),
                "duplicate_tenor_rows": duplicate_tenors,
                "status": "PASS" if ok else "FAIL",
            }
        )

    status = "PASS" if bad_dates == 0 else "FAIL"
    add_check(
        checks,
        f"{label}_full_expected_tenor_grid_by_date",
        status,
        f"bad_dates={bad_dates}; expected_tenors={expected_tenors}",
        component=label,
    )
    return pd.DataFrame(rows)


# ======================================================================================
# Load / normalize source panels
# ======================================================================================


def load_repaired_iv(path: Path, checks: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_parquet(path)
    missing = require_columns(df, ["trade_date", "implied_variance", "vix_style_vol"], "repaired_iv")
    tenor_col = "target_days" if "target_days" in df.columns else "tenor" if "tenor" in df.columns else None
    if tenor_col is None:
        missing.append("target_days_or_tenor")
    add_check(
        checks,
        "repaired_iv_required_columns",
        "PASS" if not missing else "FAIL",
        f"missing={missing}; rows={len(df)}; columns={len(df.columns)}",
        component="repaired_iv",
    )
    if missing:
        return df, pd.DataFrame()

    df = normalize_trade_date(df)
    out = df[["trade_date", tenor_col, "implied_variance", "vix_style_vol"]].copy()
    out = out.rename(
        columns={
            tenor_col: "tenor",
            "implied_variance": "implied_variance_repaired",
            "vix_style_vol": "vix_style_vol_repaired",
        }
    )
    out["tenor"] = pd.to_numeric(out["tenor"], errors="raise").astype(int)
    return df, out


def load_forecast_source(path: Path, checks: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_parquet(path)
    required = ["trade_date", "tenor", "implied_variance", "vix_style_vol"]
    missing = require_columns(df, required, "forecast_source")
    add_check(
        checks,
        "forecast_source_required_columns",
        "PASS" if not missing else "FAIL",
        f"missing={missing}; rows={len(df)}; columns={len(df.columns)}",
        component="forecast_source",
    )
    if missing:
        return df, pd.DataFrame()

    df = normalize_trade_date(df)
    work = df.copy()

    filters: List[str] = []
    if "model_spec" in work.columns:
        before = len(work)
        work = work[work["model_spec"].astype("string") == "unified_fds_no_min_return"].copy()
        filters.append(f"model_spec=unified_fds_no_min_return kept={len(work)}/{before}")
    if "model_source" in work.columns:
        before = len(work)
        if (work["model_source"].astype("string") == "unified_fds_no_min_return_oos_refit").any():
            work = work[work["model_source"].astype("string") == "unified_fds_no_min_return_oos_refit"].copy()
            filters.append(f"model_source=unified_fds_no_min_return_oos_refit kept={len(work)}/{before}")
    if "fit_status_candidate" in work.columns:
        before = len(work)
        if (work["fit_status_candidate"].astype("string") == "candidate_fit").any():
            work = work[work["fit_status_candidate"].astype("string") == "candidate_fit"].copy()
            filters.append(f"fit_status_candidate=candidate_fit kept={len(work)}/{before}")

    add_check(
        checks,
        "forecast_source_unified_candidate_filter",
        "PASS" if len(work) > 0 else "FAIL",
        "; ".join(filters) if filters else "no model filter columns found; using full file",
        component="forecast_source",
    )

    out = work[["trade_date", "tenor", "implied_variance", "vix_style_vol"]].copy()
    out = out.rename(
        columns={
            "implied_variance": "implied_variance_forecast_source",
            "vix_style_vol": "vix_style_vol_forecast_source",
        }
    )
    out["tenor"] = pd.to_numeric(out["tenor"], errors="raise").astype(int)
    return df, out


def load_final_panel(path: Path, checks: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_parquet(path)
    required = ["trade_date", "tenor", "implied_variance_final", "vix_style_vol_final"]
    missing = require_columns(df, required, "final_panel")
    add_check(
        checks,
        "final_panel_required_columns",
        "PASS" if not missing else "FAIL",
        f"missing={missing}; rows={len(df)}; columns={len(df.columns)}",
        component="final_panel",
    )
    if missing:
        return df, pd.DataFrame()

    df = normalize_trade_date(df)
    out = df[["trade_date", "tenor", "implied_variance_final", "vix_style_vol_final"]].copy()
    out["tenor"] = pd.to_numeric(out["tenor"], errors="raise").astype(int)
    return df, out


# ======================================================================================
# Comparisons
# ======================================================================================


def date_gap_rows(panels: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    date_sets: Dict[str, set] = {}
    for label, df in panels.items():
        if "trade_date" in df.columns and len(df) > 0:
            date_sets[label] = set(pd.to_datetime(df["trade_date"]).dt.normalize().unique().tolist())
        else:
            date_sets[label] = set()

    all_dates = sorted(set().union(*date_sets.values())) if date_sets else []
    rows: List[Dict[str, Any]] = []
    for dt in all_dates:
        row: Dict[str, Any] = {"trade_date": date_str(dt)}
        for label, s in date_sets.items():
            row[f"in_{label}"] = dt in s
        rows.append(row)
    return pd.DataFrame(rows)


def compare_two_sources(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_label: str,
    right_label: str,
    left_var_col: str,
    right_var_col: str,
    left_vol_col: str,
    right_vol_col: str,
    variance_material_threshold: float,
    vol_material_threshold: float,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    key = ["trade_date", "tenor"]
    if left.empty or right.empty:
        return pd.DataFrame(), {
            "comparison": f"{left_label}_vs_{right_label}",
            "overlap_rows": 0,
            "overlap_dates": 0,
            "overlap_tenors": 0,
            "status": "NO_OVERLAP_OR_EMPTY_INPUT",
        }

    m = left.merge(right, on=key, how="inner")
    if m.empty:
        return m, {
            "comparison": f"{left_label}_vs_{right_label}",
            "overlap_rows": 0,
            "overlap_dates": 0,
            "overlap_tenors": 0,
            "status": "NO_OVERLAP",
        }

    m["variance_diff"] = pd.to_numeric(m[left_var_col], errors="coerce") - pd.to_numeric(m[right_var_col], errors="coerce")
    m["variance_abs_diff"] = m["variance_diff"].abs()
    m["vol_diff"] = pd.to_numeric(m[left_vol_col], errors="coerce") - pd.to_numeric(m[right_vol_col], errors="coerce")
    m["vol_abs_diff"] = m["vol_diff"].abs()
    m["variance_material_diff"] = m["variance_abs_diff"] > variance_material_threshold
    m["vol_material_diff"] = m["vol_abs_diff"] > vol_material_threshold
    m["material_diff"] = m["variance_material_diff"] | m["vol_material_diff"]

    summary: Dict[str, Any] = {
        "comparison": f"{left_label}_vs_{right_label}",
        "left_label": left_label,
        "right_label": right_label,
        "overlap_rows": int(len(m)),
        "overlap_dates": int(m["trade_date"].nunique()),
        "overlap_tenors": int(m["tenor"].nunique()),
        "date_min": date_str(m["trade_date"].min()),
        "date_max": date_str(m["trade_date"].max()),
        "variance_abs_diff_mean": float(m["variance_abs_diff"].mean()),
        "variance_abs_diff_p95": float(m["variance_abs_diff"].quantile(0.95)),
        "variance_abs_diff_max": float(m["variance_abs_diff"].max()),
        "vol_abs_diff_mean": float(m["vol_abs_diff"].mean()),
        "vol_abs_diff_p95": float(m["vol_abs_diff"].quantile(0.95)),
        "vol_abs_diff_max": float(m["vol_abs_diff"].max()),
        "variance_material_threshold": variance_material_threshold,
        "vol_material_threshold": vol_material_threshold,
        "material_diff_rows": int(m["material_diff"].sum()),
        "material_diff_dates": int(m.loc[m["material_diff"], "trade_date"].nunique()),
        "material_diff_tenors": int(m.loc[m["material_diff"], "tenor"].nunique()),
        "status": "PASS" if int(m["material_diff"].sum()) == 0 else "MATERIAL_DIFFS_FOUND",
    }
    return m, summary


# ======================================================================================
# Main
# ======================================================================================


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Validate candidate canonical implied-variance source.")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--repaired-iv-path", default=None)
    parser.add_argument("--forecast-source-path", default=None)
    parser.add_argument("--final-panel-path", default=None)
    parser.add_argument("--audit-out-dir", default=None)
    parser.add_argument(
        "--variance-material-threshold",
        type=float,
        default=1e-8,
        help="Variance absolute-difference threshold used to flag material source mismatches.",
    )
    parser.add_argument(
        "--vol-material-threshold",
        type=float,
        default=0.01,
        help="Vol-point absolute-difference threshold used to flag material source mismatches.",
    )
    parser.add_argument(
        "--reconstruction-tolerance",
        type=float,
        default=1e-8,
        help="Tolerance for vix_style_vol == sqrt(implied_variance) * 100 checks.",
    )
    parser.add_argument(
        "--fail-on-material-diff",
        action="store_true",
        help="Treat source comparison material differences as hard failures instead of warnings.",
    )

    args = parser.parse_args()
    project_root = Path(args.project_root)
    return Config(
        project_root=project_root,
        repaired_iv_path=resolve_path(args.repaired_iv_path, project_root, DEFAULT_REPAIRED_IV_REL),
        forecast_source_path=resolve_path(args.forecast_source_path, project_root, DEFAULT_FORECAST_SOURCE_REL),
        final_panel_path=resolve_path(args.final_panel_path, project_root, DEFAULT_FINAL_PANEL_REL),
        audit_out_dir=resolve_path(args.audit_out_dir, project_root, DEFAULT_AUDIT_OUT_REL),
        variance_material_threshold=float(args.variance_material_threshold),
        vol_material_threshold=float(args.vol_material_threshold),
        reconstruction_tolerance=float(args.reconstruction_tolerance),
        fail_on_material_diff=bool(args.fail_on_material_diff),
        run_timestamp=now_stamp(),
    )


def main() -> int:
    cfg = parse_args()
    cfg.audit_out_dir.mkdir(parents=True, exist_ok=True)

    checks: List[Dict[str, Any]] = []
    file_rows: List[Dict[str, Any]] = []
    panel_summary_rows: List[Dict[str, Any]] = []
    tenor_grid_frames: List[pd.DataFrame] = []

    print_header("VRP implied variance source validation v1")
    print(f"Project root:                  {cfg.project_root}")
    print(f"Run timestamp:                 {cfg.run_timestamp}")
    print(f"Audit dir:                     {cfg.audit_out_dir}")
    print(f"Variance material threshold:   {cfg.variance_material_threshold}")
    print(f"Vol material threshold:        {cfg.vol_material_threshold}")
    print(f"Fail on material diff:         {cfg.fail_on_material_diff}")

    paths = {
        "repaired_iv": cfg.repaired_iv_path,
        "forecast_source": cfg.forecast_source_path,
        "final_panel": cfg.final_panel_path,
    }

    print_header("File existence")
    for label, path in paths.items():
        meta = file_meta(path)
        meta["component"] = label
        file_rows.append(meta)
        add_check(
            checks,
            f"{label}_file_exists",
            "PASS" if meta["exists"] else "FAIL",
            str(path),
            component=label,
        )
        print(f"{label:<18} exists={meta['exists']}  path={path}")

    if not all(Path(p).exists() for p in paths.values()):
        print_header("Production implied variance source validation summary")
        print("IMPLIED VARIANCE SOURCE VALIDATION: FAIL")
        print("One or more required files are missing. No comparisons run.")
        validation_df = pd.DataFrame(checks)
        file_df = pd.DataFrame(file_rows)
        validation_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_{cfg.run_timestamp}.csv"
        file_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_files_{cfg.run_timestamp}.csv"
        manifest_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_manifest_{cfg.run_timestamp}.json"
        validation_df.to_csv(validation_path, index=False)
        file_df.to_csv(file_path, index=False)
        manifest_path.write_text(json.dumps({"config": asdict(cfg), "status": "FAIL"}, indent=2, default=str), encoding="utf-8")
        print(f"validation    {validation_path}")
        print(f"files         {file_path}")
        print(f"manifest      {manifest_path}")
        return 1

    print_header("Loading source files")
    repaired_raw, repaired_norm = load_repaired_iv(cfg.repaired_iv_path, checks)
    forecast_raw, forecast_norm = load_forecast_source(cfg.forecast_source_path, checks)
    final_raw, final_norm = load_final_panel(cfg.final_panel_path, checks)

    panel_summary_rows.append(summarize_panel(repaired_raw, "repaired_iv_raw", "target_days" if "target_days" in repaired_raw.columns else "tenor" if "tenor" in repaired_raw.columns else None))
    panel_summary_rows.append(summarize_panel(repaired_norm, "repaired_iv_normalized", "tenor"))
    panel_summary_rows.append(summarize_panel(forecast_raw, "forecast_source_raw", "tenor" if "tenor" in forecast_raw.columns else None))
    panel_summary_rows.append(summarize_panel(forecast_norm, "forecast_source_unified_candidate", "tenor"))
    panel_summary_rows.append(summarize_panel(final_raw, "final_panel_raw", "tenor" if "tenor" in final_raw.columns else None))
    panel_summary_rows.append(summarize_panel(final_norm, "final_panel_normalized", "tenor"))

    # Basic validations.
    if not repaired_norm.empty:
        check_no_duplicate_keys(repaired_norm, "repaired_iv", ["trade_date", "tenor"], checks)
        check_positive_and_vol_recon(
            repaired_norm,
            "repaired_iv",
            "implied_variance_repaired",
            "vix_style_vol_repaired",
            cfg.reconstruction_tolerance,
            checks,
        )
        tenor_grid_frames.append(check_tenor_grid(repaired_norm, "repaired_iv", "tenor", EXPECTED_TENORS, checks))

    if not forecast_norm.empty:
        check_no_duplicate_keys(forecast_norm, "forecast_source", ["trade_date", "tenor"], checks)
        check_positive_and_vol_recon(
            forecast_norm,
            "forecast_source",
            "implied_variance_forecast_source",
            "vix_style_vol_forecast_source",
            cfg.reconstruction_tolerance,
            checks,
        )
        tenor_grid_frames.append(check_tenor_grid(forecast_norm, "forecast_source", "tenor", EXPECTED_TENORS, checks))

    if not final_norm.empty:
        check_no_duplicate_keys(final_norm, "final_panel", ["trade_date", "tenor"], checks)
        check_positive_and_vol_recon(
            final_norm,
            "final_panel",
            "implied_variance_final",
            "vix_style_vol_final",
            cfg.reconstruction_tolerance,
            checks,
        )
        tenor_grid_frames.append(check_tenor_grid(final_norm, "final_panel", "tenor", EXPECTED_TENORS, checks))

    # Date gaps.
    date_gaps = date_gap_rows(
        {
            "repaired_iv": repaired_norm,
            "forecast_source": forecast_norm,
            "final_panel": final_norm,
        }
    )
    if not date_gaps.empty:
        date_gaps["missing_from_repaired_but_in_forecast_or_final"] = (
            (~date_gaps["in_repaired_iv"])
            & (date_gaps["in_forecast_source"] | date_gaps["in_final_panel"])
        )
        missing_from_repaired = int(date_gaps["missing_from_repaired_but_in_forecast_or_final"].sum())
        add_check(
            checks,
            "dates_missing_from_repaired_relative_to_forecast_or_final",
            "PASS" if missing_from_repaired == 0 else "WARN",
            f"missing_date_count={missing_from_repaired}",
            severity="WARN",
            component="date_alignment",
        )

        date_gaps["missing_from_forecast_but_in_repaired"] = date_gaps["in_repaired_iv"] & (~date_gaps["in_forecast_source"])
        missing_from_forecast = int(date_gaps["missing_from_forecast_but_in_repaired"].sum())
        add_check(
            checks,
            "dates_missing_from_forecast_relative_to_repaired",
            "PASS" if missing_from_forecast == 0 else "INFO",
            f"missing_date_count={missing_from_forecast}",
            severity="INFO",
            component="date_alignment",
        )

    # Source comparisons.
    comparison_summaries: List[Dict[str, Any]] = []
    comparison_frames: List[pd.DataFrame] = []

    comp_rf, summary_rf = compare_two_sources(
        repaired_norm,
        forecast_norm,
        "repaired_iv",
        "forecast_source",
        "implied_variance_repaired",
        "implied_variance_forecast_source",
        "vix_style_vol_repaired",
        "vix_style_vol_forecast_source",
        cfg.variance_material_threshold,
        cfg.vol_material_threshold,
    )
    comparison_summaries.append(summary_rf)
    if not comp_rf.empty:
        comp_rf.insert(0, "comparison", "repaired_iv_vs_forecast_source")
        comparison_frames.append(comp_rf)

    comp_rfinal, summary_rfinal = compare_two_sources(
        repaired_norm,
        final_norm,
        "repaired_iv",
        "final_panel",
        "implied_variance_repaired",
        "implied_variance_final",
        "vix_style_vol_repaired",
        "vix_style_vol_final",
        cfg.variance_material_threshold,
        cfg.vol_material_threshold,
    )
    comparison_summaries.append(summary_rfinal)
    if not comp_rfinal.empty:
        comp_rfinal.insert(0, "comparison", "repaired_iv_vs_final_panel")
        comparison_frames.append(comp_rfinal)

    for s in comparison_summaries:
        material_rows = int(s.get("material_diff_rows", 0) or 0)
        no_overlap = int(s.get("overlap_rows", 0) or 0) == 0
        if no_overlap:
            add_check(
                checks,
                f"{s['comparison']}_overlap",
                "FAIL",
                f"overlap_rows={s.get('overlap_rows')}; status={s.get('status')}",
                component="source_comparison",
            )
        else:
            add_check(
                checks,
                f"{s['comparison']}_overlap",
                "PASS",
                f"overlap_rows={s.get('overlap_rows')}; overlap_dates={s.get('overlap_dates')}; date_range={s.get('date_min')} to {s.get('date_max')}",
                component="source_comparison",
            )
            severity = "HARD" if cfg.fail_on_material_diff else "WARN"
            status = "PASS" if material_rows == 0 else ("FAIL" if cfg.fail_on_material_diff else "WARN")
            add_check(
                checks,
                f"{s['comparison']}_material_differences",
                status,
                f"material_diff_rows={material_rows}; material_diff_dates={s.get('material_diff_dates')}; "
                f"max_var_abs_diff={s.get('variance_abs_diff_max')}; max_vol_abs_diff={s.get('vol_abs_diff_max')}",
                severity=severity,
                component="source_comparison",
            )

    comparisons_all = pd.concat(comparison_frames, ignore_index=True) if comparison_frames else pd.DataFrame()
    material_diffs = comparisons_all[comparisons_all["material_diff"]].copy() if not comparisons_all.empty else pd.DataFrame()
    if not material_diffs.empty:
        material_diffs = material_diffs.sort_values(
            ["comparison", "vol_abs_diff", "variance_abs_diff"], ascending=[True, False, False]
        )

    # Final status.
    validation_df = pd.DataFrame(checks)
    hard_fail_count = int(((validation_df["severity"] == "HARD") & (validation_df["status"] == "FAIL")).sum())
    warn_count = int((validation_df["status"] == "WARN").sum())
    overall_status = "FAIL" if hard_fail_count > 0 else "WARN" if warn_count > 0 else "PASS"

    print_header("Panel summaries")
    panel_summary_df = pd.DataFrame(panel_summary_rows)
    print(panel_summary_df[["component", "rows", "columns", "date_min", "date_max", "unique_dates", "tenor_values"]].to_string(index=False))

    print_header("Comparison summary")
    comparison_summary_df = pd.DataFrame(comparison_summaries)
    if comparison_summary_df.empty:
        print("No comparisons available.")
    else:
        display_cols = [
            "comparison",
            "overlap_rows",
            "overlap_dates",
            "date_min",
            "date_max",
            "material_diff_rows",
            "material_diff_dates",
            "variance_abs_diff_max",
            "vol_abs_diff_max",
            "status",
        ]
        print(comparison_summary_df[[c for c in display_cols if c in comparison_summary_df.columns]].to_string(index=False))

    print_header("Source validation summary")
    print(f"IMPLIED VARIANCE SOURCE VALIDATION: {overall_status}")
    print(f"Hard checks passed: {int(((validation_df['severity'] == 'HARD') & (validation_df['status'] == 'PASS')).sum())}")
    print(f"Hard checks failed: {hard_fail_count}")
    print(f"Warnings:          {warn_count}")
    if hard_fail_count > 0:
        print("\nBlocking failures:")
        print(validation_df[(validation_df["severity"] == "HARD") & (validation_df["status"] == "FAIL")][["component", "check", "detail"]].to_string(index=False))
    if warn_count > 0:
        print("\nWarnings:")
        print(validation_df[validation_df["status"] == "WARN"][["component", "check", "detail"]].to_string(index=False))

    # Save outputs.
    file_df = pd.DataFrame(file_rows)
    tenor_grid_df = pd.concat([x for x in tenor_grid_frames if x is not None and not x.empty], ignore_index=True) if tenor_grid_frames else pd.DataFrame()

    validation_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_{cfg.run_timestamp}.csv"
    file_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_files_{cfg.run_timestamp}.csv"
    panel_summary_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_panel_summary_{cfg.run_timestamp}.csv"
    tenor_grid_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_tenor_grid_{cfg.run_timestamp}.csv"
    date_gaps_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_date_gaps_{cfg.run_timestamp}.csv"
    comparison_summary_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_comparison_summary_{cfg.run_timestamp}.csv"
    comparison_rows_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_comparison_rows_{cfg.run_timestamp}.csv"
    material_diffs_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_material_diffs_{cfg.run_timestamp}.csv"
    manifest_path = cfg.audit_out_dir / f"vrp_implied_variance_source_validation_manifest_{cfg.run_timestamp}.json"

    validation_df.to_csv(validation_path, index=False)
    file_df.to_csv(file_path, index=False)
    panel_summary_df.to_csv(panel_summary_path, index=False)
    tenor_grid_df.to_csv(tenor_grid_path, index=False)
    date_gaps.to_csv(date_gaps_path, index=False)
    comparison_summary_df.to_csv(comparison_summary_path, index=False)
    comparisons_all.to_csv(comparison_rows_path, index=False)
    material_diffs.to_csv(material_diffs_path, index=False)

    manifest = {
        "script_version": SCRIPT_VERSION,
        "decision_context": DECISION_CONTEXT,
        "run_timestamp": cfg.run_timestamp,
        "config": asdict(cfg),
        "overall_status": overall_status,
        "hard_checks_failed": hard_fail_count,
        "warnings": warn_count,
        "expected_tenors": EXPECTED_TENORS,
        "outputs": {
            "validation": str(validation_path),
            "files": str(file_path),
            "panel_summary": str(panel_summary_path),
            "tenor_grid": str(tenor_grid_path),
            "date_gaps": str(date_gaps_path),
            "comparison_summary": str(comparison_summary_path),
            "comparison_rows": str(comparison_rows_path),
            "material_diffs": str(material_diffs_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print_header("Saved implied-variance source validation outputs")
    print(f"validation          {validation_path}")
    print(f"files               {file_path}")
    print(f"panel_summary       {panel_summary_path}")
    print(f"tenor_grid          {tenor_grid_path}")
    print(f"date_gaps           {date_gaps_path}")
    print(f"comparison_summary  {comparison_summary_path}")
    print(f"comparison_rows     {comparison_rows_path}")
    print(f"material_diffs      {material_diffs_path}")
    print(f"manifest            {manifest_path}")

    if overall_status == "FAIL":
        print("\nDONE — implied-variance source validation failed.")
        return 1
    if overall_status == "WARN":
        print("\nDONE — implied-variance source validation completed with warnings.")
        return 0
    print("\nDONE — implied-variance source validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
