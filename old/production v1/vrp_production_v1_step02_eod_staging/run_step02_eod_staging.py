"""
VRP Production v1 — Step 02 EOD Staging Updater

Builds missing VIX-style term-structure rows for a date range by loading the
function definitions from the existing v0.7 market-close notebook, then calling
run_vix_term_structure_batch_v7(...).

Safety principle:
    - This script DOES NOT call update_term_structure_history_for_range_v7(...)
    - This script DOES NOT call upsert_term_structure_history(...)
    - This script DOES NOT overwrite the canonical repaired history file

Outputs are staging-only under:
    data/processed/staging/
    data/audit/production_v1/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
CANONICAL_REPAIRED_NAME = "vix_term_structure_history_v0_7_1_repaired_total_variance.parquet"
USAGE_MARKER = "Run the definition cells above first"
FINAL_METHOD_VERSION = "v0.7.1_repaired_total_variance_cboe_anchors"


def parse_yyyymmdd(value: Any) -> int:
    """Return YYYYMMDD int from int/string/Timestamp-like value."""
    if pd.isna(value):
        raise ValueError("Cannot parse missing date")
    if isinstance(value, (pd.Timestamp, datetime)):
        return int(pd.Timestamp(value).strftime("%Y%m%d"))
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    # Already YYYYMMDD integer/string
    if s.isdigit() and len(s) == 8:
        return int(s)
    # Date-like string
    ts = pd.to_datetime(s, errors="raise")
    return int(ts.strftime("%Y%m%d"))


def yyyymmdd_to_date(value: Any) -> pd.Timestamp:
    return pd.to_datetime(str(parse_yyyymmdd(value)), format="%Y%m%d")


def ensure_dirs(project_root: Path) -> Tuple[Path, Path]:
    staging_dir = project_root / "data" / "processed" / "staging"
    audit_dir = project_root / "data" / "audit" / "production_v1"
    staging_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir, audit_dir


def read_notebook_code_until_marker(notebook_path: Path, marker: str = USAGE_MARKER) -> str:
    """Read code cells from an .ipynb file until a markdown/code cell contains marker."""
    nb = json.loads(notebook_path.read_text(encoding="utf-8"))
    code_parts: List[str] = []
    for cell_idx, cell in enumerate(nb.get("cells", [])):
        source_obj = cell.get("source", "")
        source = "".join(source_obj) if isinstance(source_obj, list) else str(source_obj)
        if marker in source:
            print(f"Stopping notebook load at cell {cell_idx}: found usage marker.")
            break
        if cell.get("cell_type") == "code":
            code_parts.append(f"\n# --- notebook cell {cell_idx} ---\n")
            code_parts.append(source)
            code_parts.append("\n")
    return "".join(code_parts)


def load_notebook_namespace(project_root: Path, notebook_path: Path, refresh_sofr: bool = False) -> Dict[str, Any]:
    """Execute definition cells from the builder notebook in a controlled namespace."""
    old_cwd = Path.cwd()
    os.chdir(project_root)
    try:
        ns: Dict[str, Any] = {
            "__name__": "__vrp_notebook_definitions__",
            "display": lambda *args, **kwargs: None,
        }
        code = read_notebook_code_until_marker(notebook_path)
        exec(compile(code, str(notebook_path), "exec"), ns)

        if refresh_sofr and "update_fred_sofr_history" in ns:
            print("Refreshing/loading FRED SOFR history from notebook helper...")
            try:
                ns["update_fred_sofr_history"](force_refresh=True)
            except TypeError:
                ns["update_fred_sofr_history"]()

        if "update_spx_trading_dates_file" in ns:
            print("Refreshing SPX trading-date calendar through 2026-12-31...")
            try:
                ns["update_spx_trading_dates_file"](start_date=20180625, end_date=20261231)
            except Exception as exc:
                print(f"WARNING: calendar refresh failed, continuing with existing local calendar. Error: {exc}")

        required = ["run_vix_term_structure_batch_v7"]
        missing = [x for x in required if x not in ns]
        if missing:
            raise RuntimeError(f"Notebook namespace missing required functions: {missing}")
        return ns
    finally:
        os.chdir(old_cwd)


def get_trading_dates_from_notebook(ns: Dict[str, Any], start_date: int, end_date: int) -> List[int]:
    if "get_spx_trade_dates_between" in ns:
        dates = ns["get_spx_trade_dates_between"](start_date, end_date)
        return [parse_yyyymmdd(x) for x in dates]
    # Fallback: use pandas business days only. This should rarely be used.
    bdays = pd.bdate_range(yyyymmdd_to_date(start_date), yyyymmdd_to_date(end_date))
    return [int(x.strftime("%Y%m%d")) for x in bdays]


def normalize_result_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" in out.columns:
        out["trade_date"] = out["trade_date"].map(parse_yyyymmdd).astype(int)
    if "target_days" in out.columns:
        out["target_days"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")
    return out


def run_basic_curve_qa(results_df: pd.DataFrame, expected_dates: Iterable[int]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    expected_dates = [int(x) for x in expected_dates]
    df = normalize_result_dates(results_df) if not results_df.empty else results_df.copy()

    for d in expected_dates:
        g = df[df["trade_date"] == d].copy() if not df.empty and "trade_date" in df.columns else pd.DataFrame()
        row: Dict[str, Any] = {"trade_date": d}
        row["row_count"] = int(len(g))
        row["has_9_tenors"] = set(g.get("target_days", [])) == set(TARGET_TENORS)
        row["duplicate_tenor_rows"] = int(g.duplicated(subset=["target_days"]).sum()) if not g.empty and "target_days" in g.columns else 0

        if g.empty or "implied_variance" not in g.columns:
            row.update({
                "missing_result": True,
                "invalid_variance_rows": None,
                "min_implied_variance": None,
                "max_implied_variance": None,
                "min_total_variance_diff": None,
                "negative_forward_variance_flag": True,
                "qa_status": "RED",
            })
            rows.append(row)
            continue

        g = g.sort_values("target_days").copy()
        ivar = pd.to_numeric(g["implied_variance"], errors="coerce")
        target = pd.to_numeric(g["target_days"], errors="coerce")
        total_var = ivar * target / 365.0
        diffs = total_var.diff().dropna()

        invalid_var = int((ivar.isna() | (ivar <= 0)).sum())
        min_diff = float(diffs.min()) if len(diffs) else None
        neg_fwd_flag = bool(min_diff is not None and min_diff < -1e-8)

        row.update({
            "missing_result": False,
            "invalid_variance_rows": invalid_var,
            "min_implied_variance": float(ivar.min()) if len(ivar) else None,
            "max_implied_variance": float(ivar.max()) if len(ivar) else None,
            "min_total_variance_diff": min_diff,
            "negative_forward_variance_flag": neg_fwd_flag,
        })

        if row["row_count"] != 9 or not row["has_9_tenors"] or invalid_var > 0:
            row["qa_status"] = "RED"
        elif neg_fwd_flag:
            row["qa_status"] = "YELLOW"
        else:
            row["qa_status"] = "GREEN"
        rows.append(row)

    qa_df = pd.DataFrame(rows)
    summary = {
        "expected_dates": expected_dates,
        "expected_date_count": len(expected_dates),
        "results_rows": int(len(df)),
        "expected_rows": int(len(expected_dates) * len(TARGET_TENORS)),
        "qa_counts": qa_df["qa_status"].value_counts(dropna=False).to_dict() if not qa_df.empty else {},
        "all_green": bool((qa_df["qa_status"] == "GREEN").all()) if not qa_df.empty else False,
    }
    return qa_df, summary


def harmonize_for_candidate_append(new_df: pd.DataFrame, canonical_df: pd.DataFrame) -> pd.DataFrame:
    """Return new rows aligned to canonical repaired-history schema."""
    out = normalize_result_dates(new_df)
    if out.empty:
        return out

    source_method = out["methodology_version"].iloc[0] if "methodology_version" in out.columns else "v0.7_exchange_calendar_fred_sofr_market_close"

    if "raw_implied_variance" not in out.columns and "implied_variance" in out.columns:
        out["raw_implied_variance"] = out["implied_variance"]
    if "raw_vix_style_vol" not in out.columns and "vix_style_vol" in out.columns:
        out["raw_vix_style_vol"] = out["vix_style_vol"]
    if "is_repaired" not in out.columns:
        out["is_repaired"] = False
    if "repair_method" not in out.columns:
        out["repair_method"] = ""
    if "source_methodology_version" not in out.columns:
        out["source_methodology_version"] = source_method
    if "methodology_version" in canonical_df.columns:
        out["methodology_version"] = FINAL_METHOD_VERSION

    for col in canonical_df.columns:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[list(canonical_df.columns)]
    return out


def safe_write_parquet_csv(df: pd.DataFrame, base_path_no_ext: Path) -> Dict[str, str]:
    paths: Dict[str, str] = {}
    csv_path = base_path_no_ext.with_suffix(".csv")
    parquet_path = base_path_no_ext.with_suffix(".parquet")
    df.to_csv(csv_path, index=False)
    paths["csv"] = str(csv_path)
    try:
        df.to_parquet(parquet_path, index=False)
        paths["parquet"] = str(parquet_path)
    except Exception as exc:
        paths["parquet_error"] = repr(exc)
    return paths


def write_report(audit_dir: Path, payload: Dict[str, Any], qa_df: pd.DataFrame, errors_df: pd.DataFrame) -> Tuple[Path, Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = audit_dir / f"step02_eod_staging_{ts}.json"
    md_path = audit_dir / f"step02_eod_staging_{ts}.md"

    json_safe = json.loads(json.dumps(payload, default=str))
    json_path.write_text(json.dumps(json_safe, indent=2), encoding="utf-8")

    lines: List[str] = []
    lines.append("# VRP Production v1 - Step 02 EOD Staging Update\n")
    lines.append(f"Run timestamp: `{payload.get('run_timestamp')}`\n")
    lines.append(f"Project root: `{payload.get('project_root')}`\n")
    lines.append(f"Notebook path: `{payload.get('notebook_path')}`\n")
    lines.append(f"Requested date range: `{payload.get('start_date')}` to `{payload.get('end_date')}`\n")
    lines.append(f"Trading dates: `{payload.get('trading_dates')}`\n")
    lines.append(f"Results rows: `{payload.get('results_rows')}`\n")
    lines.append(f"Errors rows: `{payload.get('errors_rows')}`\n")
    lines.append("\n## Output files\n")
    for k, v in payload.get("output_paths", {}).items():
        lines.append(f"- **{k}**: `{v}`\n")
    lines.append("\n## QA summary\n")
    for k, v in payload.get("qa_summary", {}).items():
        lines.append(f"- **{k}**: `{v}`\n")
    lines.append("\n## QA by date\n")
    if not qa_df.empty:
        lines.append(qa_df.to_markdown(index=False))
        lines.append("\n")
    else:
        lines.append("No QA rows.\n")
    lines.append("\n## Errors\n")
    if not errors_df.empty:
        lines.append(errors_df.to_markdown(index=False))
        lines.append("\n")
    else:
        lines.append("No errors.\n")

    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True, help="VRP project root, e.g. C:\\Users\\patri\\vrp_project")
    parser.add_argument("--notebook-path", default=None, help="Path to 01_clean_vix_replication_v0_7_market_close_final.ipynb")
    parser.add_argument("--start-date", required=True, type=int, help="YYYYMMDD")
    parser.add_argument("--end-date", required=True, type=int, help="YYYYMMDD")
    parser.add_argument("--refresh-sofr", action="store_true", help="Refresh FRED SOFR via notebook helper before running")
    parser.add_argument("--write-candidate-history", action="store_true", help="Also write a candidate repaired-history append file under staging")
    args = parser.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    notebook_path = Path(args.notebook_path).expanduser().resolve() if args.notebook_path else project_root / "notebooks v0" / "01_clean_vix_replication_v0_7_market_close_final.ipynb"
    staging_dir, audit_dir = ensure_dirs(project_root)

    payload: Dict[str, Any] = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "notebook_path": str(notebook_path),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "refresh_sofr": bool(args.refresh_sofr),
        "write_candidate_history": bool(args.write_candidate_history),
    }

    try:
        if not notebook_path.exists():
            raise FileNotFoundError(f"Notebook not found: {notebook_path}")

        print("Loading notebook function definitions...")
        ns = load_notebook_namespace(project_root, notebook_path, refresh_sofr=args.refresh_sofr)

        trading_dates = get_trading_dates_from_notebook(ns, args.start_date, args.end_date)
        payload["trading_dates"] = trading_dates
        print(f"Trading dates to process: {trading_dates}")
        if not trading_dates:
            raise RuntimeError("No trading dates found for requested range.")

        print("Running VIX-style term-structure batch into memory only...")
        old_cwd = Path.cwd()
        os.chdir(project_root)
        try:
            results_df, errors_df = ns["run_vix_term_structure_batch_v7"](trade_dates=trading_dates)
        finally:
            os.chdir(old_cwd)

        results_df = normalize_result_dates(results_df) if isinstance(results_df, pd.DataFrame) else pd.DataFrame()
        errors_df = errors_df if isinstance(errors_df, pd.DataFrame) else pd.DataFrame()

        start_label = str(args.start_date)
        end_label = str(args.end_date)
        update_base = staging_dir / f"vix_term_structure_eod_update_{start_label}_{end_label}"
        output_paths = safe_write_parquet_csv(results_df, update_base)

        errors_path = staging_dir / f"vix_term_structure_eod_update_{start_label}_{end_label}_errors.csv"
        errors_df.to_csv(errors_path, index=False)
        output_paths["errors_csv"] = str(errors_path)

        qa_df, qa_summary = run_basic_curve_qa(results_df, trading_dates)
        qa_path = audit_dir / f"step02_eod_staging_qa_{start_label}_{end_label}.csv"
        qa_df.to_csv(qa_path, index=False)
        output_paths["qa_csv"] = str(qa_path)

        if args.write_candidate_history:
            canonical_path = project_root / "data" / "processed" / CANONICAL_REPAIRED_NAME
            if not canonical_path.exists():
                raise FileNotFoundError(f"Canonical repaired history not found: {canonical_path}")
            canonical_df = pd.read_parquet(canonical_path)
            new_for_append = harmonize_for_candidate_append(results_df, canonical_df)
            # Remove any overlapping date/tenor rows, then append.
            canonical_norm = canonical_df.copy()
            canonical_norm["trade_date"] = canonical_norm["trade_date"].map(parse_yyyymmdd).astype(int)
            new_keys = set(zip(new_for_append["trade_date"], new_for_append["target_days"]))
            keep_mask = [
                (int(td), int(tn)) not in new_keys
                for td, tn in zip(canonical_norm["trade_date"], canonical_norm["target_days"])
            ]
            candidate_df = pd.concat([canonical_norm.loc[keep_mask], new_for_append], ignore_index=True)
            candidate_df = candidate_df.sort_values(["trade_date", "target_days"]).reset_index(drop=True)
            candidate_base = staging_dir / f"vix_term_structure_history_v0_7_1_candidate_through_{end_label}"
            candidate_paths = safe_write_parquet_csv(candidate_df, candidate_base)
            output_paths["candidate_history_csv"] = candidate_paths.get("csv")
            output_paths["candidate_history_parquet"] = candidate_paths.get("parquet")
            if "parquet_error" in candidate_paths:
                output_paths["candidate_history_parquet_error"] = candidate_paths["parquet_error"]
            payload["candidate_history_rows"] = int(len(candidate_df))

        payload.update({
            "status": "SUCCESS",
            "results_rows": int(len(results_df)),
            "errors_rows": int(len(errors_df)),
            "output_paths": output_paths,
            "qa_summary": qa_summary,
        })
        json_path, md_path = write_report(audit_dir, payload, qa_df, errors_df)
        print("\nStep 02 staging update complete.")
        print(f"Report: {md_path}")
        print(f"JSON:   {json_path}")
        print(f"Rows:   {len(results_df)}")
        print(f"Errors: {len(errors_df)}")
        print(f"QA:     {qa_summary}")
        return 0

    except Exception as exc:
        payload.update({
            "status": "FAILED",
            "error": repr(exc),
            "traceback": traceback.format_exc(),
            "output_paths": {},
            "qa_summary": {},
            "results_rows": 0,
            "errors_rows": 0,
        })
        json_path, md_path = write_report(audit_dir, payload, pd.DataFrame(), pd.DataFrame())
        print("\nStep 02 staging update FAILED.")
        print(f"Error: {exc}")
        print(f"Report: {md_path}")
        print(f"JSON:   {json_path}")
        print(payload["traceback"])
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
