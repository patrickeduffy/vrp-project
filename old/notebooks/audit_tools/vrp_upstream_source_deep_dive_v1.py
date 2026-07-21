#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_upstream_source_deep_dive_v1.py

Read-only deep dive for the VRP production upstream source chain.

Purpose
-------
Inspect the most likely existing project notebooks/scripts/data panels that feed the final
VRP production signal, with special focus on:

    1. VIX-style implied variance construction
    2. ThetaData option-chain pulls
    3. ThetaData intraday / 5-minute price pulls
    4. Corsi/FDS feature construction
    5. Forecast model coefficient / refit logic
    6. forecast_variance_candidate construction
    7. RSI14 construction
    8. RV21D construction
    9. Final signal rule / sizing handoff

This script is intentionally read-only except for audit outputs.
It does NOT fetch data, rebuild panels, change production files, or modify notebooks.

Example
-------
py vrp_upstream_source_deep_dive_v1.py --project-root "C:\\Users\\patri\\vrp_project"
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# ======================================================================================
# Defaults
# ======================================================================================

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_AUDIT_REL = Path(r"data\audit\production_inventory")
DEFAULT_MAX_TEXT_SCAN_MB = 12.0
DEFAULT_INCLUDE_INVENTORY_TOP = 40
DEFAULT_EXCERPT_CONTEXT_LINES = 8
DEFAULT_MAX_EXCERPT_CHARS = 5000

# Hand-picked candidate files from production inventory results.
DEFAULT_TARGET_RELS = [
    # VIX-style implied variance / ThetaData option-chain candidates
    r"notebooks v0\12_forecast_vrp_signals_v0_1.ipynb",
    r"notebooks v0\15_thetadata_naked_atm_put_data_update_v0_1.ipynb",
    r"notebooks v0\old\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb",
    r"notebooks v0\old\01_clean_vix_replication_v0_6_friday_cycle_holiday_adjusted_clean.ipynb",
    r"notebooks v0\old\01_clean_vix_replication_v0_5_cache_batch_update_cleaned.ipynb",

    # Corsi/FDS forecast construction candidates
    r"notebooks\vrp_core_bucket_parameters_v1\vrp_unified_fds_no_min_return_locked_model_clean.ipynb",
    r"notebooks\vrp_core_bucket_parameters_v1\07_unified_fds_core_signal_threshold_research_v1_RV21D_repaired_v2_CLEANED.ipynb",
    r"notebooks\vrp_core_bucket_parameters_v1\old\06_front_middle_corsi_forecast_repair_v1_patched.ipynb",
    r"notebooks\vrp_core_bucket_parameters_v1\old\05_core_middle_front_parameter_research.ipynb",

    # Current production scripts
    r"notebooks\vrp_final_signal_panel_build_v1.py",
    r"notebooks\vrp_market_data_build_v1.py",
    r"notebooks\vrp_production_health_check_v1.py",

    # Important production/processed panels
    r"data\processed\vrp_front_middle_corsi_forecast_repair_v1\07A_unified_fds_no_min_return_oos_forecast_panel_20200102_20260701_20260704_203242.parquet",
    r"data\processed\vrp_unified_fds_core_signal_threshold_research_v1\01R_unified_fds_signal_base_panel_with_rv21d_20200102_20260701_20260705_011348.parquet",
    r"data\processed\vrp_final_signal\vrp_final_corsi_signal_base_panel_v1.parquet",
    r"data\processed\vrp_final_signal\vrp_final_corsi_latest_snapshot_v1.parquet",
    r"data\processed\vrp_final_signal\vrp_final_corsi_selected_trades_v1.parquet",
    r"data\processed\market_data\spy_eod_prices_v1.parquet",
    r"data\processed\market_data\spy_realized_vol_history_v1.parquet",
    r"data\processed\market_data\spy_corsi_har_input_panel_v1.parquet",
]

CATEGORIES: Dict[str, List[str]] = {
    "vix_style_implied_variance": [
        "vix_style", "vix-style", "vix style", "implied_variance", "implied variance",
        "variance swap", "zero_bid", "zero bid", "forward", "atm", "interpol",
        "near-term", "next-term", "vix calculation", "vix methodology", "sigma_sq",
    ],
    "thetadata_option_chain": [
        "thetadata", "option chain", "option_chain", "snapshot", "expiration", "expiry",
        "strike", "right=", "spxw", "spx", "v3/option", "option/history", "bulk_at_time",
        "quote", "bid", "ask", "mid", "root=", "exp=",
    ],
    "intraday_price_data": [
        "intraday", "5m", "5min", "5 min", "5-minute", "5 minute", "ohlc", "timestamp",
        "bar", "minute", "stock/history", "price through", "live price", "current price",
    ],
    "corsi_fds_features": [
        "unified_fds", "fds", "corsi", "har_total", "har_total_simple", "har",
        "downside", "shock", "overnight", "realized_variance", "realized variance",
        "forward_realized_variance", "rv_5", "rv_21", "rv_63", "rv5", "rv21", "rv63",
        "log_variance", "target_log_variance", "feature_cols", "feature_columns",
    ],
    "forecast_model_coefficients": [
        "ols", "sm.ols", "statsmodels", "linearregression", "ridge", "params",
        "coef_", "coefficient", "coefficients", "fit(", ".fit", "refit",
        "candidate_fit", "fit_status", "fitted_model", "model_spec", "model_source",
        "intercept", "alpha",
    ],
    "forecast_variance_candidate": [
        "forecast_variance_candidate", "predicted_log_variance_candidate",
        "candidate_forecast_vol_pct", "model_vrp_log", "model_vrp_z_3m",
        "model_vrp_z_1y", "unified_fds_no_min_return", "forecast_vol",
    ],
    "rsi14": [
        "rsi14", "rsi_14", "rsi", "avg_gain", "avg_loss", "relative strength",
        "delta.clip", "ewm", "rolling", "wilder",
    ],
    "rv21d": [
        "rv21d_vol_pct", "realized_vol_21d", "spy_vol_21d_pct", "spy_rv_21d",
        "rolling(21", "rolling(window=21", "window=21", "rv21", "21d vol",
    ],
    "final_signal_rules_sizing": [
        "core_pass", "secondary_pass", "locked_size_pct", "selected_size_pct",
        "selection_rule", "one_trade_per_day", "one trade per day", "sleeve_id",
        "selected_trade", "signal_layer", "tenor_bucket", "core", "secondary",
    ],
}

ENDPOINT_RE = re.compile(
    r"""(?ix)
    (https?://[^\s"'`<>),]+)
    |
    ((?:/)?v\d+/(?:option|stock|index|bulk|list|expirations|strikes)[A-Za-z0-9_/\-?=&.%]*)
    |
    ((?:option|stock|index)/history/[A-Za-z0-9_/\-?=&.%]*)
    """
)

FUNCTION_RE = re.compile(r"(?m)^\s*(def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|:)")
PATH_RE = re.compile(
    r"""(?ix)
    (?:
        [A-Za-z]:\\[^\n\r"']+
        |
        [\w .\\/-]+?\.(?:parquet|csv|json|pkl|pickle|joblib|xlsx|xls|ipynb|py)
    )
    """
)


# ======================================================================================
# Helpers
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    audit_dir: Path
    run_timestamp: str
    max_text_scan_mb: float
    include_inventory_top: int
    excerpt_context_lines: int
    max_excerpt_chars: int


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def rel_path(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def file_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".ipynb":
        return "notebook"
    if suffix == ".py":
        return "python"
    if suffix in {".txt", ".md", ".json", ".csv", ".yaml", ".yml"}:
        return "text"
    if suffix == ".parquet":
        return "parquet"
    return "other"


def safe_read_text(path: Path, max_mb: float) -> Tuple[Optional[str], Optional[str]]:
    try:
        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb > max_mb:
            return None, f"skipped_text_scan_size_mb={size_mb:.2f}"
        return path.read_text(encoding="utf-8", errors="ignore"), None
    except Exception as exc:
        return None, f"read_error={type(exc).__name__}: {exc}"


def normalize_source(source: Any) -> str:
    if source is None:
        return ""
    if isinstance(source, list):
        return "".join(str(x) for x in source)
    return str(source)


def notebook_chunks(path: Path, max_mb: float) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    text, err = safe_read_text(path, max_mb=max_mb)
    if err:
        return [], err
    try:
        nb = json.loads(text or "{}")
    except Exception as exc:
        return [], f"notebook_json_error={type(exc).__name__}: {exc}"

    chunks: List[Dict[str, Any]] = []
    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "")
        src = normalize_source(cell.get("source", ""))
        chunks.append({
            "chunk_id": f"cell_{i:04d}",
            "cell_index": i,
            "cell_type": cell_type,
            "text": src,
            "line_offset": None,
        })
    return chunks, None


def text_chunks(path: Path, max_mb: float, chunk_lines: int = 120, overlap: int = 20) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    text, err = safe_read_text(path, max_mb=max_mb)
    if err:
        return [], err
    lines = (text or "").splitlines()
    chunks: List[Dict[str, Any]] = []
    start = 0
    idx = 0
    while start < len(lines):
        end = min(len(lines), start + chunk_lines)
        chunks.append({
            "chunk_id": f"lines_{start + 1:05d}_{end:05d}",
            "cell_index": None,
            "cell_type": "text",
            "text": "\n".join(lines[start:end]),
            "line_offset": start + 1,
        })
        if end == len(lines):
            break
        start = max(0, end - overlap)
        idx += 1
    return chunks, None


def chunks_for_file(path: Path, cfg: Config) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    kind = file_kind(path)
    if kind == "notebook":
        return notebook_chunks(path, cfg.max_text_scan_mb)
    if kind in {"python", "text"}:
        return text_chunks(path, cfg.max_text_scan_mb)
    return [], None


def keyword_hits(text: str, keywords: Sequence[str]) -> List[str]:
    lower = text.lower()
    hits = []
    for kw in keywords:
        if kw.lower() in lower:
            hits.append(kw)
    return hits


def score_categories(text: str) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for cat, kws in CATEGORIES.items():
        hits = keyword_hits(text, kws)
        if not hits:
            continue
        # Score: number of matched unique keywords plus a small saturation bonus.
        score = min(50, len(hits) * 4 + min(20, text.lower().count(cat.lower())))
        result[cat] = {"score": score, "matched_keywords": hits}
    return result


def extract_context_excerpt(text: str, matched_keywords: Sequence[str], context_lines: int, max_chars: int) -> str:
    lines = text.splitlines()
    if not lines:
        return ""

    lower_lines = [ln.lower() for ln in lines]
    first_idx = None
    for i, ln in enumerate(lower_lines):
        if any(kw.lower() in ln for kw in matched_keywords):
            first_idx = i
            break

    if first_idx is None:
        excerpt = "\n".join(lines[: min(len(lines), 30)])
    else:
        start = max(0, first_idx - context_lines)
        end = min(len(lines), first_idx + context_lines + 1)
        excerpt = "\n".join(lines[start:end])

    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars] + "\n...[truncated]"
    return excerpt


def extract_endpoints(text: str) -> List[str]:
    vals = []
    for match in ENDPOINT_RE.finditer(text):
        vals.append(next(g for g in match.groups() if g))
    return sorted(set(vals))


def extract_functions(text: str) -> List[Tuple[str, str]]:
    return [(m.group(1), m.group(2)) for m in FUNCTION_RE.finditer(text)]


def extract_path_refs(text: str) -> List[str]:
    vals = []
    for match in PATH_RE.finditer(text):
        val = match.group(0).strip()
        if len(val) < 4:
            continue
        # avoid obvious false positives from prose.
        if any(ext in val.lower() for ext in [".parquet", ".csv", ".json", ".pkl", ".pickle", ".joblib", ".xlsx", ".xls", ".ipynb", ".py"]):
            vals.append(val)
    return sorted(set(vals))


def find_latest_inventory_top_candidates(audit_dir: Path) -> Optional[Path]:
    files = sorted(audit_dir.glob("vrp_production_source_inventory_top_candidates_*.csv"))
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def load_inventory_targets(cfg: Config) -> List[str]:
    if cfg.include_inventory_top <= 0:
        return []

    latest = find_latest_inventory_top_candidates(cfg.audit_dir)
    if latest is None:
        return []

    try:
        df = pd.read_csv(latest)
    except Exception:
        return []

    if "rel_path" not in df.columns:
        return []

    # Preserve order while de-duplicating. Category output already contains top candidates.
    rels: List[str] = []
    for val in df["rel_path"].dropna().astype(str).tolist():
        if val not in rels:
            rels.append(val)
        if len(rels) >= cfg.include_inventory_top:
            break
    return rels


def build_target_list(cfg: Config) -> List[Path]:
    rels = list(DEFAULT_TARGET_RELS)
    rels.extend(load_inventory_targets(cfg))

    paths: List[Path] = []
    seen = set()
    for rel in rels:
        path = cfg.project_root / Path(rel)
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        paths.append(path)
    return paths


def summarize_parquet(path: Path, root: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str]]:
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        return {}, [], f"parquet_read_error={type(exc).__name__}: {exc}"

    date_cols = [c for c in df.columns if str(c).lower() in {"trade_date", "date", "timestamp", "datetime", "quote_datetime"}]
    tenor_cols = [c for c in df.columns if str(c).lower() in {"tenor", "dte", "tenor_days", "days_to_expiration"}]

    date_col = date_cols[0] if date_cols else None
    date_min = None
    date_max = None
    if date_col is not None:
        try:
            dates = pd.to_datetime(df[date_col], errors="coerce")
            if dates.notna().any():
                date_min = str(dates.min().date())
                date_max = str(dates.max().date())
        except Exception:
            pass

    tenor_col = tenor_cols[0] if tenor_cols else None
    tenor_values = None
    if tenor_col is not None:
        try:
            vals = sorted(pd.Series(df[tenor_col]).dropna().unique().tolist())
            tenor_values = vals[:50]
        except Exception:
            tenor_values = None

    unique_summary: Dict[str, Any] = {}
    for col in ["model_spec", "model_source", "fit_status", "fit_status_candidate", "signal_layer", "tenor_bucket", "sleeve_id"]:
        if col in df.columns:
            try:
                vals = sorted(pd.Series(df[col]).dropna().astype(str).unique().tolist())
                unique_summary[col] = vals[:25]
            except Exception:
                pass

    schema = {
        "rel_path": rel_path(path, root),
        "num_rows": int(len(df)),
        "num_columns": int(len(df.columns)),
        "date_column": date_col,
        "date_min": date_min,
        "date_max": date_max,
        "tenor_column": tenor_col,
        "tenor_values": json.dumps(tenor_values, default=str),
        "unique_values_summary": json.dumps(unique_summary, default=str),
        "columns_json": json.dumps(list(map(str, df.columns))),
    }

    col_rows: List[Dict[str, Any]] = []
    col_categories = {
        "implied_variance": ["implied", "vix_style", "vix"],
        "forecast": ["forecast", "predicted", "candidate"],
        "vrp_z": ["vrp", "z_3m", "z_1y", "zscore", "z_score"],
        "corsi_fds_feature": ["rv_", "realized", "downside", "shock", "overnight", "har", "fds", "corsi", "variance"],
        "signal_rule": ["core", "secondary", "rsi", "rv21d", "selected", "locked_size", "signal_layer", "sleeve"],
        "price": ["spy", "spx", "close", "open", "high", "low", "volume", "return"],
    }
    for col in map(str, df.columns):
        lower = col.lower()
        matched = [cat for cat, kws in col_categories.items() if any(kw in lower for kw in kws)]
        if matched:
            col_rows.append({
                "rel_path": rel_path(path, root),
                "column": col,
                "column_categories": ",".join(matched),
                "dtype": str(df[col].dtype),
                "non_null": int(df[col].notna().sum()),
                "null_count": int(df[col].isna().sum()),
                "sample_values": json.dumps(pd.Series(df[col]).dropna().head(5).astype(str).tolist()),
            })

    return schema, col_rows, None


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # Create an empty marker file with no columns.
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    # Put common fields first.
    preferred = [
        "category", "category_score", "total_score", "rel_path", "kind",
        "chunk_id", "cell_index", "cell_type", "line_start", "line_end",
        "matched_keywords", "excerpt", "endpoint", "object_type", "object_name",
        "path_ref", "exists", "size_kb", "modified",
    ]
    ordered = [c for c in preferred if c in fieldnames] + [c for c in fieldnames if c not in preferred]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def status_row(name: str, status: str, detail: str, severity: str = "info") -> Dict[str, Any]:
    return {
        "check": name,
        "status": status,
        "severity": severity,
        "detail": detail,
    }


# ======================================================================================
# Main
# ======================================================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VRP upstream source deep dive v1")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--audit-dir", default=None)
    parser.add_argument("--max-text-scan-mb", type=float, default=DEFAULT_MAX_TEXT_SCAN_MB)
    parser.add_argument("--include-inventory-top", type=int, default=DEFAULT_INCLUDE_INVENTORY_TOP)
    parser.add_argument("--excerpt-context-lines", type=int, default=DEFAULT_EXCERPT_CONTEXT_LINES)
    parser.add_argument("--max-excerpt-chars", type=int, default=DEFAULT_MAX_EXCERPT_CHARS)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    project_root = Path(args.project_root)
    audit_dir = Path(args.audit_dir) if args.audit_dir else project_root / DEFAULT_AUDIT_REL
    audit_dir.mkdir(parents=True, exist_ok=True)

    cfg = Config(
        project_root=project_root,
        audit_dir=audit_dir,
        run_timestamp=now_stamp(),
        max_text_scan_mb=float(args.max_text_scan_mb),
        include_inventory_top=int(args.include_inventory_top),
        excerpt_context_lines=int(args.excerpt_context_lines),
        max_excerpt_chars=int(args.max_excerpt_chars),
    )

    print_header("VRP upstream source deep dive v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Audit dir:                 {cfg.audit_dir}")
    print(f"Max text scan MB:          {cfg.max_text_scan_mb}")
    print(f"Include inventory top:     {cfg.include_inventory_top}")
    print(f"Excerpt context lines:     {cfg.excerpt_context_lines}")

    targets = build_target_list(cfg)

    target_rows: List[Dict[str, Any]] = []
    excerpt_rows: List[Dict[str, Any]] = []
    endpoint_rows: List[Dict[str, Any]] = []
    function_rows: List[Dict[str, Any]] = []
    path_ref_rows: List[Dict[str, Any]] = []
    parquet_rows: List[Dict[str, Any]] = []
    parquet_col_rows: List[Dict[str, Any]] = []
    status_rows: List[Dict[str, Any]] = []

    print_header("Target files")
    found_count = 0
    missing_count = 0
    for path in targets:
        exists = path.exists()
        if exists:
            found_count += 1
            stat = path.stat()
            modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            size_kb = round(stat.st_size / 1024, 2)
        else:
            missing_count += 1
            modified = None
            size_kb = None

        row = {
            "rel_path": rel_path(path, cfg.project_root),
            "abs_path": str(path),
            "exists": bool(exists),
            "kind": file_kind(path),
            "size_kb": size_kb,
            "modified": modified,
        }
        target_rows.append(row)
        print(f"exists={exists:<5} kind={row['kind']:<8} rel_path={row['rel_path']}")

    status_rows.append(status_row("target_files_found", "PASS" if found_count > 0 else "FAIL", f"found={found_count}; missing={missing_count}", "hard"))

    print_header("Deep-diving text/notebook files")
    files_scanned = 0
    text_scan_errors = 0
    category_counts: Dict[str, int] = {cat: 0 for cat in CATEGORIES}
    endpoint_count = 0
    function_count = 0
    path_ref_count = 0

    for path in targets:
        if not path.exists():
            continue

        kind = file_kind(path)
        if kind == "parquet":
            schema, col_rows, err = summarize_parquet(path, cfg.project_root)
            if err:
                status_rows.append(status_row("parquet_inspection", "WARN", f"{rel_path(path, cfg.project_root)}: {err}", "info"))
            else:
                parquet_rows.append(schema)
                parquet_col_rows.extend(col_rows)
            continue

        if kind not in {"notebook", "python", "text"}:
            continue

        chunks, err = chunks_for_file(path, cfg)
        if err:
            text_scan_errors += 1
            status_rows.append(status_row("text_scan", "WARN", f"{rel_path(path, cfg.project_root)}: {err}", "info"))
            continue

        files_scanned += 1

        # Whole-file object extraction from concatenated chunks.
        whole_text = "\n".join(chunk["text"] for chunk in chunks)

        for endpoint in extract_endpoints(whole_text):
            endpoint_count += 1
            endpoint_rows.append({
                "rel_path": rel_path(path, cfg.project_root),
                "kind": kind,
                "endpoint": endpoint,
            })

        for object_type, object_name in extract_functions(whole_text):
            function_count += 1
            function_rows.append({
                "rel_path": rel_path(path, cfg.project_root),
                "kind": kind,
                "object_type": object_type,
                "object_name": object_name,
            })

        for path_ref in extract_path_refs(whole_text):
            path_ref_count += 1
            path_ref_rows.append({
                "rel_path": rel_path(path, cfg.project_root),
                "kind": kind,
                "path_ref": path_ref[:1000],
            })

        for chunk in chunks:
            scores = score_categories(chunk["text"])
            if not scores:
                continue

            total_score = sum(v["score"] for v in scores.values())

            for category, info in scores.items():
                category_counts[category] += 1
                matched_keywords = info["matched_keywords"]
                excerpt = extract_context_excerpt(
                    chunk["text"],
                    matched_keywords,
                    cfg.excerpt_context_lines,
                    cfg.max_excerpt_chars,
                )
                line_start = chunk["line_offset"]
                line_end = None
                if line_start is not None:
                    line_end = line_start + max(0, len(chunk["text"].splitlines()) - 1)

                excerpt_rows.append({
                    "category": category,
                    "category_score": info["score"],
                    "total_score": total_score,
                    "rel_path": rel_path(path, cfg.project_root),
                    "kind": kind,
                    "chunk_id": chunk["chunk_id"],
                    "cell_index": chunk["cell_index"],
                    "cell_type": chunk["cell_type"],
                    "line_start": line_start,
                    "line_end": line_end,
                    "matched_keywords": ", ".join(matched_keywords),
                    "excerpt": excerpt,
                })

    print(f"Files scanned:             {files_scanned}")
    print(f"Text scan warnings:        {text_scan_errors}")
    print(f"Category excerpt rows:     {len(excerpt_rows)}")
    print(f"Endpoints extracted:       {endpoint_count}")
    print(f"Functions/classes found:   {function_count}")
    print(f"Path refs extracted:       {path_ref_count}")
    print(f"Parquet files inspected:   {len(parquet_rows)}")
    print(f"Parquet signal columns:    {len(parquet_col_rows)}")

    print_header("Category hit counts")
    cat_summary_rows: List[Dict[str, Any]] = []
    for cat, count in sorted(category_counts.items(), key=lambda kv: kv[1], reverse=True):
        max_score = max([r["category_score"] for r in excerpt_rows if r["category"] == cat], default=0)
        files = sorted(set(r["rel_path"] for r in excerpt_rows if r["category"] == cat))
        cat_summary_rows.append({
            "category": cat,
            "excerpt_count": count,
            "candidate_file_count": len(files),
            "max_score": max_score,
            "candidate_files": json.dumps(files[:20]),
        })
        print(f"{cat:<34} excerpts={count:<5} files={len(files):<4} max_score={max_score}")

    print_header("Top excerpts by category")
    top_excerpt_rows: List[Dict[str, Any]] = []
    for cat in CATEGORIES:
        rows = [r for r in excerpt_rows if r["category"] == cat]
        rows = sorted(rows, key=lambda r: (r["category_score"], r["total_score"]), reverse=True)[:12]
        top_excerpt_rows.extend(rows)
        if not rows:
            continue
        print("\n" + "-" * 100)
        print(cat)
        print("-" * 100)
        for r in rows[:5]:
            print(f"score={r['category_score']:<3} kind={r['kind']:<8} rel_path={r['rel_path']} chunk={r['chunk_id']}")
            kws = r["matched_keywords"]
            print(f"  matched: {kws[:250]}")

    print_header("Parquet panels inspected")
    if parquet_rows:
        pq_print = pd.DataFrame(parquet_rows)[[
            "rel_path", "num_rows", "num_columns", "date_column", "date_min", "date_max", "tenor_column", "tenor_values"
        ]]
        print(pq_print.to_string(index=False, max_colwidth=90))
    else:
        print("No parquet panels inspected.")

    # Simple recommendation rows based on top files and parquet schemas.
    recommendation_rows: List[Dict[str, Any]] = []
    recommendation_rows.append({
        "production_need": "EOD/intraday VIX-style implied variance builder",
        "primary_candidates": json.dumps([
            r"notebooks v0\12_forecast_vrp_signals_v0_1.ipynb",
            r"notebooks v0\old\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb",
            r"notebooks v0\15_thetadata_naked_atm_put_data_update_v0_1.ipynb",
        ]),
        "what_to_extract_next": "ThetaData option-chain endpoint, strike filters, zero-bid rules, forward/ATM selection, variance integration, tenor interpolation.",
    })
    recommendation_rows.append({
        "production_need": "EOD/intraday Corsi/FDS forecast builder",
        "primary_candidates": json.dumps([
            r"notebooks\vrp_core_bucket_parameters_v1\vrp_unified_fds_no_min_return_locked_model_clean.ipynb",
            r"notebooks\vrp_core_bucket_parameters_v1\07_unified_fds_core_signal_threshold_research_v1_RV21D_repaired_v2_CLEANED.ipynb",
            r"notebooks\vrp_core_bucket_parameters_v1\old\06_front_middle_corsi_forecast_repair_v1_patched.ipynb",
        ]),
        "what_to_extract_next": "Feature list, target construction, rolling/refit window, model coefficients/refit mechanics, prediction transform exp(log variance).",
    })
    recommendation_rows.append({
        "production_need": "Intraday signal input calculation",
        "primary_candidates": json.dumps([
            r"notebooks\vrp_market_data_build_v1.py",
            r"notebooks v0\15_thetadata_naked_atm_put_data_update_v0_1.ipynb",
            r"notebooks\vrp_core_bucket_parameters_v1\vrp_unified_fds_no_min_return_locked_model_clean.ipynb",
        ]),
        "what_to_extract_next": "5-minute price endpoint and current-as-of-T RV/RSI/Corsi feature construction.",
    })

    # Save outputs.
    paths = {
        "targets": cfg.audit_dir / f"vrp_upstream_source_deep_dive_targets_{cfg.run_timestamp}.csv",
        "category_summary": cfg.audit_dir / f"vrp_upstream_source_deep_dive_category_summary_{cfg.run_timestamp}.csv",
        "top_excerpts": cfg.audit_dir / f"vrp_upstream_source_deep_dive_top_excerpts_{cfg.run_timestamp}.csv",
        "all_excerpts": cfg.audit_dir / f"vrp_upstream_source_deep_dive_all_excerpts_{cfg.run_timestamp}.csv",
        "endpoints": cfg.audit_dir / f"vrp_upstream_source_deep_dive_endpoints_{cfg.run_timestamp}.csv",
        "functions": cfg.audit_dir / f"vrp_upstream_source_deep_dive_functions_{cfg.run_timestamp}.csv",
        "path_refs": cfg.audit_dir / f"vrp_upstream_source_deep_dive_path_refs_{cfg.run_timestamp}.csv",
        "parquet_schema": cfg.audit_dir / f"vrp_upstream_source_deep_dive_parquet_schema_{cfg.run_timestamp}.csv",
        "parquet_columns": cfg.audit_dir / f"vrp_upstream_source_deep_dive_parquet_columns_{cfg.run_timestamp}.csv",
        "recommendations": cfg.audit_dir / f"vrp_upstream_source_deep_dive_recommendations_{cfg.run_timestamp}.csv",
        "status": cfg.audit_dir / f"vrp_upstream_source_deep_dive_status_{cfg.run_timestamp}.csv",
        "manifest": cfg.audit_dir / f"vrp_upstream_source_deep_dive_manifest_{cfg.run_timestamp}.json",
    }

    write_csv(paths["targets"], target_rows)
    write_csv(paths["category_summary"], cat_summary_rows)
    write_csv(paths["top_excerpts"], top_excerpt_rows)
    write_csv(paths["all_excerpts"], excerpt_rows)
    write_csv(paths["endpoints"], endpoint_rows)
    write_csv(paths["functions"], function_rows)
    write_csv(paths["path_refs"], path_ref_rows)
    write_csv(paths["parquet_schema"], parquet_rows)
    write_csv(paths["parquet_columns"], parquet_col_rows)
    write_csv(paths["recommendations"], recommendation_rows)
    write_csv(paths["status"], status_rows)

    manifest = {
        "script": "vrp_upstream_source_deep_dive_v1.py",
        "run_timestamp": cfg.run_timestamp,
        "project_root": str(cfg.project_root),
        "audit_dir": str(cfg.audit_dir),
        "max_text_scan_mb": cfg.max_text_scan_mb,
        "include_inventory_top": cfg.include_inventory_top,
        "target_count": len(targets),
        "target_found_count": found_count,
        "target_missing_count": missing_count,
        "files_scanned": files_scanned,
        "text_scan_errors": text_scan_errors,
        "excerpt_rows": len(excerpt_rows),
        "endpoint_rows": len(endpoint_rows),
        "function_rows": len(function_rows),
        "path_ref_rows": len(path_ref_rows),
        "parquet_rows": len(parquet_rows),
        "parquet_column_rows": len(parquet_col_rows),
        "outputs": {k: str(v) for k, v in paths.items()},
        "notes": [
            "Read-only inspection except audit outputs.",
            "This does not rebuild implied variance, forecast variance, final signal, or dashboard files.",
            "Use top_excerpts/endpoints/functions/parquet_columns to choose canonical production source code.",
        ],
    }
    paths["manifest"].write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print_header("Saved deep-dive outputs")
    for label, path in paths.items():
        print(f"{label:<20} {path}")

    print("\nDONE — upstream source deep dive complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
