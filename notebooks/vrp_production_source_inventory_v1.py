#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_production_source_inventory_v1.py

Read-only production source inventory for the VRP project.

Purpose
-------
Find and summarize existing project files that are likely to contain the upstream
production logic needed for:
    1. VIX-style implied variance by tenor
    2. Corsi/FDS feature construction
    3. Corsi/FDS model coefficients / fitted model artifacts
    4. forecast_variance_candidate
    5. RSI14
    6. RV21D
    7. ThetaData option-chain pulls
    8. ThetaData intraday / 5-minute price pulls

This script is intentionally read-only with respect to production data. It writes only
audit/inventory outputs under data/audit/production_inventory.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional fallback
    pq = None


# ======================================================================================
# Defaults / keyword configuration
# ======================================================================================

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_AUDIT_REL = Path(r"data\audit\production_inventory")

TEXT_EXTENSIONS = {
    ".py",
    ".ipynb",
    ".txt",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ps1",
    ".bat",
}

TABLE_EXTENSIONS = {".csv"}
PARQUET_EXTENSIONS = {".parquet"}
PICKLE_EXTENSIONS = {".pkl", ".pickle", ".joblib"}

EXCLUDED_DIR_NAMES_DEFAULT = {
    ".git",
    ".hg",
    ".svn",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "node_modules",
    ".ipynb_checkpoints",
}

CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    "vix_style_implied_variance": [
        "vix_style",
        "vix-style",
        "vix style",
        "implied_variance",
        "implied variance",
        "vix_style_vol",
        "variance strip",
        "strip variance",
        "otm option",
        "delta_k",
        "forward price",
        "risk-free",
        "risk_free",
        "zero_bid",
        "quote_mid",
        "mid_price",
        "spxw",
    ],
    "thetadata_option_chain": [
        "option/history",
        "option/chain",
        "option/quote",
        "option/eod",
        "v3/option",
        "thetadata",
        "root=spx",
        "root=spxw",
        "exp=",
        "strike=",
        "right=",
        "expiration",
        "option chain",
        "option_chain",
        "bulk_snapshot",
        "snapshot",
    ],
    "intraday_price_data": [
        "intraday",
        "5-minute",
        "5 minute",
        "5min",
        "5m",
        "minute",
        "ms_of_day",
        "interval",
        "ohlc",
        "history/quote",
        "history/trade",
        "history/ohlc",
        "stock/history",
        "quote_time",
        "timestamp",
    ],
    "corsi_fds_features": [
        "corsi",
        "har",
        "fds",
        "unified_fds",
        "har_total",
        "har_total_simple",
        "realized_variance",
        "realized variance",
        "rv_5",
        "rv5",
        "rv_21",
        "rv21",
        "rv_63",
        "rv63",
        "overnight",
        "downside",
        "shock",
        "jump",
        "bipower",
        "forward_realized_variance",
        "target_variance",
    ],
    "forecast_model_coefficients": [
        "coefficient",
        "coefficients",
        "coef_",
        "intercept",
        "params",
        "model_params",
        "fitted_model",
        "joblib",
        "pickle",
        "statsmodels",
        "ols",
        "linearregression",
        "ridge",
        "lasso",
        "fit_status",
        "candidate_fit",
        "refit",
        "train_window",
        "model_spec",
    ],
    "forecast_variance_candidate": [
        "forecast_variance_candidate",
        "predicted_log_variance_candidate",
        "candidate_forecast_vol_pct",
        "unified_fds_no_min_return",
        "forecast_vol_final",
        "forecast_variance_final",
        "model_vrp_log",
        "model_vrp_z_3m",
        "model_vrp_z_1y",
    ],
    "rsi14": [
        "rsi14",
        "rsi_14",
        "rsi",
        "relative strength index",
        "avg_gain",
        "avg_loss",
    ],
    "rv21d": [
        "rv21d_vol_pct",
        "spy_vol_21d_pct",
        "spy_rv_21d",
        "realized_vol_21d",
        "rolling(21",
        "window=21",
        "21d vol",
        "rv21",
    ],
    "final_signal_rules_sizing": [
        "core_pass",
        "secondary_pass",
        "selected_trade",
        "selected_size_pct",
        "locked_size_pct",
        "signal_layer",
        "sleeve_id",
        "tenor_bucket",
        "one_trade_per_day",
        "selection_rule",
        "final_signal",
    ],
}

DATE_COLUMN_CANDIDATES = [
    "trade_date",
    "date",
    "as_of_date",
    "quote_date",
    "calculation_date",
    "timestamp",
    "datetime",
]

TENOR_COLUMN_CANDIDATES = [
    "tenor",
    "dte",
    "days_to_expiration",
    "expiration_days",
]

UNIQUE_VALUE_COLUMNS = [
    "model_spec",
    "model_source",
    "fit_status_candidate",
    "fit_status",
    "signal_layer",
    "tenor_bucket",
]


# ======================================================================================
# Data classes
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    audit_dir: Path
    max_text_mb: float
    top_n: int
    include_checkpoints: bool
    skip_parquet_date_stats: bool
    run_timestamp: str


# ======================================================================================
# General helpers
# ======================================================================================

def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def file_modified_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds")
    except Exception:
        return ""


def read_text_safely(path: Path, max_bytes: int) -> Tuple[Optional[str], Optional[str], bool]:
    """Return text, error, skipped_due_to_size."""
    try:
        size = path.stat().st_size
        if size > max_bytes:
            return None, None, True
        return path.read_text(encoding="utf-8", errors="ignore"), None, False
    except Exception as exc:
        return None, repr(exc), False


def normalize_for_search(value: Any) -> str:
    return str(value).lower().replace("\\", "/")


def keyword_hits(text: str, keywords: Sequence[str]) -> List[str]:
    lower = normalize_for_search(text)
    hits = []
    for kw in keywords:
        if kw.lower() in lower:
            hits.append(kw)
    return hits


def count_keyword_occurrences(text: str, keywords: Sequence[str]) -> int:
    lower = normalize_for_search(text)
    count = 0
    for kw in keywords:
        count += lower.count(kw.lower())
    return count


def extract_snippets(text: str, keywords: Sequence[str], max_snippets: int = 5) -> str:
    if not text:
        return ""
    lower_keywords = [kw.lower() for kw in keywords]
    snippets: List[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lower = line.lower()
        if any(kw in lower for kw in lower_keywords):
            if len(line) > 220:
                line = line[:217] + "..."
            snippets.append(line)
            if len(snippets) >= max_snippets:
                break
    return " | ".join(snippets)


def classify_file_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in TEXT_EXTENSIONS:
        return "text_or_code"
    if suffix in TABLE_EXTENSIONS:
        return "csv"
    if suffix in PARQUET_EXTENSIONS:
        return "parquet"
    if suffix in PICKLE_EXTENSIONS:
        return "model_pickle"
    return "other"


def list_files(cfg: Config) -> List[Path]:
    excluded = set(EXCLUDED_DIR_NAMES_DEFAULT)
    if cfg.include_checkpoints:
        excluded.discard(".ipynb_checkpoints")

    files: List[Path] = []
    for root, dirs, filenames in os.walk(cfg.project_root):
        dirs[:] = [d for d in dirs if d not in excluded]
        for name in filenames:
            files.append(Path(root) / name)
    return files


# ======================================================================================
# Table / parquet metadata helpers
# ======================================================================================

def get_csv_columns(path: Path) -> Tuple[List[str], Optional[str]]:
    try:
        df = pd.read_csv(path, nrows=0)
        return list(df.columns), None
    except Exception as exc:
        return [], repr(exc)


def parquet_schema_metadata(path: Path, cfg: Config) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "num_rows": None,
        "num_columns": None,
        "columns": [],
        "schema_error": None,
        "date_column": None,
        "date_min": None,
        "date_max": None,
        "tenor_column": None,
        "tenor_values": None,
        "unique_values_summary": None,
    }

    try:
        if pq is not None:
            pf = pq.ParquetFile(path)
            out["num_rows"] = pf.metadata.num_rows if pf.metadata is not None else None
            schema_names = list(pf.schema.names)
            out["columns"] = schema_names
            out["num_columns"] = len(schema_names)
        else:
            df0 = pd.read_parquet(path)
            schema_names = list(df0.columns)
            out["num_rows"] = len(df0)
            out["columns"] = schema_names
            out["num_columns"] = len(schema_names)
    except Exception as exc:
        out["schema_error"] = repr(exc)
        return out

    if cfg.skip_parquet_date_stats:
        return out

    columns_lower = {c.lower(): c for c in out["columns"]}

    date_col = None
    for cand in DATE_COLUMN_CANDIDATES:
        if cand.lower() in columns_lower:
            date_col = columns_lower[cand.lower()]
            break

    if date_col is not None:
        out["date_column"] = date_col
        try:
            s = pd.read_parquet(path, columns=[date_col])[date_col]
            parsed = pd.to_datetime(s, errors="coerce")
            if parsed.notna().any():
                out["date_min"] = parsed.min().date().isoformat()
                out["date_max"] = parsed.max().date().isoformat()
        except Exception as exc:
            out["date_min"] = f"ERROR: {repr(exc)}"
            out["date_max"] = f"ERROR: {repr(exc)}"

    tenor_col = None
    for cand in TENOR_COLUMN_CANDIDATES:
        if cand.lower() in columns_lower:
            tenor_col = columns_lower[cand.lower()]
            break

    if tenor_col is not None:
        out["tenor_column"] = tenor_col
        try:
            s = pd.read_parquet(path, columns=[tenor_col])[tenor_col]
            vals = sorted(pd.Series(s.dropna().unique()).tolist())
            out["tenor_values"] = json.dumps(vals[:50], default=str)
        except Exception as exc:
            out["tenor_values"] = f"ERROR: {repr(exc)}"

    unique_summaries: Dict[str, Any] = {}
    for col in UNIQUE_VALUE_COLUMNS:
        if col.lower() not in columns_lower:
            continue
        actual_col = columns_lower[col.lower()]
        try:
            s = pd.read_parquet(path, columns=[actual_col])[actual_col]
            vals = sorted(pd.Series(s.dropna().unique()).astype(str).tolist())
            unique_summaries[actual_col] = vals[:30]
        except Exception as exc:
            unique_summaries[actual_col] = f"ERROR: {repr(exc)}"
    if unique_summaries:
        out["unique_values_summary"] = json.dumps(unique_summaries, default=str)

    return out


# ======================================================================================
# Inventory logic
# ======================================================================================

def score_categories(
    rel_path: str,
    name: str,
    columns: Optional[Sequence[str]] = None,
    text: Optional[str] = None,
    suffix: str = "",
) -> Tuple[Dict[str, int], Dict[str, List[str]], Dict[str, str]]:
    """Return category scores, keyword hits, and snippets."""
    path_text = f"{rel_path} {name} {suffix}"
    columns_text = " ".join(columns or [])
    score_by_cat: Dict[str, int] = {}
    hits_by_cat: Dict[str, List[str]] = {}
    snippets_by_cat: Dict[str, str] = {}

    for cat, kws in CATEGORY_KEYWORDS.items():
        path_hits = keyword_hits(path_text, kws)
        col_hits = keyword_hits(columns_text, kws) if columns else []
        text_hits = keyword_hits(text or "", kws) if text else []

        # File path/name hits matter, but column and code-body hits matter more.
        score = len(set(path_hits)) * 2 + len(set(col_hits)) * 5
        if text:
            score += min(count_keyword_occurrences(text, kws), 50)

        all_hits = sorted(set(path_hits + col_hits + text_hits), key=lambda x: x.lower())
        if score > 0 or all_hits:
            score_by_cat[cat] = int(score)
            hits_by_cat[cat] = all_hits
            if text:
                snippets_by_cat[cat] = extract_snippets(text, kws, max_snippets=4)

    return score_by_cat, hits_by_cat, snippets_by_cat


def build_inventory(cfg: Config) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    files = list_files(cfg)
    max_text_bytes = int(cfg.max_text_mb * 1024 * 1024)

    inventory_rows: List[Dict[str, Any]] = []
    parquet_rows: List[Dict[str, Any]] = []

    for path in files:
        try:
            stat = path.stat()
        except Exception:
            continue

        rel_path = safe_rel(path, cfg.project_root)
        suffix = path.suffix.lower()
        kind = classify_file_kind(path)

        columns: List[str] = []
        text: Optional[str] = None
        read_error: Optional[str] = None
        skipped_text_due_to_size = False
        table_error: Optional[str] = None

        if kind == "text_or_code":
            text, read_error, skipped_text_due_to_size = read_text_safely(path, max_text_bytes)
            if suffix == ".ipynb" and text:
                # Notebook JSON is still useful as text; normalize escaped newlines slightly.
                text = text.replace("\\n", "\n")
        elif kind == "csv":
            columns, table_error = get_csv_columns(path)
        elif kind == "parquet":
            meta = parquet_schema_metadata(path, cfg)
            columns = list(meta.get("columns") or [])
            parquet_rows.append(
                {
                    "path": str(path),
                    "rel_path": rel_path,
                    "name": path.name,
                    "modified": file_modified_iso(path),
                    "size_kb": round(stat.st_size / 1024, 2),
                    **{k: v for k, v in meta.items() if k != "columns"},
                    "columns_pipe": "|".join(columns),
                    "columns_json": json.dumps(columns),
                }
            )
        elif kind == "model_pickle":
            # Do not unpickle. Only use name/path metadata for safety.
            pass

        score_by_cat, hits_by_cat, snippets_by_cat = score_categories(
            rel_path=rel_path,
            name=path.name,
            columns=columns,
            text=text,
            suffix=suffix,
        )

        matched_categories = sorted(score_by_cat.keys())
        total_score = int(sum(score_by_cat.values()))
        category_scores = json.dumps(score_by_cat, sort_keys=True)
        matched_keywords = json.dumps(hits_by_cat, sort_keys=True)
        snippets = json.dumps(snippets_by_cat, sort_keys=True)

        inventory_rows.append(
            {
                "path": str(path),
                "rel_path": rel_path,
                "name": path.name,
                "suffix": suffix,
                "kind": kind,
                "parent": str(path.parent),
                "modified": file_modified_iso(path),
                "size_kb": round(stat.st_size / 1024, 2),
                "text_scanned": text is not None,
                "text_skipped_due_to_size": skipped_text_due_to_size,
                "read_error": read_error,
                "table_error": table_error,
                "num_columns_detected": len(columns),
                "columns_preview": ", ".join(columns[:40]),
                "matched_categories": ";".join(matched_categories),
                "total_score": total_score,
                "category_scores_json": category_scores,
                "matched_keywords_json": matched_keywords,
                "sample_matches_json": snippets,
            }
        )

    inventory = pd.DataFrame(inventory_rows)
    if inventory.empty:
        return inventory, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(parquet_rows)

    # Expand category scores into columns for easier sorting/filtering.
    for cat in CATEGORY_KEYWORDS:
        inventory[f"score__{cat}"] = inventory["category_scores_json"].apply(
            lambda x, c=cat: int(json.loads(x).get(c, 0)) if isinstance(x, str) and x else 0
        )

    category_summary_rows = []
    top_rows = []
    for cat in CATEGORY_KEYWORDS:
        score_col = f"score__{cat}"
        hits = inventory[inventory[score_col] > 0].copy()
        category_summary_rows.append(
            {
                "category": cat,
                "candidate_file_count": int(len(hits)),
                "max_score": int(hits[score_col].max()) if len(hits) else 0,
                "total_score": int(hits[score_col].sum()) if len(hits) else 0,
            }
        )
        if len(hits):
            top = hits.sort_values([score_col, "total_score", "modified"], ascending=[False, False, False]).head(cfg.top_n)
            for _, row in top.iterrows():
                snippets_dict = {}
                hits_dict = {}
                try:
                    snippets_dict = json.loads(row.get("sample_matches_json") or "{}")
                except Exception:
                    snippets_dict = {}
                try:
                    hits_dict = json.loads(row.get("matched_keywords_json") or "{}")
                except Exception:
                    hits_dict = {}
                top_rows.append(
                    {
                        "category": cat,
                        "category_score": int(row[score_col]),
                        "total_score": int(row["total_score"]),
                        "rel_path": row["rel_path"],
                        "kind": row["kind"],
                        "modified": row["modified"],
                        "size_kb": row["size_kb"],
                        "matched_keywords": ", ".join(hits_dict.get(cat, [])),
                        "sample_matches": snippets_dict.get(cat, ""),
                    }
                )

    category_summary = pd.DataFrame(category_summary_rows).sort_values(
        ["candidate_file_count", "max_score"], ascending=[False, False]
    )
    top_candidates = pd.DataFrame(top_rows)
    if not top_candidates.empty:
        top_candidates = top_candidates.sort_values(["category", "category_score", "total_score"], ascending=[True, False, False])

    parquet_df = pd.DataFrame(parquet_rows)
    if not parquet_df.empty:
        # Add category scores to parquet schema table too.
        for cat in CATEGORY_KEYWORDS:
            score_col = f"score__{cat}"
            score_map = inventory.set_index("rel_path")[score_col].to_dict()
            parquet_df[score_col] = parquet_df["rel_path"].map(score_map).fillna(0).astype(int)
        parquet_df["total_score"] = parquet_df["rel_path"].map(
            inventory.set_index("rel_path")["total_score"].to_dict()
        ).fillna(0).astype(int)

    inventory = inventory.sort_values(["total_score", "modified"], ascending=[False, False]).reset_index(drop=True)
    return inventory, category_summary.reset_index(drop=True), top_candidates.reset_index(drop=True), parquet_df.reset_index(drop=True)


# ======================================================================================
# Output / console reporting
# ======================================================================================

def write_outputs(
    cfg: Config,
    inventory: pd.DataFrame,
    category_summary: pd.DataFrame,
    top_candidates: pd.DataFrame,
    parquet_df: pd.DataFrame,
) -> Dict[str, str]:
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)
    ts = cfg.run_timestamp

    paths = {
        "inventory": cfg.audit_dir / f"vrp_production_source_inventory_{ts}.csv",
        "category_summary": cfg.audit_dir / f"vrp_production_source_inventory_category_summary_{ts}.csv",
        "top_candidates": cfg.audit_dir / f"vrp_production_source_inventory_top_candidates_{ts}.csv",
        "parquet_schema": cfg.audit_dir / f"vrp_production_source_inventory_parquet_schema_{ts}.csv",
        "manifest": cfg.audit_dir / f"vrp_production_source_inventory_manifest_{ts}.json",
    }

    inventory.to_csv(paths["inventory"], index=False)
    category_summary.to_csv(paths["category_summary"], index=False)
    top_candidates.to_csv(paths["top_candidates"], index=False)
    parquet_df.to_csv(paths["parquet_schema"], index=False)

    manifest = {
        "script": "vrp_production_source_inventory_v1.py",
        "run_timestamp": ts,
        "project_root": str(cfg.project_root),
        "audit_dir": str(cfg.audit_dir),
        "max_text_mb": cfg.max_text_mb,
        "top_n": cfg.top_n,
        "include_checkpoints": cfg.include_checkpoints,
        "skip_parquet_date_stats": cfg.skip_parquet_date_stats,
        "file_count": int(len(inventory)),
        "text_scanned_count": int(inventory["text_scanned"].sum()) if not inventory.empty else 0,
        "parquet_file_count": int(len(parquet_df)),
        "category_keyword_config": CATEGORY_KEYWORDS,
        "outputs": {k: str(v) for k, v in paths.items()},
        "notes": [
            "Read-only inventory. Production data files are not modified.",
            "Pickle/joblib model files are not opened for safety; only name/path metadata is used.",
            "Parquet files are inspected for schema and lightweight date/tenor metadata where possible.",
        ],
    }
    paths["manifest"].write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    return {k: str(v) for k, v in paths.items()}


def print_report(cfg: Config, inventory: pd.DataFrame, category_summary: pd.DataFrame, top_candidates: pd.DataFrame, parquet_df: pd.DataFrame, outputs: Dict[str, str]) -> None:
    print_header("VRP production source inventory v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Audit dir:                 {cfg.audit_dir}")
    print(f"Max text scan MB:          {cfg.max_text_mb}")
    print(f"Include checkpoints:       {cfg.include_checkpoints}")
    print(f"Skip parquet date stats:   {cfg.skip_parquet_date_stats}")

    print_header("Inventory summary")
    print(f"Files inventoried:         {len(inventory):,}")
    if not inventory.empty:
        print(f"Text/code files scanned:   {int(inventory['text_scanned'].sum()):,}")
        print(f"Parquet files inspected:   {len(parquet_df):,}")
        print(f"Files with score > 0:      {int((inventory['total_score'] > 0).sum()):,}")
        print(f"Top total score:           {int(inventory['total_score'].max())}")

    print_header("Category summary")
    if category_summary.empty:
        print("No category candidates found.")
    else:
        cols = ["category", "candidate_file_count", "max_score", "total_score"]
        print(category_summary[cols].to_string(index=False))

    print_header("Top candidates by category")
    if top_candidates.empty:
        print("No top candidates found.")
    else:
        for cat in CATEGORY_KEYWORDS:
            subset = top_candidates[top_candidates["category"] == cat].head(min(cfg.top_n, 8))
            if subset.empty:
                continue
            print("\n" + "-" * 100)
            print(cat)
            print("-" * 100)
            display_cols = ["category_score", "total_score", "kind", "rel_path", "matched_keywords"]
            print(subset[display_cols].to_string(index=False, max_colwidth=120))

    print_header("High-scoring parquet/data files")
    if parquet_df.empty:
        print("No parquet files found or inspected.")
    else:
        cols = [
            "total_score",
            "rel_path",
            "num_rows",
            "num_columns",
            "date_column",
            "date_min",
            "date_max",
            "tenor_column",
            "tenor_values",
            "unique_values_summary",
        ]
        existing_cols = [c for c in cols if c in parquet_df.columns]
        top_parquet = parquet_df.sort_values(["total_score", "date_max"], ascending=[False, False]).head(20)
        print(top_parquet[existing_cols].to_string(index=False, max_colwidth=120))

    print_header("Saved inventory outputs")
    for label, path in outputs.items():
        print(f"{label:20s} {path}")

    print("\nDONE — production source inventory complete.")


# ======================================================================================
# CLI
# ======================================================================================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only VRP production source inventory.")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT), help="VRP project root.")
    parser.add_argument("--audit-dir", default=None, help="Audit output directory. Defaults to project_root/data/audit/production_inventory.")
    parser.add_argument("--max-text-mb", type=float, default=8.0, help="Maximum text/code file size to scan in MB.")
    parser.add_argument("--top-n", type=int, default=15, help="Top candidates per category to save/print.")
    parser.add_argument("--include-checkpoints", action="store_true", help="Include .ipynb_checkpoints directories.")
    parser.add_argument("--skip-parquet-date-stats", action="store_true", help="Only inspect parquet schemas; do not read date/tenor columns.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root)
    audit_dir = Path(args.audit_dir) if args.audit_dir else project_root / DEFAULT_AUDIT_REL

    if not project_root.exists():
        print(f"ERROR: project root does not exist: {project_root}", file=sys.stderr)
        return 2

    cfg = Config(
        project_root=project_root,
        audit_dir=audit_dir,
        max_text_mb=float(args.max_text_mb),
        top_n=int(args.top_n),
        include_checkpoints=bool(args.include_checkpoints),
        skip_parquet_date_stats=bool(args.skip_parquet_date_stats),
        run_timestamp=now_stamp(),
    )

    inventory, category_summary, top_candidates, parquet_df = build_inventory(cfg)
    outputs = write_outputs(cfg, inventory, category_summary, top_candidates, parquet_df)
    print_report(cfg, inventory, category_summary, top_candidates, parquet_df, outputs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
