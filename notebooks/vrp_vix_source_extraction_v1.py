#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_vix_source_extraction_v1.py

Targeted read-only extraction for VIX-style implied-variance productionization.

Purpose
-------
Inspect the known VIX / option-chain research sources and extract the exact
methodology clues needed before building a production implied-variance surface
builder.

This script does NOT:
    - fetch data
    - modify production outputs
    - rebuild implied variance
    - apply signals
    - infer or choose final methodology automatically

It only reads notebooks / scripts and writes audit CSV/JSON files.

Default target sources
----------------------
1. notebooks v0\\old\\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb
2. notebooks v0\\old\\01_clean_vix_replication_v0_6_friday_cycle_holiday_adjusted_clean.ipynb
3. notebooks v0\\old\\01_clean_vix_replication_v0_5_cache_batch_update_cleaned.ipynb
4. notebooks v0\\15_thetadata_naked_atm_put_data_update_v0_1.ipynb
5. old\\production v1\\vrp_production_v1_step07_signal_snapshot\\run_step07_signal_snapshot.py

Outputs
-------
data\\audit\\production_inventory\\vrp_vix_source_extraction_targets_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_contract_summary_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_all_excerpts_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_top_excerpts_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_endpoints_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_functions_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_path_refs_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_assignment_refs_*.csv
data\\audit\\production_inventory\\vrp_vix_source_extraction_manifest_*.json
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_AUDIT_REL = Path(r"data\audit\production_inventory")

TARGET_RELS = [
    Path(r"notebooks v0\old\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb"),
    Path(r"notebooks v0\old\01_clean_vix_replication_v0_6_friday_cycle_holiday_adjusted_clean.ipynb"),
    Path(r"notebooks v0\old\01_clean_vix_replication_v0_5_cache_batch_update_cleaned.ipynb"),
    Path(r"notebooks v0\15_thetadata_naked_atm_put_data_update_v0_1.ipynb"),
    Path(r"old\production v1\vrp_production_v1_step07_signal_snapshot\run_step07_signal_snapshot.py"),
]

# Contract items we need before writing a production implied-variance builder.
CONTRACT_CATEGORIES: Dict[str, List[str]] = {
    "thetadata_endpoint": [
        "thetadata",
        "localhost",
        "127.0.0.1",
        "25503",
        "v3/option",
        "option/history",
        "option/quote",
        "snapshot",
        "bulk",
        "root=",
        "exp=",
        "expiration",
    ],
    "root_and_expiration_handling": [
        "spxw",
        "spx",
        "root",
        "expiration",
        "expiry",
        "expirations",
        "friday",
        "weekly",
        "standard",
        "monthly",
        "third friday",
        "holiday",
        "calendar",
    ],
    "quote_and_mid_construction": [
        "bid",
        "ask",
        "mid",
        "quote",
        "nbbo",
        "mark",
        "last",
        "price",
        "best_bid",
        "best_ask",
    ],
    "zero_bid_filter": [
        "zero_bid",
        "zero bid",
        "consecutive",
        "bid == 0",
        "bid<=0",
        "bid <= 0",
        "nonzero",
        "non-zero",
        "stop",
        "truncate",
    ],
    "forward_and_k0": [
        "forward",
        "k0",
        "atm",
        "at-the-money",
        "call-put",
        "call put",
        "min_abs",
        "abs_diff",
        "strike below",
        "strike_below",
    ],
    "variance_strip_formula": [
        "delta_k",
        "deltak",
        "contribution",
        "q_k",
        "qk",
        "sigma2",
        "sigma_sq",
        "variance",
        "implied_variance",
        "2 / t",
        "2/t",
        "1 / t",
        "1/t",
        "np.exp",
        "math.exp",
    ],
    "rates_sofr_discounting": [
        "sofr",
        "fred",
        "risk_free",
        "risk-free",
        "rate",
        "discount",
        "zero rate",
        "treasury",
        "rfr",
    ],
    "tenor_interpolation": [
        "interpol",
        "interpolate",
        "target_tenor",
        "tenor",
        "near",
        "next",
        "weight",
        "calendar days",
        "dte",
        "30d",
        "vix style",
        "vix-style",
    ],
    "outputs_and_schema": [
        "to_parquet",
        "to_csv",
        "processed",
        "audit",
        "output",
        "vix_style_vol",
        "implied_variance",
        "trade_date",
        "tenor",
        "manifest",
    ],
    "intraday_readiness": [
        "timestamp",
        "ms_of_day",
        "datetime",
        "intraday",
        "asof",
        "as_of",
        "interval",
        "minute",
        "snapshot_time",
    ],
}


ASSIGNMENT_KEYWORDS = [
    "BASE_URL",
    "THETADATA",
    "URL",
    "ROOT",
    "SYMBOL",
    "TARGET",
    "TENOR",
    "DTE",
    "RATE",
    "SOFR",
    "EXPIR",
    "EXPIRATION",
    "ZERO",
    "BID",
    "ASK",
    "MID",
    "OUTPUT",
    "PATH",
    "CACHE",
    "PROCESSED",
    "AUDIT",
]

PATH_REF_RE = re.compile(
    r"(?P<quote>['\"])(?P<path>(?:[A-Za-z]:\\|\.{0,2}[\\/]|data[\\/]|notebooks[\\/]|old[\\/]|raw[\\/]|processed[\\/]|audit[\\/])[^'\"]+?)(?P=quote)",
    flags=re.IGNORECASE,
)

ENDPOINT_RE = re.compile(
    r"(https?://[^\s'\"\)]+|/v\d+/[A-Za-z0-9_\-/]+|v\d+/(?:option|stock|bulk)[A-Za-z0-9_\-/]*)",
    flags=re.IGNORECASE,
)

FUNC_RE = re.compile(
    r"^\s*(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]",
    flags=re.MULTILINE,
)

ASSIGN_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$",
    flags=re.MULTILINE,
)


@dataclass
class Config:
    project_root: Path
    audit_dir: Path
    run_timestamp: str
    max_context_lines: int
    max_text_mb: float


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def read_notebook_code(path: Path) -> List[Dict[str, Any]]:
    """Return source chunks from notebook cells."""
    raw = path.read_text(encoding="utf-8", errors="ignore")
    data = json.loads(raw)
    chunks: List[Dict[str, Any]] = []
    for idx, cell in enumerate(data.get("cells", [])):
        cell_type = cell.get("cell_type", "")
        if cell_type not in {"code", "markdown"}:
            continue
        src = cell.get("source", "")
        if isinstance(src, list):
            text = "".join(src)
        else:
            text = str(src)
        if not text.strip():
            continue
        chunks.append(
            {
                "chunk_id": f"cell_{idx:04d}",
                "chunk_type": cell_type,
                "text": text,
                "line_offset": 1,
            }
        )
    return chunks


def read_text_chunks(path: Path, max_text_mb: float) -> List[Dict[str, Any]]:
    size_mb = path.stat().st_size / (1024 * 1024)
    if size_mb > max_text_mb:
        return [
            {
                "chunk_id": "skipped_large_file",
                "chunk_type": "warning",
                "text": f"Skipped text scan because file is {size_mb:.2f} MB > max_text_mb={max_text_mb}",
                "line_offset": 1,
            }
        ]

    text = path.read_text(encoding="utf-8", errors="ignore")
    return [{"chunk_id": "full_text", "chunk_type": "text", "text": text, "line_offset": 1}]


def source_chunks(path: Path, max_text_mb: float) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".ipynb":
        return read_notebook_code(path)
    return read_text_chunks(path, max_text_mb)


def score_text(text: str, terms: Iterable[str]) -> Tuple[int, List[str]]:
    low = text.lower()
    matched: List[str] = []
    score = 0
    for term in terms:
        t = term.lower()
        if t in low:
            matched.append(term)
            # Slightly overweight distinctive exact terms.
            if len(t) >= 8:
                score += 4
            else:
                score += 2
    return score, matched


def relevant_excerpt(text: str, matched_terms: List[str], context_lines: int) -> str:
    lines = text.splitlines()
    if not lines:
        return ""

    low_terms = [m.lower() for m in matched_terms]
    hit_idxs: List[int] = []
    for i, line in enumerate(lines):
        low = line.lower()
        if any(term in low for term in low_terms):
            hit_idxs.append(i)

    if not hit_idxs:
        start = 0
        end = min(len(lines), context_lines * 2 + 1)
    else:
        center = hit_idxs[0]
        start = max(0, center - context_lines)
        end = min(len(lines), center + context_lines + 1)

    snippet_lines = []
    for i in range(start, end):
        snippet_lines.append(f"{i + 1:04d}: {lines[i]}")
    return "\n".join(snippet_lines)


def extract_endpoints(text: str) -> List[str]:
    vals = sorted(set(m.group(1).rstrip(".,;") for m in ENDPOINT_RE.finditer(text)))
    return vals


def extract_functions(text: str) -> List[str]:
    return sorted(set(m.group(1) for m in FUNC_RE.finditer(text)))


def extract_path_refs(text: str) -> List[str]:
    vals = []
    for m in PATH_REF_RE.finditer(text):
        vals.append(m.group("path"))
    return sorted(set(vals))


def extract_assignments(text: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for name, value in ASSIGN_RE.findall(text):
        upper = name.upper()
        if any(k in upper for k in ASSIGNMENT_KEYWORDS):
            value_clean = value.strip()
            if len(value_clean) > 240:
                value_clean = value_clean[:240] + "..."
            out.append((name, value_clean))
    return out


def inspect_target_files(cfg: Config) -> Dict[str, pd.DataFrame]:
    target_rows: List[Dict[str, Any]] = []
    excerpt_rows: List[Dict[str, Any]] = []
    endpoint_rows: List[Dict[str, Any]] = []
    function_rows: List[Dict[str, Any]] = []
    path_ref_rows: List[Dict[str, Any]] = []
    assignment_rows: List[Dict[str, Any]] = []

    for rel in TARGET_RELS:
        abs_path = cfg.project_root / rel
        exists = abs_path.exists()
        kind = "notebook" if rel.suffix.lower() == ".ipynb" else "python" if rel.suffix.lower() == ".py" else "text"
        size_kb = round(abs_path.stat().st_size / 1024, 2) if exists else None

        target_rows.append(
            {
                "rel_path": str(rel),
                "abs_path": str(abs_path),
                "exists": bool(exists),
                "kind": kind,
                "size_kb": size_kb,
            }
        )

        if not exists:
            continue

        chunks = source_chunks(abs_path, cfg.max_text_mb)

        for chunk in chunks:
            text = chunk["text"]
            chunk_id = chunk["chunk_id"]
            chunk_type = chunk["chunk_type"]

            for category, terms in CONTRACT_CATEGORIES.items():
                score, matched = score_text(text, terms)
                if score <= 0:
                    continue
                excerpt_rows.append(
                    {
                        "category": category,
                        "score": score,
                        "matched_terms": ", ".join(matched),
                        "rel_path": str(rel),
                        "kind": kind,
                        "chunk_id": chunk_id,
                        "chunk_type": chunk_type,
                        "excerpt": relevant_excerpt(text, matched, cfg.max_context_lines),
                    }
                )

            for endpoint in extract_endpoints(text):
                endpoint_rows.append(
                    {
                        "rel_path": str(rel),
                        "kind": kind,
                        "chunk_id": chunk_id,
                        "endpoint_or_route": endpoint,
                    }
                )

            for func in extract_functions(text):
                function_rows.append(
                    {
                        "rel_path": str(rel),
                        "kind": kind,
                        "chunk_id": chunk_id,
                        "function_or_class": func,
                    }
                )

            for path_ref in extract_path_refs(text):
                path_ref_rows.append(
                    {
                        "rel_path": str(rel),
                        "kind": kind,
                        "chunk_id": chunk_id,
                        "path_ref": path_ref,
                    }
                )

            for name, value in extract_assignments(text):
                assignment_rows.append(
                    {
                        "rel_path": str(rel),
                        "kind": kind,
                        "chunk_id": chunk_id,
                        "assignment_name": name,
                        "assignment_value": value,
                    }
                )

    targets = pd.DataFrame(target_rows)
    excerpts = pd.DataFrame(excerpt_rows)
    endpoints = pd.DataFrame(endpoint_rows).drop_duplicates() if endpoint_rows else pd.DataFrame()
    functions = pd.DataFrame(function_rows).drop_duplicates() if function_rows else pd.DataFrame()
    path_refs = pd.DataFrame(path_ref_rows).drop_duplicates() if path_ref_rows else pd.DataFrame()
    assignments = pd.DataFrame(assignment_rows).drop_duplicates() if assignment_rows else pd.DataFrame()

    if not excerpts.empty:
        excerpts = excerpts.sort_values(["category", "score", "rel_path", "chunk_id"], ascending=[True, False, True, True]).reset_index(drop=True)

    return {
        "targets": targets,
        "excerpts": excerpts,
        "endpoints": endpoints,
        "functions": functions,
        "path_refs": path_refs,
        "assignments": assignments,
    }


def build_contract_summary(excerpts: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if excerpts.empty:
        return pd.DataFrame(
            columns=[
                "contract_item",
                "candidate_file_count",
                "excerpt_count",
                "max_score",
                "top_source_1",
                "top_source_2",
                "top_source_3",
            ]
        )

    for category, g in excerpts.groupby("category"):
        top = (
            g.sort_values(["score", "rel_path", "chunk_id"], ascending=[False, True, True])
            .drop_duplicates("rel_path")
            .head(3)
        )
        row = {
            "contract_item": category,
            "candidate_file_count": int(g["rel_path"].nunique()),
            "excerpt_count": int(len(g)),
            "max_score": int(g["score"].max()),
        }
        for i, (_, r) in enumerate(top.iterrows(), start=1):
            row[f"top_source_{i}"] = f"{r['rel_path']} :: {r['chunk_id']} :: score={r['score']}"
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["max_score", "excerpt_count"], ascending=[False, False]).reset_index(drop=True)


def build_recommendations(excerpts: pd.DataFrame) -> pd.DataFrame:
    recommendations = [
        {
            "production_question": "Which notebook should be treated as primary VIX methodology source?",
            "recommendation": (
                "Start with v0_7_exchange_calendar_fred_sofr and compare against v0_6/v0_5 excerpts. "
                "v0_7 is likely the latest VIX replication source with exchange calendar and FRED/SOFR handling."
            ),
            "validation_needed": (
                "Confirm exact variance strip formula, zero-bid truncation, expiration schedule, rate treatment, "
                "and tenor interpolation from the extracted excerpts before coding production."
            ),
        },
        {
            "production_question": "Which source should be used for ThetaData chain pull mechanics?",
            "recommendation": (
                "Use 15_thetadata_naked_atm_put_data_update_v0_1 for current ThetaData quote/snapshot mechanics, "
                "then adapt the chain pulls to the VIX replication methodology."
            ),
            "validation_needed": (
                "Confirm endpoint version, params, root handling, response schema, quote columns, "
                "and how timestamp/as-of selection works."
            ),
        },
        {
            "production_question": "Should the production builder choose methodology automatically?",
            "recommendation": (
                "No. Use this extraction to define the production contract first. The builder should hard-code "
                "the approved VIX-style contract and validate output against existing historical implied variance."
            ),
            "validation_needed": (
                "Run reproduction test against existing 07A/final panel implied_variance and vix_style_vol fields "
                "through 2026-07-01 before using it in the daily runner."
            ),
        },
    ]
    return pd.DataFrame(recommendations)


def write_outputs(cfg: Config, frames: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = cfg.run_timestamp

    contract_summary = build_contract_summary(frames["excerpts"])
    recommendations = build_recommendations(frames["excerpts"])

    top_excerpts = (
        frames["excerpts"]
        .sort_values(["category", "score", "rel_path", "chunk_id"], ascending=[True, False, True, True])
        .groupby("category", group_keys=False)
        .head(10)
        .reset_index(drop=True)
        if not frames["excerpts"].empty
        else pd.DataFrame()
    )

    outputs = {
        "targets": cfg.audit_dir / f"vrp_vix_source_extraction_targets_{stamp}.csv",
        "contract_summary": cfg.audit_dir / f"vrp_vix_source_extraction_contract_summary_{stamp}.csv",
        "all_excerpts": cfg.audit_dir / f"vrp_vix_source_extraction_all_excerpts_{stamp}.csv",
        "top_excerpts": cfg.audit_dir / f"vrp_vix_source_extraction_top_excerpts_{stamp}.csv",
        "endpoints": cfg.audit_dir / f"vrp_vix_source_extraction_endpoints_{stamp}.csv",
        "functions": cfg.audit_dir / f"vrp_vix_source_extraction_functions_{stamp}.csv",
        "path_refs": cfg.audit_dir / f"vrp_vix_source_extraction_path_refs_{stamp}.csv",
        "assignments": cfg.audit_dir / f"vrp_vix_source_extraction_assignment_refs_{stamp}.csv",
        "recommendations": cfg.audit_dir / f"vrp_vix_source_extraction_recommendations_{stamp}.csv",
        "manifest": cfg.audit_dir / f"vrp_vix_source_extraction_manifest_{stamp}.json",
    }

    frames["targets"].to_csv(outputs["targets"], index=False)
    contract_summary.to_csv(outputs["contract_summary"], index=False)
    frames["excerpts"].to_csv(outputs["all_excerpts"], index=False)
    top_excerpts.to_csv(outputs["top_excerpts"], index=False)
    frames["endpoints"].to_csv(outputs["endpoints"], index=False)
    frames["functions"].to_csv(outputs["functions"], index=False)
    frames["path_refs"].to_csv(outputs["path_refs"], index=False)
    frames["assignments"].to_csv(outputs["assignments"], index=False)
    recommendations.to_csv(outputs["recommendations"], index=False)

    manifest = {
        "script": "vrp_vix_source_extraction_v1.py",
        "run_timestamp": stamp,
        "project_root": str(cfg.project_root),
        "audit_dir": str(cfg.audit_dir),
        "max_context_lines": cfg.max_context_lines,
        "max_text_mb": cfg.max_text_mb,
        "target_count": int(len(frames["targets"])),
        "targets_existing": int(frames["targets"]["exists"].sum()) if not frames["targets"].empty else 0,
        "excerpt_rows": int(len(frames["excerpts"])),
        "endpoint_rows": int(len(frames["endpoints"])),
        "function_rows": int(len(frames["functions"])),
        "path_ref_rows": int(len(frames["path_refs"])),
        "assignment_rows": int(len(frames["assignments"])),
        "contract_categories": list(CONTRACT_CATEGORIES.keys()),
        "outputs": {k: str(v) for k, v in outputs.items() if k != "manifest"},
    }
    outputs["manifest"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    frames["contract_summary"] = contract_summary
    frames["top_excerpts"] = top_excerpts
    frames["recommendations"] = recommendations

    return outputs


def display_console_summary(frames: Dict[str, pd.DataFrame], outputs: Dict[str, Path]) -> None:
    print_header("Target files")
    for _, r in frames["targets"].iterrows():
        print(f"exists={int(bool(r['exists'])):<5} kind={r['kind']:<8} rel_path={r['rel_path']}")

    print_header("Extraction summary")
    print(f"Target files:              {len(frames['targets']):,}")
    print(f"Targets existing:          {int(frames['targets']['exists'].sum()):,}")
    print(f"Contract excerpt rows:     {len(frames['excerpts']):,}")
    print(f"Endpoints/routes found:    {len(frames['endpoints']):,}")
    print(f"Functions/classes found:   {len(frames['functions']):,}")
    print(f"Path refs found:           {len(frames['path_refs']):,}")
    print(f"Assignment refs found:     {len(frames['assignments']):,}")

    print_header("Contract summary")
    if frames["contract_summary"].empty:
        print("No contract excerpts found.")
    else:
        cols = ["contract_item", "candidate_file_count", "excerpt_count", "max_score", "top_source_1"]
        print(frames["contract_summary"][cols].to_string(index=False))

    print_header("Endpoints/routes")
    if frames["endpoints"].empty:
        print("No endpoints/routes extracted.")
    else:
        print(frames["endpoints"].head(30).to_string(index=False))

    print_header("Function/class names")
    if frames["functions"].empty:
        print("No function/class definitions extracted.")
    else:
        print(frames["functions"].head(40).to_string(index=False))

    print_header("Top excerpts by contract item")
    if frames["top_excerpts"].empty:
        print("No excerpts extracted.")
    else:
        for category, g in frames["top_excerpts"].groupby("category"):
            print("\n" + "-" * 100)
            print(category)
            print("-" * 100)
            for _, r in g.head(3).iterrows():
                print(f"score={r['score']} rel_path={r['rel_path']} chunk={r['chunk_id']}")
                print(f"  matched: {r['matched_terms']}")

    print_header("Saved VIX source extraction outputs")
    for label, path in outputs.items():
        print(f"{label:<18} {path}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Targeted VIX implied-variance source extraction.")
    parser.add_argument("--project-root", type=str, default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--audit-dir", type=str, default=None)
    parser.add_argument("--max-context-lines", type=int, default=8)
    parser.add_argument("--max-text-mb", type=float, default=12.0)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root)
    audit_dir = Path(args.audit_dir) if args.audit_dir else project_root / DEFAULT_AUDIT_REL

    cfg = Config(
        project_root=project_root,
        audit_dir=audit_dir,
        run_timestamp=datetime.now().strftime("%Y%m%d_%H%M%S"),
        max_context_lines=int(args.max_context_lines),
        max_text_mb=float(args.max_text_mb),
    )

    print_header("VRP VIX source extraction v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Audit dir:                 {cfg.audit_dir}")
    print(f"Max context lines:         {cfg.max_context_lines}")
    print(f"Max text scan MB:          {cfg.max_text_mb}")

    frames = inspect_target_files(cfg)
    outputs = write_outputs(cfg, frames)
    display_console_summary(frames, outputs)

    print("\nDONE — VIX source extraction complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
