#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_vix_function_source_dump_v1.py

Read-only source extraction utility for the VRP VIX-style implied variance build.

Purpose
-------
Dump the exact function bodies from the historical VIX replication notebooks/scripts so the
production implied-variance builder can be written from the real working source rather than
from memory.

This script intentionally does NOT:
    - call ThetaData
    - pull option chains
    - calculate implied variance
    - modify processed production data
    - modify notebooks

It only reads source notebooks/scripts and writes audit/source-dump files under:
    data/audit/production_inventory

Example
-------
py vrp_vix_function_source_dump_v1.py --project-root "C:\\Users\\patri\\vrp_project"
"""

from __future__ import annotations

import argparse
import ast
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

PRIMARY_SOURCE_REL = Path(
    r"notebooks v0\old\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb"
)

SECONDARY_SOURCE_RELS = [
    Path(r"notebooks v0\old\01_clean_vix_replication_v0_6_friday_cycle_holiday_adjusted_clean.ipynb"),
    Path(r"notebooks v0\old\01_clean_vix_replication_v0_5_cache_batch_update_cleaned.ipynb"),
    Path(r"notebooks v0\15_thetadata_naked_atm_put_data_update_v0_1.ipynb"),
    Path(r"old\production v1\vrp_production_v1_step07_signal_snapshot\run_step07_signal_snapshot.py"),
]

CORE_FUNCTION_NAMES = [
    "list_expirations_v3",
    "get_interest_rate_for_date_v3",
    "get_chain_at_time",
    "_prepare_call_put_tables",
    "_select_otm_options_with_bid_rule",
    "calc_single_term_variance",
    "minutes_to_expiry_vix_method",
    "choose_expiration_pair_for_target_days",
    "get_required_chains_for_target_tenors",
    "preferred_root_for_expiration_v7",
    "interpolate_variance_to_target_days",
    "calculate_vix_term_structure_for_date_v7_cached",
    "upsert_term_structure_history",
]

DEPENDENCY_FUNCTION_NAMES = [
    "cache_time_label",
    "int_date_from_date",
    "is_third_friday",
    "ms_to_time_string",
    "yyyymmdd_to_dash_string",
    "yyyymmdd_to_date",
    "load_fred_sofr_history",
    "update_fred_sofr_history",
    "settlement_minutes_after_midnight_et",
    "get_friday_cycle_expiration_candidates",
    "is_friday_cycle_expiration_v7",
    "is_holiday_adjusted_monthly_expiration_v7",
    "is_last_trading_day_before_closed_friday",
    "next_calendar_friday_after_date",
    "_pull_one_chain",
    "get_chain_at_time_cached",
    "get_chain_cache_path",
    "pull_unique_chains_parallel_cached",
    "calculate_variance_for_unique_chains",
    "load_existing_term_structure_history",
    "save_term_structure_history",
    "find_missing_trade_dates_v7",
]

ALL_TARGET_FUNCTION_NAMES = CORE_FUNCTION_NAMES + DEPENDENCY_FUNCTION_NAMES

# Useful non-def patterns to excerpt because they often define constants/contracts in notebooks.
CONTRACT_PATTERNS = {
    "thetadata_base_url": [r"BASE_URL\s*=", r"127\.0\.0\.1", r"25503", r"/v3"],
    "target_tenors": [r"TARGET_TENORS", r"target_tenors", r"\b9\b.*\b12\b.*\b15\b"],
    "risk_free_rate": [r"SOFR", r"FRED", r"risk_free", r"risk-free", r"rate"],
    "root_spx_spxw": [r"SPXW", r"SPX", r"root"],
    "zero_bid_rule": [r"zero_bid", r"consecutive", r"nonzero", r"stop"],
    "variance_formula": [r"delta_k", r"contribution", r"qk", r"2\s*/\s*t", r"math\.exp"],
    "output_paths": [r"to_parquet", r"to_csv", r"term_structure", r"vix_style", r"implied_variance"],
}


# ======================================================================================
# Data classes / helpers
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    audit_dir: Path
    run_timestamp: str
    include_secondary: bool
    max_cell_chars: int


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


def read_ipynb_code_cells(path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    cells = []
    for idx, cell in enumerate(raw.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        cells.append(
            {
                "chunk_id": f"cell_{idx:04d}",
                "source": str(src),
                "cell_index": idx,
            }
        )
    return cells


def read_python_chunks(path: Path) -> List[Dict[str, Any]]:
    return [
        {
            "chunk_id": "full_text",
            "source": path.read_text(encoding="utf-8", errors="ignore"),
            "cell_index": None,
        }
    ]


def read_source_chunks(path: Path) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".ipynb":
            return "notebook", read_ipynb_code_cells(path), None
        if suffix == ".py":
            return "python", read_python_chunks(path), None
        return "text", read_python_chunks(path), None
    except Exception as exc:
        return "unknown", [], str(exc)


def extract_definitions_ast(source: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Return top-level and nested def/class nodes with source extracted by lineno/end_lineno."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [], f"SyntaxError: {exc}"
    except Exception as exc:
        return [], f"ParseError: {exc}"

    lines = source.splitlines()
    out: List[Dict[str, Any]] = []

    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if not hasattr(node, "lineno") or not hasattr(node, "end_lineno"):
            continue
        start = int(node.lineno)
        end = int(node.end_lineno)
        code = "\n".join(lines[start - 1 : end])
        out.append(
            {
                "name": getattr(node, "name", None),
                "definition_type": type(node).__name__,
                "start_line": start,
                "end_line": end,
                "source_code": code,
                "extract_method": "ast",
            }
        )

    out.sort(key=lambda x: (x["start_line"], x["end_line"], x["name"] or ""))
    return out, None


def extract_definitions_regex(source: str) -> List[Dict[str, Any]]:
    """Fallback top-level def/class extraction for notebook cells with non-parseable syntax."""
    lines = source.splitlines()
    starts: List[Tuple[int, str, str]] = []
    pattern = re.compile(r"^(def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]")
    for i, line in enumerate(lines, start=1):
        if line.startswith("def ") or line.startswith("class "):
            m = pattern.match(line)
            if m:
                starts.append((i, m.group(1), m.group(2)))

    out: List[Dict[str, Any]] = []
    for idx, (start, kind, name) in enumerate(starts):
        end = starts[idx + 1][0] - 1 if idx + 1 < len(starts) else len(lines)
        # Trim trailing blank lines for cleaner output.
        while end > start and not lines[end - 1].strip():
            end -= 1
        code = "\n".join(lines[start - 1 : end])
        out.append(
            {
                "name": name,
                "definition_type": "FunctionDef" if kind == "def" else "ClassDef",
                "start_line": start,
                "end_line": end,
                "source_code": code,
                "extract_method": "regex_fallback",
            }
        )
    return out


def compile_regexes(patterns: Sequence[str]) -> List[re.Pattern[str]]:
    return [re.compile(p, flags=re.IGNORECASE | re.MULTILINE) for p in patterns]


def excerpt_around_line(source: str, line_no: int, context: int = 6, max_chars: int = 8000) -> Tuple[int, int, str]:
    lines = source.splitlines()
    start = max(1, line_no - context)
    end = min(len(lines), line_no + context)
    text = "\n".join(lines[start - 1 : end])
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...[truncated]"
    return start, end, text


def find_contract_excerpts(source: str, max_cell_chars: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item, patterns in CONTRACT_PATTERNS.items():
        regexes = compile_regexes(patterns)
        matches = []
        for rgx in regexes:
            m = rgx.search(source)
            if m:
                line_no = source[: m.start()].count("\n") + 1
                matches.append((rgx.pattern, line_no))
        if not matches:
            continue
        first_line = min(x[1] for x in matches)
        start, end, excerpt = excerpt_around_line(source, first_line, context=8, max_chars=max_cell_chars)
        out.append(
            {
                "contract_item": item,
                "matched_patterns": ", ".join(sorted({x[0] for x in matches})),
                "start_line": start,
                "end_line": end,
                "excerpt": excerpt,
            }
        )
    return out


def required_paths(project_root: Path, include_secondary: bool) -> List[Path]:
    rels = [PRIMARY_SOURCE_REL]
    if include_secondary:
        rels.extend(SECONDARY_SOURCE_RELS)
    return [project_root / rel for rel in rels]


def detect_target_group(name: str) -> str:
    if name in CORE_FUNCTION_NAMES:
        return "core"
    if name in DEPENDENCY_FUNCTION_NAMES:
        return "dependency"
    return "other"


def write_text_dump(path: Path, rows: pd.DataFrame, project_root: Path) -> None:
    lines: List[str] = []
    lines.append("# VRP VIX source function dump v1")
    lines.append("# Auto-generated source extraction. Do not edit source notebooks based on this file alone.")
    lines.append("")

    if rows.empty:
        lines.append("# No functions found.")
        path.write_text("\n".join(lines), encoding="utf-8")
        return

    sort_cols = ["target_group", "function_name", "source_priority", "rel_path", "chunk_id", "start_line"]
    rows2 = rows.sort_values(sort_cols).reset_index(drop=True)

    for _, r in rows2.iterrows():
        lines.append("\n" + "#" * 100)
        lines.append(f"# function_name: {r['function_name']}")
        lines.append(f"# target_group:  {r['target_group']}")
        lines.append(f"# source:        {r['rel_path']}")
        lines.append(f"# chunk_id:      {r['chunk_id']}")
        lines.append(f"# lines:         {r['start_line']} - {r['end_line']}")
        lines.append(f"# method:        {r['extract_method']}")
        lines.append("#" * 100)
        lines.append(str(r["source_code"]))
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def write_markdown_summary(path: Path, cfg: Config, target_df: pd.DataFrame, found_df: pd.DataFrame, missing_df: pd.DataFrame) -> None:
    lines = []
    lines.append("# VRP VIX Function Source Dump v1")
    lines.append("")
    lines.append(f"Run timestamp: `{cfg.run_timestamp}`")
    lines.append(f"Project root: `{cfg.project_root}`")
    lines.append("")
    lines.append("## Target files")
    lines.append("")
    for _, r in target_df.iterrows():
        lines.append(f"- exists={int(bool(r['exists']))} priority={r['source_priority']}: `{r['rel_path']}`")
    lines.append("")
    lines.append("## Core functions")
    lines.append("")
    for name in CORE_FUNCTION_NAMES:
        count = int((found_df["function_name"] == name).sum()) if not found_df.empty else 0
        lines.append(f"- {name}: {count} source body/bodies found")
    lines.append("")
    lines.append("## Dependency functions")
    lines.append("")
    for name in DEPENDENCY_FUNCTION_NAMES:
        count = int((found_df["function_name"] == name).sum()) if not found_df.empty else 0
        lines.append(f"- {name}: {count} source body/bodies found")
    lines.append("")
    lines.append("## Missing requested functions")
    lines.append("")
    if missing_df.empty:
        lines.append("None.")
    else:
        for _, r in missing_df.iterrows():
            lines.append(f"- {r['function_name']} ({r['target_group']})")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


# ======================================================================================
# Main run
# ======================================================================================


def run(cfg: Config) -> int:
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)

    print_header("VRP VIX function source dump v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Audit dir:                 {cfg.audit_dir}")
    print(f"Include secondary sources: {cfg.include_secondary}")
    print(f"Core requested functions:  {len(CORE_FUNCTION_NAMES)}")
    print(f"Dependency functions:      {len(DEPENDENCY_FUNCTION_NAMES)}")

    paths = required_paths(cfg.project_root, cfg.include_secondary)

    target_rows: List[Dict[str, Any]] = []
    function_rows: List[Dict[str, Any]] = []
    all_definition_rows: List[Dict[str, Any]] = []
    contract_rows: List[Dict[str, Any]] = []
    warning_rows: List[Dict[str, Any]] = []

    print_header("Target files")
    for idx, path in enumerate(paths):
        rel = safe_rel(path, cfg.project_root)
        priority = "primary" if idx == 0 else "secondary"
        exists = path.exists()
        print(f"exists={int(exists):<5} priority={priority:<9} rel_path={rel}")
        target_rows.append(
            {
                "source_priority": priority,
                "path": str(path),
                "rel_path": rel,
                "exists": bool(exists),
                "suffix": path.suffix.lower(),
            }
        )
        if not exists:
            warning_rows.append({"rel_path": rel, "warning": "target_file_missing"})
            continue

        kind, chunks, read_warning = read_source_chunks(path)
        if read_warning:
            warning_rows.append({"rel_path": rel, "warning": read_warning})
            continue

        for chunk in chunks:
            chunk_id = chunk["chunk_id"]
            source = chunk["source"] or ""

            ast_defs, parse_warning = extract_definitions_ast(source)
            defs = ast_defs if ast_defs else extract_definitions_regex(source)
            if parse_warning and not defs:
                warning_rows.append({"rel_path": rel, "warning": f"{chunk_id}: {parse_warning}"})

            for d in defs:
                row = {
                    "source_priority": priority,
                    "kind": kind,
                    "rel_path": rel,
                    "path": str(path),
                    "chunk_id": chunk_id,
                    "function_name": d["name"],
                    "definition_type": d["definition_type"],
                    "start_line": d["start_line"],
                    "end_line": d["end_line"],
                    "line_count": int(d["end_line"]) - int(d["start_line"]) + 1,
                    "extract_method": d["extract_method"],
                    "target_group": detect_target_group(str(d["name"])),
                    "source_code": d["source_code"],
                }
                all_definition_rows.append(row)
                if d["name"] in ALL_TARGET_FUNCTION_NAMES:
                    function_rows.append(row)

            for c in find_contract_excerpts(source, cfg.max_cell_chars):
                contract_rows.append(
                    {
                        "source_priority": priority,
                        "kind": kind,
                        "rel_path": rel,
                        "path": str(path),
                        "chunk_id": chunk_id,
                        **c,
                    }
                )

    target_df = pd.DataFrame(target_rows)
    found_df = pd.DataFrame(function_rows)
    all_defs_df = pd.DataFrame(all_definition_rows)
    contract_df = pd.DataFrame(contract_rows)
    warnings_df = pd.DataFrame(warning_rows)

    found_names = set(found_df["function_name"].dropna().astype(str)) if not found_df.empty else set()
    missing_rows = [
        {"function_name": name, "target_group": detect_target_group(name)}
        for name in ALL_TARGET_FUNCTION_NAMES
        if name not in found_names
    ]
    missing_df = pd.DataFrame(missing_rows)

    print_header("Extraction summary")
    print(f"Target files:              {len(target_df)}")
    print(f"Targets existing:          {int(target_df['exists'].sum()) if not target_df.empty else 0}")
    print(f"All definitions found:     {len(all_defs_df)}")
    print(f"Requested funcs found:     {found_df['function_name'].nunique() if not found_df.empty else 0} / {len(ALL_TARGET_FUNCTION_NAMES)}")
    print(f"Core funcs found:          {found_df.loc[found_df['target_group'].eq('core'), 'function_name'].nunique() if not found_df.empty else 0} / {len(CORE_FUNCTION_NAMES)}")
    print(f"Dependency funcs found:    {found_df.loc[found_df['target_group'].eq('dependency'), 'function_name'].nunique() if not found_df.empty else 0} / {len(DEPENDENCY_FUNCTION_NAMES)}")
    print(f"Missing requested funcs:   {len(missing_df)}")
    print(f"Contract excerpts:         {len(contract_df)}")
    print(f"Warnings:                  {len(warnings_df)}")

    if not found_df.empty:
        print_header("Found requested functions")
        summary = (
            found_df.groupby(["target_group", "function_name"], dropna=False)
            .agg(source_count=("rel_path", "nunique"), body_count=("source_code", "count"), min_lines=("line_count", "min"), max_lines=("line_count", "max"))
            .reset_index()
            .sort_values(["target_group", "function_name"])
        )
        print(summary.to_string(index=False, max_rows=200))

    if not missing_df.empty:
        print_header("Missing requested functions")
        print(missing_df.to_string(index=False))

    if not contract_df.empty:
        print_header("Contract excerpt summary")
        csum = (
            contract_df.groupby("contract_item", dropna=False)
            .agg(file_count=("rel_path", "nunique"), excerpt_count=("excerpt", "count"))
            .reset_index()
            .sort_values(["excerpt_count", "contract_item"], ascending=[False, True])
        )
        print(csum.to_string(index=False, max_rows=200))

    # File outputs
    prefix = f"vrp_vix_function_source_dump_{cfg.run_timestamp}"
    outputs = {
        "targets": cfg.audit_dir / f"{prefix}_targets.csv",
        "function_sources": cfg.audit_dir / f"{prefix}_function_sources.csv",
        "all_definitions": cfg.audit_dir / f"{prefix}_all_definitions.csv",
        "missing_functions": cfg.audit_dir / f"{prefix}_missing_functions.csv",
        "contract_excerpts": cfg.audit_dir / f"{prefix}_contract_excerpts.csv",
        "warnings": cfg.audit_dir / f"{prefix}_warnings.csv",
        "source_dump_py": cfg.audit_dir / f"{prefix}_source_dump.py",
        "summary_md": cfg.audit_dir / f"{prefix}_summary.md",
        "manifest": cfg.audit_dir / f"{prefix}_manifest.json",
    }

    target_df.to_csv(outputs["targets"], index=False)
    found_df.to_csv(outputs["function_sources"], index=False, quoting=csv.QUOTE_MINIMAL)
    all_defs_df.to_csv(outputs["all_definitions"], index=False, quoting=csv.QUOTE_MINIMAL)
    missing_df.to_csv(outputs["missing_functions"], index=False)
    contract_df.to_csv(outputs["contract_excerpts"], index=False, quoting=csv.QUOTE_MINIMAL)
    warnings_df.to_csv(outputs["warnings"], index=False)
    write_text_dump(outputs["source_dump_py"], found_df, cfg.project_root)
    write_markdown_summary(outputs["summary_md"], cfg, target_df, found_df, missing_df)

    manifest = {
        "script": "vrp_vix_function_source_dump_v1.py",
        "run_timestamp": cfg.run_timestamp,
        "project_root": str(cfg.project_root),
        "audit_dir": str(cfg.audit_dir),
        "include_secondary": cfg.include_secondary,
        "core_function_names": CORE_FUNCTION_NAMES,
        "dependency_function_names": DEPENDENCY_FUNCTION_NAMES,
        "target_files": target_rows,
        "counts": {
            "target_files": int(len(target_df)),
            "targets_existing": int(target_df["exists"].sum()) if not target_df.empty else 0,
            "all_definitions_found": int(len(all_defs_df)),
            "requested_function_bodies_found": int(len(found_df)),
            "requested_unique_functions_found": int(found_df["function_name"].nunique()) if not found_df.empty else 0,
            "missing_requested_functions": int(len(missing_df)),
            "contract_excerpts": int(len(contract_df)),
            "warnings": int(len(warnings_df)),
        },
        "outputs": {k: str(v) for k, v in outputs.items()},
    }
    outputs["manifest"].write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print_header("Saved VIX function source dump outputs")
    for label, path in outputs.items():
        print(f"{label:<20} {path}")

    print("\nDONE — VIX function source dump complete.")
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dump exact VIX source function bodies from VRP notebooks/scripts.")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT), help="Project root path.")
    parser.add_argument("--audit-dir", default=None, help="Optional audit output directory.")
    parser.add_argument("--primary-only", action="store_true", help="Only inspect the primary v0.7 source notebook.")
    parser.add_argument("--max-cell-chars", type=int, default=8000, help="Maximum characters per contract excerpt.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root)
    audit_dir = Path(args.audit_dir) if args.audit_dir else project_root / DEFAULT_AUDIT_REL
    cfg = Config(
        project_root=project_root,
        audit_dir=audit_dir,
        run_timestamp=now_stamp(),
        include_secondary=not bool(args.primary_only),
        max_cell_chars=int(args.max_cell_chars),
    )
    try:
        return run(cfg)
    except Exception as exc:
        print("\nERROR — VIX function source dump failed.", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
