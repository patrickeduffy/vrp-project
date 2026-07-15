
from pathlib import Path
from datetime import datetime
import ast
import json
import re
import traceback

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")

NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "old"
    / "notebooks forecast model corsi v1"
    / "01_corsi_har_rv_forecast_model_research_cleaned.ipynb"
)

OUT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

CONTRACT_TERMS = [
    # Project/output contract
    "PROJECT_ROOT",
    "CORSI_PROCESSED_DIR",
    "CORSI_AUDIT_DIR",
    "RUN_TIMESTAMP",
    "SPY_STOCK_VENUE",

    # ThetaData / request contract
    "THETADATA",
    "ThetaData",
    "BASE_URL",
    "v3/stock/history/eod",
    "stock/history/eod",
    "v2/hist/stock/ohlc",
    "stock/history/ohlc",
    "interval",
    "ivl",
    "start_date",
    "end_date",
    "YYYYMMDD",
    "chunk",

    # Core source artifacts
    "spy_5m_clean_ohlc_full",
    "spy_eod_full",
    "spy_daily_realized_variance_corsi_v1",
    "spy_daily_realized_variance_corsi_v1_sanitized",
    "spy_daily_realized_variance_corsi_v1_model_ready",
    "corsi_forward_realized_variance_targets_v1",
    "corsi_feature_target_panel_v1",
    "corsi_model_feature_panel_v1",

    # Calculations / columns
    "intraday_realized_variance_raw",
    "overnight_realized_variance_raw",
    "total_realized_variance_raw",
    "spx_close",
    "spx_log_return",
    "spy_total_return",
    "corsi_quality_status",
    "corsi_model_usable",
    "target_log_variance",
    "last_forward_rv_date",

    # Validation / safety
    "validation",
    "manifest",
    "assert",
    "raise",
    "FileNotFoundError",
    "RuntimeError",
]

OUTPUT_STEMS = [
    "spy_5m_clean_ohlc_full",
    "spy_eod_full",
    "spy_daily_realized_variance_corsi_v1",
    "spy_daily_realized_variance_corsi_v1_sanitized",
    "spy_daily_realized_variance_corsi_v1_model_ready",
    "corsi_forward_realized_variance_targets_v1",
    "corsi_feature_target_panel_v1",
    "corsi_model_feature_panel_v1",
]

ROLE_RULES = {
    "setup_paths_theta_audit": [
        "setup", "ThetaData", "stock endpoint audit", "BASE_URL", "SPY_STOCK_VENUE",
    ],
    "fetch_5m_and_eod": [
        "monthly_clean_ohlc_list", "monthly_eod_list", "stock/history/ohlc", "stock/history/eod",
        "spy_5m_clean_ohlc_full", "spy_eod_full",
    ],
    "daily_rv_build": [
        "intraday_realized_variance_raw", "overnight_realized_variance_raw",
        "add_eod_and_overnight_variance", "spy_daily_rv_full",
    ],
    "sanitize_model_ready": [
        "spy_daily_realized_variance_corsi_v1_model_ready",
        "corsi_quality_status", "corsi_model_usable",
    ],
    "forward_targets": [
        "corsi_forward_realized_variance_targets_v1",
        "target_log_variance", "last_forward_rv_date",
    ],
    "feature_target_panel": [
        "corsi_feature_target_panel_v1",
        "feature_target_panel",
    ],
    "model_feature_panel": [
        "corsi_model_feature_panel_v1",
        "model_panel",
        "feature sets",
    ],
    "validation_output": [
        "validation", "manifest", "summary", "sample",
    ],
}


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def read_notebook(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Notebook not found: {path}")

    nb = json.loads(path.read_text(encoding="utf-8"))
    return nb.get("cells", [])


def source_text(cell: dict) -> str:
    return "".join(cell.get("source", []))


def classify_cell(src: str) -> str:
    roles = []
    lower_src = src.lower()

    for role, terms in ROLE_RULES.items():
        for term in terms:
            if term.lower() in lower_src:
                roles.append(role)
                break

    if not roles:
        return "other"

    return "|".join(roles)


def matched_terms(src: str) -> list[str]:
    lower_src = src.lower()
    return [t for t in CONTRACT_TERMS if t.lower() in lower_src]


def source_lines(src: str) -> list[str]:
    return src.splitlines()


def context_for_line(lines: list[str], line_no_1based: int, radius: int = 8) -> str:
    i = line_no_1based - 1
    lo = max(0, i - radius)
    hi = min(len(lines), i + radius + 1)
    return "\n".join(f"{j+1:05d}: {lines[j]}" for j in range(lo, hi))


def extract_matches(cell_idx: int, src: str) -> list[dict]:
    rows = []
    lines = source_lines(src)

    for term in CONTRACT_TERMS:
        term_lower = term.lower()
        for i, line in enumerate(lines, start=1):
            if term_lower in line.lower():
                rows.append({
                    "cell_index": cell_idx,
                    "matched_term": term,
                    "line_number": i,
                    "line": line,
                    "context": context_for_line(lines, i),
                })

    return rows


def extract_simple_assignments_from_ast(cell_idx: int, src: str) -> list[dict]:
    """
    Extract simple top-level assignment names and safe literal values where possible.
    This is intentionally conservative; no code execution.
    """
    rows = []

    try:
        tree = ast.parse(src)
    except Exception as exc:
        return [{
            "cell_index": cell_idx,
            "name": "__AST_PARSE_ERROR__",
            "kind": "parse_error",
            "value_repr": repr(exc),
            "line_number": np.nan,
        }]

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            names = []
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.append(target.id)
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            names.append(elt.id)

            if not names:
                continue

            try:
                value = ast.literal_eval(node.value)
                value_repr = repr(value)
                kind = type(value).__name__
            except Exception:
                value_repr = ast.unparse(node.value) if hasattr(ast, "unparse") else "<non_literal>"
                kind = "non_literal"

            for name in names:
                rows.append({
                    "cell_index": cell_idx,
                    "name": name,
                    "kind": kind,
                    "value_repr": value_repr[:2000],
                    "line_number": getattr(node, "lineno", np.nan),
                })

        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            try:
                value = ast.literal_eval(node.value)
                value_repr = repr(value)
                kind = type(value).__name__
            except Exception:
                value_repr = ast.unparse(node.value) if hasattr(ast, "unparse") else "<non_literal>"
                kind = "non_literal"

            rows.append({
                "cell_index": cell_idx,
                "name": node.target.id,
                "kind": kind,
                "value_repr": value_repr[:2000],
                "line_number": getattr(node, "lineno", np.nan),
            })

    return rows


def extract_functions_from_ast(cell_idx: int, src: str) -> list[dict]:
    rows = []

    try:
        tree = ast.parse(src)
    except Exception:
        return rows

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            args = []
            for arg in node.args.args:
                args.append(arg.arg)

            body_text = ""
            try:
                body_text = ast.get_source_segment(src, node) or ""
            except Exception:
                body_text = ""

            rows.append({
                "cell_index": cell_idx,
                "function_name": node.name,
                "args": "|".join(args),
                "line_number": getattr(node, "lineno", np.nan),
                "contains_thetadata": "theta" in body_text.lower(),
                "contains_request": "request" in body_text.lower() or "requests." in body_text.lower(),
                "contains_eod": "eod" in body_text.lower(),
                "contains_ohlc": "ohlc" in body_text.lower(),
                "contains_rv": "variance" in body_text.lower() or "_rv" in body_text.lower(),
                "source_preview": body_text[:2500],
            })

    return rows


def extract_output_writes(cell_idx: int, src: str) -> list[dict]:
    rows = []
    lines = source_lines(src)

    write_patterns = [
        ".to_parquet(",
        ".to_csv(",
        "json.dump(",
        ".write_text(",
        "open(",
    ]

    for i, line in enumerate(lines, start=1):
        lower_line = line.lower()
        if any(p.lower() in lower_line for p in write_patterns):
            context = context_for_line(lines, i, radius=10)

            output_stem_hits = [stem for stem in OUTPUT_STEMS if stem.lower() in context.lower()]

            rows.append({
                "cell_index": cell_idx,
                "line_number": i,
                "write_line": line.strip(),
                "output_stem_hits": "|".join(output_stem_hits),
                "context": context,
            })

    return rows


def extract_endpoint_contexts(cell_idx: int, src: str) -> list[dict]:
    rows = []
    lines = source_lines(src)

    endpoint_patterns = [
        "stock/history/eod",
        "stock/history/ohlc",
        "v3/stock/history/eod",
        "v2/hist/stock/ohlc",
        "BASE_URL",
        "THETADATA",
        "params",
        "requests.get",
    ]

    for i, line in enumerate(lines, start=1):
        if any(p.lower() in line.lower() for p in endpoint_patterns):
            rows.append({
                "cell_index": cell_idx,
                "line_number": i,
                "line": line.strip(),
                "context": context_for_line(lines, i, radius=10),
            })

    return rows


def extract_date_controls(cell_idx: int, src: str) -> list[dict]:
    rows = []
    lines = source_lines(src)

    date_patterns = [
        "START_DATE",
        "END_DATE",
        "start_date",
        "end_date",
        "safe_start",
        "safe_end",
        "strftime(\"%Y%m%d\")",
        "strftime('%Y%m%d')",
        "MonthEnd",
        "date_range",
        "chunk",
        "2018",
        "2026",
    ]

    for i, line in enumerate(lines, start=1):
        if any(p.lower() in line.lower() for p in date_patterns):
            rows.append({
                "cell_index": cell_idx,
                "line_number": i,
                "line": line.strip(),
                "context": context_for_line(lines, i, radius=8),
            })

    return rows


def score_cell(src: str, terms: list[str], role: str) -> int:
    score = len(terms)

    for stem in OUTPUT_STEMS:
        if stem.lower() in src.lower():
            score += 15

    if "requests.get" in src or "http" in src.lower():
        score += 10

    if ".to_parquet(" in src or ".to_csv(" in src:
        score += 10

    if role != "other":
        score += 8

    return int(score)


def build_sequential_run_map(cell_inventory: pd.DataFrame) -> pd.DataFrame:
    """
    Suggest likely cell groups needed to rerun upstream source extension.
    This is heuristic and read-only.
    """
    wanted_roles = [
        "setup_paths_theta_audit",
        "fetch_5m_and_eod",
        "daily_rv_build",
        "sanitize_model_ready",
        "forward_targets",
        "feature_target_panel",
        "model_feature_panel",
    ]

    rows = []
    for role in wanted_roles:
        hit = cell_inventory[cell_inventory["role"].astype(str).str.contains(role, regex=False)].copy()
        if hit.empty:
            rows.append({
                "role": role,
                "cell_indices": "",
                "min_cell_index": np.nan,
                "max_cell_index": np.nan,
                "status": "MISSING",
                "note": "No matching cell found by heuristic.",
            })
        else:
            rows.append({
                "role": role,
                "cell_indices": "|".join(hit["cell_index"].astype(str).tolist()),
                "min_cell_index": int(hit["cell_index"].min()),
                "max_cell_index": int(hit["cell_index"].max()),
                "status": "FOUND",
                "note": "Review source excerpts before running.",
            })

    return pd.DataFrame(rows)


def main():
    section("VRP Corsi builder contract extract v1")
    print("Project root:", PROJECT_ROOT)
    print("Notebook:", NOTEBOOK_PATH)
    print("Run timestamp:", RUN_TS)
    print("Read-only: no ThetaData calls, no feature rebuild, no model fitting, no production/research output replacement.")

    cells = read_notebook(NOTEBOOK_PATH)

    section("Notebook loaded")
    print("Total cells:", len(cells))

    cell_rows = []
    all_matches = []
    all_assignments = []
    all_functions = []
    all_writes = []
    all_endpoints = []
    all_dates = []

    relevant_source_blocks = []

    for i, cell in enumerate(cells):
        src = source_text(cell)
        ctype = cell.get("cell_type", "")

        terms = matched_terms(src)
        role = classify_cell(src)
        score = score_cell(src, terms, role)

        lines = source_lines(src)

        cell_rows.append({
            "cell_index": i,
            "cell_type": ctype,
            "role": role,
            "score": score,
            "line_count": len(lines),
            "matched_term_count": len(terms),
            "matched_terms": "|".join(terms),
            "first_line": lines[0][:300] if lines else "",
        })

        if terms or role != "other":
            all_matches.extend(extract_matches(i, src))
            all_assignments.extend(extract_simple_assignments_from_ast(i, src) if ctype == "code" else [])
            all_functions.extend(extract_functions_from_ast(i, src) if ctype == "code" else [])
            all_writes.extend(extract_output_writes(i, src) if ctype == "code" else [])
            all_endpoints.extend(extract_endpoint_contexts(i, src))
            all_dates.extend(extract_date_controls(i, src))

            relevant_source_blocks.append(
                "\n" + "=" * 120
                + f"\nCELL_INDEX={i} | TYPE={ctype} | ROLE={role} | SCORE={score}"
                + f"\nMATCHED_TERMS={ '|'.join(terms) }"
                + "\n" + "=" * 120
                + "\n"
                + src
            )

    cell_inventory = pd.DataFrame(cell_rows).sort_values(
        ["score", "matched_term_count", "cell_index"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    matches = pd.DataFrame(all_matches)
    assignments = pd.DataFrame(all_assignments)
    functions = pd.DataFrame(all_functions)
    writes = pd.DataFrame(all_writes)
    endpoints = pd.DataFrame(all_endpoints)
    date_controls = pd.DataFrame(all_dates)

    sequential_map = build_sequential_run_map(cell_inventory)

    # Filter assignment table to contract-relevant names.
    assignment_keep_regex = re.compile(
        r"(ROOT|DIR|URL|BASE|THETA|SPY|DATE|START|END|CHUNK|INTERVAL|VENUE|TIMESTAMP|PROCESSED|AUDIT|RAW|OUTPUT)",
        re.IGNORECASE,
    )
    if not assignments.empty:
        assignments_relevant = assignments[
            assignments["name"].astype(str).str.contains(assignment_keep_regex, regex=True)
        ].copy()
    else:
        assignments_relevant = pd.DataFrame(columns=assignments.columns)

    # Save outputs.
    cell_inventory_path = OUT_DIR / f"corsi_builder_contract_cell_inventory_{RUN_TS}.csv"
    matches_path = OUT_DIR / f"corsi_builder_contract_term_matches_{RUN_TS}.csv"
    assignments_path = OUT_DIR / f"corsi_builder_contract_assignments_relevant_{RUN_TS}.csv"
    functions_path = OUT_DIR / f"corsi_builder_contract_functions_{RUN_TS}.csv"
    writes_path = OUT_DIR / f"corsi_builder_contract_output_writes_{RUN_TS}.csv"
    endpoints_path = OUT_DIR / f"corsi_builder_contract_endpoint_contexts_{RUN_TS}.csv"
    dates_path = OUT_DIR / f"corsi_builder_contract_date_controls_{RUN_TS}.csv"
    run_map_path = OUT_DIR / f"corsi_builder_contract_sequential_run_map_{RUN_TS}.csv"
    relevant_source_path = OUT_DIR / f"corsi_builder_contract_relevant_source_{RUN_TS}.txt"
    manifest_path = OUT_DIR / f"corsi_builder_contract_manifest_{RUN_TS}.json"

    cell_inventory.to_csv(cell_inventory_path, index=False)
    matches.to_csv(matches_path, index=False)
    assignments_relevant.to_csv(assignments_path, index=False)
    functions.to_csv(functions_path, index=False)
    writes.to_csv(writes_path, index=False)
    endpoints.to_csv(endpoints_path, index=False)
    date_controls.to_csv(dates_path, index=False)
    sequential_map.to_csv(run_map_path, index=False)
    relevant_source_path.write_text("\n".join(relevant_source_blocks), encoding="utf-8")

    manifest = {
        "run_ts": RUN_TS,
        "project_root": str(PROJECT_ROOT),
        "notebook_path": str(NOTEBOOK_PATH),
        "read_only": True,
        "outputs": {
            "cell_inventory": str(cell_inventory_path),
            "term_matches": str(matches_path),
            "assignments_relevant": str(assignments_path),
            "functions": str(functions_path),
            "output_writes": str(writes_path),
            "endpoint_contexts": str(endpoints_path),
            "date_controls": str(dates_path),
            "sequential_run_map": str(run_map_path),
            "relevant_source": str(relevant_source_path),
        },
        "notes": [
            "This script does not execute the upstream notebook.",
            "This script does not call ThetaData.",
            "This script does not write forecast_model_corsi_v1 outputs.",
            "This script only extracts the builder contract for review.",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    section("Likely sequential run map")
    print(sequential_map.to_string(index=False))

    section("Top relevant cells")
    top_cols = ["cell_index", "cell_type", "role", "score", "line_count", "matched_term_count", "matched_terms", "first_line"]
    print(cell_inventory[top_cols].head(30).to_string(index=False))

    section("Relevant assignments / constants")
    if not assignments_relevant.empty:
        print(assignments_relevant.sort_values(["cell_index", "line_number"]).head(120).to_string(index=False))
    else:
        print("No relevant assignments extracted.")

    section("Functions likely relevant")
    if not functions.empty:
        display_cols = [
            "cell_index", "function_name", "args", "line_number",
            "contains_thetadata", "contains_request", "contains_eod", "contains_ohlc", "contains_rv",
        ]
        print(functions[display_cols].sort_values(["cell_index", "line_number"]).head(120).to_string(index=False))
    else:
        print("No function definitions extracted.")

    section("Output writes")
    if not writes.empty:
        print(writes[["cell_index", "line_number", "write_line", "output_stem_hits"]].sort_values(["cell_index", "line_number"]).to_string(index=False))
    else:
        print("No output writes extracted.")

    section("Endpoint contexts")
    if not endpoints.empty:
        print(endpoints[["cell_index", "line_number", "line"]].sort_values(["cell_index", "line_number"]).head(120).to_string(index=False))
    else:
        print("No endpoint contexts extracted.")

    section("Date controls")
    if not date_controls.empty:
        print(date_controls[["cell_index", "line_number", "line"]].sort_values(["cell_index", "line_number"]).head(160).to_string(index=False))
    else:
        print("No date controls extracted.")

    section("Saved audit files")
    for p in [
        cell_inventory_path,
        matches_path,
        assignments_path,
        functions_path,
        writes_path,
        endpoints_path,
        dates_path,
        run_map_path,
        relevant_source_path,
        manifest_path,
    ]:
        print(p)

    section("DONE")
    print("READ_ONLY_CONTRACT_EXTRACT_COMPLETE: True")
    print("Next step: review the run map and relevant source, then decide whether to extract a production wrapper or run selected notebook cells.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
