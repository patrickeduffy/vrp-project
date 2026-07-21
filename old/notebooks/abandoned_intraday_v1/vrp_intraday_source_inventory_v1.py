
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


EXPECTED_COMPONENTS = [
    "vix_style_implied_variance_construction",
    "thetadata_option_chain_pull",
    "intraday_spx_spy_price_pull",
    "corsi_fds_feature_construction",
    "forecast_model_fit_or_scoring",
    "forecast_model_coefficients_or_reproducible_fit",
    "rsi14_formula",
    "rv21d_formula",
    "final_signal_threshold_sizing_selection",
    "existing_intraday_data_outputs",
]


CATEGORY_TERMS = {
    "vix_style_implied_variance_construction": [
        "vix_style",
        "vix style",
        "implied_variance",
        "implied variance",
        "variance swap",
        "variance_swap",
        "spx_vix_style",
        "deltak",
        "forward price",
        "risk free",
        "risk_free",
        "target_days",
        "target tenor",
    ],
    "thetadata_option_chain_pull": [
        "thetadata",
        "theta data",
        "option chain",
        "option_chain",
        "root=SPX",
        "root=\"SPX\"",
        "expirations",
        "strikes",
        "quote",
        "bid",
        "ask",
        "bulk_hist",
        "snapshot",
        "spxw",
    ],
    "intraday_spx_spy_price_pull": [
        "intraday",
        "minute",
        "5min",
        "5-minute",
        "ohlc",
        "bar",
        "bars",
        "price through",
        "live price",
        "quote",
        "SPY",
        "SPX",
    ],
    "corsi_fds_feature_construction": [
        "corsi",
        "fds",
        "unified_fds",
        "candidate_log_downside_rv_5d",
        "candidate_log_downside_rv_10d",
        "candidate_log_downside_rv_21d",
        "candidate_log_downside_rv_63d",
        "candidate_downside_share_5d",
        "candidate_downside_share_10d",
        "candidate_max_abs_return_3d",
        "candidate_max_abs_return_5d",
        "candidate_max_abs_return_10d",
        "feature_panel",
    ],
    "forecast_model_fit_or_scoring": [
        "Ridge",
        "StandardScaler",
        "Pipeline",
        "fit_log",
        "alpha",
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
        "target_log_variance",
        "last_forward_rv_date",
    ],
    "forecast_model_coefficients_or_reproducible_fit": [
        "coef_",
        "intercept_",
        "joblib",
        "pickle",
        ".pkl",
        ".pickle",
        ".joblib",
        "model_artifact",
        "coefficients",
        "Ridge",
        "StandardScaler",
        "fit_log",
    ],
    "rsi14_formula": [
        "rsi14",
        "RSI14",
        "relative strength",
        "avg_gain",
        "avg_loss",
        "wilder",
    ],
    "rv21d_formula": [
        "rv21d",
        "rv21d_vol_pct",
        "realized_vol_history",
        "21d",
        "sqrt(252)",
        "rolling(21)",
        "rolling(window=21)",
        "log_return",
    ],
    "final_signal_threshold_sizing_selection": [
        "core_pass",
        "secondary_pass",
        "core_signal",
        "secondary_signal",
        "selected_trade",
        "selected_size_pct",
        "selected_sleeve_id",
        "z_3m_final",
        "z_1y_final",
        "model_vrp_log_final",
        "LOCKED_SIZE",
        "selected_layer",
    ],
    "existing_intraday_data_outputs": [
        "intraday",
        "live",
        "snapshot",
        "asof",
        "as_of",
        "minute",
        "5min",
    ],
}


TEXT_EXTENSIONS = {
    ".py",
    ".ipynb",
    ".txt",
    ".md",
    ".json",
    ".bat",
    ".ps1",
    ".yaml",
    ".yml",
}

SKIP_DIR_NAMES = {
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    ".ipynb_checkpoints",
    ".venv",
    "venv",
    "env",
    "node_modules",
}


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def normalized_text(s: str) -> str:
    return s.lower()


def match_categories(text: str, file_name: str = "") -> dict[str, list[str]]:
    haystack = normalized_text(file_name + "\n" + text)
    out: dict[str, list[str]] = {}

    for cat, terms in CATEGORY_TERMS.items():
        hits = []
        for term in terms:
            if term.lower() in haystack:
                hits.append(term)
        if hits:
            out[cat] = sorted(set(hits), key=str.lower)

    return out


def should_skip_dir(path: Path) -> bool:
    return any(part in SKIP_DIR_NAMES for part in path.parts)


def discover_files(project_root: Path) -> tuple[list[Path], list[Path], list[Path]]:
    text_files = []
    parquet_files = []
    other_candidate_files = []

    for dirpath, dirnames, filenames in os.walk(project_root):
        d = Path(dirpath)

        dirnames[:] = [x for x in dirnames if x not in SKIP_DIR_NAMES]

        if should_skip_dir(d):
            continue

        for name in filenames:
            p = d / name
            suffix = p.suffix.lower()

            if suffix in TEXT_EXTENSIONS:
                # Avoid reading thousands of audit JSON/CSV-like files unless in notebooks/root.
                if "data" in p.parts and suffix in {".json", ".txt"}:
                    if "audit" in p.parts:
                        other_candidate_files.append(p)
                        continue
                text_files.append(p)

            elif suffix == ".parquet":
                parquet_files.append(p)

            else:
                lower = name.lower()
                if any(x in lower for x in ["intraday", "theta", "model", "coef", "joblib", "pickle", ".pkl"]):
                    other_candidate_files.append(p)

    return text_files, parquet_files, other_candidate_files


def scan_text_files(project_root: Path, text_files: list[Path], max_text_mb: float) -> pd.DataFrame:
    rows = []
    max_bytes = int(max_text_mb * 1024 * 1024)

    for p in text_files:
        try:
            stat = p.stat()
        except Exception:
            continue

        if stat.st_size > max_bytes:
            rows.append({
                "file_path": str(p),
                "relative_path": safe_rel(p, project_root),
                "extension": p.suffix.lower(),
                "size_mb": stat.st_size / (1024 * 1024),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                "scan_status": "SKIPPED_TOO_LARGE",
                "categories": "",
                "matched_terms": "",
                "match_count": 0,
            })
            continue

        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            status = "SCANNED"
        except Exception as exc:
            text = ""
            status = f"READ_ERROR: {exc}"

        matches = match_categories(text, p.name)

        rows.append({
            "file_path": str(p),
            "relative_path": safe_rel(p, project_root),
            "extension": p.suffix.lower(),
            "size_mb": stat.st_size / (1024 * 1024),
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            "scan_status": status,
            "categories": "|".join(matches.keys()),
            "matched_terms": json.dumps(matches, sort_keys=True),
            "match_count": sum(len(v) for v in matches.values()),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["match_count", "size_mb"], ascending=[False, True])


def scan_parquet_schemas(project_root: Path, parquet_files: list[Path]) -> pd.DataFrame:
    rows = []

    try:
        import pyarrow.parquet as pq
        pyarrow_available = True
    except Exception:
        pq = None
        pyarrow_available = False

    for p in parquet_files:
        stat = None
        try:
            stat = p.stat()
        except Exception:
            pass

        columns: list[str] = []
        num_rows = None
        scan_status = "SCANNED"

        try:
            if pyarrow_available:
                pf = pq.ParquetFile(p)
                columns = list(pf.schema.names)
                num_rows = pf.metadata.num_rows if pf.metadata is not None else None
            else:
                # fallback reads only zero rows when possible
                df0 = pd.read_parquet(p).head(0)
                columns = list(df0.columns)
                num_rows = None
                scan_status = "SCANNED_PANDAS_FALLBACK"
        except Exception as exc:
            scan_status = f"SCHEMA_READ_ERROR: {exc}"

        column_text = " ".join(columns)
        matches = match_categories(column_text, p.name)

        rows.append({
            "file_path": str(p),
            "relative_path": safe_rel(p, project_root),
            "extension": ".parquet",
            "size_mb": (stat.st_size / (1024 * 1024)) if stat else None,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds") if stat else "",
            "scan_status": scan_status,
            "num_rows": num_rows,
            "num_columns": len(columns),
            "categories": "|".join(matches.keys()),
            "matched_terms": json.dumps(matches, sort_keys=True),
            "matched_columns": "|".join([c for c in columns if any(t.lower() in c.lower() for terms in CATEGORY_TERMS.values() for t in terms)]),
            "match_count": sum(len(v) for v in matches.values()),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["match_count", "size_mb"], ascending=[False, True])


def scan_other_candidates(project_root: Path, files: list[Path]) -> pd.DataFrame:
    rows = []

    for p in files:
        try:
            stat = p.stat()
        except Exception:
            continue

        matches = match_categories("", p.name)

        rows.append({
            "file_path": str(p),
            "relative_path": safe_rel(p, project_root),
            "extension": p.suffix.lower(),
            "size_mb": stat.st_size / (1024 * 1024),
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            "categories": "|".join(matches.keys()),
            "matched_terms": json.dumps(matches, sort_keys=True),
            "match_count": sum(len(v) for v in matches.values()),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(["match_count", "size_mb"], ascending=[False, True])


def top_candidates_for_category(
    text_df: pd.DataFrame,
    parquet_df: pd.DataFrame,
    other_df: pd.DataFrame,
    category: str,
    n: int = 8,
) -> list[str]:
    candidates = []

    for source_name, df in [
        ("text", text_df),
        ("parquet_schema", parquet_df),
        ("other", other_df),
    ]:
        if df.empty or "categories" not in df.columns:
            continue

        hit = df[df["categories"].fillna("").str.contains(category, regex=False)].copy()
        if hit.empty:
            continue

        hit = hit.sort_values("match_count", ascending=False).head(n)

        for _, r in hit.iterrows():
            candidates.append(f"{source_name}: {r.get('relative_path')}")

    return candidates[:n]


def determine_readiness(
    text_df: pd.DataFrame,
    parquet_df: pd.DataFrame,
    other_df: pd.DataFrame,
) -> tuple[pd.DataFrame, bool]:
    rows = []

    component_requirements = {
        "vix_style_implied_variance_construction": {
            "required": True,
            "description": "Code/schema that builds VIX-style implied variance by tenor.",
        },
        "thetadata_option_chain_pull": {
            "required": True,
            "description": "ThetaData option chain / quote pull path for SPX/SPXW.",
        },
        "intraday_spx_spy_price_pull": {
            "required": True,
            "description": "Live or intraday SPX/SPY price pull needed for intraday RSI/RV/features.",
        },
        "corsi_fds_feature_construction": {
            "required": True,
            "description": "Locked Corsi/FDS feature construction contract.",
        },
        "forecast_model_fit_or_scoring": {
            "required": True,
            "description": "Code that fits or scores the locked forecast variance model.",
        },
        "forecast_model_coefficients_or_reproducible_fit": {
            "required": True,
            "description": "Saved coefficients OR reproducible locked fit/scoring path.",
        },
        "rsi14_formula": {
            "required": True,
            "description": "RSI14 formula source.",
        },
        "rv21d_formula": {
            "required": True,
            "description": "RV21D formula source.",
        },
        "final_signal_threshold_sizing_selection": {
            "required": True,
            "description": "Core/Secondary thresholds, sizing, and selection logic.",
        },
        "existing_intraday_data_outputs": {
            "required": False,
            "description": "Existing intraday data/snapshot outputs, optional but useful.",
        },
    }

    for component in EXPECTED_COMPONENTS:
        candidates = top_candidates_for_category(text_df, parquet_df, other_df, component, n=8)
        required = component_requirements[component]["required"]

        ready = len(candidates) > 0

        if component == "forecast_model_coefficients_or_reproducible_fit":
            # This component can be satisfied by actual coefficients/artifacts OR reproducible fit/scoring code.
            model_artifact_terms = [".pkl", ".pickle", ".joblib", "coef", "coefficient", "model_artifact"]
            artifact_hits = []

            for df in [other_df, text_df]:
                if df.empty:
                    continue
                for _, r in df.iterrows():
                    rel = str(r.get("relative_path", "")).lower()
                    terms = str(r.get("matched_terms", "")).lower()
                    if any(t in rel or t in terms for t in model_artifact_terms):
                        artifact_hits.append(str(r.get("relative_path")))

            if artifact_hits:
                candidates = [f"artifact_or_code: {x}" for x in artifact_hits[:8]] + candidates
                ready = True
            elif len(candidates) > 0:
                ready = True

        if required and not ready:
            status = "BLOCKING_GAP"
            reason = "No candidate found."
        elif required and ready:
            status = "FOUND"
            reason = "Candidate source(s) found; review required before coding intraday."
        elif not required and ready:
            status = "OPTIONAL_FOUND"
            reason = "Optional candidate source(s) found."
        else:
            status = "OPTIONAL_MISSING"
            reason = "Optional; not a blocker."

        rows.append({
            "component": component,
            "required_for_intraday": required,
            "status": status,
            "description": component_requirements[component]["description"],
            "candidate_count": len(candidates),
            "top_candidates": "\n".join(candidates),
            "reason": reason,
        })

    readiness = pd.DataFrame(rows)
    ready_for_intraday_build = not (
        readiness["required_for_intraday"]
        & readiness["status"].eq("BLOCKING_GAP")
    ).any()

    return readiness, bool(ready_for_intraday_build)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    parser.add_argument("--max-text-mb", type=float, default=8.0)
    args = parser.parse_args()

    project_root = Path(args.project_root)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    audit_dir = project_root / "data" / "audit" / "intraday_source_inventory"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday source inventory v1")
    print(f"Project root: {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Audit dir: {audit_dir}")
    print(f"Max text file scan size MB: {args.max_text_mb}")

    banner("Discovering files")
    text_files, parquet_files, other_files = discover_files(project_root)

    print(f"Text/code-like files discovered: {len(text_files)}")
    print(f"Parquet files discovered:        {len(parquet_files)}")
    print(f"Other candidate files:           {len(other_files)}")

    banner("Scanning text/code files")
    text_df = scan_text_files(project_root, text_files, max_text_mb=args.max_text_mb)
    text_path = audit_dir / f"intraday_source_inventory_text_hits_{run_ts}.csv"
    text_df.to_csv(text_path, index=False)
    print(f"Saved text/code scan: {text_path}")

    if not text_df.empty:
        print("\nTop text/code candidates:")
        print(
            text_df[
                ["relative_path", "extension", "size_mb", "categories", "match_count"]
            ]
            .head(25)
            .to_string(index=False)
        )

    banner("Scanning parquet schemas")
    parquet_df = scan_parquet_schemas(project_root, parquet_files)
    parquet_path = audit_dir / f"intraday_source_inventory_parquet_schema_hits_{run_ts}.csv"
    parquet_df.to_csv(parquet_path, index=False)
    print(f"Saved parquet schema scan: {parquet_path}")

    if not parquet_df.empty:
        print("\nTop parquet schema candidates:")
        print(
            parquet_df[
                ["relative_path", "num_rows", "num_columns", "categories", "match_count"]
            ]
            .head(25)
            .to_string(index=False)
        )

    banner("Scanning other candidate files")
    other_df = scan_other_candidates(project_root, other_files)
    other_path = audit_dir / f"intraday_source_inventory_other_candidates_{run_ts}.csv"
    other_df.to_csv(other_path, index=False)
    print(f"Saved other candidate scan: {other_path}")

    if not other_df.empty:
        print("\nTop other candidates:")
        print(
            other_df[
                ["relative_path", "extension", "size_mb", "categories", "match_count"]
            ]
            .head(25)
            .to_string(index=False)
        )

    banner("Readiness assessment")
    readiness_df, ready_for_intraday_build = determine_readiness(text_df, parquet_df, other_df)
    readiness_path = audit_dir / f"intraday_source_inventory_readiness_{run_ts}.csv"
    readiness_df.to_csv(readiness_path, index=False)

    print(readiness_df.to_string(index=False))
    print()
    print(f"READY_FOR_INTRADAY_BUILD: {ready_for_intraday_build}")

    blocking = readiness_df[
        readiness_df["required_for_intraday"] & readiness_df["status"].eq("BLOCKING_GAP")
    ]

    if not blocking.empty:
        print("\nBlocking gaps:")
        print(blocking[["component", "reason"]].to_string(index=False))
    else:
        print("\nNo blocking gaps found by inventory scan.")
        print("Manual review of candidate source files is still required before writing intraday signal code.")

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "audit_dir": str(audit_dir),
        "max_text_mb": args.max_text_mb,
        "text_files_discovered": len(text_files),
        "parquet_files_discovered": len(parquet_files),
        "other_candidate_files_discovered": len(other_files),
        "text_scan_csv": str(text_path),
        "parquet_schema_scan_csv": str(parquet_path),
        "other_candidate_scan_csv": str(other_path),
        "readiness_csv": str(readiness_path),
        "ready_for_intraday_build": ready_for_intraday_build,
        "blocking_components": blocking["component"].tolist(),
        "method_note": "Read-only source inventory. No production data or models modified.",
    }

    manifest_path = audit_dir / f"intraday_source_inventory_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    banner("Saved audit outputs")
    print(f"text_scan:       {text_path}")
    print(f"parquet_scan:    {parquet_path}")
    print(f"other_scan:      {other_path}")
    print(f"readiness:       {readiness_path}")
    print(f"manifest:        {manifest_path}")

    banner("Final result")
    print(f"READY_FOR_INTRADAY_BUILD: {ready_for_intraday_build}")
    print("DONE — read-only intraday source inventory complete.")


if __name__ == "__main__":
    main()
