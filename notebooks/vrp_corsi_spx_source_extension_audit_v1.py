
from pathlib import Path
from datetime import datetime
import json
import re
import traceback

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "vrp_core_bucket_parameters_v1" / "vrp_unified_fds_no_min_return_locked_model_clean.ipynb"

CORSI_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "forecast_model_corsi_v1"
CORSI_AUDIT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_model_corsi_v1"
BRANCH_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1"
MARKET_DATA_DIR = PROJECT_ROOT / "data" / "processed" / "market_data"

OUT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
OUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
TARGET_DATE = pd.Timestamp("2026-07-06")

KEY_COLS = [
    "trade_date",
    "date",
    "spx_close",
    "spx_log_return",
    "spx_total_return",
    "spy_total_return",
    "spy_total_rv_raw",
    "spy_downside_rv_raw",
    "spy_total_rv_1d_ann",
    "spy_downside_rv_1d_ann",
    "spy_downside_rv_mean_5d_ann",
    "log_spy_downside_rv_mean_5d_ann",
    "spx_close_for_features",
    "candidate_log_downside_rv_5d",
    "candidate_downside_share_5d",
    "candidate_max_abs_return_5d",
]


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_date_series(s: pd.Series) -> pd.Series:
    """
    Robustly parse either YYYYMMDD int/string or normal date-like values.
    """
    raw = s.copy()

    nonnull = raw.dropna()
    if len(nonnull) == 0:
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = nonnull.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    yyyymmdd_like = as_str.str.fullmatch(r"\d{8}").mean() > 0.8

    if yyyymmdd_like:
        parsed = pd.to_datetime(raw.astype(str).str.replace(r"\.0$", "", regex=True), format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, errors="coerce")

    return parsed.dt.normalize()


def latest_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def safe_read_table(path: Path, max_rows: int | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        if max_rows is None:
            return pd.read_csv(path)
        return pd.read_csv(path, nrows=max_rows)
    raise ValueError(f"Unsupported table type: {path}")


def summarize_table(path: Path) -> dict:
    row = {
        "path": str(path),
        "exists": path.exists(),
        "suffix": path.suffix.lower(),
        "read_ok": False,
        "rows": np.nan,
        "cols": np.nan,
        "date_col_used": None,
        "min_date": None,
        "max_date": None,
        "has_target_date": False,
        "has_spx_log_return": False,
        "has_spx_close": False,
        "has_spy_total_return": False,
        "has_corsi_quality_status": False,
        "has_intraday_raw": False,
        "has_overnight_raw": False,
        "candidate_relevance_score": 0,
        "columns": "",
        "error": "",
    }

    try:
        df = safe_read_table(path)
        row["read_ok"] = True
        row["rows"] = len(df)
        row["cols"] = len(df.columns)
        row["columns"] = "|".join(map(str, df.columns))

        for c in ["trade_date", "date"]:
            if c in df.columns:
                d = parse_date_series(df[c])
                if d.notna().any():
                    row["date_col_used"] = c
                    row["min_date"] = d.min().date().isoformat()
                    row["max_date"] = d.max().date().isoformat()
                    row["has_target_date"] = bool((d == TARGET_DATE).any())
                    break

        cols = set(map(str, df.columns))
        row["has_spx_log_return"] = "spx_log_return" in cols
        row["has_spx_close"] = "spx_close" in cols
        row["has_spy_total_return"] = "spy_total_return" in cols
        row["has_corsi_quality_status"] = "corsi_quality_status" in cols
        row["has_intraday_raw"] = any("intraday" in c.lower() and "raw" in c.lower() for c in cols)
        row["has_overnight_raw"] = any("overnight" in c.lower() and "raw" in c.lower() for c in cols)

        score = 0
        for flag in [
            "has_spx_log_return",
            "has_spx_close",
            "has_spy_total_return",
            "has_corsi_quality_status",
            "has_intraday_raw",
            "has_overnight_raw",
            "has_target_date",
        ]:
            score += int(bool(row[flag]))
        row["candidate_relevance_score"] = score

    except Exception as exc:
        row["error"] = repr(exc)

    return row


def extract_cell4_source() -> tuple[str, list[dict]]:
    if not NOTEBOOK_PATH.exists():
        raise FileNotFoundError(f"Notebook not found: {NOTEBOOK_PATH}")

    nb = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    cells = nb.get("cells", [])

    # Cell 4 in notebook display is usually code cell index 4, but search defensively.
    selected = None
    selected_i = None
    for i, cell in enumerate(cells):
        src = "".join(cell.get("source", []))
        if "Cell 4" in src and "Construct candidate date-level features" in src:
            selected = src
            selected_i = i
            break

    if selected is None:
        for i, cell in enumerate(cells):
            src = "".join(cell.get("source", []))
            if "feature_daily = spx_daily.copy()" in src:
                selected = src
                selected_i = i
                break

    if selected is None:
        raise RuntimeError("Could not locate Cell 4 source in notebook.")

    lines = selected.splitlines()
    patterns = [
        "spx_daily",
        "spx_source",
        "spx_log_return",
        "daily SPX",
        "SOURCE_CORSI_PROCESSED_DIR",
        "corsi_model_feature_panel",
        "candidate_log_downside_rv",
        "feature_daily",
    ]

    contexts = []
    for j, line in enumerate(lines, start=1):
        if any(p in line for p in patterns):
            lo = max(1, j - 8)
            hi = min(len(lines), j + 12)
            contexts.append({
                "cell_index": selected_i,
                "line": j,
                "matched_line": line,
                "context": "\n".join(f"{k:04d}: {lines[k-1]}" for k in range(lo, hi + 1)),
            })

    return selected, contexts


def print_contexts(contexts: list[dict], max_contexts: int = 80) -> None:
    for item in contexts[:max_contexts]:
        print("\n" + "-" * 100)
        print(f"cell_index={item['cell_index']} line={item['line']}")
        print("-" * 100)
        print(item["context"])


def make_one_row_per_date(df: pd.DataFrame, date_col: str = "trade_date") -> pd.DataFrame:
    out = df.copy()
    out[date_col] = parse_date_series(out[date_col])
    sort_cols = [date_col]
    if "tenor" in out.columns:
        sort_cols.append("tenor")
    out = out.sort_values(sort_cols).drop_duplicates(date_col).reset_index(drop=True)
    return out


def main():
    section("VRP Corsi/SPX source extension audit v1")
    print("Project root:", PROJECT_ROOT)
    print("Run timestamp:", RUN_TS)
    print("Target date:", TARGET_DATE.date())

    section("Extract Cell 4 source-selection code")
    cell4_source, contexts = extract_cell4_source()

    cell4_source_path = OUT_DIR / f"cell4_candidate_feature_source_full_{RUN_TS}.txt"
    context_path = OUT_DIR / f"cell4_candidate_feature_source_contexts_{RUN_TS}.json"

    cell4_source_path.write_text(cell4_source, encoding="utf-8")
    with open(context_path, "w", encoding="utf-8") as f:
        json.dump(contexts, f, indent=2)

    print("Notebook:", NOTEBOOK_PATH)
    print("Saved full Cell 4 source:", cell4_source_path)
    print("Saved Cell 4 source contexts:", context_path)
    print_contexts(contexts)

    section("Inventory candidate Corsi/SPX source tables")

    candidate_paths = []
    for directory in [
        CORSI_PROCESSED_DIR,
        CORSI_AUDIT_DIR,
        BRANCH_PROCESSED_DIR,
        MARKET_DATA_DIR,
    ]:
        if directory.exists():
            candidate_paths.extend(directory.glob("*.parquet"))
            candidate_paths.extend(directory.glob("*.csv"))

    # Keep files likely relevant to source/features/returns, plus canonical market data for contrast.
    keep_terms = [
        "feature",
        "target",
        "corsi",
        "realized",
        "rv",
        "market",
        "spy",
        "spx",
        "panel",
    ]

    filtered = []
    for p in candidate_paths:
        name = p.name.lower()
        if any(t in name for t in keep_terms):
            filtered.append(p)

    summaries = []
    for p in sorted(set(filtered)):
        summaries.append(summarize_table(p))

    inv = pd.DataFrame(summaries)
    inv = inv.sort_values(
        ["candidate_relevance_score", "has_target_date", "max_date", "rows"],
        ascending=[False, False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    inventory_path = OUT_DIR / f"corsi_spx_source_candidate_inventory_{RUN_TS}.csv"
    inv.to_csv(inventory_path, index=False)

    display_cols = [
        "candidate_relevance_score",
        "path",
        "rows",
        "cols",
        "date_col_used",
        "min_date",
        "max_date",
        "has_target_date",
        "has_spx_log_return",
        "has_spx_close",
        "has_spy_total_return",
        "has_corsi_quality_status",
        "has_intraday_raw",
        "has_overnight_raw",
        "read_ok",
        "error",
    ]

    print(inv[display_cols].head(60).to_string(index=False))
    print("\nSaved inventory:", inventory_path)

    section("Compare key current sources")

    old_corsi_path = latest_file(CORSI_PROCESSED_DIR, "corsi_model_feature_panel_v1_*.parquet")
    old_target_path = latest_file(CORSI_PROCESSED_DIR, "corsi_forward_realized_variance_targets_v1_*.parquet")
    old_feature_target_path = latest_file(CORSI_PROCESSED_DIR, "corsi_feature_target_panel_v1_*.parquet")
    cell4_path = latest_file(BRANCH_PROCESSED_DIR, "04_front_middle_candidate_feature_panel_*.parquet")
    market_path = MARKET_DATA_DIR / "spy_corsi_har_input_panel_v1.parquet"

    compare_paths = {
        "old_corsi_model_feature_panel": old_corsi_path,
        "old_corsi_forward_targets": old_target_path,
        "old_corsi_feature_target_panel": old_feature_target_path,
        "cell4_candidate_feature_panel": cell4_path,
        "new_market_spy_corsi_har_input": market_path,
    }

    for label, path in compare_paths.items():
        print(f"{label}: {path} exists={path.exists() if path is not None else False}")

    source_rows = []

    for label, path in compare_paths.items():
        if path is None or not path.exists():
            continue

        df = safe_read_table(path)

        date_col = None
        for c in ["trade_date", "date"]:
            if c in df.columns:
                parsed = parse_date_series(df[c])
                if parsed.notna().any():
                    df = df.copy()
                    df["_parsed_date"] = parsed
                    date_col = "_parsed_date"
                    break

        if date_col is None:
            continue

        one = df.sort_values(date_col).drop_duplicates(date_col).copy()

        available_cols = [c for c in KEY_COLS if c in one.columns]
        subset = one[
            (one[date_col] >= pd.Timestamp("2026-06-20"))
            & (one[date_col] <= TARGET_DATE)
        ][available_cols + [date_col]].copy()

        subset = subset.rename(columns={date_col: "parsed_trade_date"})
        subset.insert(0, "source_label", label)
        subset.insert(1, "source_path", str(path))

        source_rows.append(subset)

    if source_rows:
        source_compare = pd.concat(source_rows, ignore_index=True)
    else:
        source_compare = pd.DataFrame()

    source_compare_path = OUT_DIR / f"corsi_spx_source_recent_key_rows_{RUN_TS}.csv"
    source_compare.to_csv(source_compare_path, index=False)

    print("\nRecent key rows by source:")
    if not source_compare.empty:
        print(source_compare.to_string(index=False))
    else:
        print("No comparable source rows found.")
    print("\nSaved source comparison:", source_compare_path)

    section("Cell 4 source extensibility conclusion inputs")

    # Determine best candidate that has spx_log_return and max date.
    spx_candidates = inv[
        inv["read_ok"].eq(True)
        & inv["has_spx_log_return"].eq(True)
        & inv["max_date"].notna()
    ].copy()

    if not spx_candidates.empty:
        print("Top SPX-log-return candidates:")
        print(spx_candidates[display_cols].head(20).to_string(index=False))

        best = spx_candidates.iloc[0]
        print("\nBest current SPX-log-return candidate:")
        print(best["path"])
        print("max_date:", best["max_date"])
        print("has_target_date:", best["has_target_date"])
    else:
        print("No readable SPX-log-return candidate found.")

    target_available = bool(
        not spx_candidates.empty
        and spx_candidates["has_target_date"].any()
    )

    section("Audit result")
    print("TARGET_DATE:", TARGET_DATE.date())
    print("Any candidate SPX-log-return source has target date:", target_available)
    print("READ_ONLY_AUDIT_COMPLETE: True")

    if not target_available:
        print("\nConclusion: current stored Corsi/SPX feature sources do not yet prove target-date availability.")
        print("Next action is to locate/run the upstream forecast_model_corsi_v1 builder that creates corsi_model_feature_panel_v1.")
    else:
        print("\nConclusion: at least one SPX-log-return source already includes the target date; use source comparison output to choose canonical extension input.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
