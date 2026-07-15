
from pathlib import Path
import json
import traceback
from datetime import datetime

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

AUDIT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

KNOWN_FORECAST_SOURCE = PROJECT_ROOT / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1" / "07A_unified_fds_no_min_return_oos_forecast_panel_20200102_20260701_20260704_203242.parquet"

SEARCH_DIRS = [
    PROJECT_ROOT / "data" / "processed",
    PROJECT_ROOT / "data" / "audit",
    PROJECT_ROOT / "notebooks",
    PROJECT_ROOT / "notebooks v0",
]


def section(title):
    print("=" * 100)
    print(title)
    print("=" * 100)


def safe_read_table(path):
    suffix = path.suffix.lower()

    if suffix == ".parquet":
        return pd.read_parquet(path)

    if suffix == ".csv":
        return pd.read_csv(path)

    return None


def summarize_table(path):
    out = {
        "path": str(path),
        "exists": path.exists(),
        "suffix": path.suffix.lower(),
        "read_status": "not_read",
        "rows": None,
        "cols": None,
        "date_min": None,
        "date_max": None,
        "unique_dates": None,
        "unique_tenors": None,
        "columns": None,
        "error": None,
    }

    try:
        df = safe_read_table(path)

        if df is None:
            out["read_status"] = "unsupported"
            return out

        out["read_status"] = "ok"
        out["rows"] = len(df)
        out["cols"] = len(df.columns)
        out["columns"] = "|".join(map(str, df.columns))

        date_col = None
        for c in ["trade_date", "date", "asof_date", "timestamp"]:
            if c in df.columns:
                date_col = c
                break

        if date_col is not None:
            s = pd.to_datetime(df[date_col], errors="coerce")
            out["date_min"] = str(s.min().date()) if s.notna().any() else None
            out["date_max"] = str(s.max().date()) if s.notna().any() else None
            out["unique_dates"] = int(s.nunique(dropna=True))

        tenor_col = None
        for c in ["tenor", "target_days", "target_dte", "days"]:
            if c in df.columns:
                tenor_col = c
                break

        if tenor_col is not None:
            vals = sorted(pd.to_numeric(df[tenor_col], errors="coerce").dropna().astype(int).unique().tolist())
            out["unique_tenors"] = str(vals)

        return out

    except Exception as exc:
        out["read_status"] = "failed"
        out["error"] = repr(exc)
        return out


def score_candidate(path):
    name = str(path).lower()
    score = 0

    terms = {
        "corsi": 10,
        "fds": 10,
        "forecast": 8,
        "unified": 6,
        "coeff": 12,
        "coef": 12,
        "model": 6,
        "fit": 6,
        "oos": 5,
        "refit": 5,
        "no_min_return": 8,
        "har": 4,
        "variance": 4,
    }

    for term, pts in terms.items():
        if term in name:
            score += pts

    if path.suffix.lower() in [".parquet", ".csv", ".json", ".pkl", ".pickle", ".joblib"]:
        score += 2

    return score


def find_candidates():
    candidates = []

    for base in SEARCH_DIRS:
        if not base.exists():
            continue

        for path in base.rglob("*"):
            if not path.is_file():
                continue

            low = str(path).lower()

            if not any(x in low for x in ["corsi", "fds", "forecast", "coeff", "coef", "model", "fit"]):
                continue

            if path.suffix.lower() not in [".parquet", ".csv", ".json", ".pkl", ".pickle", ".joblib", ".txt", ".py", ".ipynb"]:
                continue

            try:
                stat = path.stat()
                candidates.append({
                    "path": str(path),
                    "name": path.name,
                    "suffix": path.suffix.lower(),
                    "size_mb": round(stat.st_size / 1024 / 1024, 4),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "score": score_candidate(path),
                })
            except Exception:
                continue

    out = pd.DataFrame(candidates)

    if out.empty:
        return out

    return out.sort_values(["score", "modified"], ascending=[False, False]).reset_index(drop=True)


def inspect_known_forecast_source():
    section("Known forecast source")

    print(KNOWN_FORECAST_SOURCE)
    print("exists:", KNOWN_FORECAST_SOURCE.exists())

    if not KNOWN_FORECAST_SOURCE.exists():
        return None

    df = pd.read_parquet(KNOWN_FORECAST_SOURCE)

    print("rows:", f"{len(df):,}")
    print("cols:", len(df.columns))
    print("columns:")
    for c in df.columns:
        print(" ", c)

    if "trade_date" in df.columns:
        d = pd.to_datetime(df["trade_date"], errors="coerce")
        print("date range:", d.min().date(), "to", d.max().date())
        print("unique dates:", d.nunique())

    if "tenor" in df.columns:
        print("tenors:", sorted(df["tenor"].dropna().astype(int).unique().tolist()))

    key_cols = [
        "model_spec",
        "model_source",
        "fit_status_candidate",
        "forecast_variance_candidate",
        "forecast_vol_candidate",
        "implied_variance",
        "target_days",
        "tenor",
    ]

    print("\nImportant column presence:")
    for c in key_cols:
        print(f"{c:35s}", c in df.columns)

    group_cols = [c for c in ["model_spec", "model_source", "fit_status_candidate"] if c in df.columns]

    if group_cols:
        print("\nModel/source group counts:")
        counts = (
            df.groupby(group_cols, dropna=False)
            .size()
            .reset_index(name="rows")
            .sort_values("rows", ascending=False)
        )
        print(counts.to_string(index=False))

    sample_path = AUDIT_DIR / f"forecast_known_source_sample_{RUN_TS}.csv"
    df.head(1000).to_csv(sample_path, index=False)

    schema_path = AUDIT_DIR / f"forecast_known_source_schema_{RUN_TS}.csv"
    pd.DataFrame({
        "column": df.columns,
        "dtype": [str(df[c].dtype) for c in df.columns],
        "non_null": [int(df[c].notna().sum()) for c in df.columns],
    }).to_csv(schema_path, index=False)

    print("\nSaved sample:", sample_path)
    print("Saved schema:", schema_path)

    return df


def inspect_market_inputs():
    section("Market / Corsi input files")

    paths = [
        PROJECT_ROOT / "data" / "processed" / "market_data" / "spy_eod_prices_v1.parquet",
        PROJECT_ROOT / "data" / "processed" / "market_data" / "spy_realized_vol_history_v1.parquet",
        PROJECT_ROOT / "data" / "processed" / "market_data" / "spy_corsi_har_input_panel_v1.parquet",
    ]

    rows = []

    for path in paths:
        row = summarize_table(path)
        rows.append(row)

        print("\n", path)
        print("exists:", row["exists"])
        print("read_status:", row["read_status"])
        print("rows:", row["rows"])
        print("date range:", row["date_min"], "to", row["date_max"])
        print("columns:", row["columns"])

    out = pd.DataFrame(rows)
    out_path = AUDIT_DIR / f"market_input_inventory_{RUN_TS}.csv"
    out.to_csv(out_path, index=False)
    print("\nSaved:", out_path)


def inspect_candidates():
    section("Forecast / model artifact candidates")

    candidates = find_candidates()
    out_path = AUDIT_DIR / f"forecast_artifact_candidates_{RUN_TS}.csv"
    candidates.to_csv(out_path, index=False)

    print("Candidates found:", len(candidates))
    print("Saved:", out_path)

    if not candidates.empty:
        print("\nTop 40 candidates:")
        print(candidates.head(40).to_string(index=False))

    table_rows = []

    for path_str in candidates.head(40)["path"].tolist() if not candidates.empty else []:
        path = Path(path_str)

        if path.suffix.lower() in [".parquet", ".csv"]:
            table_rows.append(summarize_table(path))

    table_summary = pd.DataFrame(table_rows)
    table_summary_path = AUDIT_DIR / f"forecast_candidate_table_summaries_{RUN_TS}.csv"
    table_summary.to_csv(table_summary_path, index=False)

    print("\nSaved table summaries:", table_summary_path)

    if not table_summary.empty:
        print("\nReadable table summaries:")
        display_cols = [
            "path", "read_status", "rows", "cols", "date_min", "date_max",
            "unique_dates", "unique_tenors", "error"
        ]
        print(table_summary[display_cols].to_string(index=False))


def main():
    section("VRP Corsi/FDS forecast source inventory v1")
    print("Project root:", PROJECT_ROOT)
    print("Run timestamp:", RUN_TS)
    print("Audit dir:", AUDIT_DIR)

    inspect_known_forecast_source()
    inspect_market_inputs()
    inspect_candidates()

    section("DONE")
    print("Inventory complete. Paste the console output and the top candidates if needed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
