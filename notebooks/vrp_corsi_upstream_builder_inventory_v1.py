
from pathlib import Path
from datetime import datetime
import json
import re
import traceback

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
PROCESSED_CORSI_DIR = PROJECT_ROOT / "data" / "processed" / "forecast_model_corsi_v1"
AUDIT_CORSI_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_model_corsi_v1"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
AUDIT_OUT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
AUDIT_OUT_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
TARGET_DATE = pd.Timestamp("2026-07-06")

OUTPUT_STEMS = [
    "corsi_model_feature_panel_v1",
    "corsi_feature_target_panel_v1",
    "spy_daily_realized_variance_corsi_v1_model_ready",
    "spy_daily_realized_variance_corsi_v1_sanitized",
    "spy_daily_realized_variance_corsi_v1",
    "spy_5m_clean_ohlc_full",
    "spy_eod_full",
    "corsi_forward_realized_variance_targets_v1",
]

SEARCH_TERMS = [
    "forecast_model_corsi_v1",
    "corsi_model_feature_panel_v1",
    "corsi_feature_target_panel_v1",
    "spy_daily_realized_variance_corsi_v1_model_ready",
    "spy_daily_realized_variance_corsi_v1_sanitized",
    "spy_daily_realized_variance_corsi_v1",
    "spy_5m_clean_ohlc_full",
    "spy_eod_full",
    "corsi_forward_realized_variance_targets_v1",
    "SPY_5m_intraday_plus_overnight",
    "ThetaData",
    "thetadata",
    "stock/history/eod",
    "stock/history/ohlc",
    "v2/hist/stock/ohlc",
    "v3/stock/history/eod",
    "intraday_realized_variance_raw",
    "overnight_realized_variance_raw",
    "corsi_model_usable",
    "corsi_quality_status",
]

CODE_EXTENSIONS = {".py", ".ipynb", ".txt", ".md"}


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_project_date_series(s: pd.Series) -> pd.Series:
    raw = s.copy()
    nonnull = raw.dropna()
    if len(nonnull) == 0:
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = nonnull.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    yyyymmdd_like = as_str.str.fullmatch(r"\d{8}").mean() > 0.8

    if yyyymmdd_like:
        parsed = pd.to_datetime(
            raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        parsed = pd.to_datetime(raw, errors="coerce")

    return parsed.dt.normalize()


def read_table_safe(path: Path, nrows: int | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        if nrows is None:
            return pd.read_csv(path)
        return pd.read_csv(path, nrows=nrows)
    raise ValueError(f"Unsupported table type: {path}")


def summarize_artifact(path: Path) -> dict:
    row = {
        "path": str(path),
        "name": path.name,
        "suffix": path.suffix.lower(),
        "parent": str(path.parent),
        "exists": path.exists(),
        "size_mb": np.nan,
        "mtime": None,
        "read_ok": False,
        "rows": np.nan,
        "cols": np.nan,
        "date_col_used": None,
        "min_date": None,
        "max_date": None,
        "has_target_date": False,
        "has_trade_date": False,
        "has_date": False,
        "has_spx_log_return": False,
        "has_spx_close": False,
        "has_spy_total_return": False,
        "has_corsi_quality_status": False,
        "has_corsi_model_usable": False,
        "has_intraday_raw": False,
        "has_overnight_raw": False,
        "has_forward_target": False,
        "has_implied_variance": False,
        "columns": "",
        "error": "",
    }

    try:
        stat = path.stat()
        row["size_mb"] = stat.st_size / 1_000_000
        row["mtime"] = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    except Exception:
        pass

    try:
        df = read_table_safe(path)
        row["read_ok"] = True
        row["rows"] = len(df)
        row["cols"] = len(df.columns)
        row["columns"] = "|".join(map(str, df.columns))

        cols = set(map(str, df.columns))
        row["has_trade_date"] = "trade_date" in cols
        row["has_date"] = "date" in cols
        row["has_spx_log_return"] = "spx_log_return" in cols
        row["has_spx_close"] = "spx_close" in cols
        row["has_spy_total_return"] = "spy_total_return" in cols
        row["has_corsi_quality_status"] = "corsi_quality_status" in cols
        row["has_corsi_model_usable"] = "corsi_model_usable" in cols
        row["has_intraday_raw"] = any("intraday" in c.lower() and "raw" in c.lower() for c in cols)
        row["has_overnight_raw"] = any("overnight" in c.lower() and "raw" in c.lower() for c in cols)
        row["has_forward_target"] = any("forward_realized_variance" in c.lower() or "target_log_variance" in c.lower() for c in cols)
        row["has_implied_variance"] = "implied_variance" in cols

        for c in ["trade_date", "date"]:
            if c in df.columns:
                d = parse_project_date_series(df[c])
                if d.notna().any():
                    row["date_col_used"] = c
                    row["min_date"] = d.min().date().isoformat()
                    row["max_date"] = d.max().date().isoformat()
                    row["has_target_date"] = bool((d == TARGET_DATE).any())
                    break

    except Exception as exc:
        row["error"] = repr(exc)

    return row


def code_text_from_path(path: Path) -> str:
    suffix = path.suffix.lower()

    if suffix == ".ipynb":
        try:
            nb = json.loads(path.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            nb = json.loads(path.read_text(encoding="utf-8-sig"))

        parts = []
        for i, cell in enumerate(nb.get("cells", [])):
            src = "".join(cell.get("source", []))
            if src.strip():
                parts.append(f"\n# --- notebook_cell_index={i} cell_type={cell.get('cell_type')} ---\n{src}")
        return "\n".join(parts)

    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1", errors="ignore")


def find_code_matches(path: Path) -> list[dict]:
    matches = []
    try:
        text = code_text_from_path(path)
    except Exception as exc:
        return [{
            "path": str(path),
            "file_name": path.name,
            "suffix": path.suffix.lower(),
            "matched_term": "__READ_ERROR__",
            "line_number": np.nan,
            "context": repr(exc),
            "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if path.exists() else None,
        }]

    lines = text.splitlines()
    lower_lines = [line.lower() for line in lines]

    for term in SEARCH_TERMS:
        term_lower = term.lower()
        hit_indices = [i for i, line in enumerate(lower_lines) if term_lower in line]

        for i in hit_indices[:50]:
            lo = max(0, i - 6)
            hi = min(len(lines), i + 7)
            context = "\n".join(f"{j+1:05d}: {lines[j]}" for j in range(lo, hi))

            matches.append({
                "path": str(path),
                "file_name": path.name,
                "suffix": path.suffix.lower(),
                "matched_term": term,
                "line_number": i + 1,
                "context": context,
                "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds") if path.exists() else None,
            })

    return matches


def score_builder_candidate(group: pd.DataFrame) -> int:
    terms = set(group["matched_term"].astype(str))
    score = 0

    for stem in OUTPUT_STEMS:
        if stem in terms:
            score += 10

    if "forecast_model_corsi_v1" in terms:
        score += 8
    if "ThetaData" in terms or "thetadata" in terms:
        score += 5
    if "stock/history/eod" in terms or "v3/stock/history/eod" in terms:
        score += 4
    if "stock/history/ohlc" in terms or "v2/hist/stock/ohlc" in terms:
        score += 4
    if "intraday_realized_variance_raw" in terms:
        score += 3
    if "overnight_realized_variance_raw" in terms:
        score += 3
    if "corsi_quality_status" in terms or "corsi_model_usable" in terms:
        score += 3

    score += min(len(group), 50)

    return int(score)


def main():
    section("VRP Corsi upstream builder inventory v1")
    print("Project root:", PROJECT_ROOT)
    print("Run timestamp:", RUN_TS)
    print("Target date:", TARGET_DATE.date())
    print("Read-only audit: no production writes, no feature rebuild, no model fitting.")

    section("Inventory existing Corsi artifacts")

    artifact_paths = []
    for base in [PROCESSED_CORSI_DIR, AUDIT_CORSI_DIR]:
        if base.exists():
            for stem in OUTPUT_STEMS:
                artifact_paths.extend(base.glob(f"{stem}*"))
                artifact_paths.extend(base.glob(f"*{stem}*"))

    artifact_paths = sorted(set(p for p in artifact_paths if p.suffix.lower() in {".parquet", ".csv"}))

    artifact_rows = [summarize_artifact(p) for p in artifact_paths]
    artifact_inventory = pd.DataFrame(artifact_rows)

    artifact_inventory_path = AUDIT_OUT_DIR / f"corsi_upstream_artifact_inventory_{RUN_TS}.csv"
    artifact_inventory.to_csv(artifact_inventory_path, index=False)

    display_cols = [
        "name", "path", "rows", "cols", "date_col_used", "min_date", "max_date",
        "has_target_date", "has_spx_log_return", "has_spx_close", "has_corsi_quality_status",
        "has_intraday_raw", "has_overnight_raw", "mtime", "read_ok", "error",
    ]

    if not artifact_inventory.empty:
        print(artifact_inventory[display_cols].sort_values(["max_date", "name"], ascending=[False, True], na_position="last").to_string(index=False))
    else:
        print("No matching artifacts found.")

    print("\nSaved:", artifact_inventory_path)

    section("Search notebooks/scripts for upstream builder code")

    code_roots = [
        NOTEBOOKS_DIR,
        PROJECT_ROOT / "scripts",
        PROJECT_ROOT,
    ]

    code_paths = []
    for root in code_roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in CODE_EXTENSIONS:
                # Skip audit/source dump noise if possible.
                path_str = str(p).lower()
                if "\\data\\audit\\" in path_str or "/data/audit/" in path_str:
                    continue
                if "\\.ipynb_checkpoints\\" in path_str or "/.ipynb_checkpoints/" in path_str:
                    continue
                code_paths.append(p)

    code_paths = sorted(set(code_paths))

    print("Code files scanned:", len(code_paths))

    match_rows = []
    for p in code_paths:
        match_rows.extend(find_code_matches(p))

    matches = pd.DataFrame(match_rows)

    if matches.empty:
        matches = pd.DataFrame(columns=["path", "file_name", "suffix", "matched_term", "line_number", "context", "mtime"])

    matches_path = AUDIT_OUT_DIR / f"corsi_upstream_builder_code_matches_{RUN_TS}.csv"
    matches.to_csv(matches_path, index=False)

    print("Code matches:", len(matches))
    print("Saved:", matches_path)

    section("Rank likely builder files")

    if matches.empty:
        ranked = pd.DataFrame(columns=[
            "path", "file_name", "suffix", "builder_score", "match_count", "unique_terms", "terms", "mtime"
        ])
    else:
        rows = []
        for path, g in matches.groupby("path"):
            terms = sorted(set(g["matched_term"].astype(str)))
            rows.append({
                "path": path,
                "file_name": Path(path).name,
                "suffix": Path(path).suffix.lower(),
                "builder_score": score_builder_candidate(g),
                "match_count": len(g),
                "unique_terms": len(terms),
                "terms": "|".join(terms),
                "mtime": g["mtime"].dropna().iloc[0] if g["mtime"].notna().any() else None,
            })

        ranked = pd.DataFrame(rows).sort_values(
            ["builder_score", "match_count", "unique_terms"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    ranked_path = AUDIT_OUT_DIR / f"corsi_upstream_builder_ranked_candidates_{RUN_TS}.csv"
    ranked.to_csv(ranked_path, index=False)

    if not ranked.empty:
        print(ranked.head(40).to_string(index=False))
    else:
        print("No ranked builder candidates found.")

    print("\nSaved:", ranked_path)

    section("Top candidate context excerpts")

    top_paths = ranked["path"].head(5).tolist() if not ranked.empty else []

    excerpt_rows = []
    for p in top_paths:
        g = matches[matches["path"] == p].copy()
        preferred_terms = [
            "corsi_model_feature_panel_v1",
            "spy_daily_realized_variance_corsi_v1_model_ready",
            "spy_5m_clean_ohlc_full",
            "spy_eod_full",
            "corsi_forward_realized_variance_targets_v1",
            "forecast_model_corsi_v1",
            "thetadata",
            "ThetaData",
        ]
        selected = []
        for term in preferred_terms:
            h = g[g["matched_term"].eq(term)]
            if not h.empty:
                selected.append(h.head(3))
        if selected:
            ex = pd.concat(selected, ignore_index=True).drop_duplicates(["path", "matched_term", "line_number"]).head(20)
        else:
            ex = g.head(20)

        excerpt_rows.append(ex)

    if excerpt_rows:
        excerpts = pd.concat(excerpt_rows, ignore_index=True)
    else:
        excerpts = pd.DataFrame(columns=matches.columns)

    excerpts_path = AUDIT_OUT_DIR / f"corsi_upstream_builder_top_context_excerpts_{RUN_TS}.csv"
    excerpts.to_csv(excerpts_path, index=False)

    if not excerpts.empty:
        for _, r in excerpts.iterrows():
            print("\n" + "-" * 100)
            print(f"{r['file_name']} | term={r['matched_term']} | line={r['line_number']}")
            print(r["path"])
            print("-" * 100)
            print(r["context"])
    else:
        print("No excerpts available.")

    print("\nSaved:", excerpts_path)

    section("Conclusion inputs")

    current_model_feature = artifact_inventory[
        artifact_inventory["name"].astype(str).str.startswith("corsi_model_feature_panel_v1")
        & artifact_inventory["read_ok"].eq(True)
    ].copy()

    if not current_model_feature.empty:
        latest = current_model_feature.sort_values("max_date", ascending=False, na_position="last").iloc[0]
        print("Latest corsi_model_feature_panel_v1 artifact:")
        print(" ", latest["path"])
        print(" max_date:", latest["max_date"])
        print(" has_target_date:", latest["has_target_date"])

    if not ranked.empty:
        print("\nTop likely builder:")
        print(" ", ranked.iloc[0]["path"])
        print(" builder_score:", ranked.iloc[0]["builder_score"])
        print(" terms:", ranked.iloc[0]["terms"])

    section("DONE")
    print("READ_ONLY_AUDIT_COMPLETE: True")
    print("Next step: inspect the top-ranked builder candidate and run/repair only after explicit approval.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
