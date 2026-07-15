
r"""
VRP Corsi source update v1.

Controlled wrapper around selected upstream Corsi builder notebook cells:
  2, 4, 6, 8, 10, 12

Scope:
  Extend forecast_model_corsi_v1 source artifacts to requested end date.

Not in scope:
  model fitting, signal building, thresholds, sizing, final signal files.
"""

from __future__ import annotations

import argparse
import json
import re
import traceback
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


SELECTED_CELL_INDICES = [2, 4, 6, 8, 10, 12]

ARTIFACT_STEMS = [
    "spy_5m_clean_ohlc_full",
    "spy_eod_full",
    "spy_daily_realized_variance_corsi_v1",
    "spy_daily_realized_variance_corsi_v1_sanitized",
    "spy_daily_realized_variance_corsi_v1_model_ready",
    "corsi_forward_realized_variance_targets_v1",
    "corsi_feature_target_panel_v1",
    "corsi_model_feature_panel_v1",
]

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def display(obj=None, *args, **kwargs):
    if obj is None:
        print()
    elif isinstance(obj, pd.DataFrame):
        with pd.option_context("display.max_columns", 200, "display.width", 240):
            print(obj.to_string(index=False))
    elif isinstance(obj, pd.Series):
        print(obj.to_string())
    else:
        print(obj)


class DummyIPython:
    def run_line_magic(self, *args, **kwargs):
        print("Skipped IPython magic:", args)

    def system(self, cmd):
        raise RuntimeError(f"Blocked notebook shell command in wrapper: {cmd}")


def get_ipython():
    return DummyIPython()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--start-date", default="20180625")
    p.add_argument("--end-date", required=True)
    p.add_argument("--venue", default="utp_cta")
    p.add_argument("--theta-base-url", default="http://127.0.0.1:25503")
    p.add_argument("--interval", default="5m")
    p.add_argument("--force-refresh-theta", action="store_true")
    return p.parse_args()


def yyyymmdd(x: str) -> str:
    return pd.to_datetime(str(x), format="%Y%m%d").strftime("%Y%m%d")


def parse_dates(s: pd.Series) -> pd.Series:
    raw = s.copy()
    nonnull = raw.dropna()
    if len(nonnull) == 0:
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = nonnull.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if as_str.str.fullmatch(r"\d{8}").mean() > 0.8:
        out = pd.to_datetime(raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip(), format="%Y%m%d", errors="coerce")
    else:
        out = pd.to_datetime(raw, errors="coerce")
    return out.dt.normalize()


def read_notebook_cells(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Notebook not found: {path}")
    return json.loads(path.read_text(encoding="utf-8")).get("cells", [])


def source_text(cell: dict) -> str:
    return "".join(cell.get("source", []))



def make_control_date_panel(project_root: Path, start_date: str, end_date: str, run_ts: str) -> Path:
    """
    Build a production-like control panel for the old Corsi notebook.

    Requirements:
      - date / trade_date
      - tenor / target_days
      - implied_variance

    Source priority:
      1. Existing production/staging production_feature_panel*.parquet where available.
      2. Canonical implied-variance surface for missing date/tenor rows.

    This does not synthesize implied_variance.
    """
    out_dir = project_root / "data" / "audit" / "forecast_update_inventory"
    out_dir.mkdir(parents=True, exist_ok=True)

    start_ts = pd.to_datetime(start_date, format="%Y%m%d").normalize()
    end_ts = pd.to_datetime(end_date, format="%Y%m%d").normalize()

    def normalize_panel(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        out = df.copy()

        if "date" in out.columns:
            d = parse_dates(out["date"])
        elif "trade_date" in out.columns:
            d = parse_dates(out["trade_date"])
        else:
            raise ValueError(f"{source_name} missing date/trade_date column.")

        out["date"] = d
        out = out[out["date"].between(start_ts, end_ts)].copy()
        out["trade_date"] = out["date"].dt.strftime("%Y%m%d").astype(int)

        tenor_candidates = ["tenor", "target_days", "dte", "days_to_expiry", "target_tenor_days"]
        tenor_col = next((c for c in tenor_candidates if c in out.columns), None)
        if tenor_col is None:
            raise ValueError(f"{source_name} missing tenor/target_days-like column.")

        out["tenor"] = pd.to_numeric(out[tenor_col], errors="coerce")
        out = out[out["tenor"].isin(EXPECTED_TENORS)].copy()
        out["tenor"] = out["tenor"].astype(int)
        out["target_days"] = out["tenor"]

        implied_candidates = [
            "implied_variance",
            "vix_style_implied_variance",
            "model_free_implied_variance",
            "variance",
            "iv_variance",
        ]
        implied_col = next((c for c in implied_candidates if c in out.columns), None)
        if implied_col is not None and implied_col != "implied_variance":
            out["implied_variance"] = pd.to_numeric(out[implied_col], errors="coerce")

        return out

    base_frames = []

    staging_dir = project_root / "data" / "processed" / "staging"
    production_candidates = sorted(
        staging_dir.glob("production_feature_panel*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    for candidate in production_candidates:
        try:
            tmp = normalize_panel(pd.read_parquet(candidate), f"production candidate {candidate}")
            if "implied_variance" in tmp.columns and tmp["implied_variance"].notna().any():
                tmp["_control_source"] = str(candidate)
                base_frames.append(tmp)
                print("Control panel base source:", candidate)
                break
        except Exception as exc:
            print("Skipped production candidate:", candidate, "error:", repr(exc))

    implied_path = project_root / "data" / "processed" / "implied_variance" / "spx_vix_style_implied_variance_surface_v1.parquet"
    if not implied_path.exists():
        raise FileNotFoundError(f"Canonical implied variance surface not found: {implied_path}")

    implied = normalize_panel(pd.read_parquet(implied_path), f"canonical implied surface {implied_path}")
    if "implied_variance" not in implied.columns:
        raise ValueError(f"Canonical implied surface does not contain implied_variance or recognized equivalent: {implied_path}")

    implied["_control_source"] = str(implied_path)
    base_frames.append(implied)

    panel = pd.concat(base_frames, ignore_index=True, sort=False)

    panel["implied_variance"] = pd.to_numeric(panel["implied_variance"], errors="coerce")
    panel = panel[panel["implied_variance"].notna() & (panel["implied_variance"] > 0)].copy()

    # Prefer existing production/staging rows first, then canonical implied surface for missing date/tenor rows.
    panel["_source_rank"] = np.where(panel["_control_source"].astype(str).str.contains("production_feature_panel", regex=False), 0, 1)
    panel = (
        panel.sort_values(["trade_date", "tenor", "_source_rank"])
        .drop_duplicates(subset=["trade_date", "tenor"], keep="first")
        .sort_values(["date", "tenor"])
        .reset_index(drop=True)
    )

    required_pairs = pd.MultiIndex.from_product(
        [
            pd.to_datetime(panel["date"]).drop_duplicates().sort_values(),
            EXPECTED_TENORS,
        ],
        names=["date", "tenor"],
    )

    existing_pairs = pd.MultiIndex.from_frame(panel[["date", "tenor"]])
    missing_pairs = required_pairs.difference(existing_pairs)

    if len(missing_pairs):
        missing_sample = pd.DataFrame(index=missing_pairs).reset_index().head(20)
        print("WARNING: control panel missing date/tenor pairs after implied merge:")
        print(missing_sample.to_string(index=False))

    latest_rows = panel[panel["date"].eq(end_ts)]
    print("Control panel rows:", len(panel))
    print("Control panel date range:", panel["date"].min().date(), "to", panel["date"].max().date())
    print("Control panel latest-date rows:", len(latest_rows))
    print("Control panel latest-date tenors:", sorted(latest_rows["tenor"].dropna().astype(int).unique().tolist()))
    print("Control panel has implied_variance:", "implied_variance" in panel.columns)

    if latest_rows.empty:
        raise RuntimeError(f"Control panel has no rows for requested end date {end_date}.")
    latest_tenors = sorted(latest_rows["tenor"].dropna().astype(int).unique().tolist())
    if latest_tenors != EXPECTED_TENORS:
        raise RuntimeError(f"Control panel latest tenors mismatch for {end_date}: {latest_tenors}")

    out_path = out_dir / f"corsi_source_update_control_dates_{start_date}_{end_date}_{run_ts}.parquet"
    panel.to_parquet(out_path, index=False)
    return out_path


def patch_source(src: str, args: argparse.Namespace, project_root: Path) -> str:
    lines = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("%") or stripped.startswith("!"):
            lines.append(f"# stripped notebook magic/system line: {line}")
        else:
            lines.append(line)
    out = "\n".join(lines)

    out = re.sub(
        r"PROJECT_ROOT\s*=\s*Path\([rR]?[\"'].*?[\"']\)",
        lambda m: f'PROJECT_ROOT = Path(r"{project_root}")',
        out,
    )
    out = re.sub(
        r"THETA_BASE_URL\s*=\s*[\"'].*?[\"']",
        f'THETA_BASE_URL = "{args.theta_base_url}"',
        out,
    )
    out = re.sub(
        r"FALLBACK_START_DATE\s*=\s*[\"']\d{8}[\"']",
        f'FALLBACK_START_DATE = "{args.start_date}"',
        out,
    )
    out = re.sub(
        r"FALLBACK_END_DATE\s*=\s*datetime\.now\(\)\.strftime\([\"']%Y%m%d[\"']\)",
        f'FALLBACK_END_DATE = "{args.end_date}"',
        out,
    )
    out = re.sub(
        r"FORCE_REFRESH_THETA\s*=\s*(True|False)",
        f"FORCE_REFRESH_THETA = {bool(args.force_refresh_theta)}",
        out,
    )
    out = re.sub(
        r"FULL_HISTORY_INTERVAL\s*=\s*[\"'].*?[\"']",
        f'FULL_HISTORY_INTERVAL = "{args.interval}"',
        out,
    )
    out = re.sub(
        r"^SPY_STOCK_VENUE\s*=.*$",
        f'SPY_STOCK_VENUE = "{args.venue}"',
        out,
        flags=re.MULTILINE,
    )
    return out


def latest_for_run(directory: Path, stem: str, run_ts: str) -> Path | None:
    files = sorted(directory.glob(f"{stem}_*_{run_ts}.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def latest_existing(directory: Path, stem: str) -> Path | None:
    files = sorted(directory.glob(f"{stem}_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def summarize_table(path: Path, target_date: pd.Timestamp) -> dict:
    row = {
        "path": str(path),
        "rows": np.nan,
        "cols": np.nan,
        "date_col_used": None,
        "min_date": None,
        "max_date": None,
        "has_target_date": False,
        "has_spx_log_return": False,
        "has_spx_close": False,
        "has_corsi_model_usable": False,
        "has_corsi_quality_status": False,
        "error": "",
    }
    try:
        df = pd.read_parquet(path)
        row["rows"] = len(df)
        row["cols"] = len(df.columns)

        cols = set(map(str, df.columns))
        row["has_spx_log_return"] = "spx_log_return" in cols
        row["has_spx_close"] = "spx_close" in cols
        row["has_corsi_model_usable"] = "corsi_model_usable" in cols
        row["has_corsi_quality_status"] = "corsi_quality_status" in cols

        for c in ["date", "trade_date"]:
            if c in df.columns:
                d = parse_dates(df[c])
                if d.notna().any():
                    row["date_col_used"] = c
                    row["min_date"] = d.min().date().isoformat()
                    row["max_date"] = d.max().date().isoformat()
                    row["has_target_date"] = bool((d == target_date).any())
                    break
    except Exception as exc:
        row["error"] = repr(exc)
    return row


def compare_old_new_model_panel(old_path: Path, new_path: Path, out_dir: Path, run_ts: str) -> pd.DataFrame:
    old = pd.read_parquet(old_path)
    new = pd.read_parquet(new_path)

    if "date" in old.columns:
        old["_d"] = parse_dates(old["date"])
    else:
        old["_d"] = parse_dates(old["trade_date"])

    if "date" in new.columns:
        new["_d"] = parse_dates(new["date"])
    else:
        new["_d"] = parse_dates(new["trade_date"])

    tenor_col_old = "target_days" if "target_days" in old.columns else ("tenor" if "tenor" in old.columns else None)
    tenor_col_new = "target_days" if "target_days" in new.columns else ("tenor" if "tenor" in new.columns else None)

    keys = ["_d"]
    if tenor_col_old and tenor_col_new:
        old["_tenor"] = pd.to_numeric(old[tenor_col_old], errors="coerce")
        new["_tenor"] = pd.to_numeric(new[tenor_col_new], errors="coerce")
        keys.append("_tenor")

    compare_cols = [
        c for c in [
            "spx_close",
            "spx_log_return",
            "spy_total_return",
            "intraday_realized_variance_raw",
            "overnight_realized_variance_raw",
            "total_realized_variance_raw",
        ]
        if c in old.columns and c in new.columns
    ]

    merged = old[keys + compare_cols].merge(new[keys + compare_cols], on=keys, how="inner", suffixes=("_old", "_new"))

    rows = []
    for c in compare_cols:
        a = pd.to_numeric(merged[f"{c}_old"], errors="coerce")
        b = pd.to_numeric(merged[f"{c}_new"], errors="coerce")
        valid = a.notna() & b.notna()
        diff = (b[valid] - a[valid]).abs()
        rows.append({
            "column": c,
            "common_rows": int(len(merged)),
            "valid_compared_rows": int(valid.sum()),
            "max_abs_diff": float(diff.max()) if len(diff) else np.nan,
            "mean_abs_diff": float(diff.mean()) if len(diff) else np.nan,
        })

    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / f"corsi_source_update_overlap_summary_{run_ts}.csv", index=False)
    return summary



def override_path_assignment(src: str, name: str, path_text: str) -> str:
    """
    Replace either a one-line assignment:
        NAME = ...
    or a parenthesized multi-line assignment:
        NAME = (
            ...
        )
    with:
        NAME = Path(r"...")

    This avoids leaving orphan indented continuation lines in old notebook cells.
    """
    lines = src.splitlines()
    out = []
    i = 0
    pattern = re.compile(rf"^(?P<indent>\s*){re.escape(name)}\s*=")

    while i < len(lines):
        line = lines[i]
        m = pattern.match(line)

        if m:
            indent = m.group("indent")
            replacement = f'{indent}{name} = Path(r"{path_text}")'

            balance = line.count("(") - line.count(")")
            i += 1

            while i < len(lines) and balance > 0:
                balance += lines[i].count("(") - lines[i].count(")")
                i += 1

            out.append(replacement)
            continue

        out.append(line)
        i += 1

    return "\n".join(out)



def main() -> None:
    args = parse_args()
    args.start_date = yyyymmdd(args.start_date)
    args.end_date = yyyymmdd(args.end_date)

    project_root = Path(args.project_root)
    notebook_path = project_root / "old" / "notebooks forecast model corsi v1" / "01_corsi_har_rv_forecast_model_research_cleaned.ipynb"
    processed_dir = project_root / "data" / "processed" / "forecast_model_corsi_v1"
    audit_update_dir = project_root / "data" / "audit" / "forecast_update_inventory"
    processed_dir.mkdir(parents=True, exist_ok=True)
    audit_update_dir.mkdir(parents=True, exist_ok=True)

    wrapper_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_date = pd.to_datetime(args.end_date, format="%Y%m%d").normalize()

    section("VRP Corsi source update v1")
    print("Project root:", project_root)
    print("Notebook:", notebook_path)
    print("Start date:", args.start_date)
    print("End date:", args.end_date)
    print("Venue:", args.venue)
    print("Force refresh ThetaData:", args.force_refresh_theta)
    print("Cells:", SELECTED_CELL_INDICES)

    previous_model = latest_existing(processed_dir, "corsi_model_feature_panel_v1")
    print("Previous model feature panel:", previous_model)

    control_dates = make_control_date_panel(project_root, args.start_date, args.end_date, wrapper_ts)
    print("Control date panel:", control_dates)

    cells = read_notebook_cells(notebook_path)

    env = {
        "__name__": "__main__",
        "Path": Path,
        "datetime": datetime,
        "json": json,
        "re": re,
        "np": np,
        "pd": pd,
        "display": display,
        "get_ipython": get_ipython,
    }

    for idx in SELECTED_CELL_INDICES:
        section(f"Executing old notebook cell index {idx}")

        env["selected_venue"] = args.venue
        env["PRODUCTION_FEATURE_PANEL_PATH"] = control_dates
        env["IMPLIED_TERM_STRUCTURE_PATH"] = control_dates

        src = patch_source(source_text(cells[idx]), args, project_root)

        # Force the old notebook's internal date-source constants to the wrapper control panel.
        # The old assignments are parenthesized multi-line Path expressions, so replace the
        # entire assignment block rather than only the first line.
        control_path_text = str(control_dates)
        src = override_path_assignment(src, "PRODUCTION_FEATURE_PANEL_PATH", control_path_text)
        src = override_path_assignment(src, "IMPLIED_TERM_STRUCTURE_PATH", control_path_text)

        print("Patched source lines:", len(src.splitlines()))

        code = compile(src, filename=f"<old_corsi_builder_cell_{idx}>", mode="exec")
        exec(code, env)

        env["selected_venue"] = args.venue
        env["PRODUCTION_FEATURE_PANEL_PATH"] = control_dates
        env["IMPLIED_TERM_STRUCTURE_PATH"] = control_dates

    run_ts = str(env.get("RUN_TIMESTAMP", wrapper_ts))

    section("New artifacts")
    new_paths = {}
    for stem in ARTIFACT_STEMS:
        p = latest_for_run(processed_dir, stem, run_ts)
        new_paths[stem] = p
        print(f"{stem}: {p}")

    missing = [k for k, v in new_paths.items() if v is None]
    if missing:
        raise RuntimeError(f"Missing expected artifacts for RUN_TIMESTAMP={run_ts}: {missing}")

    summary = pd.DataFrame([
        {"artifact": stem, **summarize_table(path, target_date)}
        for stem, path in new_paths.items()
    ])
    summary_path = audit_update_dir / f"corsi_source_update_artifact_summary_{run_ts}.csv"
    summary.to_csv(summary_path, index=False)

    section("Artifact summary")
    print(summary.to_string(index=False))
    print("Saved:", summary_path)

    checks = []

    def check(name: str, ok: bool, detail: str):
        checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    model_path = new_paths["corsi_model_feature_panel_v1"]
    model_df = pd.read_parquet(model_path)

    date_col = "date" if "date" in model_df.columns else "trade_date"
    model_dates = parse_dates(model_df[date_col])
    check(
        "model_feature_panel_reaches_target_date",
        bool((model_dates == target_date).any()),
        f"target={target_date.date()}; max={model_dates.max().date()}",
    )

    check("model_feature_panel_has_spx_log_return", "spx_log_return" in model_df.columns, "required by locked Cell 4 source")
    check("model_feature_panel_has_spx_close", "spx_close" in model_df.columns, "required by locked Cell 4 source")

    tenor_col = "target_days" if "target_days" in model_df.columns else ("tenor" if "tenor" in model_df.columns else None)
    if tenor_col:
        target_tenors = sorted(pd.to_numeric(model_df.loc[model_dates == target_date, tenor_col], errors="coerce").dropna().astype(int).unique().tolist())
        check("target_date_has_expected_tenors", target_tenors == EXPECTED_TENORS, f"target_tenors={target_tenors}")
    else:
        check("target_date_has_expected_tenors", False, "No target_days/tenor column found")

    if previous_model and previous_model.exists():
        overlap = compare_old_new_model_panel(previous_model, model_path, audit_update_dir, run_ts)
        section("Overlap summary vs previous model feature panel")
        print(overlap.to_string(index=False))
        max_diff = overlap["max_abs_diff"].max() if not overlap.empty else np.nan
        check("overlap_summary_written", True, f"max_abs_diff={max_diff}")
    else:
        check("overlap_summary_written", False, "No previous model panel found")

    validation = pd.DataFrame(checks)
    validation_path = audit_update_dir / f"corsi_source_update_validation_{run_ts}.csv"
    validation.to_csv(validation_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "wrapper_ts": wrapper_ts,
        "project_root": str(project_root),
        "notebook_path": str(notebook_path),
        "selected_cell_indices": SELECTED_CELL_INDICES,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "venue": args.venue,
        "force_refresh_theta": bool(args.force_refresh_theta),
        "control_dates": str(control_dates),
        "new_paths": {k: str(v) for k, v in new_paths.items()},
        "summary": str(summary_path),
        "validation": str(validation_path),
    }
    manifest_path = audit_update_dir / f"corsi_source_update_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    section("Validation")
    print(validation.to_string(index=False))
    print("Saved validation:", validation_path)
    print("Saved manifest:", manifest_path)

    fails = validation[validation["status"].eq("FAIL")]
    section("Final result")
    print("Hard checks failed:", len(fails))

    if len(fails):
        print(fails.to_string(index=False))
        raise RuntimeError("Corsi source update failed validation.")

    print("CORSI_SOURCE_UPDATE_PASS: True")
    print("DONE — upstream forecast_model_corsi_v1 source extended.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
