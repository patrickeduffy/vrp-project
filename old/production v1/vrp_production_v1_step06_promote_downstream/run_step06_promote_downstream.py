from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

IMPLIED_BASENAME = "vix_term_structure_history_v0_7_1_repaired_total_variance"
TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


PANEL_SPECS = {
    "realized": {
        "base": "realized_variance_panel_v0_1",
        "key_date": "trade_date",
        "key_tenor": "target_days",
        "critical_non_missing": ["trailing_realized_variance", "trailing_realized_vol"],
        "positive_cols": ["trailing_realized_variance", "trailing_realized_vol"],
    },
    "vrp": {
        "base": "vrp_panel_v0_1",
        "key_date": "trade_date",
        "key_tenor": "target_days",
        "critical_non_missing": [
            "implied_variance", "trailing_realized_variance", "trailing_realized_vol",
            "primary_vrp_signal", "spx_close"
        ],
        "positive_cols": ["implied_variance", "trailing_realized_variance", "trailing_realized_vol", "spx_close"],
    },
    "feature": {
        "base": "production_feature_panel_v0_1",
        "key_date": "trade_date",
        "key_tenor": "tenor",
        "critical_non_missing": ["implied_variance", "forecast_variance", "forecast_vol", "vrp_log", "spx_close"],
        "positive_cols": ["implied_variance", "forecast_variance", "forecast_vol", "spx_close"],
    },
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promote QA'd Step 05 downstream candidate panels to official files.")
    p.add_argument("--project-root", required=True, help="Project root, e.g. C:\\Users\\patri\\vrp_project")
    p.add_argument("--expected-end-date", required=True, type=int, help="Expected latest trade_date, YYYYMMDD")
    p.add_argument("--confirm-promote", action="store_true", help="Required to overwrite official downstream files. Without this, dry-run only.")
    p.add_argument("--candidate-start-date", default=None, type=int, help="Optional expected candidate start date. Defaults to implied history min date.")
    return p.parse_args()


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def parse_project_date_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s).dt.normalize()
    ss = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    ymd = ss.str.fullmatch(r"\d{8}")
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if ymd.any():
        out.loc[ymd] = pd.to_datetime(ss.loc[ymd], format="%Y%m%d", errors="coerce")
    if (~ymd).any():
        out.loc[~ymd] = pd.to_datetime(ss.loc[~ymd], errors="coerce")
    return out.dt.normalize()


def normalize_trade_date(s: pd.Series) -> pd.Series:
    return parse_project_date_series(s).dt.strftime("%Y%m%d").astype(int)


def load_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def backup_file(path: Path, backup_dir: Path, stamp: str, label: str) -> Optional[Path]:
    if not path.exists():
        return None
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{path.stem}_backup_before_step06_{label}_{stamp}{path.suffix}"
    shutil.copy2(path, backup_path)
    return backup_path


def find_candidate(staging_dir: Path, base: str, start_date: int, end_date: int, suffix: str) -> Path:
    expected = staging_dir / f"{base}_candidate_{start_date}_{end_date}{suffix}"
    if expected.exists():
        return expected
    matches = sorted(
        staging_dir.glob(f"{base}_candidate_*_{end_date}{suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if matches:
        return matches[0]
    raise FileNotFoundError(f"No candidate found for {base}, end_date={end_date}, suffix={suffix}, in {staging_dir}")


def find_latest_snapshot_candidate(staging_dir: Path, end_date: int) -> Path:
    expected = staging_dir / f"production_feature_panel_latest_snapshot_v0_1_candidate_{end_date}.csv"
    if expected.exists():
        return expected
    matches = sorted(
        staging_dir.glob(f"production_feature_panel_latest_snapshot_v0_1_candidate_*{end_date}*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if matches:
        return matches[0]
    raise FileNotFoundError(f"No latest snapshot candidate found for end_date={end_date} in {staging_dir}")


def standardize_keys(df: pd.DataFrame, date_col: str, tenor_col: str) -> pd.DataFrame:
    out = df.copy()
    if date_col not in out.columns:
        raise ValueError(f"Missing date key column {date_col}. Columns: {list(out.columns)}")
    if tenor_col not in out.columns:
        raise ValueError(f"Missing tenor key column {tenor_col}. Columns: {list(out.columns)}")
    out[date_col] = normalize_trade_date(out[date_col])
    out[tenor_col] = pd.to_numeric(out[tenor_col], errors="raise").astype(int)
    return out


def panel_qa(df: pd.DataFrame, spec: Dict[str, Any], label: str, expected_rows: int, expected_start: int, expected_end: int) -> Dict[str, Any]:
    date_col = spec["key_date"]
    tenor_col = spec["key_tenor"]
    d = standardize_keys(df, date_col, tenor_col)
    dates = sorted(int(x) for x in d[date_col].dropna().unique())
    tenors = sorted(int(x) for x in d[tenor_col].dropna().unique())
    counts = d.groupby(date_col)[tenor_col].nunique() if len(d) else pd.Series(dtype=float)

    critical_missing: Dict[str, int] = {}
    for col in spec["critical_non_missing"]:
        critical_missing[col] = int(d[col].isna().sum()) if col in d.columns else -1

    nonpositive_counts: Dict[str, int] = {}
    for col in spec["positive_cols"]:
        if col in d.columns:
            vals = pd.to_numeric(d[col], errors="coerce")
            nonpositive_counts[col] = int((vals <= 0).sum())
        else:
            nonpositive_counts[col] = -1

    checks = {
        "label": label,
        "rows": int(len(d)),
        "expected_rows": int(expected_rows),
        "start_date": int(min(dates)) if dates else None,
        "end_date": int(max(dates)) if dates else None,
        "expected_start_date": int(expected_start),
        "expected_end_date": int(expected_end),
        "unique_dates": int(len(dates)),
        "tenors": tenors,
        "duplicate_key_rows": int(d.duplicated([date_col, tenor_col]).sum()),
        "dates_not_9_tenors": int((counts != 9).sum()) if len(counts) else None,
        "critical_missing_counts": critical_missing,
        "nonpositive_counts": nonpositive_counts,
    }
    checks["all_green"] = bool(
        checks["rows"] == checks["expected_rows"]
        and checks["start_date"] == checks["expected_start_date"]
        and checks["end_date"] == checks["expected_end_date"]
        and checks["duplicate_key_rows"] == 0
        and checks["dates_not_9_tenors"] == 0
        and tenors == TARGET_TENORS
        and all(v == 0 for v in critical_missing.values())
        and all(v == 0 for v in nonpositive_counts.values())
    )
    return checks


def latest_snapshot_qa(df: pd.DataFrame, expected_end: int) -> Dict[str, Any]:
    d = df.copy()
    if "trade_date" in d.columns:
        d["trade_date"] = normalize_trade_date(d["trade_date"])
    elif "date" in d.columns:
        d["trade_date"] = normalize_trade_date(d["date"])
    else:
        raise ValueError("Latest snapshot missing date/trade_date column.")
    tenor_col = "tenor" if "tenor" in d.columns else "target_days"
    d[tenor_col] = pd.to_numeric(d[tenor_col], errors="raise").astype(int)
    tenors = sorted(int(x) for x in d[tenor_col].dropna().unique())
    checks = {
        "rows": int(len(d)),
        "trade_dates": sorted(int(x) for x in d["trade_date"].dropna().unique()),
        "tenors": tenors,
        "duplicate_rows": int(d.duplicated(["trade_date", tenor_col]).sum()),
    }
    checks["all_green"] = bool(
        checks["rows"] == 9
        and checks["trade_dates"] == [int(expected_end)]
        and tenors == TARGET_TENORS
        and checks["duplicate_rows"] == 0
    )
    return checks


def make_feature_summary(feature_df: pd.DataFrame, expected_end: int) -> Dict[str, Any]:
    df = feature_df.copy()
    if "date" not in df.columns:
        df["date"] = parse_project_date_series(df["trade_date"])
    else:
        df["date"] = parse_project_date_series(df["date"])
    required_cols = [c for c in ["implied_variance", "forecast_variance", "vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"] if c in df.columns]
    eligible_cols = [c for c in ["vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"] if c in df.columns]
    eligible = df.dropna(subset=eligible_cols) if eligible_cols else pd.DataFrame()
    by_date = df.groupby("date")[[c for c in ["rv21d", "rsi14"] if c in df.columns]].nunique(dropna=False)
    inconsistent_dates = 0
    if len(by_date):
        inconsistent_dates = int(((by_date > 1).any(axis=1)).sum())
    return {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "rows": int(len(df)),
        "start_date": str(df["date"].min().date()) if len(df) else None,
        "end_date": str(df["date"].max().date()) if len(df) else None,
        "expected_end_date": str(pd.to_datetime(str(expected_end), format="%Y%m%d").date()),
        "missing_counts": {c: int(df[c].isna().sum()) for c in required_cols},
        "eligible_start_date": str(eligible["date"].min().date()) if len(eligible) else None,
        "eligible_end_date": str(eligible["date"].max().date()) if len(eligible) else None,
        "dates_with_inconsistent_rv21d_or_rsi14_across_tenors": inconsistent_dates,
    }


def write_reports(audit_dir: Path, payload: Dict[str, Any], stamp: str) -> Tuple[Path, Path]:
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = audit_dir / f"step06_promote_downstream_{stamp}.json"
    md_path = audit_dir / f"step06_promote_downstream_{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    lines: List[str] = []
    lines.append("# VRP Production v1 - Step 06 Promote Downstream Candidate")
    lines.append("")
    lines.append(f"Run timestamp: `{payload['run_timestamp']}`")
    lines.append(f"Project root: `{payload['project_root']}`")
    lines.append(f"Promoted: `{payload['promoted']}`")
    lines.append("")
    lines.append("## Summary")
    for k in [
        "expected_start_date", "expected_end_date", "expected_rows", "realized_rows", "vrp_rows",
        "feature_rows", "latest_snapshot_rows", "all_checks_green"
    ]:
        lines.append(f"- **{k}**: `{payload.get(k)}`")
    lines.append("")
    lines.append("## Candidate files")
    for k, v in payload.get("candidate_files", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## Official files")
    for k, v in payload.get("official_files", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## Backups")
    for k, v in payload.get("backups", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## QA")
    for label, checks in payload.get("qa", {}).items():
        lines.append(f"### {label}")
        for k, v in checks.items():
            lines.append(f"- **{k}**: `{v}`")
        lines.append("")
    if payload.get("failure_reasons"):
        lines.append("## Failure reasons")
        for r in payload["failure_reasons"]:
            lines.append(f"- {r}")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path


def main() -> None:
    args = parse_args()
    stamp = timestamp()
    project_root = Path(args.project_root).expanduser().resolve()
    data_dir = project_root / "data"
    processed_dir = data_dir / "processed"
    staging_dir = processed_dir / "staging"
    backup_dir = processed_dir / "backups" / "production_v1"
    audit_dir = data_dir / "audit" / "production_v1"
    legacy_audit_dir = data_dir / "audit"

    implied_path = processed_dir / f"{IMPLIED_BASENAME}.parquet"
    if not implied_path.exists():
        raise FileNotFoundError(f"Canonical implied history not found: {implied_path}")

    print("Loading canonical implied history for expected range...")
    implied_df = pd.read_parquet(implied_path)
    implied_df = standardize_keys(implied_df, "trade_date", "target_days")
    expected_start = int(args.candidate_start_date) if args.candidate_start_date else int(implied_df["trade_date"].min())
    expected_end = int(args.expected_end_date)
    implied_end = int(implied_df["trade_date"].max())
    expected_rows = int(len(implied_df))

    if implied_end != expected_end:
        raise RuntimeError(f"Canonical implied latest date {implied_end} != expected_end_date {expected_end}")

    print("Locating Step 05 candidate files...")
    candidates: Dict[str, Path] = {}
    for label, spec in PANEL_SPECS.items():
        base = spec["base"]
        candidates[f"{label}_parquet"] = find_candidate(staging_dir, base, expected_start, expected_end, ".parquet")
        candidates[f"{label}_csv"] = find_candidate(staging_dir, base, expected_start, expected_end, ".csv")
    latest_snapshot_candidate = find_latest_snapshot_candidate(staging_dir, expected_end)
    candidates["latest_snapshot_csv"] = latest_snapshot_candidate

    print("Loading candidates and running QA...")
    realized_df = load_table(candidates["realized_parquet"])
    vrp_df = load_table(candidates["vrp_parquet"])
    feature_df = load_table(candidates["feature_parquet"])
    latest_df = load_table(candidates["latest_snapshot_csv"])

    qa = {
        "realized": panel_qa(realized_df, PANEL_SPECS["realized"], "realized", expected_rows, expected_start, expected_end),
        "vrp": panel_qa(vrp_df, PANEL_SPECS["vrp"], "vrp", expected_rows, expected_start, expected_end),
        "feature": panel_qa(feature_df, PANEL_SPECS["feature"], "feature", expected_rows, expected_start, expected_end),
        "latest_snapshot": latest_snapshot_qa(latest_df, expected_end),
    }

    all_checks_green = bool(all(checks.get("all_green") for checks in qa.values()))
    failure_reasons: List[str] = []
    if not all_checks_green:
        for label, checks in qa.items():
            if not checks.get("all_green"):
                failure_reasons.append(f"{label} QA not green")

    official_files: Dict[str, Path] = {
        "realized_parquet": processed_dir / "realized_variance_panel_v0_1.parquet",
        "realized_csv": processed_dir / "realized_variance_panel_v0_1.csv",
        "vrp_parquet": processed_dir / "vrp_panel_v0_1.parquet",
        "vrp_csv": processed_dir / "vrp_panel_v0_1.csv",
        "feature_parquet": processed_dir / "production_feature_panel_v0_1.parquet",
        "feature_csv": processed_dir / "production_feature_panel_v0_1.csv",
        "latest_snapshot_csv": processed_dir / "production_feature_panel_latest_snapshot_v0_1.csv",
        "feature_audit_summary_json": legacy_audit_dir / "production_feature_panel_summary_v0_1.json",
    }

    payload: Dict[str, Any] = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "expected_start_date": expected_start,
        "expected_end_date": expected_end,
        "expected_rows": expected_rows,
        "realized_rows": int(len(realized_df)),
        "vrp_rows": int(len(vrp_df)),
        "feature_rows": int(len(feature_df)),
        "latest_snapshot_rows": int(len(latest_df)),
        "candidate_files": {k: str(v) for k, v in candidates.items()},
        "official_files": {k: str(v) for k, v in official_files.items()},
        "qa": qa,
        "all_checks_green": all_checks_green,
        "failure_reasons": failure_reasons,
        "promoted": False,
        "backups": {},
    }

    if not all_checks_green:
        _, md_path = write_reports(audit_dir, payload, stamp)
        print("\nStep 06 checks FAILED. Nothing was promoted.")
        print("Failure reasons:")
        for r in failure_reasons:
            print(f"- {r}")
        print(f"Report: {md_path}")
        raise SystemExit(1)

    if not args.confirm_promote:
        _, md_path = write_reports(audit_dir, payload, stamp)
        print("\nStep 06 dry run complete. All checks green, but official downstream files were not changed.")
        print("To promote, rerun with --confirm-promote")
        print(f"Report: {md_path}")
        return

    print("All checks green. Creating backups...")
    backups: Dict[str, Optional[str]] = {}
    for label, path in official_files.items():
        backup = backup_file(path, backup_dir, stamp, label)
        backups[label] = str(backup) if backup else None
    payload["backups"] = backups

    print("Promoting candidate downstream files to official paths...")
    # Write from dataframes to normalize parquet/csv pairs and avoid copying stale metadata.
    processed_dir.mkdir(parents=True, exist_ok=True)
    realized_df.to_parquet(official_files["realized_parquet"], index=False)
    realized_df.to_csv(official_files["realized_csv"], index=False)
    vrp_df.to_parquet(official_files["vrp_parquet"], index=False)
    vrp_df.to_csv(official_files["vrp_csv"], index=False)
    feature_df.to_parquet(official_files["feature_parquet"], index=False)
    feature_df.to_csv(official_files["feature_csv"], index=False)
    latest_df.to_csv(official_files["latest_snapshot_csv"], index=False)

    feature_summary = make_feature_summary(feature_df, expected_end)
    legacy_audit_dir.mkdir(parents=True, exist_ok=True)
    official_files["feature_audit_summary_json"].write_text(json.dumps(feature_summary, indent=2, default=str), encoding="utf-8")

    print("Verifying official downstream files after promotion...")
    final_realized = load_table(official_files["realized_parquet"])
    final_vrp = load_table(official_files["vrp_parquet"])
    final_feature = load_table(official_files["feature_parquet"])
    final_latest = load_table(official_files["latest_snapshot_csv"])
    final_qa = {
        "realized": panel_qa(final_realized, PANEL_SPECS["realized"], "final_realized", expected_rows, expected_start, expected_end),
        "vrp": panel_qa(final_vrp, PANEL_SPECS["vrp"], "final_vrp", expected_rows, expected_start, expected_end),
        "feature": panel_qa(final_feature, PANEL_SPECS["feature"], "final_feature", expected_rows, expected_start, expected_end),
        "latest_snapshot": latest_snapshot_qa(final_latest, expected_end),
    }
    final_green = bool(all(checks.get("all_green") for checks in final_qa.values()))
    payload["promoted"] = True
    payload["final_qa"] = final_qa
    payload["feature_summary"] = feature_summary
    payload["all_checks_green"] = bool(all_checks_green and final_green)
    payload["official_files"] = {k: str(v) for k, v in official_files.items()}

    if not final_green:
        payload["failure_reasons"].append("post-promotion final QA not green")
        _, md_path = write_reports(audit_dir, payload, stamp)
        print("\nStep 06 promotion completed but final verification FAILED.")
        print(f"Report: {md_path}")
        raise SystemExit(1)

    _, md_path = write_reports(audit_dir, payload, stamp)

    print("\nStep 06 promote downstream complete.")
    print(f"Expected rows:       {expected_rows}")
    print(f"Realized rows:       {len(final_realized)}")
    print(f"VRP rows:            {len(final_vrp)}")
    print(f"Feature rows:        {len(final_feature)}")
    print(f"Latest snapshot rows:{len(final_latest)}")
    print(f"Canonical range:     {expected_start} to {expected_end}")
    print(f"All checks green:    {payload['all_checks_green']}")
    print(f"Feature file:        {official_files['feature_parquet']}")
    print(f"Report:              {md_path}")


if __name__ == "__main__":
    main()
