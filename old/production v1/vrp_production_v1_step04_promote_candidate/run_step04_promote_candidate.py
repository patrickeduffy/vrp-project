from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

CANONICAL_BASENAME = "vix_term_structure_history_v0_7_1_repaired_total_variance"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promote QA'd Step 03 candidate to canonical repaired VIX term-structure history.")
    p.add_argument("--project-root", required=True, help="Project root, e.g. C:\\Users\\patri\\vrp_project")
    p.add_argument("--start-date", required=True, type=int, help="First appended trade_date, YYYYMMDD")
    p.add_argument("--end-date", required=True, type=int, help="Last appended trade_date, YYYYMMDD")
    p.add_argument("--candidate-parquet", default=None, help="Optional explicit candidate parquet path")
    p.add_argument("--confirm-promote", action="store_true", help="Required to overwrite canonical files. Without this, dry-run only.")
    return p.parse_args()


def normalize_trade_date_series(s: pd.Series) -> pd.Series:
    """Return YYYYMMDD integer trade dates from common stored formats."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.strftime("%Y%m%d").astype(int)
    if pd.api.types.is_integer_dtype(s):
        # Could already be YYYYMMDD, or ns epoch from bad parse; keep if plausible.
        vals = s.astype("int64")
        if vals.dropna().between(19000101, 21000101).all():
            return vals.astype(int)
        return pd.to_datetime(vals).dt.strftime("%Y%m%d").astype(int)
    # Strings / objects
    ss = s.astype(str).str.strip()
    # Strip .0 if read from CSV as float-like string.
    ss = ss.str.replace(r"\.0$", "", regex=True)
    # YYYYMMDD strings
    ymd_mask = ss.str.fullmatch(r"\d{8}")
    out = pd.Series(index=s.index, dtype="int64")
    if ymd_mask.any():
        out.loc[ymd_mask] = ss.loc[ymd_mask].astype(int)
    if (~ymd_mask).any():
        parsed = pd.to_datetime(ss.loc[~ymd_mask], errors="raise")
        out.loc[~ymd_mask] = parsed.dt.strftime("%Y%m%d").astype(int)
    return out.astype(int)


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" not in out.columns:
        raise ValueError("Missing required column: trade_date")
    out["trade_date"] = normalize_trade_date_series(out["trade_date"])
    if "target_days" not in out.columns:
        raise ValueError("Missing required column: target_days")
    out["target_days"] = pd.to_numeric(out["target_days"], errors="raise").astype(int)
    return out


def find_candidate(project_root: Path, start_date: int, end_date: int, explicit: Optional[str]) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise FileNotFoundError(f"Explicit candidate parquet not found: {path}")
        return path
    staging_dir = project_root / "data" / "processed" / "staging"
    pattern = f"{CANONICAL_BASENAME}_candidate_{start_date}_{end_date}.parquet"
    path = staging_dir / pattern
    if path.exists():
        return path
    # Fallback: most recent matching candidate with same dates in name.
    matches = sorted(staging_dir.glob(f"*candidate*{start_date}*{end_date}*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if matches:
        return matches[0]
    raise FileNotFoundError(f"No candidate parquet found in {staging_dir} for {start_date}-{end_date}")


def dataframe_checks(df: pd.DataFrame, label: str) -> Dict[str, Any]:
    checks: Dict[str, Any] = {"label": label}
    checks["rows"] = int(len(df))
    checks["date_min"] = int(df["trade_date"].min()) if len(df) else None
    checks["date_max"] = int(df["trade_date"].max()) if len(df) else None
    checks["duplicate_key_rows"] = int(df.duplicated(["trade_date", "target_days"]).sum())
    counts = df.groupby("trade_date")["target_days"].nunique().rename("tenor_count")
    bad_tenor_counts = counts[counts != 9]
    checks["dates_not_having_9_tenors"] = int(len(bad_tenor_counts))
    checks["date_count"] = int(counts.shape[0])
    checks["target_days"] = sorted(int(x) for x in df["target_days"].dropna().unique())

    variance_cols = [c for c in df.columns if "variance" in c.lower()]
    checks["variance_columns_checked"] = variance_cols
    invalid_counts = {}
    for col in variance_cols:
        vals = pd.to_numeric(df[col], errors="coerce")
        invalid_counts[col] = int(vals.isna().sum() + (vals <= 0).sum())
    checks["invalid_variance_counts"] = invalid_counts
    checks["all_green"] = (
        checks["duplicate_key_rows"] == 0
        and checks["dates_not_having_9_tenors"] == 0
        and all(v == 0 for v in invalid_counts.values())
    )
    return checks


def write_reports(audit_dir: Path, payload: Dict[str, Any], timestamp: str) -> Tuple[Path, Path]:
    audit_dir.mkdir(parents=True, exist_ok=True)
    json_path = audit_dir / f"step04_promote_candidate_{timestamp}.json"
    md_path = audit_dir / f"step04_promote_candidate_{timestamp}.md"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    lines: List[str] = []
    lines.append("# VRP Production v1 - Step 04 Promote Candidate")
    lines.append("")
    lines.append(f"Run timestamp: `{payload['run_timestamp']}`")
    lines.append("")
    lines.append(f"Project root: `{payload['project_root']}`")
    lines.append(f"Canonical parquet: `{payload['canonical_parquet']}`")
    lines.append(f"Candidate parquet: `{payload['candidate_parquet']}`")
    lines.append(f"Promoted: `{payload['promoted']}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    for k in [
        "old_rows", "candidate_rows", "new_rows", "old_date_max", "candidate_date_max",
        "expected_start_date", "expected_end_date", "all_checks_green"
    ]:
        lines.append(f"- **{k}**: `{payload.get(k)}`")
    lines.append("")
    lines.append("## Candidate checks")
    lines.append("")
    cand_checks = payload.get("candidate_checks", {})
    for k, v in cand_checks.items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## New-row checks")
    lines.append("")
    new_checks = payload.get("new_rows_checks", {})
    for k, v in new_checks.items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## Backup files")
    lines.append("")
    for k, v in payload.get("backups", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## Final canonical checks")
    lines.append("")
    final_checks = payload.get("final_canonical_checks")
    if final_checks:
        for k, v in final_checks.items():
            lines.append(f"- **{k}**: `{v}`")
    else:
        lines.append("Dry run only; canonical file was not overwritten.")
    lines.append("")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return json_path, md_path


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()
    data_dir = project_root / "data"
    processed_dir = data_dir / "processed"
    staging_dir = processed_dir / "staging"
    audit_dir = data_dir / "audit" / "production_v1"
    backup_dir = processed_dir / "backups" / "production_v1"

    canonical_parquet = processed_dir / f"{CANONICAL_BASENAME}.parquet"
    canonical_csv = processed_dir / f"{CANONICAL_BASENAME}.csv"
    candidate_parquet = find_candidate(project_root, args.start_date, args.end_date, args.candidate_parquet)

    if not canonical_parquet.exists():
        raise FileNotFoundError(f"Canonical parquet not found: {canonical_parquet}")
    if not candidate_parquet.exists():
        raise FileNotFoundError(f"Candidate parquet not found: {candidate_parquet}")

    print("Loading canonical repaired history...")
    old_df = normalize_dates(pd.read_parquet(canonical_parquet))
    print("Loading candidate history...")
    cand_df = normalize_dates(pd.read_parquet(candidate_parquet))

    old_max = int(old_df["trade_date"].max())
    old_rows = int(len(old_df))
    cand_rows = int(len(cand_df))
    new_df = cand_df[cand_df["trade_date"] > old_max].copy()

    candidate_checks = dataframe_checks(cand_df, "candidate")
    new_rows_checks = dataframe_checks(new_df, "new_rows") if not new_df.empty else {"all_green": False, "reason": "No new rows above old max date"}

    new_dates = sorted(int(x) for x in new_df["trade_date"].unique()) if not new_df.empty else []
    expected_start = int(args.start_date)
    expected_end = int(args.end_date)

    all_checks_green = True
    failure_reasons: List[str] = []

    if cand_rows <= old_rows:
        all_checks_green = False
        failure_reasons.append("candidate_rows <= old_rows")
    if candidate_checks.get("duplicate_key_rows") != 0:
        all_checks_green = False
        failure_reasons.append("candidate has duplicate trade_date/target_days rows")
    if not candidate_checks.get("all_green"):
        all_checks_green = False
        failure_reasons.append("candidate dataframe checks not green")
    if not new_rows_checks.get("all_green"):
        all_checks_green = False
        failure_reasons.append("new rows checks not green")
    if new_dates and min(new_dates) != expected_start:
        all_checks_green = False
        failure_reasons.append(f"new row min date {min(new_dates)} != expected start {expected_start}")
    if new_dates and max(new_dates) != expected_end:
        all_checks_green = False
        failure_reasons.append(f"new row max date {max(new_dates)} != expected end {expected_end}")
    if len(new_df) != len(new_dates) * 9:
        all_checks_green = False
        failure_reasons.append(f"new rows {len(new_df)} != new_dates*9 {len(new_dates)*9}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload: Dict[str, Any] = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "canonical_parquet": str(canonical_parquet),
        "canonical_csv": str(canonical_csv),
        "candidate_parquet": str(candidate_parquet),
        "expected_start_date": expected_start,
        "expected_end_date": expected_end,
        "old_rows": old_rows,
        "candidate_rows": cand_rows,
        "new_rows": int(len(new_df)),
        "new_dates": new_dates,
        "old_date_min": int(old_df["trade_date"].min()),
        "old_date_max": old_max,
        "candidate_date_min": int(cand_df["trade_date"].min()),
        "candidate_date_max": int(cand_df["trade_date"].max()),
        "candidate_checks": candidate_checks,
        "new_rows_checks": new_rows_checks,
        "all_checks_green": all_checks_green,
        "failure_reasons": failure_reasons,
        "promoted": False,
        "backups": {},
        "final_canonical_checks": None,
    }

    if not all_checks_green:
        json_path, md_path = write_reports(audit_dir, payload, timestamp)
        print("\nStep 04 checks FAILED. Nothing was promoted.")
        print("Failure reasons:")
        for r in failure_reasons:
            print(f"- {r}")
        print(f"Report: {md_path}")
        raise SystemExit(1)

    if not args.confirm_promote:
        json_path, md_path = write_reports(audit_dir, payload, timestamp)
        print("\nStep 04 dry run complete. All checks green, but canonical files were not changed.")
        print("To promote, rerun with --confirm-promote")
        print(f"Report: {md_path}")
        return

    print("All checks green. Creating backups...")
    backup_dir.mkdir(parents=True, exist_ok=True)
    parquet_backup = backup_dir / f"{CANONICAL_BASENAME}_backup_before_step04_{timestamp}.parquet"
    shutil.copy2(canonical_parquet, parquet_backup)
    payload["backups"]["canonical_parquet_backup"] = str(parquet_backup)

    csv_backup = None
    if canonical_csv.exists():
        csv_backup = backup_dir / f"{CANONICAL_BASENAME}_backup_before_step04_{timestamp}.csv"
        shutil.copy2(canonical_csv, csv_backup)
        payload["backups"]["canonical_csv_backup"] = str(csv_backup)

    print("Promoting candidate to canonical parquet/csv...")
    # Save using candidate dataframe as loaded/normalized. Preserve candidate columns/order except normalized dates are int YYYYMMDD.
    cand_df = cand_df.sort_values(["trade_date", "target_days"]).reset_index(drop=True)
    cand_df.to_parquet(canonical_parquet, index=False)
    cand_df.to_csv(canonical_csv, index=False)

    print("Verifying promoted canonical history...")
    final_df = normalize_dates(pd.read_parquet(canonical_parquet))
    final_checks = dataframe_checks(final_df, "final_canonical")
    payload["promoted"] = True
    payload["final_canonical_checks"] = final_checks

    if int(len(final_df)) != cand_rows or int(final_df["trade_date"].max()) != int(cand_df["trade_date"].max()) or not final_checks.get("all_green"):
        payload["all_checks_green"] = False
        payload["failure_reasons"].append("post-promotion verification failed")
        json_path, md_path = write_reports(audit_dir, payload, timestamp)
        print("\nStep 04 promotion completed but post-promotion verification FAILED.")
        print(f"Report: {md_path}")
        raise SystemExit(1)

    json_path, md_path = write_reports(audit_dir, payload, timestamp)

    print("\nStep 04 promote candidate complete.")
    print(f"Old rows:        {old_rows}")
    print(f"New rows:        {len(new_df)}")
    print(f"Canonical rows:  {len(final_df)}")
    print(f"Canonical range: {int(final_df['trade_date'].min())} to {int(final_df['trade_date'].max())}")
    print(f"All checks green: {payload['all_checks_green'] and final_checks.get('all_green')}")
    print(f"Backup parquet:  {parquet_backup}")
    print(f"Canonical file:  {canonical_parquet}")
    print(f"Report:          {md_path}")


if __name__ == "__main__":
    main()
