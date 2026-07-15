from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
CANONICAL_BASENAME = "vix_term_structure_history_v0_7_1_repaired_total_variance"
FINAL_METHODOLOGY_VERSION = "v0.7.1_repaired_total_variance_cboe_anchors"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create candidate appended repaired term-structure history.")
    p.add_argument("--project-root", required=True, help="VRP project root, e.g. C:\\Users\\patri\\vrp_project")
    p.add_argument("--start-date", required=True, type=int, help="YYYYMMDD start date of Step 02 staging update")
    p.add_argument("--end-date", required=True, type=int, help="YYYYMMDD end date of Step 02 staging update")
    p.add_argument("--allow-yellow", action="store_true", help="Allow Step 02 YELLOW QA rows. Default requires all GREEN.")
    p.add_argument("--replace-overlap", action="store_true", help="Replace existing canonical rows for staged date/tenor keys. Default fails on overlap.")
    return p.parse_args()


def ensure_dirs(project_root: Path) -> Dict[str, Path]:
    data_dir = project_root / "data"
    processed_dir = data_dir / "processed"
    staging_dir = processed_dir / "staging"
    audit_dir = data_dir / "audit" / "production_v1"
    staging_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    return {
        "data": data_dir,
        "processed": processed_dir,
        "staging": staging_dir,
        "audit": audit_dir,
    }


def read_table(base_or_file: Path) -> pd.DataFrame:
    if base_or_file.suffix.lower() == ".parquet":
        if base_or_file.exists():
            return pd.read_parquet(base_or_file)
        csv_path = base_or_file.with_suffix(".csv")
        if csv_path.exists():
            return pd.read_csv(csv_path)
    elif base_or_file.suffix.lower() == ".csv":
        if base_or_file.exists():
            return pd.read_csv(base_or_file)
        pq_path = base_or_file.with_suffix(".parquet")
        if pq_path.exists():
            return pd.read_parquet(pq_path)
    else:
        pq_path = base_or_file.with_suffix(".parquet")
        csv_path = base_or_file.with_suffix(".csv")
        if pq_path.exists():
            return pd.read_parquet(pq_path)
        if csv_path.exists():
            return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Could not find table at {base_or_file} or matching .csv/.parquet")


def normalize_date_int(series: pd.Series) -> pd.Series:
    """Return YYYYMMDD integer dates from int/string/datetime-like series."""
    if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
        s = pd.to_numeric(series, errors="coerce")
        # Values like 20260626 are already yyyymmdd. Values like pandas ns are much larger.
        # Very small numeric date values are treated as datetimes as a fallback.
        out = []
        for x in s:
            if pd.isna(x):
                out.append(np.nan)
            else:
                xi = int(x)
                if 19000101 <= xi <= 21001231:
                    out.append(xi)
                else:
                    ts = pd.to_datetime(xi, errors="coerce")
                    out.append(int(ts.strftime("%Y%m%d")) if pd.notna(ts) else np.nan)
        return pd.Series(out, index=series.index, dtype="Int64")

    dt = pd.to_datetime(series, errors="coerce")
    if dt.notna().any():
        return dt.dt.strftime("%Y%m%d").astype("Int64")

    s = series.astype(str).str.replace("-", "", regex=False).str[:8]
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def normalize_key_columns(df: pd.DataFrame, name: str) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" not in out.columns or "target_days" not in out.columns:
        raise ValueError(f"{name} is missing trade_date or target_days")
    out["trade_date"] = normalize_date_int(out["trade_date"]).astype(int)
    out["target_days"] = pd.to_numeric(out["target_days"], errors="raise").astype(int)
    return out


def load_step02_qa(audit_dir: Path, start_date: int, end_date: int) -> Optional[pd.DataFrame]:
    qa_path = audit_dir / f"step02_eod_staging_qa_{start_date}_{end_date}.csv"
    if qa_path.exists():
        return pd.read_csv(qa_path)
    return None


def require_step02_qa_green(qa_df: Optional[pd.DataFrame], allow_yellow: bool) -> Dict[str, Any]:
    if qa_df is None:
        raise FileNotFoundError("Step 02 QA CSV was not found. Re-run Step 02 before appending.")
    if "qa_status" not in qa_df.columns:
        raise ValueError("Step 02 QA CSV has no qa_status column")
    statuses = sorted(set(str(x).upper() for x in qa_df["qa_status"].dropna().unique()))
    allowed = {"GREEN", "YELLOW"} if allow_yellow else {"GREEN"}
    bad = [s for s in statuses if s not in allowed]
    if bad:
        raise ValueError(f"Step 02 QA has non-allowed statuses: {bad}. Statuses={statuses}")
    if not allow_yellow and any(s != "GREEN" for s in statuses):
        raise ValueError(f"Step 02 QA is not all GREEN. Statuses={statuses}")
    return {
        "step02_qa_found": True,
        "step02_qa_rows": int(len(qa_df)),
        "step02_qa_statuses": statuses,
        "step02_all_green": statuses == ["GREEN"],
    }


def harmonize_staging_to_canonical(canonical: pd.DataFrame, staging: pd.DataFrame) -> pd.DataFrame:
    """Align Step 02 v0.7 output to the canonical v0.7.1 repaired schema.

    New rows are not repaired, so implied_variance/vix_style_vol are preserved as both
    official values and raw audit values where those columns exist.
    """
    new = staging.copy()

    # Preserve raw v0.7 values where canonical schema supports it.
    if "raw_implied_variance" not in new.columns and "implied_variance" in new.columns:
        new["raw_implied_variance"] = new["implied_variance"]
    if "raw_vix_style_vol" not in new.columns and "vix_style_vol" in new.columns:
        new["raw_vix_style_vol"] = new["vix_style_vol"]

    if "source_methodology_version" not in new.columns:
        if "methodology_version" in new.columns:
            new["source_methodology_version"] = new["methodology_version"]
        else:
            new["source_methodology_version"] = pd.NA

    # Match final repaired dataset convention: unchanged rows inherit final methodology version.
    if "methodology_version" in canonical.columns:
        new["methodology_version"] = FINAL_METHODOLOGY_VERSION

    if "is_repaired" in canonical.columns:
        new["is_repaired"] = False
    if "repair_method" in canonical.columns and "repair_method" not in new.columns:
        new["repair_method"] = pd.NA

    # Add any other canonical columns as NA.
    for col in canonical.columns:
        if col not in new.columns:
            new[col] = pd.NA

    # Drop staging-only columns from candidate official history, preserving canonical column order.
    new = new[list(canonical.columns)].copy()

    # Best-effort dtype matching for common columns.
    for col in ["trade_date", "target_days"]:
        new[col] = pd.to_numeric(new[col], errors="raise").astype(int)
    for col in ["implied_variance", "vix_style_vol", "raw_implied_variance", "raw_vix_style_vol"]:
        if col in new.columns:
            new[col] = pd.to_numeric(new[col], errors="coerce")
    if "is_repaired" in new.columns:
        new["is_repaired"] = new["is_repaired"].fillna(False).astype(bool)
    return new


def curve_qa(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for trade_date, g in df.groupby("trade_date"):
        gg = g.sort_values("target_days")
        tenors = sorted(int(x) for x in gg["target_days"].dropna().unique())
        has_9 = tenors == TARGET_TENORS
        dupes = int(gg.duplicated(["trade_date", "target_days"]).sum())
        invalid = int((pd.to_numeric(gg["implied_variance"], errors="coerce") <= 0).sum()) if "implied_variance" in gg.columns else len(gg)
        min_iv = float(pd.to_numeric(gg["implied_variance"], errors="coerce").min()) if "implied_variance" in gg.columns and len(gg) else np.nan
        max_iv = float(pd.to_numeric(gg["implied_variance"], errors="coerce").max()) if "implied_variance" in gg.columns and len(gg) else np.nan
        min_tvar_diff = np.nan
        neg_fwd = False
        if "implied_variance" in gg.columns:
            t = pd.to_numeric(gg["target_days"], errors="coerce") / 365.0
            v = pd.to_numeric(gg["implied_variance"], errors="coerce")
            total_var = t * v
            diffs = total_var.diff().dropna()
            if len(diffs):
                min_tvar_diff = float(diffs.min())
                neg_fwd = bool((diffs < -1e-12).any())
        status = "GREEN" if has_9 and dupes == 0 and invalid == 0 and not neg_fwd else "RED"
        rows.append({
            "trade_date": int(trade_date),
            "row_count": int(len(gg)),
            "has_9_tenors": bool(has_9),
            "duplicate_tenor_rows": dupes,
            "invalid_variance_rows": invalid,
            "min_implied_variance": min_iv,
            "max_implied_variance": max_iv,
            "min_total_variance_diff": min_tvar_diff,
            "negative_forward_variance_flag": neg_fwd,
            "qa_status": status,
        })
    return pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)


def candidate_panel_qa(candidate: pd.DataFrame, new_rows: pd.DataFrame) -> Dict[str, Any]:
    duplicate_rows = int(candidate.duplicated(["trade_date", "target_days"]).sum())
    counts = candidate.groupby("trade_date")["target_days"].nunique()
    dates_not_9 = [int(x) for x in counts[counts != 9].index.tolist()]
    invalid_variance_rows = int((pd.to_numeric(candidate["implied_variance"], errors="coerce") <= 0).sum())
    new_invalid_variance_rows = int((pd.to_numeric(new_rows["implied_variance"], errors="coerce") <= 0).sum())
    new_curve_qa = curve_qa(new_rows)
    candidate_curve_qa_new_dates = curve_qa(candidate[candidate["trade_date"].isin(new_rows["trade_date"].unique())])
    return {
        "candidate_rows": int(len(candidate)),
        "candidate_date_count": int(candidate["trade_date"].nunique()),
        "candidate_min_date": int(candidate["trade_date"].min()),
        "candidate_max_date": int(candidate["trade_date"].max()),
        "duplicate_date_tenor_rows": duplicate_rows,
        "dates_not_having_9_tenors_count": int(len(dates_not_9)),
        "dates_not_having_9_tenors_sample": dates_not_9[:20],
        "invalid_variance_rows": invalid_variance_rows,
        "new_invalid_variance_rows": new_invalid_variance_rows,
        "new_curve_qa_status_counts": new_curve_qa["qa_status"].value_counts().to_dict() if not new_curve_qa.empty else {},
        "candidate_new_dates_curve_qa_status_counts": candidate_curve_qa_new_dates["qa_status"].value_counts().to_dict() if not candidate_curve_qa_new_dates.empty else {},
        "all_candidate_checks_green": (
            duplicate_rows == 0
            and len(dates_not_9) == 0
            and invalid_variance_rows == 0
            and not new_curve_qa.empty
            and set(new_curve_qa["qa_status"].unique()) == {"GREEN"}
        ),
    }


def dataframe_preview_markdown(df: pd.DataFrame, max_rows: int = 30) -> str:
    if df is None or df.empty:
        return "No rows."
    return df.head(max_rows).to_markdown(index=False)


def write_report(
    audit_dir: Path,
    payload: Dict[str, Any],
    new_curve_qa: pd.DataFrame,
    candidate_new_dates_qa: pd.DataFrame,
) -> Dict[str, str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = audit_dir / f"step03_append_candidate_{ts}.json"
    md_path = audit_dir / f"step03_append_candidate_{ts}.md"
    qa_new_path = audit_dir / f"step03_append_candidate_new_curve_qa_{payload['start_date']}_{payload['end_date']}.csv"
    qa_candidate_path = audit_dir / f"step03_append_candidate_panel_qa_{payload['start_date']}_{payload['end_date']}.csv"

    new_curve_qa.to_csv(qa_new_path, index=False)
    candidate_new_dates_qa.to_csv(qa_candidate_path, index=False)

    payload = dict(payload)
    payload["report_files"] = {
        "json": str(json_path),
        "markdown": str(md_path),
        "new_curve_qa_csv": str(qa_new_path),
        "candidate_new_dates_qa_csv": str(qa_candidate_path),
    }

    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    lines = []
    lines.append("# VRP Production v1 - Step 03 Append Candidate")
    lines.append("")
    for key in [
        "run_timestamp",
        "project_root",
        "canonical_input_parquet",
        "staging_input_parquet",
        "start_date",
        "end_date",
        "old_rows",
        "new_rows",
        "candidate_rows",
        "old_max_date",
        "new_min_date",
        "new_max_date",
        "candidate_min_date",
        "candidate_max_date",
        "duplicate_date_tenor_rows",
        "dates_not_having_9_tenors_count",
        "invalid_variance_rows",
        "all_candidate_checks_green",
    ]:
        if key in payload:
            lines.append(f"- **{key}**: `{payload[key]}`")
    lines.append("")
    lines.append("## Output files")
    for k, v in payload.get("output_files", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## Step 02 QA")
    for k, v in payload.get("step02_qa", {}).items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("")
    lines.append("## New-row curve QA")
    lines.append(dataframe_preview_markdown(new_curve_qa))
    lines.append("")
    lines.append("## Candidate panel QA on appended dates")
    lines.append(dataframe_preview_markdown(candidate_new_dates_qa))
    lines.append("")
    lines.append("## Next step")
    if payload.get("all_candidate_checks_green"):
        lines.append("Candidate checks are GREEN. Review output files, then promote with a separate explicit promotion step.")
    else:
        lines.append("Candidate checks are NOT all green. Do not promote until investigated.")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return payload["report_files"]


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()
    paths = ensure_dirs(project_root)
    start_date = int(args.start_date)
    end_date = int(args.end_date)

    canonical_parquet = paths["processed"] / f"{CANONICAL_BASENAME}.parquet"
    canonical_csv = paths["processed"] / f"{CANONICAL_BASENAME}.csv"
    staging_parquet = paths["staging"] / f"vix_term_structure_eod_update_{start_date}_{end_date}.parquet"
    staging_csv = paths["staging"] / f"vix_term_structure_eod_update_{start_date}_{end_date}.csv"

    print("Loading canonical repaired history...")
    canonical = read_table(canonical_parquet if canonical_parquet.exists() else canonical_csv)
    canonical = normalize_key_columns(canonical, "canonical")

    print("Loading Step 02 staging rows...")
    staging = read_table(staging_parquet if staging_parquet.exists() else staging_csv)
    staging = normalize_key_columns(staging, "staging")

    print("Checking Step 02 QA...")
    step02_qa_df = load_step02_qa(paths["audit"], start_date, end_date)
    step02_qa_summary = require_step02_qa_green(step02_qa_df, allow_yellow=args.allow_yellow)

    staging = staging[(staging["trade_date"] >= start_date) & (staging["trade_date"] <= end_date)].copy()
    if staging.empty:
        raise RuntimeError("No staging rows found for requested date range")

    staged_keys = staging[["trade_date", "target_days"]].drop_duplicates()
    overlap = canonical.merge(staged_keys, on=["trade_date", "target_days"], how="inner")
    if len(overlap) and not args.replace_overlap:
        sample = overlap[["trade_date", "target_days"]].head(20).to_dict(orient="records")
        raise RuntimeError(
            f"Staging overlaps canonical history on {len(overlap)} date/tenor rows. "
            f"This append is not append-only. Sample={sample}. Use --replace-overlap only if intentional."
        )

    new_aligned = harmonize_staging_to_canonical(canonical, staging)
    if args.replace_overlap and len(overlap):
        key_index = pd.MultiIndex.from_frame(staged_keys[["trade_date", "target_days"]])
        can_index = pd.MultiIndex.from_frame(canonical[["trade_date", "target_days"]])
        canonical_base = canonical.loc[~can_index.isin(key_index)].copy()
    else:
        canonical_base = canonical.copy()

    candidate = pd.concat([canonical_base, new_aligned], ignore_index=True)
    candidate = normalize_key_columns(candidate, "candidate")
    candidate = candidate.sort_values(["trade_date", "target_days"]).reset_index(drop=True)

    new_curve_qa = curve_qa(new_aligned)
    candidate_new_dates_qa = curve_qa(candidate[candidate["trade_date"].isin(new_aligned["trade_date"].unique())])
    qa_payload = candidate_panel_qa(candidate, new_aligned)

    out_base = paths["staging"] / f"{CANONICAL_BASENAME}_candidate_{start_date}_{end_date}"
    out_parquet = out_base.with_suffix(".parquet")
    out_csv = out_base.with_suffix(".csv")
    print("Writing candidate files...")
    candidate.to_parquet(out_parquet, index=False)
    candidate.to_csv(out_csv, index=False)

    payload: Dict[str, Any] = {
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "canonical_input_parquet": str(canonical_parquet),
        "staging_input_parquet": str(staging_parquet),
        "start_date": start_date,
        "end_date": end_date,
        "old_rows": int(len(canonical)),
        "new_rows": int(len(new_aligned)),
        "candidate_rows": int(len(candidate)),
        "old_min_date": int(canonical["trade_date"].min()),
        "old_max_date": int(canonical["trade_date"].max()),
        "new_min_date": int(new_aligned["trade_date"].min()),
        "new_max_date": int(new_aligned["trade_date"].max()),
        "new_date_count": int(new_aligned["trade_date"].nunique()),
        "new_tenor_count_by_date": {str(k): int(v) for k, v in new_aligned.groupby("trade_date")["target_days"].nunique().to_dict().items()},
        "step02_qa": step02_qa_summary,
        "output_files": {
            "candidate_parquet": str(out_parquet),
            "candidate_csv": str(out_csv),
        },
        **qa_payload,
    }
    report_files = write_report(paths["audit"], payload, new_curve_qa, candidate_new_dates_qa)

    print("")
    print("Step 03 append candidate complete.")
    print(f"Old rows:       {len(canonical)}")
    print(f"New rows:       {len(new_aligned)}")
    print(f"Candidate rows: {len(candidate)}")
    print(f"Candidate date range: {int(candidate['trade_date'].min())} to {int(candidate['trade_date'].max())}")
    print(f"All candidate checks green: {qa_payload['all_candidate_checks_green']}")
    print(f"Candidate parquet: {out_parquet}")
    print(f"Report: {report_files['markdown']}")
    return 0 if qa_payload["all_candidate_checks_green"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
