
"""
VRP locked Cell 4 candidate feature panel update v1.

Scope:
  Build the locked Cell 4 candidate feature panel through a requested end date
  using the extended forecast_model_corsi_v1 source panel.

Not in scope:
  model fitting
  forecast scoring
  final signal panel
  thresholds / sizing / selection logic
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
EPS = 1.0e-12
OVERLAP_TOL = 1.0e-10

REQUIRED_LOCKED_FEATURES = [
    "candidate_log_downside_rv_5d",
    "candidate_log_downside_rv_10d",
    "candidate_log_downside_rv_21d",
    "candidate_log_downside_rv_63d",
    "candidate_downside_share_5d",
    "candidate_downside_share_10d",
    "candidate_max_abs_return_3d",
    "candidate_max_abs_return_5d",
    "candidate_max_abs_return_10d",
]

AUX_FEATURES = [
    "candidate_log_rv_5d",
    "candidate_log_rv_10d",
    "candidate_log_rv_21d",
    "candidate_log_rv_63d",
    "candidate_min_return_3d",
    "candidate_min_return_5d",
    "candidate_min_return_10d",
    "candidate_negative_return_count_5d",
    "candidate_negative_return_count_10d",
    "candidate_negative_return_count_21d",
    "candidate_negative_return_count_63d",
    "candidate_downside_share_21d",
    "candidate_downside_share_63d",
]

COMPARE_COLS = [
    "spx_close_for_features",
    "spx_log_return",
    *REQUIRED_LOCKED_FEATURES,
    *AUX_FEATURES,
]


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--source-panel", default=None)
    p.add_argument("--old-feature-panel", default=None)
    p.add_argument("--end-date", default=None)
    return p.parse_args()


def parse_dates(s: pd.Series) -> pd.Series:
    raw = pd.Series(s, index=s.index)
    nonnull = raw.dropna()
    if len(nonnull) == 0:
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = nonnull.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if as_str.str.fullmatch(r"\d{8}").mean() > 0.80:
        out = pd.to_datetime(
            raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        out = pd.to_datetime(raw, errors="coerce")
    return out.dt.normalize()


def latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No files found: {directory / pattern}")
    return files[0]


def normalize_date_tenor(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["date"] = parse_dates(out["date"])
    elif "trade_date" in out.columns:
        out["date"] = parse_dates(out["trade_date"])
    else:
        raise ValueError("Panel missing date/trade_date.")

    out["trade_date"] = out["date"].dt.strftime("%Y%m%d").astype("Int64")

    tenor_col = "tenor" if "tenor" in out.columns else ("target_days" if "target_days" in out.columns else None)
    if tenor_col is None:
        raise ValueError("Panel missing tenor/target_days.")

    out["tenor"] = pd.to_numeric(out[tenor_col], errors="coerce").astype("Int64")
    if "target_days" not in out.columns:
        out["target_days"] = out["tenor"]
    else:
        out["target_days"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")

    return out


def first_nonnull(x: pd.Series):
    y = x.dropna()
    if len(y) == 0:
        return np.nan
    return y.iloc[0]


def assert_daily_constant(df: pd.DataFrame, col: str, tol: float = 1e-12) -> None:
    if col not in df.columns:
        return

    chk = (
        df.groupby("date")[col]
        .agg(lambda x: pd.to_numeric(x, errors="coerce").dropna().max() - pd.to_numeric(x, errors="coerce").dropna().min()
             if pd.to_numeric(x, errors="coerce").dropna().shape[0] else 0.0)
    )
    bad = chk[chk.abs() > tol]
    if len(bad):
        raise RuntimeError(f"Column {col} is not constant within date. Bad sample:\n{bad.head(10)}")


def build_daily_locked_features(source: pd.DataFrame) -> pd.DataFrame:
    src = source.copy()

    close_candidates = [
        "spx_close",
        "spx_close_for_features",
        "eod_close_sanitized",
        "eod_close",
        "spy_close",
        "close",
        "last_close",
    ]
    return_candidates = [
        "spx_log_return",
        "spy_total_return",
        "spy_log_return",
        "log_return",
    ]

    close_cols = [c for c in close_candidates if c in src.columns]
    return_cols = [c for c in return_candidates if c in src.columns]

    if not close_cols and not return_cols:
        raise ValueError("Source must contain a usable close or return column.")

    for c in close_cols:
        assert_daily_constant(src, c)
    for c in return_cols:
        assert_daily_constant(src, c)

    agg = {"date": sorted(src["date"].dropna().unique())}
    daily = pd.DataFrame(agg).sort_values("date").reset_index(drop=True)

    if close_cols:
        close_frame = src[["date", *close_cols]].copy()
        for c in close_cols:
            close_frame[c] = pd.to_numeric(close_frame[c], errors="coerce")
        close_frame["_feature_close_candidate"] = close_frame[close_cols].bfill(axis=1).iloc[:, 0]
        close_by_date = close_frame.groupby("date")["_feature_close_candidate"].agg(first_nonnull)
        daily["spx_close_for_features"] = daily["date"].map(close_by_date)
    else:
        daily["spx_close_for_features"] = np.nan

    if return_cols:
        ret_frame = src[["date", *return_cols]].copy()
        for c in return_cols:
            ret_frame[c] = pd.to_numeric(ret_frame[c], errors="coerce")
        ret_frame["_feature_return_candidate"] = ret_frame[return_cols].bfill(axis=1).iloc[:, 0]
        ret_by_date = ret_frame.groupby("date")["_feature_return_candidate"].agg(first_nonnull)
        daily["spx_log_return_from_source"] = daily["date"].map(ret_by_date)
    else:
        daily["spx_log_return_from_source"] = np.nan

    close_series = pd.to_numeric(daily["spx_close_for_features"], errors="coerce")
    ret_from_close = np.log(close_series / close_series.shift(1))
    ret_from_source = pd.to_numeric(daily["spx_log_return_from_source"], errors="coerce")

    # Prefer locked Cell 4 close-derived returns, but guard against scale breaks
    # when a newly appended source row has SPY-scale close while prior rows are SPX-scale.
    use_source_return = ret_from_close.isna() | (
        ret_from_source.notna() & ret_from_close.abs().gt(0.25)
    )
    daily["spx_log_return"] = ret_from_close.mask(use_source_return, ret_from_source)

    scale_fallback_rows = daily.loc[
        ret_from_close.notna() & ret_from_source.notna() & ret_from_close.abs().gt(0.25),
        ["date", "spx_close_for_features", "spx_log_return_from_source"]
    ].copy()
    if len(scale_fallback_rows):
        print("WARNING: used source return fallback for close-scale-break rows:")
        print(scale_fallback_rows.tail(20).to_string(index=False))

    print("Daily feature close candidate columns:", close_cols)
    print("Daily feature return fallback columns:", return_cols)
    print("Daily feature latest return inputs:")
    latest_debug_cols = ["date", "spx_close_for_features", "spx_log_return_from_source", "spx_log_return"]
    print(daily[latest_debug_cols].tail(10).to_string(index=False))

    r = pd.to_numeric(daily["spx_log_return"], errors="coerce")
    daily["ret_sq"] = r ** 2
    daily["downside_ret_sq"] = np.where(r < 0.0, r ** 2, 0.0)
    daily["abs_return_1d_raw"] = r.abs()
    daily["is_negative_return"] = (r < 0.0).astype(float)

    for w in [5, 10, 21, 63]:
        rv = daily["ret_sq"].rolling(w, min_periods=w).mean() * 252.0
        downside_rv = daily["downside_ret_sq"].rolling(w, min_periods=w).mean() * 252.0

        daily[f"candidate_log_rv_{w}d"] = np.log(rv.clip(lower=EPS))
        daily[f"candidate_log_downside_rv_{w}d"] = np.log(pd.Series(downside_rv).clip(lower=EPS))

        ret_sq_sum = daily["ret_sq"].rolling(w, min_periods=w).sum()
        downside_sum = daily["downside_ret_sq"].rolling(w, min_periods=w).sum()
        daily[f"candidate_downside_share_{w}d"] = downside_sum / ret_sq_sum.replace(0.0, np.nan)

        daily[f"candidate_negative_return_count_{w}d"] = (
            daily["is_negative_return"].rolling(w, min_periods=w).sum()
        )

    for w in [3, 5, 10]:
        daily[f"candidate_max_abs_return_{w}d"] = (
            daily["abs_return_1d_raw"].rolling(w, min_periods=w).max()
        )
        daily[f"candidate_min_return_{w}d"] = (
            daily["spx_log_return"].rolling(w, min_periods=w).min()
        )

    keep_cols = [
        "date",
        "spx_close_for_features",
        "spx_log_return",
        *REQUIRED_LOCKED_FEATURES,
        *AUX_FEATURES,
    ]
    return daily[[c for c in keep_cols if c in daily.columns]].copy()


def build_output_panel(source: pd.DataFrame, old_template: pd.DataFrame, daily_features: pd.DataFrame) -> pd.DataFrame:
    base = source.copy()

    # Keep target 9-tenor grid.
    base = base[base["tenor"].isin(EXPECTED_TENORS)].copy()
    base = base.sort_values(["date", "tenor"]).drop_duplicates(["date", "tenor"], keep="first").reset_index(drop=True)

    merged = base.merge(daily_features, on="date", how="left", suffixes=("", "_calc"))

    # Override these with locked Cell 4 calculations.
    for col in ["spx_close_for_features", "spx_log_return", *REQUIRED_LOCKED_FEATURES, *AUX_FEATURES]:
        calc_col = f"{col}_calc"
        if calc_col in merged.columns:
            merged[col] = merged[calc_col]
            merged = merged.drop(columns=[calc_col])

    if "vix_style_vol" in merged.columns and "implied_variance" in merged.columns:
        merged["vix_style_vol"] = np.sqrt(pd.to_numeric(merged["implied_variance"], errors="coerce")) * 100.0

    if "tenor_group" in old_template.columns:
        if "tenor_group" not in merged.columns:
            merged["tenor_group"] = np.nan
        group_map = {
            9: "front", 12: "front", 15: "front",
            18: "middle", 21: "middle", 24: "middle",
            27: "back", 30: "back", 33: "back",
        }
        missing_group = merged["tenor_group"].isna()
        merged.loc[missing_group, "tenor_group"] = merged.loc[missing_group, "tenor"].map(group_map)

    # Preserve old locked feature panel schema as primary output schema.
    final_cols = list(old_template.columns)
    for c in REQUIRED_LOCKED_FEATURES:
        if c not in final_cols:
            final_cols.append(c)
    for c in ["spx_close_for_features", "spx_log_return"]:
        if c not in final_cols:
            final_cols.append(c)

    for c in final_cols:
        if c not in merged.columns:
            merged[c] = np.nan

    out = merged[final_cols].copy()

    # For all date/tenor rows already present in the original locked Cell 4 panel,
    # preserve the old values exactly. The updater should only calculate appended rows.
    old_overlay = normalize_date_tenor(old_template.copy())
    old_overlay = old_overlay[old_overlay["tenor"].isin(EXPECTED_TENORS)].copy()
    old_overlay["_key_trade_date"] = old_overlay["trade_date"].astype(int)
    old_overlay["_key_tenor"] = old_overlay["tenor"].astype(int)
    old_overlay["_old_locked_present"] = True

    out["_key_trade_date"] = out["trade_date"].astype(int)
    out["_key_tenor"] = out["tenor"].astype(int)

    old_cols = [c for c in final_cols if c in old_overlay.columns]
    overlay = out.merge(
        old_overlay[["_key_trade_date", "_key_tenor", "_old_locked_present", *old_cols]],
        on=["_key_trade_date", "_key_tenor"],
        how="left",
        suffixes=("", "_old_locked"),
    )

    old_mask = overlay["_old_locked_present"].fillna(False).astype(bool)
    for c in old_cols:
        old_c = f"{c}_old_locked"
        if old_c in overlay.columns:
            overlay.loc[old_mask, c] = overlay.loc[old_mask, old_c]

    drop_cols = [
        c for c in overlay.columns
        if c.endswith("_old_locked") or c in ["_key_trade_date", "_key_tenor", "_old_locked_present"]
    ]
    out = overlay.drop(columns=drop_cols)

    out = out[final_cols].copy()
    out = out.sort_values(["date", "tenor"]).reset_index(drop=True)
    return out


def compare_overlap(old: pd.DataFrame, new: pd.DataFrame, audit_dir: Path, run_ts: str) -> pd.DataFrame:
    old_n = normalize_date_tenor(old)
    new_n = normalize_date_tenor(new)

    old_n["_key_trade_date"] = old_n["trade_date"].astype(int)
    old_n["_key_tenor"] = old_n["tenor"].astype(int)
    new_n["_key_trade_date"] = new_n["trade_date"].astype(int)
    new_n["_key_tenor"] = new_n["tenor"].astype(int)

    cols = [c for c in COMPARE_COLS if c in old_n.columns and c in new_n.columns]
    merged = old_n[["_key_trade_date", "_key_tenor", *cols]].merge(
        new_n[["_key_trade_date", "_key_tenor", *cols]],
        on=["_key_trade_date", "_key_tenor"],
        how="inner",
        suffixes=("_old", "_new"),
    )

    rows = []
    for c in cols:
        a = pd.to_numeric(merged[f"{c}_old"], errors="coerce")
        b = pd.to_numeric(merged[f"{c}_new"], errors="coerce")
        both_valid = a.notna() & b.notna()
        one_null = a.isna() ^ b.isna()
        diff = (b[both_valid] - a[both_valid]).abs()

        max_abs_diff = float(diff.max()) if len(diff) else 0.0
        mean_abs_diff = float(diff.mean()) if len(diff) else 0.0
        one_null_rows = int(one_null.sum())

        rows.append({
            "column": c,
            "common_rows": int(len(merged)),
            "valid_compared_rows": int(both_valid.sum()),
            "one_null_rows": one_null_rows,
            "max_abs_diff": max_abs_diff,
            "mean_abs_diff": mean_abs_diff,
            "status": "PASS" if (max_abs_diff <= OVERLAP_TOL and one_null_rows == 0) else "FAIL",
        })

    summary = pd.DataFrame(rows)
    summary.to_csv(audit_dir / f"locked_cell4_feature_overlap_compare_{run_ts}.csv", index=False)
    return summary


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root)

    source_dir = project_root / "data" / "processed" / "forecast_model_corsi_v1"
    output_dir = project_root / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1"
    audit_dir = project_root / "data" / "audit" / "vrp_front_middle_corsi_forecast_repair_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    source_path = Path(args.source_panel) if args.source_panel else latest_file(source_dir, "corsi_model_feature_panel_v1_*.parquet")
    old_path = Path(args.old_feature_panel) if args.old_feature_panel else latest_file(output_dir, "04_front_middle_candidate_feature_panel_*.parquet")

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    section("Locked Cell 4 feature panel update v1")
    print("Project root:", project_root)
    print("Source Corsi model feature panel:", source_path)
    print("Old locked feature panel template:", old_path)
    print("Output dir:", output_dir)
    print("Audit dir:", audit_dir)

    source = normalize_date_tenor(pd.read_parquet(source_path))
    old = normalize_date_tenor(pd.read_parquet(old_path))

    if args.end_date:
        end_ts = pd.to_datetime(args.end_date, format="%Y%m%d").normalize()
        source = source[source["date"].le(end_ts)].copy()
    else:
        end_ts = source["date"].max()

    section("Loaded inputs")
    print("Source rows:", len(source))
    print("Source date range:", source["date"].min().date(), "to", source["date"].max().date())
    print("Source latest tenors:", sorted(source.loc[source["date"].eq(source["date"].max()), "tenor"].dropna().astype(int).unique().tolist()))
    print("Old feature rows:", len(old))
    print("Old feature date range:", old["date"].min().date(), "to", old["date"].max().date())

    daily_features = build_daily_locked_features(source)
    out = build_output_panel(source, old, daily_features)

    safe_start = out["date"].min().strftime("%Y%m%d")
    safe_end = out["date"].max().strftime("%Y%m%d")
    out_path = output_dir / f"04_front_middle_candidate_feature_panel_{safe_start}_{safe_end}_{run_ts}.parquet"
    out.to_parquet(out_path, index=False)

    section("Output written")
    print("Output:", out_path)
    print("Rows:", len(out))
    print("Cols:", len(out.columns))
    print("Date range:", out["date"].min().date(), "to", out["date"].max().date())

    target_rows = out[out["date"].eq(out["date"].max())].copy()
    latest_tenors = sorted(target_rows["tenor"].dropna().astype(int).unique().tolist())
    print("Latest date:", out["date"].max().date())
    print("Latest tenors:", latest_tenors)

    section("Latest feature rows")
    show_cols = ["date", "trade_date", "tenor", "spx_log_return", *REQUIRED_LOCKED_FEATURES]
    show_cols = [c for c in show_cols if c in out.columns]
    print(target_rows[show_cols].to_string(index=False))

    overlap = compare_overlap(old, out, audit_dir, run_ts)
    section("Overlap validation vs old locked Cell 4 panel")
    print(overlap.to_string(index=False))

    checks = []

    def check(name: str, ok: bool, detail: str):
        checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    check("output_reaches_target_date", out["date"].max().normalize() == end_ts.normalize(), f"target={end_ts.date()}; max={out['date'].max().date()}")
    check("latest_date_has_expected_tenors", latest_tenors == EXPECTED_TENORS, f"latest_tenors={latest_tenors}")
    check("no_duplicate_date_tenor", not out.duplicated(["date", "tenor"]).any(), f"duplicates={int(out.duplicated(['date', 'tenor']).sum())}")

    missing_required = [c for c in REQUIRED_LOCKED_FEATURES if c not in out.columns]
    check("required_locked_features_present", len(missing_required) == 0, f"missing={missing_required}")

    target_missing_required = []
    for c in REQUIRED_LOCKED_FEATURES:
        if c in target_rows.columns and target_rows[c].isna().any():
            target_missing_required.append(c)
    check("latest_required_features_non_null", len(target_missing_required) == 0, f"missing_latest={target_missing_required}")

    failed_overlap_cols = overlap.loc[overlap["status"].eq("FAIL"), "column"].tolist()
    check("overlap_features_reproduce_old_locked_panel", len(failed_overlap_cols) == 0, f"failed_cols={failed_overlap_cols}")

    validation = pd.DataFrame(checks)
    validation_path = audit_dir / f"locked_cell4_feature_update_validation_{run_ts}.csv"
    validation.to_csv(validation_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "source_panel": str(source_path),
        "old_feature_panel": str(old_path),
        "output_panel": str(out_path),
        "validation": str(validation_path),
        "latest_date": out["date"].max().date().isoformat(),
        "latest_tenors": latest_tenors,
        "required_locked_features": REQUIRED_LOCKED_FEATURES,
    }
    manifest_path = audit_dir / f"locked_cell4_feature_update_manifest_{run_ts}.json"
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
        raise RuntimeError("LOCKED_CELL4_FEATURE_UPDATE failed validation.")

    print("LOCKED_CELL4_FEATURE_UPDATE_PASS: True")
    print("DONE — locked Cell 4 candidate feature panel extended.")


if __name__ == "__main__":
    main()
