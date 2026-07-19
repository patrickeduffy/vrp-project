
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
    "spy_close_for_features",
    "spy_log_return",
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
    """Build all return-dependent features from canonical SPY only.

    ``spy_close`` and ``spy_log_return`` must come from the canonical SPY EOD
    source. Returns are checked against close-derived returns. No SPX, generic
    close, or scale-break fallback is permitted. Legacy spx_* output aliases are
    retained only to preserve downstream schema compatibility.
    """
    src = source.copy()
    required = {"date", "spy_close", "spy_log_return"}
    missing = sorted(required - set(src.columns))
    if missing:
        raise ValueError(
            f"Locked Cell 4 source missing canonical SPY fields {missing}. "
            "SPX/generic-close fallback is prohibited."
        )

    assert_daily_constant(src, "spy_close")
    assert_daily_constant(src, "spy_log_return")

    close_frame = src[["date", "spy_close", "spy_log_return"]].copy()
    close_frame["spy_close"] = pd.to_numeric(close_frame["spy_close"], errors="coerce")
    close_frame["spy_log_return"] = pd.to_numeric(close_frame["spy_log_return"], errors="coerce")
    daily = (
        close_frame.groupby("date", as_index=False)
        .agg({"spy_close": first_nonnull, "spy_log_return": first_nonnull})
        .sort_values("date")
        .reset_index(drop=True)
    )
    daily = daily.rename(columns={"spy_close": "spy_close_for_features"})

    invalid_close = (
        daily["spy_close_for_features"].isna()
        | ~np.isfinite(daily["spy_close_for_features"])
        | daily["spy_close_for_features"].le(0)
    )
    if bool(invalid_close.any()):
        bad = daily.loc[invalid_close, ["date", "spy_close_for_features"]].head(20)
        raise RuntimeError(f"Canonical SPY feature closes invalid. Sample:\n{bad}")

    close_series = pd.to_numeric(daily["spy_close_for_features"], errors="raise")
    close_return = np.log(close_series / close_series.shift(1))
    source_return = pd.to_numeric(daily["spy_log_return"], errors="coerce")
    comparable = close_return.notna() & source_return.notna()
    max_return_diff = float((close_return[comparable] - source_return[comparable]).abs().max()) if comparable.any() else 0.0
    if not np.isfinite(max_return_diff) or max_return_diff > 1e-12:
        raise RuntimeError(
            "Canonical SPY return does not match log(spy_close/prior_spy_close): "
            f"max_abs_diff={max_return_diff:.3e}"
        )

    daily["spy_log_return"] = close_return.where(close_return.notna(), source_return)
    scale_break = daily["spy_log_return"].abs().gt(0.25)
    if bool(scale_break.any()):
        bad = daily.loc[scale_break, ["date", "spy_close_for_features", "spy_log_return"]].head(20)
        raise RuntimeError(
            "Canonical SPY feature source contains an unexplained close-scale break. "
            f"Sample:\n{bad.to_string(index=False)}"
        )

    daily["feature_return_source"] = "SPY"
    # Backward-compatible aliases required by existing forecast/report schemas.
    daily["spx_close_for_features"] = daily["spy_close_for_features"]
    daily["spx_log_return"] = daily["spy_log_return"]

    print("Daily feature close source: canonical spy_close only")
    print("Daily feature return source: log(spy_close / prior_spy_close)")
    print("Legacy spx_* columns are SPY-backed compatibility aliases")
    print("Daily feature latest return inputs:")
    print(
        daily[["date", "spy_close_for_features", "spy_log_return"]]
        .tail(10)
        .to_string(index=False)
    )

    r = pd.to_numeric(daily["spy_log_return"], errors="coerce")
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
        daily[f"candidate_negative_return_count_{w}d"] = daily["is_negative_return"].rolling(w, min_periods=w).sum()

    for w in [3, 5, 10]:
        daily[f"candidate_max_abs_return_{w}d"] = daily["abs_return_1d_raw"].rolling(w, min_periods=w).max()
        daily[f"candidate_min_return_{w}d"] = daily["spy_log_return"].rolling(w, min_periods=w).min()

    keep_cols = [
        "date",
        "spy_close_for_features",
        "spy_log_return",
        "feature_return_source",
        "spx_close_for_features",
        "spx_log_return",
        *REQUIRED_LOCKED_FEATURES,
        *AUX_FEATURES,
    ]
    return daily[[c for c in keep_cols if c in daily.columns]].copy()

def materialize_target_contract_columns(source: pd.DataFrame) -> pd.DataFrame:
    """Persist the authoritative forward-target contract in the locked feature panel.

    The Corsi model panel stores the validated target as
    ``log_forward_realized_variance_corsi``.  The locked forecast publisher
    consumes the schema-compatible name ``target_log_variance``.  Rebuilding the
    feature panel must therefore copy the validated Corsi target into that
    contract column rather than creating an all-null placeholder from the old
    template schema.
    """
    out = source.copy()

    explicit = (
        pd.to_numeric(out["target_log_variance"], errors="coerce")
        if "target_log_variance" in out.columns
        else pd.Series(np.nan, index=out.index, dtype=float)
    )

    if "log_forward_realized_variance_corsi" in out.columns:
        derived = pd.to_numeric(out["log_forward_realized_variance_corsi"], errors="coerce")
    elif "forward_realized_variance_corsi" in out.columns:
        variance = pd.to_numeric(out["forward_realized_variance_corsi"], errors="coerce")
        derived = np.log(variance.where(variance > 0.0))
    else:
        raise RuntimeError(
            "Source Corsi model panel has no authoritative forward target: "
            "expected log_forward_realized_variance_corsi or "
            "forward_realized_variance_corsi."
        )

    overlap = explicit.notna() & derived.notna()
    if bool(overlap.any()):
        max_diff = float((explicit[overlap] - derived[overlap]).abs().max())
        if not np.isfinite(max_diff) or max_diff > OVERLAP_TOL:
            raise RuntimeError(
                "Existing target_log_variance conflicts with the authoritative "
                f"Corsi target: max_abs_diff={max_diff:.3e}."
            )

    out["target_log_variance"] = explicit.where(explicit.notna(), derived)

    if "last_forward_rv_date" not in out.columns:
        raise RuntimeError("Source Corsi model panel is missing last_forward_rv_date.")
    out["last_forward_rv_date"] = parse_dates(out["last_forward_rv_date"])

    if "forward_window_complete_corsi" in out.columns:
        complete_raw = out["forward_window_complete_corsi"]
        if pd.api.types.is_bool_dtype(complete_raw):
            complete = complete_raw.fillna(False)
        else:
            complete = complete_raw.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
        target_values = pd.to_numeric(out["target_log_variance"], errors="coerce")
        bad = complete & (
            ~np.isfinite(target_values)
            | out["last_forward_rv_date"].isna()
        )
        if bool(bad.any()):
            sample_cols = [c for c in ["date", "tenor", "target_log_variance", "last_forward_rv_date"] if c in out.columns]
            raise RuntimeError(
                "Complete Corsi target rows lost the locked target contract. Sample:\n"
                + out.loc[bad, sample_cols].head(20).to_string(index=False)
            )

    return out


def build_output_panel(source: pd.DataFrame, old_template: pd.DataFrame, daily_features: pd.DataFrame) -> pd.DataFrame:
    base = materialize_target_contract_columns(source)

    # Keep target 9-tenor grid.
    base = base[base["tenor"].isin(EXPECTED_TENORS)].copy()
    base = base.sort_values(["date", "tenor"]).drop_duplicates(["date", "tenor"], keep="first").reset_index(drop=True)

    merged = base.merge(daily_features, on="date", how="left", suffixes=("", "_calc"))

    # Override these with locked Cell 4 calculations.
    for col in ["spy_close_for_features", "spy_log_return", "feature_return_source", "spx_close_for_features", "spx_log_return", *REQUIRED_LOCKED_FEATURES, *AUX_FEATURES]:
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
    for c in ["spy_close_for_features", "spy_log_return", "feature_return_source", "spx_close_for_features", "spx_log_return"]:
        if c not in final_cols:
            final_cols.append(c)

    for c in final_cols:
        if c not in merged.columns:
            merged[c] = np.nan

    out = merged[final_cols].copy()

    # Rebuild the complete historical panel from one SPY source. Old locked
    # values are not overlaid because that would preserve the SPX/SPY source break.
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
    show_cols = ["date", "trade_date", "tenor", "spy_log_return", "feature_return_source", *REQUIRED_LOCKED_FEATURES]
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
    check("historical_source_migration_comparison_written", True, f"report_only_failed_cols={failed_overlap_cols}")

    required_source_cols = ["spy_close_for_features", "spy_log_return", "feature_return_source"]
    missing_source_cols = [c for c in required_source_cols if c not in out.columns]
    check("canonical_spy_source_columns_present", not missing_source_cols, f"missing={missing_source_cols}")
    if not missing_source_cols:
        observed_sources = sorted(out["feature_return_source"].dropna().astype(str).unique().tolist())
        check("feature_return_source_is_spy", observed_sources == ["SPY"], f"observed={observed_sources}")
        alias_close_diff = float((pd.to_numeric(out["spx_close_for_features"], errors="coerce") - pd.to_numeric(out["spy_close_for_features"], errors="coerce")).abs().max())
        alias_return_diff = float((pd.to_numeric(out["spx_log_return"], errors="coerce") - pd.to_numeric(out["spy_log_return"], errors="coerce")).abs().max())
        check("legacy_aliases_equal_spy", alias_close_diff <= 1e-12 and alias_return_diff <= 1e-12, f"close_diff={alias_close_diff:.3e}; return_diff={alias_return_diff:.3e}")

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
