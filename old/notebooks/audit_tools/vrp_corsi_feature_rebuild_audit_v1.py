
from pathlib import Path
from datetime import datetime
import traceback

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
BRANCH = "vrp_front_middle_corsi_forecast_repair_v1"

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / BRANCH
AUDIT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
TARGET_SCORE_DATE = pd.Timestamp("2026-07-06")

ANNUALIZATION = 252.0
EPS = 1e-12

LOCKED_FEATURES = [
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


def section(title):
    print("=" * 100)
    print(title)
    print("=" * 100)


def latest_file(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No file found in {directory} matching {pattern}")
    return matches[0]


def normalize_trade_date(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.normalize()
    return out


def compute_locked_features_from_returns(market: pd.DataFrame, variant: str) -> pd.DataFrame:
    """
    variant:
      include_current: rolling windows include the trade_date return.
      lag1: rolling windows use returns through prior trade_date only.
    """
    if variant not in {"include_current", "lag1"}:
        raise ValueError(f"Unknown variant: {variant}")

    df = market[["trade_date", "spy_log_return"]].copy()
    df = df.sort_values("trade_date").reset_index(drop=True)

    r = pd.to_numeric(df["spy_log_return"], errors="coerce")

    if variant == "lag1":
        r = r.shift(1)

    abs_r = r.abs()
    sq = r.pow(2)
    downside_sq = sq.where(r < 0, 0.0)

    out = df[["trade_date"]].copy()

    for window in [5, 10, 21, 63]:
        total_var = sq.rolling(window=window, min_periods=window).mean() * ANNUALIZATION
        downside_var = downside_sq.rolling(window=window, min_periods=window).mean() * ANNUALIZATION

        out[f"_total_rv_{window}d"] = total_var
        out[f"_downside_rv_{window}d"] = downside_var
        out[f"candidate_log_downside_rv_{window}d"] = np.log(np.maximum(downside_var, EPS))

    for window in [5, 10]:
        total_var = out[f"_total_rv_{window}d"]
        downside_var = out[f"_downside_rv_{window}d"]
        out[f"candidate_downside_share_{window}d"] = np.where(
            total_var > EPS,
            downside_var / total_var,
            np.nan,
        )

    for window in [3, 5, 10]:
        out[f"candidate_max_abs_return_{window}d"] = abs_r.rolling(
            window=window,
            min_periods=window,
        ).max()

    return out[["trade_date"] + LOCKED_FEATURES]


def compare_variant(stored: pd.DataFrame, rebuilt: pd.DataFrame, variant: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = stored.merge(
        rebuilt,
        on="trade_date",
        how="inner",
        suffixes=("_stored", "_rebuilt"),
    )

    detail_rows = []
    summary_rows = []

    for col in LOCKED_FEATURES:
        stored_col = f"{col}_stored"
        rebuilt_col = f"{col}_rebuilt"

        valid = (
            pd.to_numeric(merged[stored_col], errors="coerce").notna()
            & pd.to_numeric(merged[rebuilt_col], errors="coerce").notna()
        )

        diffs = (
            pd.to_numeric(merged.loc[valid, rebuilt_col], errors="coerce")
            - pd.to_numeric(merged.loc[valid, stored_col], errors="coerce")
        )

        if len(diffs) == 0:
            max_abs = np.nan
            mean_abs = np.nan
            valid_rows = 0
        else:
            max_abs = float(diffs.abs().max())
            mean_abs = float(diffs.abs().mean())
            valid_rows = int(len(diffs))

        summary_rows.append({
            "variant": variant,
            "feature": col,
            "valid_rows": valid_rows,
            "max_abs_diff": max_abs,
            "mean_abs_diff": mean_abs,
        })

        temp = merged.loc[valid, ["trade_date", "tenor", stored_col, rebuilt_col]].copy()
        temp["variant"] = variant
        temp["feature"] = col
        temp["diff"] = pd.to_numeric(temp[rebuilt_col], errors="coerce") - pd.to_numeric(temp[stored_col], errors="coerce")
        temp = temp.rename(columns={
            stored_col: "stored_value",
            rebuilt_col: "rebuilt_value",
        })
        detail_rows.append(temp[["variant", "trade_date", "tenor", "feature", "stored_value", "rebuilt_value", "diff"]])

    summary = pd.DataFrame(summary_rows)

    if detail_rows:
        detail = pd.concat(detail_rows, ignore_index=True)
    else:
        detail = pd.DataFrame(columns=["variant", "trade_date", "tenor", "feature", "stored_value", "rebuilt_value", "diff"])

    return summary, detail


def main():
    section("VRP Corsi/FDS locked feature rebuild audit v1")
    print("Project root:", PROJECT_ROOT)
    print("Run timestamp:", RUN_TS)
    print("Target score date:", TARGET_SCORE_DATE.date())

    feature_panel_path = latest_file(
        PROCESSED_DIR,
        "04_front_middle_candidate_feature_panel_*.parquet",
    )

    market_path = PROJECT_ROOT / "data" / "processed" / "market_data" / "spy_corsi_har_input_panel_v1.parquet"

    section("Input files")
    print("Stored Cell 4 feature panel:", feature_panel_path)
    print("Updated market Corsi input:", market_path)
    print("Market exists:", market_path.exists())

    stored = pd.read_parquet(feature_panel_path)
    market = pd.read_parquet(market_path)

    stored = normalize_trade_date(stored)
    market = normalize_trade_date(market)

    stored["tenor"] = pd.to_numeric(stored["tenor"], errors="coerce").astype("Int64")
    market["spy_log_return"] = pd.to_numeric(market["spy_log_return"], errors="coerce")

    missing_stored_features = [c for c in LOCKED_FEATURES if c not in stored.columns]
    if missing_stored_features:
        raise RuntimeError(f"Stored feature panel missing locked features: {missing_stored_features}")

    section("Loaded shapes")
    print("stored feature panel:", stored.shape)
    print("market input:", market.shape)
    print("stored date range:", stored["trade_date"].min(), "to", stored["trade_date"].max())
    print("market date range:", market["trade_date"].min(), "to", market["trade_date"].max())
    print("stored tenors:", sorted(stored["tenor"].dropna().astype(int).unique().tolist()))

    stored_compare = stored[
        stored["tenor"].isin(TARGET_TENORS)
        & stored["trade_date"].notna()
    ][["trade_date", "tenor"] + LOCKED_FEATURES].copy()

    # Focus on recent overlap. This is enough to validate production formulas for current scoring,
    # while avoiding very old warmup/edge rows that are not relevant to the updater.
    overlap_max = min(stored_compare["trade_date"].max(), market["trade_date"].max())
    recent_start = overlap_max - pd.Timedelta(days=400)

    stored_recent = stored_compare[
        (stored_compare["trade_date"] >= recent_start)
        & (stored_compare["trade_date"] <= overlap_max)
    ].copy()

    section("Comparison window")
    print("overlap_max:", overlap_max.date())
    print("recent_start:", recent_start.date())
    print("stored_recent rows:", len(stored_recent))
    print("stored_recent dates:", stored_recent["trade_date"].nunique())

    variant_summaries = []
    variant_details = []

    for variant in ["include_current", "lag1"]:
        rebuilt = compute_locked_features_from_returns(market, variant=variant)
        summary, detail = compare_variant(stored_recent, rebuilt, variant=variant)
        variant_summaries.append(summary)
        variant_details.append(detail)

    summary_all = pd.concat(variant_summaries, ignore_index=True)
    detail_all = pd.concat(variant_details, ignore_index=True)

    variant_score = (
        summary_all
        .groupby("variant", as_index=False)
        .agg(
            max_abs_diff_all=("max_abs_diff", "max"),
            mean_abs_diff_all=("mean_abs_diff", "mean"),
            min_valid_rows=("valid_rows", "min"),
        )
        .sort_values(["max_abs_diff_all", "mean_abs_diff_all"], ascending=[True, True])
        .reset_index(drop=True)
    )

    selected_variant = str(variant_score.iloc[0]["variant"])
    selected_max_abs = float(variant_score.iloc[0]["max_abs_diff_all"])

    section("Variant comparison summary")
    print(variant_score.to_string(index=False))

    section("Feature-level summary")
    print(summary_all.sort_values(["variant", "feature"]).to_string(index=False))

    worst_detail = (
        detail_all[detail_all["variant"] == selected_variant]
        .assign(abs_diff=lambda x: x["diff"].abs())
        .sort_values("abs_diff", ascending=False)
        .head(50)
    )

    section("Worst 50 diffs for selected variant")
    print("selected_variant:", selected_variant)
    print(worst_detail.to_string(index=False))

    summary_path = AUDIT_DIR / f"corsi_locked_feature_rebuild_summary_{RUN_TS}.csv"
    detail_path = AUDIT_DIR / f"corsi_locked_feature_rebuild_detail_{RUN_TS}.csv"
    worst_path = AUDIT_DIR / f"corsi_locked_feature_rebuild_worst_diffs_{RUN_TS}.csv"

    summary_all.to_csv(summary_path, index=False)
    detail_all.to_csv(detail_path, index=False)
    worst_detail.to_csv(worst_path, index=False)

    section("Saved audit files")
    print("summary:", summary_path)
    print("detail:", detail_path)
    print("worst:", worst_path)

    pass_flag = bool(selected_max_abs < 1e-10)

    section("Formula audit result")
    print("selected_variant:", selected_variant)
    print("selected_max_abs_diff:", selected_max_abs)
    print("FEATURE_REBUILD_AUDIT_PASS:", pass_flag)

    if not pass_flag:
        raise RuntimeError("Feature rebuild audit failed. Do not build production updater until formula mismatch is resolved.")

    # Build target-date scoring feature rows for audit visibility only.
    rebuilt_selected = compute_locked_features_from_returns(market, variant=selected_variant)

    target_one_row = rebuilt_selected[
        rebuilt_selected["trade_date"] == TARGET_SCORE_DATE
    ].copy()

    if target_one_row.empty:
        raise RuntimeError(f"Target score date {TARGET_SCORE_DATE.date()} not found in rebuilt market feature rows.")

    target_rows = pd.DataFrame({"tenor": TARGET_TENORS})
    target_rows["trade_date"] = TARGET_SCORE_DATE
    for col in LOCKED_FEATURES:
        target_rows[col] = float(target_one_row.iloc[0][col])

    target_path = AUDIT_DIR / f"corsi_locked_features_target_{TARGET_SCORE_DATE.strftime('%Y%m%d')}_{RUN_TS}.csv"
    target_rows.to_csv(target_path, index=False)

    section("Target-date rebuilt scoring features")
    print(target_rows.to_string(index=False))
    print("\nSaved target-date feature audit:", target_path)

    section("DONE")
    print("Locked feature rebuild confirmed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
