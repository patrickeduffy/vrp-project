
"""
VRP final signal panel update v1.

Scope:
  Extend the final cross-tenor signal panel using the locked unified FDS forecast panel.

Not in scope:
  forecast fitting
  alpha retuning
  threshold changes
  sizing changes
  methodology changes
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
MODEL_NAME = "unified_fds_no_min_return"

SIZE_BY_LABEL = {
    "Core_Middle_21D": 0.0350,
    "Core_Middle_24D": 0.0425,
    "Core_Back_27D": 0.0450,
    "Core_Back_30D": 0.0475,
    "Core_Back_33D": 0.0500,
    "Secondary_Front_12D": 0.0150,
    "Secondary_Front_15D": 0.0200,
    "Secondary_Front_18D": 0.0275,
    "Secondary_Middle_21D": 0.0350,
    "Secondary_Middle_24D": 0.0375,
    "Secondary_Back_27D": 0.0400,
    "Secondary_Back_30D": 0.0425,
    "Secondary_Back_33D": 0.0450,
}

CORE_THRESHOLDS = {
    "middle": {"vrp": 0.65, "z3": 0.70, "z1": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
    "back": {"vrp": 0.70, "z3": 0.70, "z1": 0.70, "rsi_cap": 70.0, "rv_floor": 8.5},
}

SECONDARY_THRESHOLDS = {
    "front": {"vrp": 0.65, "z3": 0.20, "z1": 0.20, "rsi_cap": 75.0, "rv_floor": 7.0},
    "middle": {"vrp": 0.65, "z3": 0.20, "z1": 0.20, "rsi_cap": 76.0, "rv_floor": 7.0},
    "back": {"vrp": 0.65, "z3": 0.00, "z1": 0.00, "rsi_cap": 77.0, "rv_floor": 6.5},
}


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--forecast-panel", required=True)
    p.add_argument("--old-signal-panel", required=True)
    p.add_argument("--end-date", required=True)
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


def normalize_date_tenor(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["date"] = parse_dates(out["date"])
    elif "trade_date" in out.columns:
        out["date"] = parse_dates(out["trade_date"])
    else:
        raise ValueError("Missing date/trade_date.")

    out["trade_date"] = out["date"].dt.strftime("%Y%m%d").astype("Int64")

    tenor_col = "tenor" if "tenor" in out.columns else ("target_days" if "target_days" in out.columns else None)
    if tenor_col is None:
        raise ValueError("Missing tenor/target_days.")

    out["tenor"] = pd.to_numeric(out[tenor_col], errors="coerce").astype("Int64")
    if "target_days" not in out.columns:
        out["target_days"] = out["tenor"]
    else:
        out["target_days"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")

    return out


def tenor_bucket(tenor: int) -> str | None:
    if tenor in [12, 15, 18]:
        return "front"
    if tenor in [21, 24]:
        return "middle"
    if tenor in [27, 30, 33]:
        return "back"
    return None


def select_locked_forecast_rows(forecast: pd.DataFrame) -> pd.DataFrame:
    rows = forecast.copy()
    rows = rows[rows["tenor"].isin(EXPECTED_TENORS)].copy()

    for c in ["forecast_variance_candidate", "predicted_log_variance_candidate", "implied_variance"]:
        if c not in rows.columns:
            raise ValueError(f"Forecast panel missing required column: {c}")
        rows[c] = pd.to_numeric(rows[c], errors="coerce")

    model_hit = pd.Series(False, index=rows.index)
    for c in ["model_spec", "model_source", "locked_model_name"]:
        if c in rows.columns:
            model_hit = model_hit | rows[c].astype(str).str.contains(MODEL_NAME, case=False, na=False)

    if model_hit.any():
        rows = rows[model_hit].copy()

    rows = rows[
        rows["forecast_variance_candidate"].notna()
        & rows["forecast_variance_candidate"].gt(0.0)
        & rows["implied_variance"].notna()
        & rows["implied_variance"].gt(0.0)
    ].copy()

    if rows.empty:
        raise RuntimeError("No usable locked forecast rows after filtering.")

    rows["_row_order"] = np.arange(len(rows))
    rows = (
        rows.sort_values(["date", "tenor", "_row_order"])
        .drop_duplicates(["date", "tenor"], keep="last")
        .drop(columns=["_row_order"])
        .reset_index(drop=True)
    )

    return rows


def old_col(old: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in old.columns:
            return c
    return None


def compute_prior_z_from_old_history(old_signal: pd.DataFrame, generated: pd.DataFrame) -> pd.DataFrame:
    old_log_col = old_col(old_signal, ["model_vrp_log_final", "model_vrp_log"])
    if old_log_col is None:
        raise ValueError("Old signal panel missing model_vrp_log/model_vrp_log_final needed for prior z history.")

    hist = old_signal[["date", "trade_date", "tenor", old_log_col]].copy()
    hist = hist.rename(columns={old_log_col: "model_vrp_log_for_z"})
    hist["source_priority"] = 0

    new_hist = generated[["date", "trade_date", "tenor", "model_vrp_log_final"]].copy()
    new_hist = new_hist.rename(columns={"model_vrp_log_final": "model_vrp_log_for_z"})
    new_hist["source_priority"] = 1

    zbase = pd.concat([hist, new_hist], ignore_index=True, sort=False)
    zbase["model_vrp_log_for_z"] = pd.to_numeric(zbase["model_vrp_log_for_z"], errors="coerce")
    zbase = (
        zbase.sort_values(["date", "tenor", "source_priority"])
        .drop_duplicates(["date", "tenor"], keep="first")
        .sort_values(["tenor", "date"])
        .reset_index(drop=True)
    )

    outs = []
    for tenor, sub in zbase.groupby("tenor", sort=True):
        sub = sub.sort_values("date").copy()
        prior = sub["model_vrp_log_for_z"].shift(1)

        mean_3m = prior.rolling(63, min_periods=63).mean()
        std_3m = prior.rolling(63, min_periods=63).std()
        mean_1y = prior.rolling(252, min_periods=252).mean()
        std_1y = prior.rolling(252, min_periods=252).std()

        sub["z_3m_final"] = (sub["model_vrp_log_for_z"] - mean_3m) / std_3m
        sub["z_1y_final"] = (sub["model_vrp_log_for_z"] - mean_1y) / std_1y
        outs.append(sub)

    z = pd.concat(outs, ignore_index=True)
    return z[["date", "tenor", "z_3m_final", "z_1y_final"]].copy()


def compute_rsi14_fallback(old_signal: pd.DataFrame, generated: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a close-based RSI14 fallback for appended dates only.

    Uses old signal history for prior closes, then reconstructs appended closes from
    prior close * exp(spx_log_return) when the appended close column has a scale break
    such as SPY-scale close after SPX-scale history.
    """
    close_col = old_col(old_signal, ["spx_close_for_features", "spx_close", "close"])
    if close_col is None:
        return pd.DataFrame(columns=["date", "rsi14_fallback"])

    old_daily = (
        old_signal[["date", close_col]]
        .dropna()
        .drop_duplicates("date", keep="last")
        .rename(columns={close_col: "close"})
        .copy()
    )
    old_daily["source_priority"] = 0
    old_daily["spx_log_return"] = np.nan

    gen_cols = ["date"]
    if "spx_close_for_features" in generated.columns:
        gen_cols.append("spx_close_for_features")
    if "spx_log_return" in generated.columns:
        gen_cols.append("spx_log_return")

    gen_daily = generated[gen_cols].drop_duplicates("date", keep="last").copy()
    gen_daily["close"] = (
        pd.to_numeric(gen_daily["spx_close_for_features"], errors="coerce")
        if "spx_close_for_features" in gen_daily.columns
        else np.nan
    )
    if "spx_log_return" not in gen_daily.columns:
        gen_daily["spx_log_return"] = np.nan
    gen_daily["spx_log_return"] = pd.to_numeric(gen_daily["spx_log_return"], errors="coerce")
    gen_daily["source_priority"] = 1
    gen_daily = gen_daily[["date", "close", "spx_log_return", "source_priority"]]

    daily = pd.concat(
        [old_daily[["date", "close", "spx_log_return", "source_priority"]], gen_daily],
        ignore_index=True,
        sort=False,
    )
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily = daily.sort_values(["date", "source_priority"]).drop_duplicates("date", keep="last")
    daily = daily.sort_values("date").reset_index(drop=True)

    # Repair appended close scale breaks using source log return.
    for i in range(1, len(daily)):
        ret = daily.loc[i, "spx_log_return"]
        prev_close = daily.loc[i - 1, "close"]
        cur_close = daily.loc[i, "close"]

        if pd.notna(ret) and pd.notna(prev_close):
            implied_close = prev_close * np.exp(ret)
            if pd.isna(cur_close):
                daily.loc[i, "close"] = implied_close
            else:
                observed_ret = np.log(cur_close / prev_close) if prev_close > 0 and cur_close > 0 else np.nan
                if pd.notna(observed_ret) and abs(observed_ret) > 0.25:
                    daily.loc[i, "close"] = implied_close

    delta = daily["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    daily["rsi14_fallback"] = 100.0 - (100.0 / (1.0 + rs))
    daily.loc[(avg_loss == 0.0) & (avg_gain > 0.0), "rsi14_fallback"] = 100.0
    daily.loc[(avg_loss == 0.0) & (avg_gain == 0.0), "rsi14_fallback"] = 50.0

    return daily[["date", "rsi14_fallback"]].copy()


def compute_generated_rows(forecast_rows: pd.DataFrame, old_signal: pd.DataFrame, end_ts: pd.Timestamp) -> pd.DataFrame:
    old_max = old_signal["date"].max()

    gen = forecast_rows[
        forecast_rows["date"].gt(old_max)
        & forecast_rows["date"].le(end_ts)
        & forecast_rows["tenor"].isin(EXPECTED_TENORS)
    ].copy()

    if gen.empty:
        raise RuntimeError(f"No forecast rows after old signal max date {old_max.date()} through {end_ts.date()}.")

    gen["forecast_variance_final"] = pd.to_numeric(gen["forecast_variance_candidate"], errors="coerce")
    gen["forecast_vol_final"] = np.sqrt(gen["forecast_variance_final"]) * 100.0
    gen["model_vrp_log_final"] = np.log(pd.to_numeric(gen["implied_variance"], errors="coerce") / gen["forecast_variance_final"])

    if "vix_style_vol" in gen.columns:
        gen["implied_vol_final"] = pd.to_numeric(gen["vix_style_vol"], errors="coerce")
    else:
        gen["implied_vol_final"] = np.sqrt(pd.to_numeric(gen["implied_variance"], errors="coerce")) * 100.0

    if "RSI14" in gen.columns:
        gen["rsi14_final"] = pd.to_numeric(gen["RSI14"], errors="coerce")
    elif "rsi14" in gen.columns:
        gen["rsi14_final"] = pd.to_numeric(gen["rsi14"], errors="coerce")
    else:
        gen["rsi14_final"] = np.nan

    if gen["rsi14_final"].isna().any():
        rsi_fallback = compute_rsi14_fallback(old_signal, gen)
        gen = gen.merge(rsi_fallback, on="date", how="left")
        gen["rsi14_final"] = gen["rsi14_final"].where(
            gen["rsi14_final"].notna(),
            gen["rsi14_fallback"],
        )
        gen = gen.drop(columns=["rsi14_fallback"])

    if "rv21d_vol_pct" in gen.columns:
        gen["rv21d_vol_pct_final"] = pd.to_numeric(gen["rv21d_vol_pct"], errors="coerce")
    elif "candidate_log_rv_21d" in gen.columns:
        gen["rv21d_vol_pct_final"] = np.sqrt(np.exp(pd.to_numeric(gen["candidate_log_rv_21d"], errors="coerce"))) * 100.0
    else:
        gen["rv21d_vol_pct_final"] = np.nan

    z = compute_prior_z_from_old_history(old_signal, gen)
    gen = gen.merge(z, on=["date", "tenor"], how="left")

    gen["tenor_bucket_final"] = gen["tenor"].astype(int).map(tenor_bucket)

    return gen


def threshold_pass(row: pd.Series, threshold: dict[str, float]) -> bool:
    vals = {
        "vrp": row.get("model_vrp_log_final", np.nan),
        "z3": row.get("z_3m_final", np.nan),
        "z1": row.get("z_1y_final", np.nan),
        "rsi": row.get("rsi14_final", np.nan),
        "rv": row.get("rv21d_vol_pct_final", np.nan),
    }

    if any(pd.isna(v) for v in vals.values()):
        return False

    return (
        vals["vrp"] > threshold["vrp"]
        and vals["z3"] > threshold["z3"]
        and vals["z1"] > threshold["z1"]
        and vals["rsi"] < threshold["rsi_cap"]
        and vals["rv"] > threshold["rv_floor"]
    )


def apply_locked_thresholds(gen: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = gen.copy()

    rows["core_signal_final"] = False
    rows["secondary_signal_final"] = False
    rows["core_label_final"] = pd.NA
    rows["secondary_label_final"] = pd.NA
    rows["core_size_pct_final"] = np.nan
    rows["secondary_size_pct_final"] = np.nan

    for idx, row in rows.iterrows():
        tenor = int(row["tenor"])
        bucket = row["tenor_bucket_final"]

        if bucket in CORE_THRESHOLDS and threshold_pass(row, CORE_THRESHOLDS[bucket]):
            label = f"Core_{bucket.capitalize()}_{tenor}D"
            if label in SIZE_BY_LABEL:
                rows.loc[idx, "core_signal_final"] = True
                rows.loc[idx, "core_label_final"] = label
                rows.loc[idx, "core_size_pct_final"] = SIZE_BY_LABEL[label]

        if bucket in SECONDARY_THRESHOLDS and threshold_pass(row, SECONDARY_THRESHOLDS[bucket]):
            label = f"Secondary_{bucket.capitalize()}_{tenor}D"
            if label in SIZE_BY_LABEL:
                rows.loc[idx, "secondary_signal_final"] = True
                rows.loc[idx, "secondary_label_final"] = label
                rows.loc[idx, "secondary_size_pct_final"] = SIZE_BY_LABEL[label]

    candidates = []

    for idx, row in rows.iterrows():
        if bool(row["core_signal_final"]):
            candidates.append({
                "row_index": idx,
                "date": row["date"],
                "trade_date": row["trade_date"],
                "tenor": int(row["tenor"]),
                "selected_tier_final": "Core",
                "selected_label_final": row["core_label_final"],
                "selected_size_pct_final": float(row["core_size_pct_final"]),
                "tier_priority": 1,
                "tenor_priority": int(row["tenor"]),
            })
        if bool(row["secondary_signal_final"]):
            candidates.append({
                "row_index": idx,
                "date": row["date"],
                "trade_date": row["trade_date"],
                "tenor": int(row["tenor"]),
                "selected_tier_final": "Secondary",
                "selected_label_final": row["secondary_label_final"],
                "selected_size_pct_final": float(row["secondary_size_pct_final"]),
                "tier_priority": 0,
                "tenor_priority": int(row["tenor"]),
            })

    rows["row_trade_signal_final"] = rows["core_signal_final"] | rows["secondary_signal_final"]
    rows["selected_trade_final"] = False
    rows["selected_tier_final"] = pd.NA
    rows["selected_label_final"] = pd.NA
    rows["selected_size_pct_final"] = np.nan

    if candidates:
        cand = pd.DataFrame(candidates)
        cand = cand.sort_values(
            ["date", "selected_size_pct_final", "tier_priority", "tenor_priority"],
            ascending=[True, False, False, False],
        )
        selected = cand.drop_duplicates(["date"], keep="first").copy()

        for _, s in selected.iterrows():
            idx = int(s["row_index"])
            rows.loc[idx, "selected_trade_final"] = True
            rows.loc[idx, "selected_tier_final"] = s["selected_tier_final"]
            rows.loc[idx, "selected_label_final"] = s["selected_label_final"]
            rows.loc[idx, "selected_size_pct_final"] = s["selected_size_pct_final"]

        decision = selected.copy()
    else:
        decision = pd.DataFrame(columns=[
            "date", "trade_date", "tenor", "selected_tier_final",
            "selected_label_final", "selected_size_pct_final",
        ])

    return rows, decision


def build_append_rows(gen: pd.DataFrame, old_signal: pd.DataFrame) -> pd.DataFrame:
    old_cols = list(old_signal.columns)
    append = pd.DataFrame(index=gen.index)

    # Start with old schema.
    for c in old_cols:
        if c in gen.columns:
            append[c] = gen[c].values
        else:
            append[c] = np.nan

    # Fill common old column aliases if they exist in old schema.
    alias_map = {
        "forecast_variance_final": "forecast_variance_final",
        "forecast_vol_final": "forecast_vol_final",
        "model_vrp_log_final": "model_vrp_log_final",
        "z_3m_final": "z_3m_final",
        "z_1y_final": "z_1y_final",
        "rsi14_final": "rsi14_final",
        "rv21d_vol_pct_final": "rv21d_vol_pct_final",
        "model_vrp_log": "model_vrp_log_final",
        "model_vrp_z_3m": "z_3m_final",
        "model_vrp_z_1y": "z_1y_final",
        "RSI14": "rsi14_final",
        "forecast_variance_candidate": "forecast_variance_candidate",
        "candidate_forecast_vol_pct": "candidate_forecast_vol_pct",
    }

    for old_c, gen_c in alias_map.items():
        if old_c in append.columns and gen_c in gen.columns:
            append[old_c] = gen[gen_c].values

    append["date"] = gen["date"].values
    append["trade_date"] = gen["trade_date"].values
    append["tenor"] = gen["tenor"].values
    if "target_days" in append.columns:
        append["target_days"] = gen["tenor"].values

    # Add any new standardized final fields after preserving old schema.
    new_cols = [
        "forecast_variance_final",
        "forecast_vol_final",
        "implied_vol_final",
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
        "tenor_bucket_final",
        "core_signal_final",
        "secondary_signal_final",
        "core_label_final",
        "secondary_label_final",
        "core_size_pct_final",
        "secondary_size_pct_final",
        "row_trade_signal_final",
        "selected_trade_final",
        "selected_tier_final",
        "selected_label_final",
        "selected_size_pct_final",
    ]

    for c in new_cols:
        if c not in append.columns:
            append[c] = gen[c].values if c in gen.columns else np.nan

    return append.copy()


def compare_old_overlap(old_signal: pd.DataFrame, combined: pd.DataFrame, audit_dir: Path, run_ts: str) -> pd.DataFrame:
    old = old_signal.copy()
    new = combined.copy()

    old["_key_trade_date"] = old["trade_date"].astype(int)
    old["_key_tenor"] = old["tenor"].astype(int)
    new["_key_trade_date"] = new["trade_date"].astype(int)
    new["_key_tenor"] = new["tenor"].astype(int)

    old_cols = [c for c in old_signal.columns if c in new.columns and c not in ["date"]]
    merged = old[["_key_trade_date", "_key_tenor", *old_cols]].merge(
        new[["_key_trade_date", "_key_tenor", *old_cols]],
        on=["_key_trade_date", "_key_tenor"],
        how="inner",
        suffixes=("_old", "_new"),
    )

    rows = []
    for c in old_cols:
        a = merged[f"{c}_old"]
        b = merged[f"{c}_new"]

        a_num = pd.to_numeric(a, errors="coerce")
        b_num = pd.to_numeric(b, errors="coerce")
        numeric_possible = a_num.notna().sum() + b_num.notna().sum() > 0

        if numeric_possible:
            both = a_num.notna() & b_num.notna()
            one_null = a_num.isna() ^ b_num.isna()
            diff = (a_num[both] - b_num[both]).abs()
            max_diff = float(diff.max()) if len(diff) else 0.0
            status = "PASS" if max_diff == 0.0 and int(one_null.sum()) == 0 else "FAIL"
            rows.append({
                "column": c,
                "common_rows": len(merged),
                "valid_compared_rows": int(both.sum()),
                "one_null_rows": int(one_null.sum()),
                "max_abs_diff": max_diff,
                "status": status,
            })
        else:
            a_str = a.astype("string").fillna("<NA>")
            b_str = b.astype("string").fillna("<NA>")
            mismatches = int((a_str != b_str).sum())
            rows.append({
                "column": c,
                "common_rows": len(merged),
                "valid_compared_rows": len(merged),
                "one_null_rows": 0,
                "max_abs_diff": 0.0,
                "status": "PASS" if mismatches == 0 else "FAIL",
                "string_mismatches": mismatches,
            })

    summary = pd.DataFrame(rows)
    summary_path = audit_dir / f"final_signal_old_overlap_compare_{run_ts}.csv"
    summary.to_csv(summary_path, index=False)
    return summary


def main() -> None:
    args = parse_args()

    project_root = Path(args.project_root)
    forecast_path = Path(args.forecast_panel)
    old_signal_path = Path(args.old_signal_panel)
    end_ts = pd.to_datetime(args.end_date, format="%Y%m%d").normalize()
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    old_output_dir = old_signal_path.parent
    audit_dir = project_root / "data" / "audit" / "vrp_final_signal_panel_v1"
    audit_dir.mkdir(parents=True, exist_ok=True)

    section("Final signal panel update v1")
    print("Project root:", project_root)
    print("Forecast panel:", forecast_path)
    print("Old signal panel:", old_signal_path)
    print("End date:", end_ts.date())
    print("Output dir:", old_output_dir)
    print("Audit dir:", audit_dir)

    forecast = normalize_date_tenor(pd.read_parquet(forecast_path))
    old_signal = normalize_date_tenor(pd.read_parquet(old_signal_path))

    forecast = forecast[forecast["date"].le(end_ts)].copy()
    old_signal = old_signal[old_signal["date"].le(old_signal["date"].max())].copy()

    section("Loaded inputs")
    print("Forecast rows:", len(forecast))
    print("Forecast date range:", forecast["date"].min().date(), "to", forecast["date"].max().date())
    print("Old signal rows:", len(old_signal))
    print("Old signal date range:", old_signal["date"].min().date(), "to", old_signal["date"].max().date())

    locked_forecast = select_locked_forecast_rows(forecast)
    generated = compute_generated_rows(locked_forecast, old_signal, end_ts)
    generated, decision = apply_locked_thresholds(generated)

    append_rows = build_append_rows(generated, old_signal)

    combined = pd.concat([old_signal, append_rows], ignore_index=True, sort=False)
    combined = normalize_date_tenor(combined)
    combined = combined.sort_values(["date", "tenor"]).reset_index(drop=True)

    safe_start = combined["date"].min().strftime("%Y%m%d")
    safe_end = combined["date"].max().strftime("%Y%m%d")

    out_path = old_output_dir / f"02C_cross_tenor_core_signal_base_panel_{safe_start}_{safe_end}_{run_ts}.parquet"
    combined.to_parquet(out_path, index=False)

    decision_path = audit_dir / f"final_signal_selected_decision_{safe_start}_{safe_end}_{run_ts}.csv"
    decision.to_csv(decision_path, index=False)

    section("Generated appended rows")
    show_cols = [
        "date", "trade_date", "tenor",
        "forecast_variance_final", "forecast_vol_final",
        "model_vrp_log_final", "z_3m_final", "z_1y_final",
        "rsi14_final", "rv21d_vol_pct_final",
        "core_signal_final", "secondary_signal_final",
        "selected_trade_final", "selected_label_final", "selected_size_pct_final",
    ]
    show_cols = [c for c in show_cols if c in generated.columns]
    print(generated[show_cols].sort_values(["date", "tenor"]).to_string(index=False))

    section("Selected decisions")
    if len(decision):
        print(decision.to_string(index=False))
    else:
        print("No selected trades in appended dates.")

    overlap = compare_old_overlap(old_signal, combined, audit_dir, run_ts)

    section("Old overlap validation")
    fail_overlap = overlap[overlap["status"].eq("FAIL")]
    print("Compared columns:", len(overlap))
    print("Failed columns:", len(fail_overlap))
    if len(fail_overlap):
        print(fail_overlap.head(50).to_string(index=False))
    else:
        print("Old signal panel rows preserved exactly.")

    latest = combined[combined["date"].eq(end_ts)].copy()
    latest_tenors = sorted(latest["tenor"].dropna().astype(int).unique().tolist())

    checks = []

    def check(name: str, ok: bool, detail: str):
        checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    check("output_reaches_target_date", combined["date"].max() == end_ts, f"target={end_ts.date()}; max={combined['date'].max().date()}")
    check("latest_has_expected_tenors", latest_tenors == EXPECTED_TENORS, f"latest_tenors={latest_tenors}")
    check("old_overlap_exact", len(fail_overlap) == 0, f"failed_cols={fail_overlap['column'].tolist() if len(fail_overlap) else []}")
    check("append_rows_exist", len(append_rows) > 0, f"append_rows={len(append_rows)}")

    required_latest = [
        "forecast_variance_final",
        "forecast_vol_final",
        "model_vrp_log_final",
        "z_3m_final",
        "z_1y_final",
        "rsi14_final",
        "rv21d_vol_pct_final",
        "core_signal_final",
        "secondary_signal_final",
        "selected_trade_final",
    ]
    missing_required = [c for c in required_latest if c not in latest.columns]
    null_required = [
        c for c in required_latest
        if c in latest.columns and latest[c].isna().any()
    ]
    check("latest_final_fields_present", len(missing_required) == 0, f"missing={missing_required}")
    check("latest_final_fields_non_null", len(null_required) == 0, f"null={null_required}")

    validation = pd.DataFrame(checks)
    validation_path = audit_dir / f"final_signal_update_validation_{run_ts}.csv"
    validation.to_csv(validation_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "forecast_panel": str(forecast_path),
        "old_signal_panel": str(old_signal_path),
        "output_panel": str(out_path),
        "decision_csv": str(decision_path),
        "validation": str(validation_path),
        "thresholds": {
            "core": CORE_THRESHOLDS,
            "secondary": SECONDARY_THRESHOLDS,
        },
        "sizes": SIZE_BY_LABEL,
    }
    manifest_path = audit_dir / f"final_signal_update_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    section("Output written")
    print("Final signal panel:", out_path)
    print("Selected decision CSV:", decision_path)
    print("Validation:", validation_path)
    print("Manifest:", manifest_path)

    section("Validation")
    print(validation.to_string(index=False))

    fails = validation[validation["status"].eq("FAIL")]

    section("Final result")
    print("Hard checks failed:", len(fails))
    if len(fails):
        print(fails.to_string(index=False))
        raise RuntimeError("FINAL_SIGNAL_PANEL_UPDATE failed validation.")

    print("FINAL_SIGNAL_PANEL_UPDATE_PASS: True")
    print("DONE — final signal panel extended.")


if __name__ == "__main__":
    main()
