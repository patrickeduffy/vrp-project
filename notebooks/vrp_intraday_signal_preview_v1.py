
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

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

CORE_THRESHOLDS = {
    "Middle": {"vrp_log": 0.65, "z_3m": 0.70, "z_1y": 0.70, "rsi_cap": 70.0, "rv21d_floor": 8.5},
    "Back":   {"vrp_log": 0.70, "z_3m": 0.70, "z_1y": 0.70, "rsi_cap": 70.0, "rv21d_floor": 8.5},
}

SECONDARY_THRESHOLDS = {
    "Front":  {"vrp_log": 0.65, "z_3m": 0.20, "z_1y": 0.20, "rsi_cap": 75.0, "rv21d_floor": 7.0},
    "Middle": {"vrp_log": 0.65, "z_3m": 0.20, "z_1y": 0.20, "rsi_cap": 76.0, "rv21d_floor": 7.0},
    "Back":   {"vrp_log": 0.65, "z_3m": 0.00, "z_1y": 0.00, "rsi_cap": 77.0, "rv21d_floor": 6.5},
}

SIZE_MAP = {
    ("Core", "Middle", 21): 3.50,
    ("Core", "Middle", 24): 4.25,
    ("Core", "Back", 27): 4.50,
    ("Core", "Back", 30): 4.75,
    ("Core", "Back", 33): 5.00,

    ("Secondary", "Front", 12): 1.50,
    ("Secondary", "Front", 15): 2.00,
    ("Secondary", "Front", 18): 2.75,
    ("Secondary", "Middle", 21): 3.50,
    ("Secondary", "Middle", 24): 3.75,
    ("Secondary", "Back", 27): 4.00,
    ("Secondary", "Back", 30): 4.25,
    ("Secondary", "Back", 33): 4.50,
}


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def default_quote_time() -> str:
    return datetime.now().replace(second=0, microsecond=0).strftime("%H:%M:%S.000")


def quote_time_to_ms(q: str) -> int:
    q = str(q).strip()
    if "." in q:
        main, frac = q.split(".", 1)
    else:
        main, frac = q, "0"
    hh, mm, ss = [int(x) for x in main.split(":")]
    ms = int((frac + "000")[:3])
    return ((hh * 3600 + mm * 60 + ss) * 1000) + ms


def normalize_trade_date_series(s: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().mean() > 0.80 and numeric.dropna().between(19000101, 22000101).mean() > 0.80:
        return pd.to_datetime(
            numeric.astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y%m%d").astype("Int64")

    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().mean() < 0.80:
        raise RuntimeError("Could not parse date series.")
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def tenor_bucket(tenor: int) -> str:
    tenor = int(tenor)
    if tenor in [12, 15, 18]:
        return "Front"
    if tenor in [21, 24]:
        return "Middle"
    if tenor in [27, 30, 33]:
        return "Back"
    return "Inactive"


def load_module_helpers() -> tuple[object, object, object]:
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

    import vrp_intraday_implied_variance_smoke_v1 as iv
    import vrp_intraday_feature_vector_smoke_v1 as fv
    import vrp_intraday_forecast_vrp_smoke_v1 as fr

    return iv, fv, fr


def build_intraday_implied_variance(
    *,
    project_root: Path,
    base_url: str,
    trade_date: int,
    quote_time: str,
    target_tenors: list[int],
    max_expiration_days: int,
    default_risk_free_rate: float,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame | dict]]:
    iv, _, _ = load_module_helpers()

    calc_time_ms = iv.quote_time_to_ms(quote_time)

    spxw_exps, spxw_meta = iv.list_expirations(base_url, "SPXW")
    spx_exps, spx_meta = iv.list_expirations(base_url, "SPX")

    if not spxw_exps:
        raise RuntimeError("No SPXW expirations loaded.")
    if not spx_exps:
        raise RuntimeError("No SPX expirations loaded.")

    trading_dates = iv.get_trading_dates(project_root)

    candidates = iv.expiration_candidates(
        trade_date=int(trade_date),
        calc_time_ms=int(calc_time_ms),
        spx_exps=set(spx_exps),
        spxw_exps=set(spxw_exps),
        trading_dates=trading_dates,
        max_days=int(max_expiration_days),
    )

    required_by_tenor, unique_chains = iv.required_chains_for_target_tenors(
        candidates=candidates,
        target_tenors=target_tenors,
    )

    rate, rate_source = iv.load_sofr_rate_or_default(
        project_root=project_root,
        trade_date=int(trade_date),
        default_rate=float(default_risk_free_rate),
    )

    chain_results = {}
    quality_rows = []
    request_meta_rows = [spxw_meta, spx_meta]

    for _, row in unique_chains.iterrows():
        root = str(row["root"])
        exp = int(row["expiration"])

        chain, meta = iv.get_chain_at_time(
            base_url=base_url,
            root=root,
            expiration=exp,
            trade_date=int(trade_date),
            quote_time=quote_time,
        )

        request_meta_rows.append(meta)
        quality_rows.append(iv.chain_quality_row(chain, meta, root, exp))

        if chain.empty:
            raise RuntimeError(f"Empty option chain for {root} {exp}")

        chain_results[(root, exp)] = chain

    chain_quality = pd.DataFrame(quality_rows)

    variance_table, _calc_results = iv.calculate_variance_for_unique_chains(
        unique_chains=unique_chains,
        chain_results=chain_results,
        r=rate,
    )

    variance_lookup = {
        (row["root"], int(row["expiration"])): row
        for _, row in variance_table.iterrows()
    }

    rows = []

    for target_days in target_tenors:
        pair = required_by_tenor[required_by_tenor["target_days"].eq(int(target_days))].copy()
        if len(pair) != 2:
            raise RuntimeError(f"Expected two expiration rows for target {target_days}d.")

        term_rows = []
        for _, leg in pair.iterrows():
            key = (leg["root"], int(leg["expiration"]))
            vrow = variance_lookup[key]
            term_rows.append({
                "term": leg["leg"],
                "root": leg["root"],
                "expiration": int(leg["expiration"]),
                "minutes": int(leg["minutes"]),
                "days": float(leg["days"]),
                "variance": float(vrow["variance"]),
                "vix_style_vol": float(vrow["vix_style_vol"]),
                "num_options": int(vrow["num_options"]),
            })

        term_df = pd.DataFrame(term_rows).sort_values("minutes").reset_index(drop=True)

        implied_variance = iv.interpolate_variance_to_target_days(
            term_df=term_df,
            target_days=int(target_days),
        )
        implied_vol = 100.0 * math.sqrt(implied_variance)

        rows.append({
            "trade_date": int(trade_date),
            "quote_time": quote_time,
            "calc_time_ms": int(calc_time_ms),
            "tenor": int(target_days),
            "target_days": int(target_days),
            "risk_free_rate": float(rate),
            "rate_source": rate_source,
            "implied_variance_intraday": float(implied_variance),
            "vix_style_vol_intraday": float(implied_vol),
            "near_root": term_df.loc[0, "root"],
            "near_expiration": int(term_df.loc[0, "expiration"]),
            "near_days": float(term_df.loc[0, "days"]),
            "near_variance": float(term_df.loc[0, "variance"]),
            "near_num_options": int(term_df.loc[0, "num_options"]),
            "next_root": term_df.loc[1, "root"],
            "next_expiration": int(term_df.loc[1, "expiration"]),
            "next_days": float(term_df.loc[1, "days"]),
            "next_variance": float(term_df.loc[1, "variance"]),
            "next_num_options": int(term_df.loc[1, "num_options"]),
        })

    panel = pd.DataFrame(rows).sort_values("tenor").reset_index(drop=True)

    aux = {
        "required_by_tenor": required_by_tenor,
        "unique_chains": unique_chains,
        "chain_quality": chain_quality,
        "variance_table": variance_table,
        "request_meta": pd.DataFrame(request_meta_rows),
        "risk_free_rate": rate,
        "rate_source": rate_source,
    }

    return panel, aux


def build_intraday_feature_vector(
    *,
    project_root: Path,
    base_url: str,
    trade_date: int,
    quote_time: str,
    interval: str,
    validation_lookback_rows: int,
    validation_tolerance: float,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame | dict | str]]:
    _, fv, _ = load_module_helpers()

    quote_ms = quote_time_to_ms(quote_time)

    feature_history, feature_path, feature_dispersion = fv.load_locked_feature_history(project_root)

    prior = feature_history[feature_history["feature_trade_date_int"] < int(trade_date)].copy()
    if prior.empty:
        raise RuntimeError(f"No prior locked feature history before {trade_date}.")

    prior_feature_close = float(prior.sort_values("feature_trade_date_int").iloc[-1]["spx_close_for_features"])

    live_candidates, request_meta, ohlc_samples = fv.fetch_intraday_ohlc_candidates(
        base_url=base_url,
        trade_date_int=int(trade_date),
        quote_time=quote_time,
        interval=interval,
    )

    if live_candidates.empty:
        raise RuntimeError("No live OHLC candidates returned.")

    live_candidates = live_candidates.copy()
    live_candidates["scale_ratio_to_prior_feature_close"] = live_candidates["live_close"] / prior_feature_close
    live_candidates["scale_compatible"] = live_candidates["scale_ratio_to_prior_feature_close"].between(0.5, 2.0)

    live_source = fv.select_live_source(live_candidates, prior_feature_close)

    selected_formulas, feature_validation = fv.validate_and_select_formulas(
        feature_history=feature_history,
        trade_date_int=int(trade_date),
        lookback_rows=int(validation_lookback_rows),
        tolerance=float(validation_tolerance),
    )

    live_feature_panel, return_history_live, live_meta = fv.build_live_feature_panel(
        feature_history=feature_history,
        selected_formulas=selected_formulas,
        trade_date_int=int(trade_date),
        quote_time=quote_time,
        quote_ms=int(quote_ms),
        live_source=live_source,
    )

    aux = {
        "feature_path": str(feature_path),
        "feature_dispersion": feature_dispersion,
        "live_candidates": live_candidates,
        "request_meta": request_meta,
        "ohlc_samples": ohlc_samples,
        "selected_formulas": {k: v.split("__", 1)[1] for k, v in selected_formulas.items()},
        "feature_validation": feature_validation,
        "return_history_live": return_history_live,
        "live_meta": live_meta,
        "live_source": live_source,
    }

    return live_feature_panel, aux


def score_forecast_and_vrp(
    *,
    project_root: Path,
    feature_panel: pd.DataFrame,
    implied_panel: pd.DataFrame,
    trade_date: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    _, _, fr = load_module_helpers()

    artifact_path = project_root / "data" / "processed" / "vrp_model_artifacts" / "unified_fds_no_min_return_model_artifacts_v1.joblib"
    if not artifact_path.exists():
        raise FileNotFoundError(f"Missing model artifact: {artifact_path}")

    artifact = joblib.load(artifact_path)

    feature_keep = feature_panel.copy()
    implied_keep = implied_panel.copy()

    live = feature_keep.merge(
        implied_keep.drop(columns=["quote_time"], errors="ignore"),
        on=["trade_date", "tenor"],
        how="inner",
        validate="one_to_one",
    )

    if len(live) != len(EXPECTED_TENORS):
        raise RuntimeError(f"Expected 9 merged live rows, found {len(live)}")

    scored, score_summary = fr.score_live_panel(live, artifact)

    scored["forecast_variance_intraday"] = scored["forecast_variance_candidate"]
    scored["forecast_vol_intraday"] = scored["candidate_forecast_vol_pct"]
    scored["model_vrp_log_intraday"] = np.log(
        pd.to_numeric(scored["implied_variance_intraday"], errors="coerce")
        / pd.to_numeric(scored["forecast_variance_candidate"], errors="coerce")
    )

    history, history_path = fr.load_final_history(project_root)

    scored_z, zscore_audit = fr.compute_prior_only_zscores(
        live_panel=scored,
        history=history,
        trade_date=int(trade_date),
        windows={"3m": 63, "1y": 252},
    )

    aux = {
        "artifact_path": str(artifact_path),
        "history_path": str(history_path),
        "artifact_type": str(type(artifact)),
        "artifact_keys": fr.artifact_keys(artifact),
    }

    return scored_z.sort_values("tenor").reset_index(drop=True), score_summary, zscore_audit, aux


def load_eod_prices(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "market_data" / "spy_eod_prices_v1.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing EOD prices: {path}")

    raw = pd.read_parquet(path)

    if "trade_date" not in raw.columns or "spy_close" not in raw.columns:
        raise RuntimeError(f"EOD prices missing trade_date/spy_close. Columns: {list(raw.columns)}")

    out = raw.copy()
    out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype(int)
    out["spy_close"] = pd.to_numeric(out["spy_close"], errors="coerce")

    out = (
        out.dropna(subset=["trade_date", "spy_close"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    out["spy_log_return"] = np.log(out["spy_close"] / out["spy_close"].shift(1))

    return out, path


def load_rv_history(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "market_data" / "spy_realized_vol_history_v1.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing RV history: {path}")

    raw = pd.read_parquet(path)

    required = ["trade_date", "spy_close", "spy_log_return", "rv21d_vol_pct"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise RuntimeError(f"RV history missing {missing}. Columns: {list(raw.columns)}")

    out = raw.copy()
    out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype(int)

    for c in ["spy_close", "spy_log_return", "rv21d_vol_pct"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = (
        out.dropna(subset=["trade_date", "spy_close"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    return out, path


def load_final_signal_full(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "vrp_final_signal" / "vrp_final_corsi_signal_base_panel_v1.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing final signal base panel: {path}")

    raw = pd.read_parquet(path)
    out = raw.copy()
    out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype(int)
    out["tenor"] = pd.to_numeric(out["tenor"], errors="coerce").astype("Int64")

    return out, path


def compute_rsi_simple(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = gain.rolling(window, min_periods=window).mean()
    avg_loss = loss.rolling(window, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0)

    return rsi


def compute_rsi_wilder_seeded(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = pd.Series(np.nan, index=close.index, dtype=float)
    avg_loss = pd.Series(np.nan, index=close.index, dtype=float)

    if len(close) <= window:
        return avg_gain

    first = window
    avg_gain.iloc[first] = gain.iloc[1:first + 1].mean()
    avg_loss.iloc[first] = loss.iloc[1:first + 1].mean()

    for i in range(first + 1, len(close)):
        avg_gain.iloc[i] = ((avg_gain.iloc[i - 1] * (window - 1)) + gain.iloc[i]) / window
        avg_loss.iloc[i] = ((avg_loss.iloc[i - 1] * (window - 1)) + loss.iloc[i]) / window

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0)

    return rsi


def compute_rsi_ewm(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0)

    return rsi


def validate_rsi_formula(
    *,
    eod_prices: pd.DataFrame,
    final_signal: pd.DataFrame,
    trade_date: int,
    tolerance: float,
) -> tuple[str, pd.DataFrame]:
    if "rsi14_final" not in final_signal.columns:
        raise RuntimeError("Final signal history missing rsi14_final.")

    hist_final = (
        final_signal[final_signal["trade_date"] < int(trade_date)]
        [["trade_date", "rsi14_final"]]
        .dropna()
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .tail(252)
        .copy()
    )

    if hist_final.empty:
        raise RuntimeError("No final-signal RSI history for validation.")

    base = eod_prices[["trade_date", "spy_close"]].copy().sort_values("trade_date").reset_index(drop=True)
    close = base["spy_close"].astype(float)

    candidates = {
        "simple_rolling_avg_gain_loss_14": compute_rsi_simple(close, 14),
        "wilder_seeded_14": compute_rsi_wilder_seeded(close, 14),
        "ewm_alpha_1_over_14_adjust_false": compute_rsi_ewm(close, 14),
    }

    rows = []
    best_name = None
    best_tuple = None

    for name, series in candidates.items():
        tmp = base[["trade_date"]].copy()
        tmp["_candidate_rsi"] = series.values

        joined = hist_final.merge(tmp, on="trade_date", how="inner")
        mask = joined["rsi14_final"].notna() & joined["_candidate_rsi"].notna()

        if mask.sum() == 0:
            max_abs = np.inf
            mean_abs = np.inf
        else:
            diff = joined.loc[mask, "_candidate_rsi"] - joined.loc[mask, "rsi14_final"]
            max_abs = float(diff.abs().max())
            mean_abs = float(diff.abs().mean())

        rows.append({
            "formula": name,
            "validation_rows": int(mask.sum()),
            "first_validation_date": int(joined.loc[mask, "trade_date"].min()) if mask.sum() else None,
            "last_validation_date": int(joined.loc[mask, "trade_date"].max()) if mask.sum() else None,
            "max_abs_diff": max_abs,
            "mean_abs_diff": mean_abs,
            "status": "PASS" if max_abs <= tolerance else "FAIL",
        })

        tup = (max_abs, mean_abs)
        if best_tuple is None or tup < best_tuple:
            best_tuple = tup
            best_name = name

    audit = pd.DataFrame(rows).sort_values(["max_abs_diff", "mean_abs_diff"]).reset_index(drop=True)

    if audit.iloc[0]["max_abs_diff"] > tolerance:
        print(audit.to_string(index=False))
        raise RuntimeError("No RSI formula candidate matched rsi14_final within tolerance.")

    return str(best_name), audit


def compute_rsi_by_formula(close: pd.Series, formula: str) -> pd.Series:
    if formula == "simple_rolling_avg_gain_loss_14":
        return compute_rsi_simple(close, 14)
    if formula == "wilder_seeded_14":
        return compute_rsi_wilder_seeded(close, 14)
    if formula == "ewm_alpha_1_over_14_adjust_false":
        return compute_rsi_ewm(close, 14)
    raise RuntimeError(f"Unknown RSI formula: {formula}")


def compute_rv_candidates(log_return: pd.Series, window: int = 21) -> dict[str, pd.Series]:
    r = pd.to_numeric(log_return, errors="coerce")
    return {
        "std_ddof1_sqrt252_pct": r.rolling(window, min_periods=window).std(ddof=1) * math.sqrt(252.0) * 100.0,
        "std_ddof0_sqrt252_pct": r.rolling(window, min_periods=window).std(ddof=0) * math.sqrt(252.0) * 100.0,
        "rms_sqrt252_pct": np.sqrt((r ** 2).rolling(window, min_periods=window).mean() * 252.0) * 100.0,
    }


def validate_rv21d_formula(
    *,
    rv_history: pd.DataFrame,
    trade_date: int,
    tolerance: float,
) -> tuple[str, pd.DataFrame]:
    hist = (
        rv_history[rv_history["trade_date"] < int(trade_date)]
        [["trade_date", "spy_log_return", "rv21d_vol_pct"]]
        .dropna()
        .sort_values("trade_date")
        .reset_index(drop=True)
    )

    if hist.empty:
        raise RuntimeError("No RV history for validation.")

    candidates = compute_rv_candidates(hist["spy_log_return"], 21)

    rows = []
    best_name = None
    best_tuple = None

    for name, series in candidates.items():
        joined = hist[["trade_date", "rv21d_vol_pct"]].copy()
        joined["_candidate_rv21d"] = series.values
        joined = joined.tail(252).copy()

        mask = joined["rv21d_vol_pct"].notna() & joined["_candidate_rv21d"].notna()

        if mask.sum() == 0:
            max_abs = np.inf
            mean_abs = np.inf
        else:
            diff = joined.loc[mask, "_candidate_rv21d"] - joined.loc[mask, "rv21d_vol_pct"]
            max_abs = float(diff.abs().max())
            mean_abs = float(diff.abs().mean())

        rows.append({
            "formula": name,
            "validation_rows": int(mask.sum()),
            "first_validation_date": int(joined.loc[mask, "trade_date"].min()) if mask.sum() else None,
            "last_validation_date": int(joined.loc[mask, "trade_date"].max()) if mask.sum() else None,
            "max_abs_diff": max_abs,
            "mean_abs_diff": mean_abs,
            "status": "PASS" if max_abs <= tolerance else "FAIL",
        })

        tup = (max_abs, mean_abs)
        if best_tuple is None or tup < best_tuple:
            best_tuple = tup
            best_name = name

    audit = pd.DataFrame(rows).sort_values(["max_abs_diff", "mean_abs_diff"]).reset_index(drop=True)

    if audit.iloc[0]["max_abs_diff"] > tolerance:
        print(audit.to_string(index=False))
        raise RuntimeError("No RV21D formula candidate matched rv21d_vol_pct within tolerance.")

    return str(best_name), audit


def compute_live_rsi14_from_production_fallback(
    *,
    project_root: Path,
    final_signal: pd.DataFrame,
    trade_date: int,
    live_close: float,
    live_log_return: float,
) -> tuple[float, pd.DataFrame]:
    """
    Intraday RSI uses the same fallback path as production generated rows:
    vrp_final_signal_panel_update_v1.compute_rsi14_fallback.

    The canonical final signal panel may not preserve a close column, so this
    function injects canonical SPY EOD closes into old_signal before calling
    the production fallback.
    """
    notebooks = project_root / "notebooks"
    if str(notebooks) not in sys.path:
        sys.path.insert(0, str(notebooks))

    import vrp_final_signal_panel_update_v1 as final_mod

    old_signal = final_signal[final_signal["trade_date"].astype(int) < int(trade_date)].copy()

    if old_signal.empty:
        raise RuntimeError(f"No prior final signal history before trade_date={trade_date} for RSI fallback.")

    if "date" not in old_signal.columns:
        old_signal["date"] = pd.to_datetime(old_signal["trade_date"].astype(int).astype(str), format="%Y%m%d")
    else:
        old_signal["date"] = pd.to_datetime(old_signal["date"], errors="coerce")

    # Inject canonical SPY EOD close because the canonical final signal panel
    # does not necessarily preserve spx_close_for_features/spx_close/close.
    eod_prices, eod_path = load_eod_prices(project_root)
    eod_close = eod_prices[["trade_date", "spy_close"]].copy()
    eod_close["trade_date"] = eod_close["trade_date"].astype(int)
    eod_close["spy_close"] = pd.to_numeric(eod_close["spy_close"], errors="coerce")

    for c in ["spx_close_for_features", "spx_close", "close", "spy_close"]:
        if c in old_signal.columns:
            old_signal = old_signal.drop(columns=[c])

    old_signal = old_signal.merge(
        eod_close,
        on="trade_date",
        how="left",
        validate="many_to_one",
    )

    old_signal["spx_close_for_features"] = pd.to_numeric(old_signal["spy_close"], errors="coerce")
    old_signal["spx_close"] = pd.to_numeric(old_signal["spy_close"], errors="coerce")
    old_signal["close"] = pd.to_numeric(old_signal["spy_close"], errors="coerce")

    if old_signal["spx_close_for_features"].isna().all():
        raise RuntimeError(
            f"Could not inject canonical EOD SPY close into final signal history. EOD path: {eod_path}"
        )

    live_date = pd.to_datetime(str(int(trade_date)), format="%Y%m%d")

    generated = pd.DataFrame([{
        "date": live_date,
        "trade_date": int(trade_date),
        "spx_log_return": float(live_log_return),
        "spx_close_for_features": float(live_close),
        "spx_close": float(live_close),
        "close": float(live_close),
        "spy_close": float(live_close),
    }])

    rsi_fallback = final_mod.compute_rsi14_fallback(old_signal, generated)

    if rsi_fallback.empty or "rsi14_fallback" not in rsi_fallback.columns:
        raise RuntimeError("Production RSI fallback returned no rsi14_fallback value.")

    rsi_fallback["date"] = pd.to_datetime(rsi_fallback["date"], errors="coerce")
    live_row = rsi_fallback[rsi_fallback["date"].eq(live_date)].copy()

    if live_row.empty:
        raise RuntimeError("Production RSI fallback did not return the live trade_date row.")

    rsi14_live = float(pd.to_numeric(live_row["rsi14_fallback"], errors="coerce").iloc[-1])

    if not np.isfinite(rsi14_live):
        raise RuntimeError(f"Production RSI fallback returned non-finite RSI: {rsi14_live}")

    audit = pd.DataFrame([{
        "formula": "production_compute_rsi14_fallback_with_injected_spy_eod_close",
        "validation_rows": np.nan,
        "first_validation_date": np.nan,
        "last_validation_date": np.nan,
        "max_abs_diff": 0.0,
        "mean_abs_diff": 0.0,
        "status": "PASS",
        "eod_close_path": str(eod_path),
        "trade_date": int(trade_date),
        "live_close": float(live_close),
        "live_log_return": float(live_log_return),
        "rsi14_intraday": rsi14_live,
        "note": "Used exact production fallback function after injecting canonical SPY EOD close into old final-signal history.",
    }])

    return rsi14_live, audit

def compute_live_rsi_rv(
    *,
    project_root: Path,
    trade_date: int,
    live_close: float,
    final_signal: pd.DataFrame,
    rsi_tolerance: float,
    rv_tolerance: float,
) -> tuple[float, float, dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    eod_prices, eod_path = load_eod_prices(project_root)
    rv_history, rv_path = load_rv_history(project_root)

    prior_eod = eod_prices[eod_prices["trade_date"] < int(trade_date)].copy().sort_values("trade_date")
    if prior_eod.empty:
        raise RuntimeError(f"No prior EOD prices before trade_date={trade_date}")

    prior_row = prior_eod.iloc[-1]
    prior_close = float(prior_row["spy_close"])
    live_log_return = float(math.log(float(live_close) / prior_close))

    # Use production final-signal fallback path, not generic RSI candidates.
    rsi14_live, rsi_audit = compute_live_rsi14_from_production_fallback(
        project_root=project_root,
        final_signal=final_signal,
        trade_date=int(trade_date),
        live_close=float(live_close),
        live_log_return=float(live_log_return),
    )

    # RV21D is explicitly canonical in vrp_market_data_build_v1:
    # rolling 21 trading-day std(log return) * sqrt(252) * 100.
    rv_formula, rv_audit = validate_rv21d_formula(
        rv_history=rv_history,
        trade_date=int(trade_date),
        tolerance=float(rv_tolerance),
    )

    live_price_history = pd.concat(
        [
            prior_eod[["trade_date", "spy_close", "spy_log_return"]],
            pd.DataFrame([{
                "trade_date": int(trade_date),
                "spy_close": float(live_close),
                "spy_log_return": float(live_log_return),
            }]),
        ],
        ignore_index=True,
    ).sort_values("trade_date").reset_index(drop=True)

    rv_candidates = compute_rv_candidates(live_price_history["spy_log_return"], 21)
    rv21d_live = float(rv_candidates[rv_formula].iloc[-1])

    meta = {
        "eod_price_path": str(eod_path),
        "rv_history_path": str(rv_path),
        "prior_eod_trade_date": int(prior_row["trade_date"]),
        "prior_eod_close": prior_close,
        "live_close": float(live_close),
        "live_log_return": live_log_return,
        "rsi_formula": "production_compute_rsi14_fallback",
        "rv21d_formula": rv_formula,
        "rsi14_intraday": rsi14_live,
        "rv21d_vol_pct_intraday": rv21d_live,
    }

    return rsi14_live, rv21d_live, meta, rsi_audit, rv_audit, live_price_history.tail(90)


def apply_signal_logic(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = panel.copy()

    out["tenor_bucket"] = out["tenor"].astype(int).map(tenor_bucket)

    out["core_pass"] = False
    out["secondary_pass"] = False

    for idx, row in out.iterrows():
        tenor = int(row["tenor"])
        bucket = row["tenor_bucket"]

        vrp = float(row["model_vrp_log_intraday"])
        z3 = float(row["z_3m_intraday"])
        z1 = float(row["z_1y_intraday"])
        rsi = float(row["rsi14_intraday"])
        rv21 = float(row["rv21d_vol_pct_intraday"])

        if bucket in CORE_THRESHOLDS and (("Core", bucket, tenor) in SIZE_MAP):
            t = CORE_THRESHOLDS[bucket]
            out.loc[idx, "core_pass"] = bool(
                vrp > t["vrp_log"]
                and z3 > t["z_3m"]
                and z1 > t["z_1y"]
                and rsi < t["rsi_cap"]
                and rv21 > t["rv21d_floor"]
            )

        if bucket in SECONDARY_THRESHOLDS and (("Secondary", bucket, tenor) in SIZE_MAP):
            t = SECONDARY_THRESHOLDS[bucket]
            out.loc[idx, "secondary_pass"] = bool(
                vrp > t["vrp_log"]
                and z3 > t["z_3m"]
                and z1 > t["z_1y"]
                and rsi < t["rsi_cap"]
                and rv21 > t["rv21d_floor"]
            )

    candidates = []

    for _, row in out.iterrows():
        tenor = int(row["tenor"])
        bucket = row["tenor_bucket"]

        for layer, pass_col in [("Core", "core_pass"), ("Secondary", "secondary_pass")]:
            if not bool(row[pass_col]):
                continue

            size = SIZE_MAP.get((layer, bucket, tenor))
            if size is None:
                continue

            candidates.append({
                "trade_date": int(row["trade_date"]),
                "quote_time": row.get("quote_time"),
                "tenor": tenor,
                "tenor_bucket": bucket,
                "selected_layer": layer,
                "selected_tenor_bucket": bucket,
                "selected_tenor": tenor,
                "selected_sleeve_id": f"{layer}_{bucket}_{tenor}D",
                "selected_size_pct": float(size),
                "selected_size_label": f"{size:.2f}%",
                "selected_sizing_quality_score": float(size),
                "selected_win_rate": np.nan,
                "selected_1pct_expected_loss_positive": np.nan,
                "layer_priority": 0 if layer == "Core" else 1,
                "tie_break_tenor": tenor,
            })

    candidates_df = pd.DataFrame(candidates)

    out["selected"] = False
    out["selected_layer"] = ""
    out["selected_tenor_bucket"] = ""
    out["selected_tenor"] = np.nan
    out["selected_sleeve_id"] = ""
    out["selected_size_pct"] = np.nan
    out["selected_size_label"] = ""
    out["selected_sizing_quality_score"] = np.nan
    out["selected_win_rate"] = np.nan
    out["selected_1pct_expected_loss_positive"] = np.nan
    out["selection_rank"] = np.nan

    if not candidates_df.empty:
        ranked = candidates_df.sort_values(
            by=[
                "selected_size_pct",
                "layer_priority",
                "selected_sizing_quality_score",
                "tie_break_tenor",
            ],
            ascending=[False, True, False, False],
        ).reset_index(drop=True)

        ranked["selection_rank"] = np.arange(1, len(ranked) + 1)

        winner = ranked.iloc[0].to_dict()
        mask = out["tenor"].astype(int).eq(int(winner["tenor"]))

        for c in [
            "selected_layer",
            "selected_tenor_bucket",
            "selected_tenor",
            "selected_sleeve_id",
            "selected_size_pct",
            "selected_size_label",
            "selected_sizing_quality_score",
            "selected_win_rate",
            "selected_1pct_expected_loss_positive",
            "selection_rank",
        ]:
            out.loc[mask, c] = winner.get(c, np.nan)

        out.loc[mask, "selected"] = True

        summary = pd.DataFrame([{
            "asof_timestamp_local": out["asof_timestamp_local"].iloc[0],
            "trade_date": int(out["trade_date"].iloc[0]),
            "quote_time": out["quote_time"].iloc[0],
            "preview_decision": "TRADE",
            "selected": True,
            **{k: winner.get(k) for k in [
                "selected_layer",
                "selected_tenor_bucket",
                "selected_tenor",
                "selected_sleeve_id",
                "selected_size_pct",
                "selected_size_label",
            ]},
            "candidate_count": int(len(candidates_df)),
        }])
    else:
        summary = pd.DataFrame([{
            "asof_timestamp_local": out["asof_timestamp_local"].iloc[0],
            "trade_date": int(out["trade_date"].iloc[0]),
            "quote_time": out["quote_time"].iloc[0],
            "preview_decision": "NO_TRADE",
            "selected": False,
            "selected_layer": "",
            "selected_tenor_bucket": "",
            "selected_tenor": np.nan,
            "selected_sleeve_id": "",
            "selected_size_pct": np.nan,
            "selected_size_label": "",
            "candidate_count": 0,
        }])

    return out, summary


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--base-url", default="http://127.0.0.1:25503/v3")
    p.add_argument("--trade-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--quote-time", default=None)
    p.add_argument("--interval", default="1m")
    p.add_argument("--target-tenors", default="9,12,15,18,21,24,27,30,33")
    p.add_argument("--max-expiration-days", type=int, default=90)
    p.add_argument("--default-risk-free-rate", type=float, default=0.05)
    p.add_argument("--feature-validation-lookback-rows", type=int, default=252)
    p.add_argument("--feature-validation-tolerance", type=float, default=1e-6)
    p.add_argument("--rsi-validation-tolerance", type=float, default=1e-6)
    p.add_argument("--rv21d-validation-tolerance", type=float, default=1e-6)
    args = p.parse_args()

    project_root = Path(args.project_root)
    trade_date = int(pd.to_datetime(str(args.trade_date), format="%Y%m%d").strftime("%Y%m%d"))
    quote_time = args.quote_time or default_quote_time()
    run_ts = now_stamp()
    asof_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    target_tenors = sorted(int(x.strip()) for x in str(args.target_tenors).split(",") if x.strip())

    processed_dir = project_root / "data" / "processed" / "vrp_intraday_signal"
    audit_dir = project_root / "data" / "audit" / "intraday_signal_preview"
    processed_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday signal preview v1")
    print(f"Project root:  {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"As-of local:   {asof_ts}")
    print(f"Trade date:    {trade_date}")
    print(f"Quote time:    {quote_time}")
    print(f"Target tenors: {target_tenors}")
    print(f"Processed dir: {processed_dir}")
    print(f"Audit dir:     {audit_dir}")
    print("Mode:          intraday preview only; no EOD production files modified")

    banner("Step 1 — intraday implied variance")
    implied_panel, implied_aux = build_intraday_implied_variance(
        project_root=project_root,
        base_url=args.base_url,
        trade_date=trade_date,
        quote_time=quote_time,
        target_tenors=target_tenors,
        max_expiration_days=int(args.max_expiration_days),
        default_risk_free_rate=float(args.default_risk_free_rate),
    )

    print(implied_panel[["tenor", "implied_variance_intraday", "vix_style_vol_intraday"]].to_string(index=False))

    banner("Step 2 — intraday feature vector")
    feature_panel, feature_aux = build_intraday_feature_vector(
        project_root=project_root,
        base_url=args.base_url,
        trade_date=trade_date,
        quote_time=quote_time,
        interval=args.interval,
        validation_lookback_rows=int(args.feature_validation_lookback_rows),
        validation_tolerance=float(args.feature_validation_tolerance),
    )

    live_source = feature_aux["live_source"]
    live_close = float(live_source["live_close"])

    print("Live source:")
    print(json.dumps({k: str(v) for k, v in live_source.items()}, indent=2))
    print(feature_panel[["trade_date", "quote_time", "tenor"] + LOCKED_FEATURES].to_string(index=False))

    banner("Step 3 — forecast / VRP / prior-only z-scores")
    forecast_panel, score_summary, zscore_audit, forecast_aux = score_forecast_and_vrp(
        project_root=project_root,
        feature_panel=feature_panel,
        implied_panel=implied_panel,
        trade_date=trade_date,
    )

    print(forecast_panel[[
        "tenor",
        "vix_style_vol_intraday",
        "candidate_forecast_vol_pct",
        "model_vrp_log_intraday",
        "z_3m_intraday",
        "z_1y_intraday",
    ]].to_string(index=False))

    banner("Step 4 — live RSI14 / RV21D")
    final_signal, final_signal_path = load_final_signal_full(project_root)

    rsi14_live, rv21d_live, live_market_meta, rsi_audit, rv_audit, live_price_tail = compute_live_rsi_rv(
        project_root=project_root,
        trade_date=trade_date,
        live_close=live_close,
        final_signal=final_signal,
        rsi_tolerance=float(args.rsi_validation_tolerance),
        rv_tolerance=float(args.rv21d_validation_tolerance),
    )

    print(json.dumps(live_market_meta, indent=2, default=str))

    banner("Step 5 — apply locked signal logic")
    panel = forecast_panel.copy()
    panel["asof_timestamp_local"] = asof_ts
    panel["final_signal_version"] = "intraday_preview_v1"
    panel["final_signal_decision_id"] = "intraday_preview_v1"
    panel["model_spec"] = "unified_fds_no_min_return"
    panel["model_source"] = "saved_model_artifact"
    panel["live_spy_close"] = live_close
    panel["rsi14_intraday"] = rsi14_live
    panel["rv21d_vol_pct_intraday"] = rv21d_live

    signal_panel, summary = apply_signal_logic(panel)

    print(signal_panel[[
        "tenor",
        "tenor_bucket",
        "model_vrp_log_intraday",
        "z_3m_intraday",
        "z_1y_intraday",
        "rsi14_intraday",
        "rv21d_vol_pct_intraday",
        "core_pass",
        "secondary_pass",
        "selected",
        "selected_layer",
        "selected_size_pct",
    ]].to_string(index=False))

    print("\nSummary:")
    print(summary.to_string(index=False))

    banner("Step 6 — validate and save")
    tenors_ok = sorted(signal_panel["tenor"].astype(int).unique().tolist()) == EXPECTED_TENORS
    rows_ok = len(signal_panel) == len(EXPECTED_TENORS)

    finite_required_cols = [
        "implied_variance_intraday",
        "vix_style_vol_intraday",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
        "model_vrp_log_intraday",
        "z_3m_intraday",
        "z_1y_intraday",
        "rsi14_intraday",
        "rv21d_vol_pct_intraday",
    ]

    finite_ok = bool(np.isfinite(signal_panel[finite_required_cols].to_numpy(dtype=float)).all())

    formula_validation_ok = bool(feature_aux["feature_validation"]["status"].eq("PASS").all())
    rsi_validation_ok = bool(rsi_audit.iloc[0]["status"] == "PASS")
    rv_validation_ok = bool(rv_audit.iloc[0]["status"] == "PASS")

    processed_panel_path = processed_dir / "vrp_intraday_corsi_tenor_panel_v1.parquet"
    processed_snapshot_path = processed_dir / "vrp_intraday_corsi_signal_snapshot_v1.parquet"

    audit_panel_path = audit_dir / f"intraday_signal_preview_panel_{trade_date}_{run_ts}.csv"
    audit_summary_path = audit_dir / f"intraday_signal_preview_summary_{trade_date}_{run_ts}.csv"
    implied_audit_path = audit_dir / f"intraday_signal_preview_implied_panel_{trade_date}_{run_ts}.csv"
    feature_audit_path = audit_dir / f"intraday_signal_preview_feature_panel_{trade_date}_{run_ts}.csv"
    score_summary_path = audit_dir / f"intraday_signal_preview_score_summary_{trade_date}_{run_ts}.csv"
    zscore_audit_path = audit_dir / f"intraday_signal_preview_zscore_audit_{trade_date}_{run_ts}.csv"
    rsi_audit_path = audit_dir / f"intraday_signal_preview_rsi_validation_{trade_date}_{run_ts}.csv"
    rv_audit_path = audit_dir / f"intraday_signal_preview_rv21d_validation_{trade_date}_{run_ts}.csv"
    live_price_tail_path = audit_dir / f"intraday_signal_preview_live_price_tail_{trade_date}_{run_ts}.csv"
    chain_quality_path = audit_dir / f"intraday_signal_preview_chain_quality_{trade_date}_{run_ts}.csv"

    signal_panel.to_parquet(processed_panel_path, index=False)
    summary.to_parquet(processed_snapshot_path, index=False)

    signal_panel.to_csv(audit_panel_path, index=False)
    summary.to_csv(audit_summary_path, index=False)
    implied_panel.to_csv(implied_audit_path, index=False)
    feature_panel.to_csv(feature_audit_path, index=False)
    score_summary.to_csv(score_summary_path, index=False)
    zscore_audit.to_csv(zscore_audit_path, index=False)
    rsi_audit.to_csv(rsi_audit_path, index=False)
    rv_audit.to_csv(rv_audit_path, index=False)
    live_price_tail.to_csv(live_price_tail_path, index=False)
    implied_aux["chain_quality"].to_csv(chain_quality_path, index=False)

    preview_pass = bool(
        tenors_ok
        and rows_ok
        and finite_ok
        and formula_validation_ok
        and rsi_validation_ok
        and rv_validation_ok
    )

    manifest = {
        "run_ts": run_ts,
        "asof_timestamp_local": asof_ts,
        "project_root": str(project_root),
        "trade_date": int(trade_date),
        "quote_time": quote_time,
        "base_url": args.base_url,
        "interval": args.interval,
        "target_tenors": target_tenors,
        "inputs": {
            "final_signal_base_panel": str(final_signal_path),
            "model_artifact": forecast_aux["artifact_path"],
            "model_history": forecast_aux["history_path"],
            "locked_feature_panel": feature_aux["feature_path"],
        },
        "live_market_meta": live_market_meta,
        "selected_formulas": feature_aux["selected_formulas"],
        "forecast_aux": forecast_aux,
        "checks": {
            "tenors_ok": tenors_ok,
            "rows_ok": rows_ok,
            "finite_ok": finite_ok,
            "feature_formula_validation_ok": formula_validation_ok,
            "rsi_validation_ok": rsi_validation_ok,
            "rv21d_validation_ok": rv_validation_ok,
        },
        "processed_outputs": {
            "tenor_panel": str(processed_panel_path),
            "snapshot": str(processed_snapshot_path),
        },
        "audit_outputs": {
            "panel": str(audit_panel_path),
            "summary": str(audit_summary_path),
            "implied_panel": str(implied_audit_path),
            "feature_panel": str(feature_audit_path),
            "score_summary": str(score_summary_path),
            "zscore_audit": str(zscore_audit_path),
            "rsi_validation": str(rsi_audit_path),
            "rv21d_validation": str(rv_audit_path),
            "live_price_tail": str(live_price_tail_path),
            "chain_quality": str(chain_quality_path),
        },
        "INTRADAY_SIGNAL_PREVIEW_PASS": preview_pass,
        "method_note": "Intraday preview only. Writes separate intraday files under data/processed/vrp_intraday_signal and does not modify EOD production history.",
    }

    manifest_path = audit_dir / f"intraday_signal_preview_manifest_{trade_date}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"processed tenor panel: {processed_panel_path}")
    print(f"processed snapshot:    {processed_snapshot_path}")
    print(f"audit panel:           {audit_panel_path}")
    print(f"audit summary:         {audit_summary_path}")
    print(f"manifest:              {manifest_path}")

    banner("Final result")
    print(f"tenors_ok:                  {tenors_ok}")
    print(f"rows_ok:                    {rows_ok}")
    print(f"finite_ok:                  {finite_ok}")
    print(f"feature_formula_validation: {formula_validation_ok}")
    print(f"rsi_validation_ok:          {rsi_validation_ok}")
    print(f"rv21d_validation_ok:        {rv_validation_ok}")
    print(f"INTRADAY_SIGNAL_PREVIEW_PASS: {preview_pass}")

    if not preview_pass:
        raise RuntimeError("INTRADAY_SIGNAL_PREVIEW_PASS is False.")

    print("DONE — intraday signal preview complete.")


if __name__ == "__main__":
    main()
