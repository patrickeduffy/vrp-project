
from __future__ import annotations

import argparse
import io
import json
import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests


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


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def quote_time_to_ms(q: str) -> int:
    q = str(q).strip()
    if "." in q:
        main, frac = q.split(".", 1)
    else:
        main, frac = q, "0"
    hh, mm, ss = [int(x) for x in main.split(":")]
    ms = int((frac + "000")[:3])
    return ((hh * 3600 + mm * 60 + ss) * 1000) + ms


def parse_yyyymmdd(x: str | int) -> pd.Timestamp:
    return pd.to_datetime(str(int(x)), format="%Y%m%d")


def normalize_date_series(s: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().mean() > 0.8 and numeric.dropna().between(19000101, 22000101).mean() > 0.8:
        return pd.to_datetime(numeric.astype("Int64").astype(str), format="%Y%m%d", errors="coerce").dt.strftime("%Y%m%d").astype("Int64")

    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().mean() < 0.8:
        raise RuntimeError("Could not parse date series.")
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def latest_file(base_dirs: list[Path], pattern: str) -> Path:
    paths = []
    for d in base_dirs:
        if d.exists():
            paths.extend(d.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No files found for pattern={pattern}")
    return sorted(paths, key=lambda p: (p.stat().st_mtime, str(p)))[-1]


def parse_theta_payload(resp: requests.Response) -> pd.DataFrame:
    txt = resp.text or ""

    try:
        obj = resp.json()

        if isinstance(obj, list):
            return pd.DataFrame(obj)

        if isinstance(obj, dict):
            list_values = {k: v for k, v in obj.items() if isinstance(v, list)}
            if list_values:
                lengths = [len(v) for v in list_values.values()]
                max_len = max(lengths) if lengths else 0
                if max_len > 0:
                    normalized = {}
                    for k, v in obj.items():
                        if isinstance(v, list):
                            if len(v) == max_len:
                                normalized[k] = v
                            elif len(v) == 1:
                                normalized[k] = v * max_len
                            else:
                                normalized[k] = pd.Series(v)
                        else:
                            normalized[k] = [v] * max_len
                    return pd.DataFrame(normalized)

            for key in ["response", "data", "rows", "results"]:
                if key in obj:
                    val = obj[key]
                    if isinstance(val, list):
                        return pd.DataFrame(val)
                    if isinstance(val, dict):
                        return pd.DataFrame(val)

            return pd.DataFrame([obj])

    except Exception:
        pass

    try:
        return pd.read_csv(io.StringIO(txt))
    except Exception:
        return pd.DataFrame({"raw_text": [txt[:2000]]})


def theta_get_frame(url: str, params: dict, timeout: int = 90, label: str = "") -> tuple[pd.DataFrame, dict]:
    meta = {
        "label": label,
        "url": url,
        "params_json": json.dumps(params, sort_keys=True, default=str),
        "full_url": "",
        "status_code": None,
        "ok": False,
        "rows": 0,
        "cols": 0,
        "columns": "",
        "error": "",
        "text_head": "",
    }

    try:
        r = requests.get(url, params=params, timeout=timeout)
        meta["full_url"] = r.url
        meta["status_code"] = int(r.status_code)
        meta["ok"] = bool(r.ok)
        meta["text_head"] = (r.text or "")[:1000].replace("\n", " ")

        if not r.ok:
            return pd.DataFrame(), meta

        df = parse_theta_payload(r)
        meta["rows"] = int(len(df))
        meta["cols"] = int(len(df.columns))
        meta["columns"] = "|".join(map(str, df.columns))
        return df, meta

    except Exception as exc:
        meta["error"] = repr(exc)
        return pd.DataFrame(), meta


def load_locked_feature_history(project_root: Path) -> tuple[pd.DataFrame, Path, pd.DataFrame]:
    feature_dir = project_root / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1"
    feature_path = latest_file(
        [feature_dir, project_root / "data" / "processed"],
        "04_front_middle_candidate_feature_panel_*.parquet",
    )

    raw = pd.read_parquet(feature_path)

    missing = [c for c in LOCKED_FEATURES if c not in raw.columns]
    if missing:
        raise RuntimeError(f"Locked feature panel missing features: {missing}")

    if "spx_log_return" not in raw.columns:
        raise RuntimeError("Locked feature panel missing spx_log_return.")

    if "spx_close_for_features" not in raw.columns:
        raise RuntimeError("Locked feature panel missing spx_close_for_features.")

    date_col = "date" if "date" in raw.columns else "trade_date"
    tenor_col = "tenor" if "tenor" in raw.columns else "_work_tenor" if "_work_tenor" in raw.columns else None

    if date_col not in raw.columns:
        raise RuntimeError(f"Could not find date column in feature panel. Columns: {list(raw.columns)}")

    df = raw.copy()
    df["feature_trade_date_int"] = normalize_date_series(df[date_col]).astype(int)

    if tenor_col is not None:
        df["_feature_tenor"] = pd.to_numeric(df[tenor_col], errors="coerce")
    else:
        df["_feature_tenor"] = np.nan

    df["spx_log_return"] = pd.to_numeric(df["spx_log_return"], errors="coerce")
    df["spx_close_for_features"] = pd.to_numeric(df["spx_close_for_features"], errors="coerce")

    for c in LOCKED_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    dispersion_rows = []
    for c in ["spx_log_return", "spx_close_for_features"] + LOCKED_FEATURES:
        d = (
            df.groupby("feature_trade_date_int")[c]
            .agg(["count", "min", "max"])
            .reset_index()
        )
        d["field"] = c
        d["max_minus_min"] = d["max"] - d["min"]
        dispersion_rows.append(d[["feature_trade_date_int", "field", "count", "min", "max", "max_minus_min"]])

    dispersion = pd.concat(dispersion_rows, ignore_index=True)

    # Market-level features are repeated across tenor. Use first tenor per date.
    one = (
        df.sort_values(["feature_trade_date_int", "_feature_tenor"])
        .drop_duplicates("feature_trade_date_int", keep="first")
        [["feature_trade_date_int", "spx_close_for_features", "spx_log_return"] + LOCKED_FEATURES]
        .dropna(subset=["feature_trade_date_int", "spx_log_return"])
        .sort_values("feature_trade_date_int")
        .reset_index(drop=True)
    )

    return one, feature_path, dispersion


def add_ohlc_time_columns(df: pd.DataFrame, trade_date_int: int) -> pd.DataFrame:
    out = df.copy()
    if "timestamp" not in out.columns:
        raise RuntimeError(f"OHLC data missing timestamp column. Columns: {list(out.columns)}")

    numeric = pd.to_numeric(out["timestamp"], errors="coerce")

    if numeric.notna().mean() > 0.9:
        max_abs = float(numeric.dropna().abs().max())

        if max_abs < 24 * 3600 * 1000 * 10:
            out["timestamp_ms_of_day"] = numeric.astype(float)
            out["timestamp_parsed"] = parse_yyyymmdd(trade_date_int) + pd.to_timedelta(out["timestamp_ms_of_day"], unit="ms")
            return out

        if max_abs > 1e17:
            parsed = pd.to_datetime(numeric, unit="ns", utc=True, errors="coerce").dt.tz_convert("America/New_York")
        elif max_abs > 1e14:
            parsed = pd.to_datetime(numeric, unit="us", utc=True, errors="coerce").dt.tz_convert("America/New_York")
        elif max_abs > 1e11:
            parsed = pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce").dt.tz_convert("America/New_York")
        else:
            parsed = pd.to_datetime(numeric, unit="s", utc=True, errors="coerce").dt.tz_convert("America/New_York")

        out["timestamp_parsed"] = parsed
        out["timestamp_ms_of_day"] = (
            parsed.dt.hour * 3600000
            + parsed.dt.minute * 60000
            + parsed.dt.second * 1000
            + parsed.dt.microsecond // 1000
        )
        return out

    parsed = pd.to_datetime(out["timestamp"], errors="coerce")
    if parsed.notna().mean() < 0.8:
        raise RuntimeError("Could not parse OHLC timestamp column.")

    out["timestamp_parsed"] = parsed
    out["timestamp_ms_of_day"] = (
        parsed.dt.hour * 3600000
        + parsed.dt.minute * 60000
        + parsed.dt.second * 1000
        + parsed.dt.microsecond // 1000
    )
    return out


def fetch_intraday_ohlc_candidates(
    *,
    base_url: str,
    trade_date_int: int,
    quote_time: str,
    interval: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quote_ms = quote_time_to_ms(quote_time)
    base = base_url.rstrip("/")

    attempts = [
        {
            "label": "SPX_index_history_ohlc_symbol",
            "underlying": "SPX",
            "source_type": "index",
            "url": base + "/index/history/ohlc",
            "params": {"symbol": "SPX", "start_date": str(trade_date_int), "end_date": str(trade_date_int), "interval": interval, "format": "json"},
        },
        {
            "label": "SPX_index_history_ohlc_root",
            "underlying": "SPX",
            "source_type": "index",
            "url": base + "/index/history/ohlc",
            "params": {"root": "SPX", "start_date": str(trade_date_int), "end_date": str(trade_date_int), "interval": interval, "format": "json"},
        },
        {
            "label": "SPX_stock_history_ohlc_symbol",
            "underlying": "SPX",
            "source_type": "stock",
            "url": base + "/stock/history/ohlc",
            "params": {"symbol": "SPX", "start_date": str(trade_date_int), "end_date": str(trade_date_int), "interval": interval, "format": "json"},
        },
        {
            "label": "SPY_stock_history_ohlc_symbol",
            "underlying": "SPY",
            "source_type": "stock",
            "url": base + "/stock/history/ohlc",
            "params": {"symbol": "SPY", "start_date": str(trade_date_int), "end_date": str(trade_date_int), "interval": interval, "format": "json"},
        },
    ]

    candidate_rows = []
    request_rows = []
    samples = []

    for a in attempts:
        df, meta = theta_get_frame(a["url"], a["params"], timeout=90, label=a["label"])
        request_rows.append(meta)

        if df.empty or not meta.get("ok"):
            continue

        if "close" not in df.columns or "timestamp" not in df.columns:
            continue

        work = df.copy()
        for c in ["open", "high", "low", "close", "volume", "vwap", "count"]:
            if c in work.columns:
                work[c] = pd.to_numeric(work[c], errors="coerce")

        try:
            work = add_ohlc_time_columns(work, trade_date_int)
        except Exception:
            continue

        usable = (
            work.dropna(subset=["close", "timestamp_ms_of_day"])
            .loc[lambda x: x["timestamp_ms_of_day"] <= quote_ms]
            .sort_values("timestamp_ms_of_day")
            .reset_index(drop=True)
        )

        if usable.empty:
            continue

        row = usable.iloc[-1]
        candidate_rows.append({
            "label": a["label"],
            "underlying": a["underlying"],
            "source_type": a["source_type"],
            "rows_returned": int(len(work)),
            "usable_rows_at_or_before_quote": int(len(usable)),
            "selected_bar_ms_of_day": float(row["timestamp_ms_of_day"]),
            "selected_bar_timestamp": str(row.get("timestamp_parsed", "")),
            "live_close": float(row["close"]),
            "status_code": meta.get("status_code"),
            "full_url": meta.get("full_url", ""),
        })

        sample = work.tail(120).copy()
        sample["probe_label"] = a["label"]
        samples.append(sample)

    candidates = pd.DataFrame(candidate_rows)
    requests_df = pd.DataFrame(request_rows)
    samples_df = pd.concat(samples, ignore_index=True) if samples else pd.DataFrame()

    return candidates, requests_df, samples_df


def select_live_source(candidates: pd.DataFrame, prior_feature_close: float) -> dict:
    if candidates.empty:
        raise RuntimeError("No live OHLC candidates found.")

    out = candidates.copy()
    out["scale_ratio_to_prior_feature_close"] = out["live_close"] / float(prior_feature_close)
    out["scale_compatible"] = out["scale_ratio_to_prior_feature_close"].between(0.5, 2.0)

    compatible = out[out["scale_compatible"]].copy()

    if compatible.empty:
        print("Live candidates found, but none are scale-compatible with spx_close_for_features:")
        print(out.to_string(index=False))
        raise RuntimeError("No scale-compatible live source. Refusing to compute return on mismatched price scales.")

    # Probe SPX first, but do not use SPX if feature history is clearly SPY-scaled.
    compatible["_priority"] = np.where(compatible["underlying"].eq("SPX"), 0, 1)
    selected = compatible.sort_values(["_priority", "label"]).iloc[0].to_dict()
    selected["selection_reason"] = "first scale-compatible source, preferring SPX over SPY"

    return selected


def compute_feature_candidates(return_df: pd.DataFrame) -> pd.DataFrame:
    df = return_df[["feature_trade_date_int", "spx_log_return"]].copy()
    df = df.sort_values("feature_trade_date_int").reset_index(drop=True)

    r = pd.to_numeric(df["spx_log_return"], errors="coerce")
    neg = r.clip(upper=0.0)
    neg_sq = neg ** 2
    total_sq = r ** 2
    abs_ret = r.abs()

    out = df[["feature_trade_date_int"]].copy()

    with np.errstate(divide="ignore", invalid="ignore"):
        for w in [5, 10, 21, 63]:
            neg_mean = neg_sq.rolling(w, min_periods=w).mean()
            neg_sum = neg_sq.rolling(w, min_periods=w).sum()

            out[f"candidate_log_downside_rv_{w}d__ann_mean"] = np.log(neg_mean * 252.0)
            out[f"candidate_log_downside_rv_{w}d__ann_sum_div_w"] = np.log((neg_sum / w) * 252.0)
            out[f"candidate_log_downside_rv_{w}d__ann_sum"] = np.log(neg_sum * 252.0)
            out[f"candidate_log_downside_rv_{w}d__unann_mean"] = np.log(neg_mean)
            out[f"candidate_log_downside_rv_{w}d__unann_sum"] = np.log(neg_sum)

        for w in [5, 10]:
            neg_sum = neg_sq.rolling(w, min_periods=w).sum()
            total_sum = total_sq.rolling(w, min_periods=w).sum()
            abs_neg_sum = neg.abs().rolling(w, min_periods=w).sum()
            abs_sum = abs_ret.rolling(w, min_periods=w).sum()
            neg_count = r.lt(0).astype(float).rolling(w, min_periods=w).mean()

            out[f"candidate_downside_share_{w}d__variance_share"] = neg_sum / total_sum
            out[f"candidate_downside_share_{w}d__abs_return_share"] = abs_neg_sum / abs_sum
            out[f"candidate_downside_share_{w}d__count_share"] = neg_count

        for w in [3, 5, 10]:
            max_abs = abs_ret.rolling(w, min_periods=w).max()
            out[f"candidate_max_abs_return_{w}d__decimal"] = max_abs
            out[f"candidate_max_abs_return_{w}d__pct"] = max_abs * 100.0

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def validate_and_select_formulas(
    *,
    feature_history: pd.DataFrame,
    trade_date_int: int,
    lookback_rows: int,
    tolerance: float,
) -> tuple[dict[str, str], pd.DataFrame]:
    actual = feature_history[["feature_trade_date_int", "spx_log_return"] + LOCKED_FEATURES].copy()
    candidates = compute_feature_candidates(actual[["feature_trade_date_int", "spx_log_return"]])

    joined = actual.merge(candidates, on="feature_trade_date_int", how="inner")
    joined = joined[joined["feature_trade_date_int"] < int(trade_date_int)].copy()
    joined = joined.sort_values("feature_trade_date_int").tail(int(lookback_rows)).reset_index(drop=True)

    if joined.empty:
        raise RuntimeError("No validation overlap for locked feature formula reconstruction.")

    validation_rows = []
    selected = {}

    for f in LOCKED_FEATURES:
        candidate_cols = [c for c in joined.columns if c.startswith(f + "__")]
        if not candidate_cols:
            raise RuntimeError(f"No candidate formulas generated for {f}")

        best = None
        actual_s = pd.to_numeric(joined[f], errors="coerce")

        for c in candidate_cols:
            pred_s = pd.to_numeric(joined[c], errors="coerce")
            mask = actual_s.notna() & pred_s.notna()

            if mask.sum() == 0:
                max_abs = np.inf
                mean_abs = np.inf
            else:
                diff = pred_s[mask] - actual_s[mask]
                max_abs = float(diff.abs().max())
                mean_abs = float(diff.abs().mean())

            row = {
                "feature": f,
                "candidate_column": c,
                "candidate_formula": c.split("__", 1)[1],
                "validation_rows": int(mask.sum()),
                "first_validation_date": int(joined.loc[mask, "feature_trade_date_int"].min()) if mask.sum() else None,
                "last_validation_date": int(joined.loc[mask, "feature_trade_date_int"].max()) if mask.sum() else None,
                "max_abs_diff": max_abs,
                "mean_abs_diff": mean_abs,
            }
            validation_rows.append(row)

            if best is None or (max_abs, mean_abs) < (best["max_abs_diff"], best["mean_abs_diff"]):
                best = row

        selected[f] = best["candidate_column"]

    validation = pd.DataFrame(validation_rows)
    best_validation = (
        validation.sort_values(["feature", "max_abs_diff", "mean_abs_diff"])
        .groupby("feature", as_index=False)
        .first()
    )
    best_validation["status"] = np.where(best_validation["max_abs_diff"] <= tolerance, "PASS", "FAIL")

    failed = best_validation[best_validation["status"].eq("FAIL")].copy()
    if not failed.empty:
        print("\nBest validation candidates failed tolerance:")
        print(failed.to_string(index=False))
        raise RuntimeError(
            f"Locked feature formula validation failed. "
            f"Tolerance={tolerance}; failed_features={failed['feature'].tolist()}"
        )

    return selected, best_validation


def build_live_feature_panel(
    *,
    feature_history: pd.DataFrame,
    selected_formulas: dict[str, str],
    trade_date_int: int,
    quote_time: str,
    quote_ms: int,
    live_source: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    prior = feature_history[feature_history["feature_trade_date_int"] < int(trade_date_int)].copy()
    prior = prior.sort_values("feature_trade_date_int").reset_index(drop=True)

    if prior.empty:
        raise RuntimeError(f"No prior feature history before trade_date={trade_date_int}")

    prior_row = prior.iloc[-1]
    prior_feature_close = float(prior_row["spx_close_for_features"])
    live_close = float(live_source["live_close"])
    live_log_return = float(math.log(live_close / prior_feature_close))

    live_return_row = {
        "feature_trade_date_int": int(trade_date_int),
        "spx_close_for_features": live_close,
        "spx_log_return": live_log_return,
    }

    return_history_live = pd.concat(
        [
            prior[["feature_trade_date_int", "spx_close_for_features", "spx_log_return"]],
            pd.DataFrame([live_return_row]),
        ],
        ignore_index=True,
    )

    candidates_live = compute_feature_candidates(
        return_history_live[["feature_trade_date_int", "spx_log_return"]]
    )

    live_candidate_row = candidates_live[candidates_live["feature_trade_date_int"].eq(int(trade_date_int))].copy()
    if live_candidate_row.empty:
        raise RuntimeError("No live feature candidate row generated.")

    live_feature_values = {}
    for f, col in selected_formulas.items():
        if col not in live_candidate_row.columns:
            raise RuntimeError(f"Selected formula column missing from live row: {col}")
        live_feature_values[f] = float(live_candidate_row.iloc[0][col])

    vector = {
        "asof_timestamp_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trade_date": int(trade_date_int),
        "quote_time": quote_time,
        "quote_ms": int(quote_ms),
        "prior_feature_trade_date": int(prior_row["feature_trade_date_int"]),
        "prior_spx_close_for_features": prior_feature_close,
        "live_source_label": live_source["label"],
        "live_source_underlying": live_source["underlying"],
        "live_source_type": live_source["source_type"],
        "live_close_for_features": live_close,
        "live_log_return": live_log_return,
        "selected_bar_ms_of_day": float(live_source["selected_bar_ms_of_day"]),
        "selected_bar_timestamp": str(live_source.get("selected_bar_timestamp", "")),
        "source_selection_reason": live_source.get("selection_reason", ""),
        **live_feature_values,
    }

    panel = pd.concat(
        [
            pd.DataFrame([{**vector, "tenor": int(t), "_work_tenor": int(t), "_work_trade_date": int(trade_date_int)}])
            for t in EXPECTED_TENORS
        ],
        ignore_index=True,
    )

    meta = {
        "prior_feature_trade_date": int(prior_row["feature_trade_date_int"]),
        "prior_spx_close_for_features": prior_feature_close,
        "live_close_for_features": live_close,
        "live_log_return": live_log_return,
        "live_source": {k: str(v) for k, v in live_source.items()},
    }

    return panel, return_history_live, meta


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--base-url", default="http://127.0.0.1:25503/v3")
    p.add_argument("--trade-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--quote-time", required=True)
    p.add_argument("--interval", default="1m")
    p.add_argument("--feature-validation-lookback-rows", type=int, default=252)
    p.add_argument("--feature-validation-tolerance", type=float, default=1e-6)
    args = p.parse_args()

    project_root = Path(args.project_root)
    run_ts = now_stamp()
    trade_date_int = int(pd.to_datetime(str(args.trade_date), format="%Y%m%d").strftime("%Y%m%d"))
    quote_time = str(args.quote_time)
    quote_ms = quote_time_to_ms(quote_time)

    audit_dir = project_root / "data" / "audit" / "intraday_feature_vector"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday feature vector smoke v1")
    print(f"Project root:   {project_root}")
    print(f"Run timestamp:  {run_ts}")
    print(f"Base URL:       {args.base_url}")
    print(f"Trade date:     {trade_date_int}")
    print(f"Quote time:     {quote_time}")
    print(f"Quote ms:       {quote_ms}")
    print(f"Interval:       {args.interval}")
    print(f"Audit dir:      {audit_dir}")
    print("Mode:           audit-only feature-vector smoke; no production signal files modified")

    banner("Load locked Cell 4 feature history")
    feature_history, feature_path, feature_dispersion = load_locked_feature_history(project_root)
    print(f"Feature panel:       {feature_path}")
    print(f"Feature date range:  {int(feature_history['feature_trade_date_int'].min())} to {int(feature_history['feature_trade_date_int'].max())}")
    print(f"Feature rows/dates:  {len(feature_history):,}")

    prior = feature_history[feature_history["feature_trade_date_int"] < trade_date_int].copy()
    if prior.empty:
        raise RuntimeError(f"No prior feature rows before {trade_date_int}")

    prior_feature_close = float(prior.sort_values("feature_trade_date_int").iloc[-1]["spx_close_for_features"])
    print(f"Prior feature close: {prior_feature_close:.6f}")

    banner("Probe live SPX first, then SPY fallback")
    live_candidates, request_meta, ohlc_samples = fetch_intraday_ohlc_candidates(
        base_url=args.base_url,
        trade_date_int=trade_date_int,
        quote_time=quote_time,
        interval=args.interval,
    )

    if live_candidates.empty:
        print(request_meta.to_string(index=False))
        raise RuntimeError("No live OHLC candidates returned.")

    live_candidates["scale_ratio_to_prior_feature_close"] = live_candidates["live_close"] / prior_feature_close
    live_candidates["scale_compatible"] = live_candidates["scale_ratio_to_prior_feature_close"].between(0.5, 2.0)

    print(live_candidates[[
        "label",
        "underlying",
        "source_type",
        "rows_returned",
        "usable_rows_at_or_before_quote",
        "selected_bar_ms_of_day",
        "live_close",
        "scale_ratio_to_prior_feature_close",
        "scale_compatible",
    ]].to_string(index=False))

    live_source = select_live_source(live_candidates, prior_feature_close)
    print("\nSelected live source:")
    print(json.dumps({k: str(v) for k, v in live_source.items()}, indent=2))

    banner("Validate locked feature formulas using feature-panel spx_log_return")
    selected_formulas, feature_validation = validate_and_select_formulas(
        feature_history=feature_history,
        trade_date_int=trade_date_int,
        lookback_rows=int(args.feature_validation_lookback_rows),
        tolerance=float(args.feature_validation_tolerance),
    )

    print("Selected formulas:")
    for f, c in selected_formulas.items():
        print(f"  {f}: {c.split('__', 1)[1]}")

    print("\nValidation summary:")
    print(feature_validation.to_string(index=False))

    banner("Compute live locked feature vector")
    live_feature_panel, return_history_live, live_meta = build_live_feature_panel(
        feature_history=feature_history,
        selected_formulas=selected_formulas,
        trade_date_int=trade_date_int,
        quote_time=quote_time,
        quote_ms=quote_ms,
        live_source=live_source,
    )

    print(json.dumps(live_meta, indent=2, default=str))

    print(live_feature_panel[["trade_date", "quote_time", "tenor"] + LOCKED_FEATURES].to_string(index=False))

    finite_features = bool(np.isfinite(live_feature_panel[LOCKED_FEATURES].to_numpy(dtype=float)).all())
    tenors_ok = sorted(live_feature_panel["tenor"].astype(int).unique().tolist()) == EXPECTED_TENORS
    rows_ok = len(live_feature_panel) == len(EXPECTED_TENORS)
    formula_validation_pass = bool(feature_validation["status"].eq("PASS").all())

    banner("Save audit outputs")
    panel_path = audit_dir / f"intraday_feature_vector_smoke_panel_{trade_date_int}_{run_ts}.csv"
    return_tail_path = audit_dir / f"intraday_feature_vector_smoke_return_tail_{trade_date_int}_{run_ts}.csv"
    formula_validation_path = audit_dir / f"intraday_feature_vector_smoke_formula_validation_{trade_date_int}_{run_ts}.csv"
    feature_dispersion_path = audit_dir / f"intraday_feature_vector_smoke_feature_dispersion_{trade_date_int}_{run_ts}.csv"
    live_candidates_path = audit_dir / f"intraday_feature_vector_smoke_live_source_candidates_{trade_date_int}_{run_ts}.csv"
    ohlc_samples_path = audit_dir / f"intraday_feature_vector_smoke_ohlc_samples_{trade_date_int}_{run_ts}.csv"
    request_meta_path = audit_dir / f"intraday_feature_vector_smoke_request_meta_{trade_date_int}_{run_ts}.csv"

    live_feature_panel.to_csv(panel_path, index=False)
    return_history_live.tail(90).to_csv(return_tail_path, index=False)
    feature_validation.to_csv(formula_validation_path, index=False)
    feature_dispersion.to_csv(feature_dispersion_path, index=False)
    live_candidates.to_csv(live_candidates_path, index=False)
    ohlc_samples.to_csv(ohlc_samples_path, index=False)
    request_meta.to_csv(request_meta_path, index=False)

    smoke_pass = bool(formula_validation_pass and finite_features and tenors_ok and rows_ok)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "trade_date": int(trade_date_int),
        "quote_time": quote_time,
        "quote_ms": int(quote_ms),
        "base_url": args.base_url,
        "interval": args.interval,
        "inputs": {
            "locked_feature_panel": str(feature_path),
        },
        "selected_formulas": {k: v.split("__", 1)[1] for k, v in selected_formulas.items()},
        "live_meta": live_meta,
        "checks": {
            "formula_validation_pass": formula_validation_pass,
            "finite_features": finite_features,
            "tenors_ok": tenors_ok,
            "rows_ok": rows_ok,
        },
        "audit_outputs": {
            "feature_panel": str(panel_path),
            "return_tail": str(return_tail_path),
            "formula_validation": str(formula_validation_path),
            "feature_dispersion": str(feature_dispersion_path),
            "live_source_candidates": str(live_candidates_path),
            "ohlc_samples": str(ohlc_samples_path),
            "request_meta": str(request_meta_path),
        },
        "INTRADAY_FEATURE_VECTOR_SMOKE_PASS": smoke_pass,
        "method_note": "Audit-only intraday feature-vector smoke. Uses locked Cell 4 spx_log_return history, appends live scale-compatible return through quote time, and repeats market-level features across tenors.",
    }

    manifest_path = audit_dir / f"intraday_feature_vector_smoke_manifest_{trade_date_int}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"feature_panel:          {panel_path}")
    print(f"return_tail:            {return_tail_path}")
    print(f"formula_validation:     {formula_validation_path}")
    print(f"feature_dispersion:     {feature_dispersion_path}")
    print(f"live_source_candidates: {live_candidates_path}")
    print(f"ohlc_samples:           {ohlc_samples_path}")
    print(f"request_meta:           {request_meta_path}")
    print(f"manifest:               {manifest_path}")

    banner("Final result")
    print(f"formula_validation_pass:          {formula_validation_pass}")
    print(f"finite_features:                  {finite_features}")
    print(f"tenors_ok:                        {tenors_ok}")
    print(f"rows_ok:                          {rows_ok}")
    print(f"INTRADAY_FEATURE_VECTOR_SMOKE_PASS: {smoke_pass}")

    if not smoke_pass:
        raise RuntimeError("INTRADAY_FEATURE_VECTOR_SMOKE_PASS is False.")

    print("DONE — intraday feature-vector smoke complete.")


if __name__ == "__main__":
    main()
