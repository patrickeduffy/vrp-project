
from __future__ import annotations

import argparse
import io
import json
import math
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


MINUTES_PER_DAY = 24 * 60
MINUTES_PER_YEAR = 365 * 24 * 60
DEFAULT_TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def default_quote_time() -> str:
    now = datetime.now().replace(second=0, microsecond=0)
    return now.strftime("%H:%M:%S.000")


def parse_target_tenors(raw: str) -> list[int]:
    return sorted(int(x.strip()) for x in str(raw).split(",") if x.strip())


def yyyymmdd_to_dash(x: int | str) -> str:
    return pd.to_datetime(str(int(x)), format="%Y%m%d").strftime("%Y-%m-%d")


def yyyymmdd_to_date(x: int | str):
    return pd.to_datetime(str(int(x)), format="%Y%m%d").date()


def date_to_yyyymmdd(dt) -> int:
    return int(pd.Timestamp(dt).strftime("%Y%m%d"))


def quote_time_to_ms(q: str) -> int:
    q = str(q).strip()
    if "." in q:
        main, frac = q.split(".", 1)
    else:
        main, frac = q, "0"

    hh, mm, ss = [int(x) for x in main.split(":")]
    ms = int((frac + "000")[:3])
    return ((hh * 60 * 60 + mm * 60 + ss) * 1000) + ms


def is_third_friday(dt) -> bool:
    return dt.weekday() == 4 and 15 <= dt.day <= 21


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
                        return parse_dict_payload(val)

            return pd.DataFrame([obj])

    except Exception:
        pass

    try:
        return pd.read_csv(io.StringIO(txt))
    except Exception:
        return pd.DataFrame({"raw_text": [txt[:2000]]})


def parse_dict_payload(obj: dict) -> pd.DataFrame:
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

    return pd.DataFrame([obj])


def theta_get_frame(url: str, params: dict, timeout: int = 180, label: str = "") -> tuple[pd.DataFrame, dict]:
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


def list_expirations(base_url: str, symbol: str) -> tuple[list[int], dict]:
    url = base_url.rstrip("/") + "/option/list/expirations"
    params = {"symbol": symbol}

    df, meta = theta_get_frame(url, params, timeout=60, label=f"list_expirations_{symbol}")

    if df.empty or "expiration" not in df.columns:
        return [], meta

    exps = (
        pd.to_datetime(df["expiration"], errors="coerce")
        .dropna()
        .dt.strftime("%Y%m%d")
        .astype(int)
        .tolist()
    )

    return sorted(set(exps)), meta


def get_trading_dates(project_root: Path, start: str = "20100101", end: str = "20351231") -> list[int]:
    candidates = [
        project_root / "data" / "external" / "spx_trading_dates.csv",
        project_root / "data" / "processed" / "spx_trading_dates.csv",
    ]

    for p in candidates:
        if p.exists():
            try:
                df = pd.read_csv(p)
                col = "trade_date" if "trade_date" in df.columns else df.columns[0]
                vals = pd.to_numeric(df[col], errors="coerce").dropna().astype(int).tolist()
                vals = [x for x in vals if int(start) <= int(x) <= int(end)]
                if vals:
                    return sorted(set(vals))
            except Exception:
                pass

    try:
        import pandas_market_calendars as mcal

        cal = mcal.get_calendar("XNYS")
        sched = cal.schedule(start_date=pd.to_datetime(start).date(), end_date=pd.to_datetime(end).date())
        return sorted(int(pd.Timestamp(x).strftime("%Y%m%d")) for x in pd.to_datetime(sched.index))
    except Exception:
        days = pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="B")
        return sorted(int(x.strftime("%Y%m%d")) for x in days)


def next_calendar_friday_after_date(dt):
    days_until_friday = (4 - dt.weekday()) % 7
    return dt + timedelta(days=days_until_friday)


def is_last_trading_day_before_closed_friday(exp_yyyymmdd: int, trading_dates: list[int]) -> bool:
    exp_dt = yyyymmdd_to_date(exp_yyyymmdd)
    friday = next_calendar_friday_after_date(exp_dt)

    if friday.weekday() != 4:
        return False

    friday_int = date_to_yyyymmdd(friday)
    trading_set = set(int(x) for x in trading_dates)

    if friday_int in trading_set:
        return False

    prior = [d for d in trading_set if d < friday_int]
    if not prior:
        return False

    return int(exp_yyyymmdd) == max(prior)


def is_friday_cycle_expiration(exp_yyyymmdd: int, trading_dates: list[int]) -> bool:
    exp_dt = yyyymmdd_to_date(exp_yyyymmdd)
    exp_int = int(exp_yyyymmdd)
    trading_set = set(int(x) for x in trading_dates)

    if exp_dt.weekday() == 4 and exp_int in trading_set:
        return True

    return is_last_trading_day_before_closed_friday(exp_int, trading_dates)


def is_holiday_adjusted_monthly_expiration(exp_yyyymmdd: int, trading_dates: list[int]) -> bool:
    exp_dt = yyyymmdd_to_date(exp_yyyymmdd)

    if exp_dt.weekday() == 4:
        return False

    next_friday = next_calendar_friday_after_date(exp_dt)
    return is_third_friday(next_friday) and is_last_trading_day_before_closed_friday(exp_yyyymmdd, trading_dates)


def preferred_root_for_expiration(exp_yyyymmdd: int, spx_exps: set[int], spxw_exps: set[int], trading_dates: list[int]) -> str:
    exp_dt = yyyymmdd_to_date(exp_yyyymmdd)

    if is_third_friday(exp_dt) and int(exp_yyyymmdd) in spx_exps:
        return "SPX"

    if is_holiday_adjusted_monthly_expiration(exp_yyyymmdd, trading_dates) and int(exp_yyyymmdd) in spx_exps:
        return "SPX"

    if int(exp_yyyymmdd) in spxw_exps:
        return "SPXW"

    if int(exp_yyyymmdd) in spx_exps:
        return "SPX"

    raise ValueError(f"Expiration {exp_yyyymmdd} not found in SPX or SPXW lists")


def settlement_minutes_after_midnight_et(root: str) -> int:
    if root == "SPX":
        return 9 * 60 + 30
    if root == "SPXW":
        return 16 * 60
    raise ValueError(f"Unknown root: {root}")


def minutes_to_expiry_vix_method(trade_date: int, exp_yyyymmdd: int, root: str, calc_time_ms: int) -> int:
    trade_dt = yyyymmdd_to_date(trade_date)
    exp_dt = yyyymmdd_to_date(exp_yyyymmdd)

    calc_minutes_after_midnight = int(calc_time_ms // 60000)
    settlement_minutes = settlement_minutes_after_midnight_et(root)
    days_diff = (exp_dt - trade_dt).days

    return days_diff * MINUTES_PER_DAY + settlement_minutes - calc_minutes_after_midnight


def expiration_candidates(
    *,
    trade_date: int,
    calc_time_ms: int,
    spx_exps: set[int],
    spxw_exps: set[int],
    trading_dates: list[int],
    max_days: int = 90,
) -> pd.DataFrame:
    rows = []

    for exp in sorted(spx_exps | spxw_exps):
        if exp <= int(trade_date):
            continue

        if not is_friday_cycle_expiration(exp, trading_dates):
            continue

        root = preferred_root_for_expiration(exp, spx_exps, spxw_exps, trading_dates)
        minutes = minutes_to_expiry_vix_method(
            trade_date=trade_date,
            exp_yyyymmdd=exp,
            root=root,
            calc_time_ms=calc_time_ms,
        )

        if minutes <= 0:
            continue

        days = minutes / MINUTES_PER_DAY
        if days > max_days:
            continue

        rows.append({
            "root": root,
            "expiration": int(exp),
            "minutes": int(minutes),
            "days": float(days),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No usable Friday-cycle expiration candidates found.")

    return out.sort_values("minutes").reset_index(drop=True)


def required_chains_for_target_tenors(candidates: pd.DataFrame, target_tenors: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []

    for target_days in target_tenors:
        target_minutes = target_days * MINUTES_PER_DAY

        before = candidates[candidates["minutes"] <= target_minutes]
        after = candidates[candidates["minutes"] >= target_minutes]

        if before.empty:
            raise RuntimeError(f"No expiration before target {target_days}d.")
        if after.empty:
            raise RuntimeError(f"No expiration after target {target_days}d.")

        near_idx = before.index[-1]
        next_idx = after.index[0]

        if near_idx == next_idx:
            if next_idx + 1 < len(candidates):
                next_idx += 1
            elif near_idx - 1 >= 0:
                near_idx -= 1
            else:
                raise RuntimeError(f"Could not form expiration pair for {target_days}d.")

        for leg, idx in [("near", near_idx), ("next", next_idx)]:
            r = candidates.loc[idx]
            rows.append({
                "target_days": int(target_days),
                "leg": leg,
                "root": r["root"],
                "expiration": int(r["expiration"]),
                "minutes": int(r["minutes"]),
                "days": float(r["days"]),
            })

    required = pd.DataFrame(rows)

    unique = (
        required[["root", "expiration", "minutes", "days"]]
        .drop_duplicates()
        .sort_values("minutes")
        .reset_index(drop=True)
    )

    return required, unique


def get_chain_at_time(
    *,
    base_url: str,
    root: str,
    expiration: int,
    trade_date: int,
    quote_time: str,
) -> tuple[pd.DataFrame, dict]:
    url = base_url.rstrip("/") + "/option/history/quote"
    params = {
        "symbol": root,
        "expiration": yyyymmdd_to_dash(expiration),
        "strike": "*",
        "right": "both",
        "start_date": yyyymmdd_to_dash(trade_date),
        "end_date": yyyymmdd_to_dash(trade_date),
        "start_time": quote_time,
        "end_time": quote_time,
        "interval": "1m",
        "format": "json",
    }

    raw, meta = theta_get_frame(
        url,
        params,
        timeout=180,
        label=f"option_history_quote_{root}_{expiration}",
    )

    if raw.empty:
        return raw, meta

    df = raw.copy()

    if "symbol" in df.columns:
        df["root"] = df["symbol"]
    else:
        df["root"] = root

    df["expiration"] = int(expiration)

    right_map = {
        "CALL": "C",
        "PUT": "P",
        "C": "C",
        "P": "P",
    }

    df["right"] = df["right"].astype(str).str.upper().map(right_map)

    for c in ["bid", "ask", "strike"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["mid"] = (df["bid"] + df["ask"]) / 2.0

    keep = [
        "root",
        "expiration",
        "strike",
        "right",
        "bid",
        "ask",
        "mid",
        "bid_size",
        "ask_size",
        "bid_exchange",
        "ask_exchange",
        "bid_condition",
        "ask_condition",
        "timestamp",
    ]

    keep = [c for c in keep if c in df.columns]
    return df[keep].copy(), meta


def chain_quality_row(chain: pd.DataFrame, meta: dict, root: str, expiration: int) -> dict:
    out = {
        "root": root,
        "expiration": int(expiration),
        "http_status": meta.get("status_code"),
        "request_ok": bool(meta.get("ok")),
        "rows": int(len(chain)),
        "columns": "|".join(map(str, chain.columns)) if not chain.empty else "",
        "call_rows": 0,
        "put_rows": 0,
        "unique_strikes": 0,
        "positive_bid_rows": 0,
        "finite_mid_rows": 0,
        "min_strike": np.nan,
        "max_strike": np.nan,
        "error": meta.get("error", ""),
        "text_head": meta.get("text_head", ""),
    }

    if chain.empty:
        return out

    out["call_rows"] = int(chain["right"].eq("C").sum())
    out["put_rows"] = int(chain["right"].eq("P").sum())
    out["unique_strikes"] = int(chain["strike"].nunique())
    out["positive_bid_rows"] = int((chain["bid"] > 0).sum())
    out["finite_mid_rows"] = int(np.isfinite(chain["mid"]).sum())
    out["min_strike"] = float(chain["strike"].min())
    out["max_strike"] = float(chain["strike"].max())

    return out


def _prepare_call_put_tables(chain: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = chain.copy()

    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
    df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
    df["mid"] = pd.to_numeric(df["mid"], errors="coerce")

    df = df.dropna(subset=["strike", "bid", "ask", "mid", "right"])
    df = df[df["ask"] >= 0]
    df = df[df["bid"] >= 0]

    calls = (
        df[df["right"] == "C"]
        .sort_values("strike")
        .drop_duplicates(subset=["strike"], keep="last")
        .set_index("strike")
    )

    puts = (
        df[df["right"] == "P"]
        .sort_values("strike")
        .drop_duplicates(subset=["strike"], keep="last")
        .set_index("strike")
    )

    return calls, puts


def _select_otm_options_with_bid_rule(options_df: pd.DataFrame, ascending: bool) -> pd.DataFrame:
    options_df = options_df.sort_values("strike", ascending=ascending)

    selected_rows = []
    consecutive_zero_bids = 0

    for _, row in options_df.iterrows():
        if row["bid"] <= 0:
            consecutive_zero_bids += 1
            if consecutive_zero_bids >= 2:
                break
            continue

        consecutive_zero_bids = 0
        selected_rows.append(row)

    if len(selected_rows) == 0:
        return pd.DataFrame(columns=options_df.columns)

    return pd.DataFrame(selected_rows)


def calc_single_term_variance(chain: pd.DataFrame, minutes_to_expiry: int, r: float) -> dict:
    T = minutes_to_expiry / MINUTES_PER_YEAR

    if T <= 0:
        raise RuntimeError("Non-positive time to expiry.")

    calls, puts = _prepare_call_put_tables(chain)

    common_strikes = sorted(set(calls.index).intersection(set(puts.index)))
    if len(common_strikes) == 0:
        raise RuntimeError("No common call/put strikes found.")

    parity_rows = []
    for K in common_strikes:
        call_mid = float(calls.loc[K, "mid"])
        put_mid = float(puts.loc[K, "mid"])
        parity_rows.append({
            "strike": float(K),
            "call_mid": call_mid,
            "put_mid": put_mid,
            "abs_call_put_diff": abs(call_mid - put_mid),
        })

    parity_df = pd.DataFrame(parity_rows)
    min_row = parity_df.loc[parity_df["abs_call_put_diff"].idxmin()]

    K_star = float(min_row["strike"])
    call_mid_star = float(min_row["call_mid"])
    put_mid_star = float(min_row["put_mid"])

    F = K_star + math.exp(r * T) * (call_mid_star - put_mid_star)

    all_strikes = sorted(set(calls.index).union(set(puts.index)))
    strikes_below_or_equal_forward = [K for K in all_strikes if K <= F]
    if len(strikes_below_or_equal_forward) == 0:
        raise RuntimeError("Could not find K0 below forward.")

    K0 = float(max(strikes_below_or_equal_forward))

    put_rows = []
    for K in sorted([x for x in puts.index if x < K0], reverse=True):
        row = puts.loc[K].copy()
        row["strike"] = float(K)
        row["QK"] = row["mid"]
        put_rows.append(row)

    put_otm_raw = pd.DataFrame(put_rows)
    put_otm = _select_otm_options_with_bid_rule(put_otm_raw, ascending=False)

    call_rows = []
    for K in sorted([x for x in calls.index if x > K0]):
        row = calls.loc[K].copy()
        row["strike"] = float(K)
        row["QK"] = row["mid"]
        call_rows.append(row)

    call_otm_raw = pd.DataFrame(call_rows)
    call_otm = _select_otm_options_with_bid_rule(call_otm_raw, ascending=True)

    if K0 not in calls.index or K0 not in puts.index:
        raise RuntimeError(f"K0={K0} missing call or put quote.")

    k0_row = calls.loc[K0].copy()
    k0_row["strike"] = K0
    k0_row["QK"] = (float(calls.loc[K0, "mid"]) + float(puts.loc[K0, "mid"])) / 2.0
    k0_row["bid"] = (float(calls.loc[K0, "bid"]) + float(puts.loc[K0, "bid"])) / 2.0
    k0_row["ask"] = (float(calls.loc[K0, "ask"]) + float(puts.loc[K0, "ask"])) / 2.0
    k0_row["right"] = "K0_AVG"

    selected_options = pd.concat(
        [put_otm, pd.DataFrame([k0_row]), call_otm],
        ignore_index=True,
    ).sort_values("strike").reset_index(drop=True)

    if len(selected_options) < 3:
        raise RuntimeError("Too few selected OTM options for variance calculation.")

    strikes = selected_options["strike"].astype(float).values
    QK = selected_options["QK"].astype(float).values

    delta_K = np.zeros(len(strikes))
    for i in range(len(strikes)):
        if i == 0:
            delta_K[i] = strikes[i + 1] - strikes[i]
        elif i == len(strikes) - 1:
            delta_K[i] = strikes[i] - strikes[i - 1]
        else:
            delta_K[i] = (strikes[i + 1] - strikes[i - 1]) / 2.0

    contribution = (delta_K / (strikes ** 2)) * math.exp(r * T) * QK
    variance = (2.0 / T) * np.sum(contribution) - (1.0 / T) * ((F / K0 - 1.0) ** 2)

    return {
        "variance": float(variance),
        "T": float(T),
        "F": float(F),
        "K0": float(K0),
        "K_star": float(K_star),
        "num_options": int(len(selected_options)),
        "num_put_otm": int(len(put_otm)),
        "num_call_otm": int(len(call_otm)),
        "min_selected_strike": float(selected_options["strike"].min()),
        "max_selected_strike": float(selected_options["strike"].max()),
    }


def calculate_variance_for_unique_chains(
    unique_chains: pd.DataFrame,
    chain_results: dict[tuple[str, int], pd.DataFrame],
    r: float,
) -> tuple[pd.DataFrame, dict]:
    rows = []
    calc_results = {}

    for _, row in unique_chains.iterrows():
        root = str(row["root"])
        expiration = int(row["expiration"])
        minutes = int(row["minutes"])
        days = float(row["days"])

        key = (root, expiration)
        chain = chain_results[key]

        calc = calc_single_term_variance(
            chain=chain,
            minutes_to_expiry=minutes,
            r=r,
        )

        calc_results[key] = calc

        rows.append({
            "root": root,
            "expiration": expiration,
            "minutes": minutes,
            "days": days,
            "variance": calc["variance"],
            "vix_style_vol": 100.0 * math.sqrt(calc["variance"]) if calc["variance"] > 0 else np.nan,
            "F": calc["F"],
            "K0": calc["K0"],
            "K_star": calc["K_star"],
            "num_options": calc["num_options"],
            "num_put_otm": calc["num_put_otm"],
            "num_call_otm": calc["num_call_otm"],
            "min_selected_strike": calc["min_selected_strike"],
            "max_selected_strike": calc["max_selected_strike"],
        })

    return pd.DataFrame(rows).sort_values("minutes").reset_index(drop=True), calc_results


def interpolate_variance_to_target_days(term_df: pd.DataFrame, target_days: int) -> float:
    if len(term_df) != 2:
        raise RuntimeError(f"Expected exactly two term rows for target {target_days}d.")

    term_df = term_df.sort_values("minutes").reset_index(drop=True)

    N1 = float(term_df.loc[0, "minutes"])
    N2 = float(term_df.loc[1, "minutes"])
    var1 = float(term_df.loc[0, "variance"])
    var2 = float(term_df.loc[1, "variance"])

    target_minutes = float(target_days * MINUTES_PER_DAY)

    if not (N1 <= target_minutes <= N2):
        raise RuntimeError(
            f"Target tenor {target_days}d not bracketed: near={N1 / MINUTES_PER_DAY:.3f}d, next={N2 / MINUTES_PER_DAY:.3f}d"
        )

    T1 = N1 / MINUTES_PER_YEAR
    T2 = N2 / MINUTES_PER_YEAR

    interpolated_variance = (
        T1 * var1 * ((N2 - target_minutes) / (N2 - N1))
        + T2 * var2 * ((target_minutes - N1) / (N2 - N1))
    ) * (MINUTES_PER_YEAR / target_minutes)

    return float(interpolated_variance)


def load_sofr_rate_or_default(project_root: Path, trade_date: int, default_rate: float) -> tuple[float, str]:
    search_dirs = [
        project_root / "data" / "external",
        project_root / "data" / "raw",
        project_root / "data" / "processed",
    ]

    files = []
    for d in search_dirs:
        if d.exists():
            files.extend(d.rglob("*sofr*.csv"))
            files.extend(d.rglob("*SOFR*.csv"))

    for p in sorted(set(files)):
        try:
            df = pd.read_csv(p)
            if df.empty:
                continue

            date_col = None
            for c in df.columns:
                cl = str(c).lower()
                if cl in {"date", "trade_date", "observation_date"} or "date" in cl:
                    date_col = c
                    break

            if date_col is None:
                continue

            value_col = None
            for c in df.columns:
                cl = str(c).lower()
                if cl in {"sofr", "value", "rate"} or "sofr" in cl:
                    if c != date_col:
                        value_col = c
                        break

            if value_col is None:
                numeric_cols = [c for c in df.columns if c != date_col and pd.to_numeric(df[c], errors="coerce").notna().sum() > 0]
                if numeric_cols:
                    value_col = numeric_cols[0]

            if value_col is None:
                continue

            dates = pd.to_datetime(df[date_col], errors="coerce")
            vals = pd.to_numeric(df[value_col], errors="coerce")

            tmp = pd.DataFrame({"date": dates, "rate_raw": vals}).dropna()
            tmp["trade_date"] = tmp["date"].dt.strftime("%Y%m%d").astype(int)
            tmp = tmp[tmp["trade_date"] <= int(trade_date)].copy()

            if tmp.empty:
                continue

            row = tmp.sort_values("trade_date").iloc[-1]
            rate = float(row["rate_raw"])
            if rate > 1.0:
                rate = rate / 100.0

            return rate, f"SOFR file {p}; observation_date={int(row['trade_date'])}"

        except Exception:
            continue

    return float(default_rate), f"default_rate={default_rate}"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--base-url", default="http://127.0.0.1:25503/v3")
    p.add_argument("--trade-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--quote-time", default=None)
    p.add_argument("--target-tenors", default=",".join(str(x) for x in DEFAULT_TARGET_TENORS))
    p.add_argument("--default-risk-free-rate", type=float, default=0.05)
    p.add_argument("--max-expiration-days", type=int, default=90)
    args = p.parse_args()

    project_root = Path(args.project_root)
    run_ts = now_stamp()

    trade_date = int(pd.to_datetime(str(args.trade_date), format="%Y%m%d").strftime("%Y%m%d"))
    quote_time = args.quote_time or default_quote_time()
    calc_time_ms = quote_time_to_ms(quote_time)
    target_tenors = parse_target_tenors(args.target_tenors)

    audit_dir = project_root / "data" / "audit" / "intraday_implied_variance"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday implied variance smoke v1")
    print(f"Project root:   {project_root}")
    print(f"Run timestamp:  {run_ts}")
    print(f"Base URL:       {args.base_url}")
    print(f"Trade date:     {trade_date}")
    print(f"Quote time:     {quote_time}")
    print(f"Calc time ms:   {calc_time_ms}")
    print(f"Target tenors:  {target_tenors}")
    print(f"Audit dir:      {audit_dir}")
    print("Mode:           audit-only smoke test; no production signal files modified")

    request_meta_rows = []

    banner("Load expirations")
    spxw_exps, meta = list_expirations(args.base_url, "SPXW")
    request_meta_rows.append(meta)
    spx_exps, meta = list_expirations(args.base_url, "SPX")
    request_meta_rows.append(meta)

    if not spxw_exps:
        raise RuntimeError("No SPXW expirations loaded.")
    if not spx_exps:
        raise RuntimeError("No SPX expirations loaded.")

    print(f"SPXW expirations: {len(spxw_exps):,}")
    print(f"SPX expirations:  {len(spx_exps):,}")

    trading_dates = get_trading_dates(project_root)

    candidates = expiration_candidates(
        trade_date=trade_date,
        calc_time_ms=calc_time_ms,
        spx_exps=set(spx_exps),
        spxw_exps=set(spxw_exps),
        trading_dates=trading_dates,
        max_days=int(args.max_expiration_days),
    )

    required_by_tenor, unique_chains = required_chains_for_target_tenors(
        candidates=candidates,
        target_tenors=target_tenors,
    )

    print("\nRequired by tenor:")
    print(required_by_tenor.to_string(index=False))
    print("\nUnique chains:")
    print(unique_chains.to_string(index=False))

    rate, rate_source = load_sofr_rate_or_default(
        project_root=project_root,
        trade_date=trade_date,
        default_rate=float(args.default_risk_free_rate),
    )
    print(f"\nRisk-free rate used: {rate:.8f}")
    print(f"Rate source:         {rate_source}")

    banner("Pull option chains at quote time")
    chain_results: dict[tuple[str, int], pd.DataFrame] = {}
    quality_rows = []

    for _, row in unique_chains.iterrows():
        root = str(row["root"])
        expiration = int(row["expiration"])

        print(f"Pulling {root} {expiration} at {quote_time} ...")

        chain, meta = get_chain_at_time(
            base_url=args.base_url,
            root=root,
            expiration=expiration,
            trade_date=trade_date,
            quote_time=quote_time,
        )
        request_meta_rows.append(meta)

        qrow = chain_quality_row(chain, meta, root, expiration)
        quality_rows.append(qrow)

        print(
            f"  rows={qrow['rows']}, calls={qrow['call_rows']}, puts={qrow['put_rows']}, "
            f"strikes={qrow['unique_strikes']}, positive_bid_rows={qrow['positive_bid_rows']}"
        )

        if chain.empty:
            raise RuntimeError(f"Empty chain for {root} {expiration}.")

        chain_results[(root, expiration)] = chain

    chain_quality = pd.DataFrame(quality_rows)

    banner("Calculate VIX-style variance by expiration")
    variance_table, calc_results = calculate_variance_for_unique_chains(
        unique_chains=unique_chains,
        chain_results=chain_results,
        r=rate,
    )

    print(variance_table.to_string(index=False))

    banner("Interpolate to target tenors")
    variance_lookup = {
        (row["root"], int(row["expiration"])): row
        for _, row in variance_table.iterrows()
    }

    output_rows = []

    for target_days in target_tenors:
        pair = required_by_tenor[required_by_tenor["target_days"].eq(target_days)].copy()
        if len(pair) != 2:
            raise RuntimeError(f"Expected two expiration rows for target {target_days}d.")

        term_rows = []

        for _, leg_row in pair.iterrows():
            key = (leg_row["root"], int(leg_row["expiration"]))
            var_row = variance_lookup[key]

            term_rows.append({
                "term": leg_row["leg"],
                "root": leg_row["root"],
                "expiration": int(leg_row["expiration"]),
                "minutes": int(leg_row["minutes"]),
                "days": float(leg_row["days"]),
                "variance": float(var_row["variance"]),
                "vix_style_vol": float(var_row["vix_style_vol"]),
                "F": float(var_row["F"]),
                "K0": float(var_row["K0"]),
                "num_options": int(var_row["num_options"]),
            })

        term_df = pd.DataFrame(term_rows).sort_values("minutes").reset_index(drop=True)

        implied_variance = interpolate_variance_to_target_days(
            term_df=term_df,
            target_days=target_days,
        )
        implied_vol = 100.0 * math.sqrt(implied_variance) if implied_variance > 0 else np.nan

        output_rows.append({
            "asof_timestamp_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "trade_date": int(trade_date),
            "quote_time": quote_time,
            "calc_time_ms": int(calc_time_ms),
            "target_days": int(target_days),
            "tenor": int(target_days),
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

    tenor_panel = pd.DataFrame(output_rows).sort_values("target_days").reset_index(drop=True)

    print(tenor_panel[[
        "target_days",
        "implied_variance_intraday",
        "vix_style_vol_intraday",
        "near_root",
        "near_expiration",
        "near_days",
        "next_root",
        "next_expiration",
        "next_days",
    ]].to_string(index=False))

    banner("Save audit outputs")
    tenor_path = audit_dir / f"intraday_implied_variance_smoke_tenor_panel_{trade_date}_{run_ts}.csv"
    required_path = audit_dir / f"intraday_implied_variance_smoke_required_chains_{trade_date}_{run_ts}.csv"
    unique_path = audit_dir / f"intraday_implied_variance_smoke_unique_chain_variance_{trade_date}_{run_ts}.csv"
    quality_path = audit_dir / f"intraday_implied_variance_smoke_chain_quality_{trade_date}_{run_ts}.csv"
    request_path = audit_dir / f"intraday_implied_variance_smoke_request_meta_{trade_date}_{run_ts}.csv"

    tenor_panel.to_csv(tenor_path, index=False)
    required_by_tenor.to_csv(required_path, index=False)
    variance_table.to_csv(unique_path, index=False)
    chain_quality.to_csv(quality_path, index=False)
    pd.DataFrame(request_meta_rows).to_csv(request_path, index=False)

    tenors_ok = sorted(tenor_panel["target_days"].astype(int).tolist()) == target_tenors
    finite_positive_variance = bool(np.isfinite(tenor_panel["implied_variance_intraday"]).all() and (tenor_panel["implied_variance_intraday"] > 0).all())
    finite_positive_vol = bool(np.isfinite(tenor_panel["vix_style_vol_intraday"]).all() and (tenor_panel["vix_style_vol_intraday"] > 0).all())
    chain_quality_ok = bool(
        (chain_quality["rows"] > 0).all()
        and (chain_quality["call_rows"] > 0).all()
        and (chain_quality["put_rows"] > 0).all()
        and (chain_quality["unique_strikes"] >= 20).all()
        and (chain_quality["positive_bid_rows"] > 0).all()
    )

    smoke_pass = bool(tenors_ok and finite_positive_variance and finite_positive_vol and chain_quality_ok)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "base_url": args.base_url,
        "trade_date": int(trade_date),
        "quote_time": quote_time,
        "calc_time_ms": int(calc_time_ms),
        "target_tenors": target_tenors,
        "risk_free_rate": float(rate),
        "rate_source": rate_source,
        "audit_outputs": {
            "tenor_panel": str(tenor_path),
            "required_chains": str(required_path),
            "unique_chain_variance": str(unique_path),
            "chain_quality": str(quality_path),
            "request_meta": str(request_path),
        },
        "checks": {
            "tenors_ok": tenors_ok,
            "finite_positive_variance": finite_positive_variance,
            "finite_positive_vol": finite_positive_vol,
            "chain_quality_ok": chain_quality_ok,
        },
        "INTRADAY_IMPLIED_VARIANCE_SMOKE_PASS": smoke_pass,
        "method_note": "Audit-only intraday VIX-style implied variance smoke test. No production signal files modified.",
    }

    manifest_path = audit_dir / f"intraday_implied_variance_smoke_manifest_{trade_date}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"tenor_panel:           {tenor_path}")
    print(f"required_chains:       {required_path}")
    print(f"unique_chain_variance: {unique_path}")
    print(f"chain_quality:         {quality_path}")
    print(f"request_meta:          {request_path}")
    print(f"manifest:              {manifest_path}")

    banner("Final result")
    print(f"tenors_ok:                         {tenors_ok}")
    print(f"finite_positive_variance:          {finite_positive_variance}")
    print(f"finite_positive_vol:               {finite_positive_vol}")
    print(f"chain_quality_ok:                  {chain_quality_ok}")
    print(f"INTRADAY_IMPLIED_VARIANCE_SMOKE_PASS: {smoke_pass}")

    if not smoke_pass:
        raise RuntimeError("INTRADAY_IMPLIED_VARIANCE_SMOKE_PASS is False.")

    print("DONE — intraday implied variance smoke test complete.")


if __name__ == "__main__":
    main()
