
from __future__ import annotations

import argparse
import io
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests


RSI_FORMULA_VERSION = "wilder_rsi14_spy_close_v2_long_warmup"
PERIOD = 14
DEFAULT_BASE_URL = "http://127.0.0.1:25503/v3"
DEFAULT_SYMBOL = "SPY"
DEFAULT_WARMUP_START_DATE = "19930129"
DEFAULT_OUTPUT_START_DATE = "20180102"
DEFAULT_CHUNK_DAYS = 350
DEFAULT_SLEEP_SECONDS = 0.20


@dataclass(frozen=True)
class Config:
    project_root: Path
    base_url: str
    symbol: str
    warmup_start_date: str
    output_start_date: str
    end_date: str
    chunk_days: int
    sleep_seconds: float
    external_validation_file: str
    rsi_tolerance: float
    close_tolerance: float
    no_thetadata: bool


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def yyyymmdd(x) -> str:
    return pd.to_datetime(str(x), format="%Y%m%d", errors="coerce").strftime("%Y%m%d")


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
        raise RuntimeError("Could not parse trade_date/date series.")

    return dt.dt.strftime("%Y%m%d").astype("Int64")


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


def standardize_thetadata_eod(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = df.copy()

    if "close" not in out.columns:
        raise RuntimeError(f"ThetaData EOD response missing close column. Columns: {list(out.columns)}")

    if "date" in out.columns:
        out["trade_date"] = normalize_trade_date_series(out["date"]).astype("Int64")
    elif "trade_date" in out.columns:
        out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype("Int64")
    elif "last_trade" in out.columns:
        out["trade_date"] = (
            pd.to_datetime(out["last_trade"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )
    elif "created" in out.columns:
        out["trade_date"] = (
            pd.to_datetime(out["created"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )
    else:
        raise RuntimeError(
            f"ThetaData EOD response missing usable date source. "
            f"Expected date/trade_date/last_trade/created. Columns: {list(out.columns)}"
        )

    out["spy_close"] = pd.to_numeric(out["close"], errors="coerce")
    out["spy_open"] = pd.to_numeric(out["open"], errors="coerce") if "open" in out.columns else np.nan
    out["spy_high"] = pd.to_numeric(out["high"], errors="coerce") if "high" in out.columns else np.nan
    out["spy_low"] = pd.to_numeric(out["low"], errors="coerce") if "low" in out.columns else np.nan
    out["spy_volume"] = pd.to_numeric(out["volume"], errors="coerce") if "volume" in out.columns else np.nan

    keep = ["trade_date", "spy_open", "spy_high", "spy_low", "spy_close", "spy_volume"]
    out = (
        out[keep]
        .dropna(subset=["trade_date", "spy_close"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    out["trade_date"] = out["trade_date"].astype(int)
    return out


def date_chunks(start_yyyymmdd: str, end_yyyymmdd: str, chunk_days: int) -> list[tuple[str, str]]:
    start = pd.to_datetime(start_yyyymmdd, format="%Y%m%d")
    end = pd.to_datetime(end_yyyymmdd, format="%Y%m%d")

    chunks = []
    cur = start

    while cur <= end:
        e = min(cur + pd.Timedelta(days=int(chunk_days) - 1), end)
        chunks.append((cur.strftime("%Y%m%d"), e.strftime("%Y%m%d")))
        cur = e + pd.Timedelta(days=1)

    return chunks


def fetch_thetadata_eod_long(cfg: Config, audit_dir: Path, run_ts: str) -> tuple[pd.DataFrame, pd.DataFrame, Path]:
    url = cfg.base_url.rstrip("/") + "/stock/history/eod"

    frames = []
    meta_rows = []

    chunks = date_chunks(cfg.warmup_start_date, cfg.end_date, cfg.chunk_days)

    for idx, (s, e) in enumerate(chunks, start=1):
        params = {
            "symbol": cfg.symbol,
            "start_date": s,
            "end_date": e,
            "format": "json",
        }

        print(f"Chunk {idx}/{len(chunks)}: {s} to {e}")

        row = {
            "chunk_number": idx,
            "request_start_date": s,
            "request_end_date": e,
            "url": url,
            "params": json.dumps(params, sort_keys=True),
            "status_code": None,
            "ok": False,
            "rows_after_parse": 0,
            "rows_after_standardize": 0,
            "error": "",
        }

        try:
            resp = requests.get(url, params=params, timeout=120)
            row["status_code"] = int(resp.status_code)
            row["ok"] = bool(resp.ok)

            if not resp.ok:
                row["error"] = (resp.text or "")[:1000].replace("\n", " ")
                meta_rows.append(row)
                continue

            parsed = parse_theta_payload(resp)
            row["rows_after_parse"] = int(len(parsed))

            std = standardize_thetadata_eod(parsed)
            row["rows_after_standardize"] = int(len(std))

            if not std.empty:
                std["request_start_date"] = s
                std["request_end_date"] = e
                frames.append(std)

        except Exception as exc:
            row["error"] = repr(exc)

        meta_rows.append(row)
        time.sleep(float(cfg.sleep_seconds))

    meta = pd.DataFrame(meta_rows)

    if frames:
        raw = (
            pd.concat(frames, ignore_index=True)
            .sort_values("trade_date")
            .drop_duplicates("trade_date", keep="last")
            .reset_index(drop=True)
        )
    else:
        raw = pd.DataFrame(columns=["trade_date", "spy_open", "spy_high", "spy_low", "spy_close", "spy_volume"])

    raw_path = audit_dir / f"spy_wilder_rsi14_long_warmup_raw_thetadata_{run_ts}.csv"
    raw.to_csv(raw_path, index=False)

    return raw, meta, raw_path


def load_canonical_spy_eod_prices(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "market_data" / "spy_eod_prices_v1.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing canonical SPY EOD price file: {path}")

    raw = pd.read_parquet(path)

    required = ["trade_date", "spy_close"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise RuntimeError(f"SPY EOD price file missing required columns {missing}. Columns: {list(raw.columns)}")

    out = raw.copy()
    out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype("Int64")
    out["spy_close"] = pd.to_numeric(out["spy_close"], errors="coerce")

    if "spy_open" not in out.columns:
        out["spy_open"] = np.nan
    if "spy_high" not in out.columns:
        out["spy_high"] = np.nan
    if "spy_low" not in out.columns:
        out["spy_low"] = np.nan
    if "spy_volume" not in out.columns:
        out["spy_volume"] = np.nan

    keep = ["trade_date", "spy_open", "spy_high", "spy_low", "spy_close", "spy_volume"]
    out = (
        out[keep]
        .dropna(subset=["trade_date", "spy_close"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    out["trade_date"] = out["trade_date"].astype(int)

    if out.empty:
        raise RuntimeError("Canonical SPY EOD price file has no usable rows after cleaning.")

    return out, path


def reconcile_long_with_canonical(
    long_raw: pd.DataFrame,
    canonical: pd.DataFrame,
    output_start_date: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Use long ThetaData warmup where available, but force the project-window
    prices to canonical stored SPY EOD values so RSI used by the signal dataset
    aligns with the production market-data source of truth.
    """
    long = long_raw.copy()
    canon = canonical.copy()

    long["source_priority"] = 0
    long["source_name"] = "thetadata_long_warmup"

    canon["source_priority"] = 1
    canon["source_name"] = "canonical_spy_eod_prices_v1"

    combined = pd.concat([long, canon], ignore_index=True, sort=False)
    combined = (
        combined.dropna(subset=["trade_date", "spy_close"])
        .sort_values(["trade_date", "source_priority"])
        .drop_duplicates("trade_date", keep="last")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )

    combined["trade_date"] = combined["trade_date"].astype(int)
    combined["spy_close"] = pd.to_numeric(combined["spy_close"], errors="coerce")

    # Close-basis comparison inside canonical window where both sources exist.
    compare = long[["trade_date", "spy_close"]].rename(columns={"spy_close": "long_warmup_close"}).merge(
        canon[["trade_date", "spy_close"]].rename(columns={"spy_close": "canonical_close"}),
        on="trade_date",
        how="inner",
    )

    compare = compare[compare["trade_date"].astype(int) >= int(output_start_date)].copy()
    compare["close_diff"] = compare["long_warmup_close"] - compare["canonical_close"]
    compare["close_abs_diff"] = compare["close_diff"].abs()

    return combined, compare


def compute_wilder_rsi14(prices: pd.DataFrame, source_price_file: str) -> pd.DataFrame:
    df = prices[["trade_date", "spy_close", "source_name"]].copy()
    df = df.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)

    close = df["spy_close"].astype(float)
    change = close.diff()
    gain = change.clip(lower=0.0)
    loss = (-change).clip(lower=0.0)

    avg_gain = pd.Series(np.nan, index=df.index, dtype=float)
    avg_loss = pd.Series(np.nan, index=df.index, dtype=float)

    if len(df) > PERIOD:
        first = PERIOD

        avg_gain.iloc[first] = gain.iloc[1:first + 1].mean()
        avg_loss.iloc[first] = loss.iloc[1:first + 1].mean()

        for i in range(first + 1, len(df)):
            avg_gain.iloc[i] = ((avg_gain.iloc[i - 1] * (PERIOD - 1)) + gain.iloc[i]) / PERIOD
            avg_loss.iloc[i] = ((avg_loss.iloc[i - 1] * (PERIOD - 1)) + loss.iloc[i]) / PERIOD

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    rsi = rsi.where(~((avg_loss == 0.0) & (avg_gain > 0.0)), 100.0)
    rsi = rsi.where(~((avg_loss == 0.0) & (avg_gain == 0.0)), 50.0)

    out = df.copy()
    out["spy_change"] = change
    out["spy_gain"] = gain
    out["spy_loss"] = loss
    out["wilder_avg_gain_14"] = avg_gain
    out["wilder_avg_loss_14"] = avg_loss
    out["spy_wilder_rsi14"] = rsi
    out["rsi_formula_version"] = RSI_FORMULA_VERSION
    out["source_price_file"] = source_price_file

    return out


def nearest_available_date(dates: pd.Series, requested: int) -> int | None:
    vals = sorted(int(x) for x in dates.dropna().astype(int).unique())
    if not vals:
        return None

    prior_or_equal = [x for x in vals if x <= int(requested)]
    if prior_or_equal:
        return int(prior_or_equal[-1])

    return int(vals[0])


def default_validation_role(label: str, trade_date: int) -> str:
    label_l = str(label).lower()

    if "early" in label_l:
        return "diagnostic"
    if "rebound" in label_l and int(trade_date) in [20200324]:
        return "diagnostic"

    return "strict"


def build_external_validation_template(rsi_output: pd.DataFrame) -> pd.DataFrame:
    latest = int(rsi_output["trade_date"].max())

    requested_points = [
        ("latest_available", latest),
        ("recent_anchor_20260709", 20260709),
        ("covid_crash_down_day", 20200316),
        ("covid_rebound_day", 20200324),
        ("2022_bear_market_stress", 20220616),
        ("2022_october_low_area", 20221012),
        ("low_vol_normal_period", 20191231),
        ("random_normal_period", 20210415),
        ("early_history_after_warmup", 20180215),
    ]

    rows = []

    for label, requested in requested_points:
        matched = nearest_available_date(rsi_output["trade_date"], requested)

        if matched is None:
            continue

        row = rsi_output[rsi_output["trade_date"].eq(matched)].tail(1)

        if row.empty:
            continue

        rr = row.iloc[0]

        rows.append({
            "validation_label": label,
            "validation_role": default_validation_role(label, matched),
            "requested_trade_date": int(requested),
            "matched_trade_date": int(matched),
            "spy_close": float(rr["spy_close"]),
            "our_wilder_rsi14": float(rr["spy_wilder_rsi14"]) if pd.notna(rr["spy_wilder_rsi14"]) else np.nan,
            "external_source_1": "",
            "external_price_basis_1": "",
            "external_close_1": np.nan,
            "external_close_diff_1": np.nan,
            "external_rsi_1": np.nan,
            "external_rsi_diff_1": np.nan,
            "external_source_2": "",
            "external_price_basis_2": "",
            "external_close_2": np.nan,
            "external_close_diff_2": np.nan,
            "external_rsi_2": np.nan,
            "external_rsi_diff_2": np.nan,
            "external_chart_config": "SPY daily close, RSI length 14, Wilder/RMA smoothing",
            "notes": "",
        })

    template = pd.DataFrame(rows).drop_duplicates("matched_trade_date", keep="first").reset_index(drop=True)
    return template


def coalesce_col(df: pd.DataFrame, candidates: list[str], default=np.nan):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df), index=df.index)


def normalize_external_validation_file(path: Path, rsi_output: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_csv(path)
    out = raw.copy()

    # Normalize old v1 columns into v2 columns.
    if "validation_role" not in out.columns:
        out["validation_role"] = [
            default_validation_role(lbl, dt)
            for lbl, dt in zip(out.get("validation_label", ""), out.get("matched_trade_date", 0))
        ]

    if "external_diff_1" in out.columns and "external_rsi_diff_1" not in out.columns:
        out["external_rsi_diff_1"] = out["external_diff_1"]

    if "external_diff_2" in out.columns and "external_rsi_diff_2" not in out.columns:
        out["external_rsi_diff_2"] = out["external_diff_2"]

    required_cols = build_external_validation_template(rsi_output).columns.tolist()
    for c in required_cols:
        if c not in out.columns:
            out[c] = np.nan if any(x in c for x in ["rsi", "close", "diff"]) else ""

    # Refresh our values from current RSI output by matched_trade_date.
    key = rsi_output[["trade_date", "spy_close", "spy_wilder_rsi14"]].rename(
        columns={
            "trade_date": "matched_trade_date",
            "spy_close": "_current_spy_close",
            "spy_wilder_rsi14": "_current_our_wilder_rsi14",
        }
    )

    out["matched_trade_date"] = pd.to_numeric(out["matched_trade_date"], errors="coerce").astype("Int64")
    out = out.merge(key, on="matched_trade_date", how="left")

    out["spy_close"] = out["_current_spy_close"].where(out["_current_spy_close"].notna(), pd.to_numeric(out["spy_close"], errors="coerce"))
    out["our_wilder_rsi14"] = out["_current_our_wilder_rsi14"].where(
        out["_current_our_wilder_rsi14"].notna(),
        pd.to_numeric(out["our_wilder_rsi14"], errors="coerce"),
    )

    out = out.drop(columns=["_current_spy_close", "_current_our_wilder_rsi14"], errors="ignore")

    for source_num in [1, 2]:
        ext_close_col = f"external_close_{source_num}"
        ext_rsi_col = f"external_rsi_{source_num}"
        close_diff_col = f"external_close_diff_{source_num}"
        rsi_diff_col = f"external_rsi_diff_{source_num}"

        out[ext_close_col] = pd.to_numeric(out[ext_close_col], errors="coerce")
        out[ext_rsi_col] = pd.to_numeric(out[ext_rsi_col], errors="coerce")
        out[close_diff_col] = out[ext_close_col] - pd.to_numeric(out["spy_close"], errors="coerce")
        out[rsi_diff_col] = out[ext_rsi_col] - pd.to_numeric(out["our_wilder_rsi14"], errors="coerce")

    # Reorder and keep any extra user-added columns at end.
    ordered = [c for c in required_cols if c in out.columns]
    extras = [c for c in out.columns if c not in ordered]
    return out[ordered + extras]


def summarize_external_validation(template: pd.DataFrame, rsi_tolerance: float, close_tolerance: float) -> pd.DataFrame:
    rows = []

    if template.empty:
        return pd.DataFrame([{
            "check": "external_validation_rows",
            "status": "WARN",
            "detail": "No external validation rows found.",
        }])

    any_entered = False
    any_strict_eligible = False
    any_strict_fail = False

    for source_num in [1, 2]:
        ext_rsi_col = f"external_rsi_{source_num}"
        ext_close_col = f"external_close_{source_num}"
        close_diff_col = f"external_close_diff_{source_num}"
        rsi_diff_col = f"external_rsi_diff_{source_num}"
        source_col = f"external_source_{source_num}"

        vals = pd.to_numeric(template[ext_rsi_col], errors="coerce")
        closes = pd.to_numeric(template[ext_close_col], errors="coerce")
        close_diff = pd.to_numeric(template[close_diff_col], errors="coerce")
        rsi_diff = pd.to_numeric(template[rsi_diff_col], errors="coerce")
        roles = template["validation_role"].astype(str).str.lower()

        entered = vals.notna()
        any_entered = any_entered or bool(entered.any())

        close_entered = closes.notna()
        close_match = close_entered & (close_diff.abs() <= float(close_tolerance))
        strict_eligible = entered & close_match & roles.eq("strict")

        any_strict_eligible = any_strict_eligible or bool(strict_eligible.any())

        strict_fail = strict_eligible & (rsi_diff.abs() > float(rsi_tolerance))
        any_strict_fail = any_strict_fail or bool(strict_fail.any())

        diagnostic_entered = entered & ~strict_eligible

        status = "PENDING"
        detail_parts = []

        if strict_eligible.any():
            max_abs = float(rsi_diff[strict_eligible].abs().max())
            mean_abs = float(rsi_diff[strict_eligible].abs().mean())
            status = "PASS" if not strict_fail.any() else "FAIL"
            detail_parts.append(
                f"strict_rows={int(strict_eligible.sum())}; "
                f"strict_max_abs_rsi_diff={max_abs:.6f}; "
                f"strict_mean_abs_rsi_diff={mean_abs:.6f}; "
                f"rsi_tolerance={rsi_tolerance}"
            )

        if diagnostic_entered.any():
            diag_max = float(rsi_diff[diagnostic_entered].abs().max())
            detail_parts.append(
                f"diagnostic_rows={int(diagnostic_entered.sum())}; "
                f"diagnostic_max_abs_rsi_diff={diag_max:.6f}"
            )

        if entered.any() and not close_entered.any():
            detail_parts.append("RSI entered but external close not entered; rows are not strict-eligible.")

        if entered.any() and (close_entered & ~close_match).any():
            detail_parts.append(
                f"rows_with_close_mismatch={int((close_entered & ~close_match).sum())}; "
                f"close_tolerance={close_tolerance}"
            )

        if not detail_parts:
            detail_parts.append("No external RSI values entered yet.")

        rows.append({
            "check": f"external_source_{source_num}",
            "status": status,
            "detail": " | ".join(detail_parts),
        })

    overall_status = "PENDING"
    if any_strict_fail:
        overall_status = "FAIL"
    elif any_strict_eligible:
        overall_status = "PASS"
    elif any_entered:
        overall_status = "PENDING"

    rows.append({
        "check": "external_validation_overall",
        "status": overall_status,
        "detail": (
            "Strict external validation passed."
            if overall_status == "PASS"
            else "Strict external validation failed."
            if overall_status == "FAIL"
            else "External values exist but no strict-eligible rows yet. Enter external closes and/or set validation_role."
        ),
    })

    return pd.DataFrame(rows)


def build_internal_checks(rsi_long: pd.DataFrame, rsi_output: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    finite_rsi = rsi_output["spy_wilder_rsi14"].notna() & np.isfinite(
        pd.to_numeric(rsi_output["spy_wilder_rsi14"], errors="coerce")
    )

    latest_date = int(rsi_output["trade_date"].max())
    latest_rsi = float(rsi_output.loc[rsi_output["trade_date"].eq(latest_date), "spy_wilder_rsi14"].iloc[-1])

    canon_dates = set(canonical["trade_date"].astype(int).tolist())
    output_dates = set(rsi_output["trade_date"].astype(int).tolist())

    rows = [
        {
            "check": "long_history_has_rows",
            "status": "PASS" if len(rsi_long) > 0 else "FAIL",
            "detail": f"long_rows={len(rsi_long)}",
        },
        {
            "check": "output_rows_match_canonical_window",
            "status": "PASS" if canon_dates.issubset(output_dates) else "FAIL",
            "detail": f"canonical_dates={len(canon_dates)}; output_dates={len(output_dates)}",
        },
        {
            "check": "trade_dates_unique_output",
            "status": "PASS" if rsi_output["trade_date"].is_unique else "FAIL",
            "detail": "trade_date unique check",
        },
        {
            "check": "close_positive_finite_output",
            "status": "PASS" if (
                np.isfinite(rsi_output["spy_close"].astype(float)).all()
                and (rsi_output["spy_close"].astype(float) > 0).all()
            ) else "FAIL",
            "detail": "SPY close finite and positive",
        },
        {
            "check": "latest_rsi_finite",
            "status": "PASS" if np.isfinite(latest_rsi) else "FAIL",
            "detail": f"latest_trade_date={latest_date}; latest_rsi={latest_rsi}",
        },
        {
            "check": "rsi_bounds",
            "status": "PASS" if rsi_output.loc[finite_rsi, "spy_wilder_rsi14"].between(0, 100).all() else "FAIL",
            "detail": "Finite RSI values must be between 0 and 100.",
        },
        {
            "check": "formula_version",
            "status": "PASS" if rsi_output["rsi_formula_version"].eq(RSI_FORMULA_VERSION).all() else "FAIL",
            "detail": RSI_FORMULA_VERSION,
        },
    ]

    return pd.DataFrame(rows)


def parse_args() -> Config:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL)
    p.add_argument("--symbol", default=DEFAULT_SYMBOL)
    p.add_argument("--warmup-start-date", default=DEFAULT_WARMUP_START_DATE)
    p.add_argument("--output-start-date", default=DEFAULT_OUTPUT_START_DATE)
    p.add_argument("--end-date", default="")
    p.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    p.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    p.add_argument("--external-validation-file", default="")
    p.add_argument("--rsi-tolerance", type=float, default=0.10)
    p.add_argument("--close-tolerance", type=float, default=0.01)
    p.add_argument("--no-thetadata", action="store_true")

    a = p.parse_args()

    return Config(
        project_root=Path(a.project_root),
        base_url=a.base_url,
        symbol=a.symbol,
        warmup_start_date=yyyymmdd(a.warmup_start_date),
        output_start_date=yyyymmdd(a.output_start_date),
        end_date=yyyymmdd(a.end_date) if a.end_date else "",
        chunk_days=int(a.chunk_days),
        sleep_seconds=float(a.sleep_seconds),
        external_validation_file=a.external_validation_file,
        rsi_tolerance=float(a.rsi_tolerance),
        close_tolerance=float(a.close_tolerance),
        no_thetadata=bool(a.no_thetadata),
    )


def main() -> None:
    cfg = parse_args()
    run_ts = now_stamp()

    processed_dir = cfg.project_root / "data" / "processed" / "market_data"
    audit_dir = cfg.project_root / "data" / "audit" / "rsi_repair_v2"
    processed_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("Wilder RSI14 repair v2 — long warmup + close-basis validation")
    print(f"Project root:        {cfg.project_root}")
    print(f"Run timestamp:       {run_ts}")
    print(f"Formula:             {RSI_FORMULA_VERSION}")
    print(f"Warmup start date:   {cfg.warmup_start_date}")
    print(f"Output start date:   {cfg.output_start_date}")
    print("Mode:                standalone RSI repair only; no final signal / parameter / sizing overwrite")

    banner("Load canonical SPY EOD prices")
    canonical, canonical_path = load_canonical_spy_eod_prices(cfg.project_root)

    if not cfg.end_date:
        cfg = Config(**{**asdict(cfg), "end_date": str(int(canonical["trade_date"].max()))})

    print(f"Canonical path:      {canonical_path}")
    print(f"Canonical rows:      {len(canonical)}")
    print(f"Canonical date range:{int(canonical['trade_date'].min())} to {int(canonical['trade_date'].max())}")
    print(f"Target end date:     {cfg.end_date}")

    banner("Load / pull long warmup SPY EOD history")
    if cfg.no_thetadata:
        long_raw = canonical.copy()
        request_meta = pd.DataFrame([{
            "mode": "no_thetadata",
            "detail": "Used canonical history only. This does not solve early Wilder seed mismatch.",
        }])
        raw_path = audit_dir / f"spy_wilder_rsi14_long_warmup_raw_canonical_only_{run_ts}.csv"
        long_raw.to_csv(raw_path, index=False)
    else:
        long_raw, request_meta, raw_path = fetch_thetadata_eod_long(cfg, audit_dir, run_ts)

        if long_raw.empty:
            raise RuntimeError("ThetaData long warmup pull returned no rows. Rerun with --no-thetadata only for debugging.")

    print(f"Long raw path:       {raw_path}")
    print(f"Long raw rows:       {len(long_raw)}")
    if len(long_raw):
        print(f"Long raw date range: {int(long_raw['trade_date'].min())} to {int(long_raw['trade_date'].max())}")

    banner("Reconcile long warmup with canonical project-window prices")
    combined_prices, close_compare = reconcile_long_with_canonical(
        long_raw=long_raw,
        canonical=canonical,
        output_start_date=int(cfg.output_start_date),
    )

    print(f"Combined rows:       {len(combined_prices)}")
    print(f"Combined date range: {int(combined_prices['trade_date'].min())} to {int(combined_prices['trade_date'].max())}")

    if not close_compare.empty:
        print("\nLong-vs-canonical close comparison inside project window:")
        print(close_compare[["trade_date", "long_warmup_close", "canonical_close", "close_diff", "close_abs_diff"]].tail(10).to_string(index=False))
        print(f"Max abs close diff: {float(close_compare['close_abs_diff'].max()):.8f}")

    banner("Compute Wilder RSI14 on long history, then trim to output window")
    source_note = f"long_warmup={raw_path}; canonical_project_window={canonical_path}"
    rsi_long = compute_wilder_rsi14(combined_prices, source_note)

    output_start = int(cfg.output_start_date)
    output_end = int(cfg.end_date)

    # Production RSI output must align exactly to the canonical SPY EOD calendar.
    # The long warmup history may contain extra dates from ThetaData that are not
    # in the canonical project market-data file. Those are valid for warmup, but
    # must not appear in the production RSI output.
    canonical_dates = set(
        canonical[
            canonical["trade_date"].astype(int).between(output_start, output_end)
        ]["trade_date"].astype(int).tolist()
    )

    rsi_output = rsi_long[
        rsi_long["trade_date"].astype(int).between(output_start, output_end)
        & rsi_long["trade_date"].astype(int).isin(canonical_dates)
    ].copy().reset_index(drop=True)

    print(rsi_output[[
        "trade_date",
        "spy_close",
        "spy_change",
        "wilder_avg_gain_14",
        "wilder_avg_loss_14",
        "spy_wilder_rsi14",
        "source_name",
    ]].tail(20).to_string(index=False))

    banner("Internal checks")
    checks = build_internal_checks(rsi_long, rsi_output, canonical)
    print(checks.to_string(index=False))
    internal_pass = bool(checks["status"].eq("PASS").all())

    banner("External validation")
    if cfg.external_validation_file:
        ext_path = Path(cfg.external_validation_file)
        if not ext_path.exists():
            raise FileNotFoundError(f"External validation file not found: {ext_path}")

        validation = normalize_external_validation_file(ext_path, rsi_output)
        print(f"Loaded external validation file: {ext_path}")
    else:
        validation = build_external_validation_template(rsi_output)
        print("Created new blank external validation template.")

    external_summary = summarize_external_validation(
        validation,
        rsi_tolerance=float(cfg.rsi_tolerance),
        close_tolerance=float(cfg.close_tolerance),
    )

    print("\nExternal validation rows:")
    print(validation.to_string(index=False))

    print("\nExternal validation summary:")
    print(external_summary.to_string(index=False))

    banner("Save outputs")
    rsi_path = processed_dir / "spy_wilder_rsi14_history_v1.parquet"
    rsi_long_path = audit_dir / f"spy_wilder_rsi14_long_history_{run_ts}.csv"
    rsi_output_csv_path = audit_dir / f"spy_wilder_rsi14_output_history_{run_ts}.csv"
    request_meta_path = audit_dir / f"spy_wilder_rsi14_thetadata_request_meta_{run_ts}.csv"
    close_compare_path = audit_dir / f"spy_wilder_rsi14_long_vs_canonical_close_compare_{run_ts}.csv"
    validation_path = audit_dir / f"spy_wilder_rsi14_external_validation_points_{run_ts}.csv"
    external_summary_path = audit_dir / f"spy_wilder_rsi14_external_validation_summary_{run_ts}.csv"
    checks_path = audit_dir / f"spy_wilder_rsi14_internal_checks_{run_ts}.csv"

    rsi_output.to_parquet(rsi_path, index=False)
    rsi_long.to_csv(rsi_long_path, index=False)
    rsi_output.to_csv(rsi_output_csv_path, index=False)
    request_meta.to_csv(request_meta_path, index=False)
    close_compare.to_csv(close_compare_path, index=False)
    validation.to_csv(validation_path, index=False)
    external_summary.to_csv(external_summary_path, index=False)
    checks.to_csv(checks_path, index=False)

    external_statuses = set(external_summary["status"].astype(str).tolist())
    strict_external_pass = "PASS" in external_statuses and "FAIL" not in external_statuses
    strict_external_failed = "FAIL" in external_statuses
    strict_external_pending = not strict_external_pass and not strict_external_failed

    build_pass = bool(internal_pass and not strict_external_failed)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(cfg.project_root),
        "formula_version": RSI_FORMULA_VERSION,
        "period": PERIOD,
        "config": {
            **asdict(cfg),
            "project_root": str(cfg.project_root),
        },
        "source_files": {
            "canonical_spy_eod": str(canonical_path),
            "long_raw": str(raw_path),
        },
        "processed_output": str(rsi_path),
        "audit_outputs": {
            "rsi_long": str(rsi_long_path),
            "rsi_output_csv": str(rsi_output_csv_path),
            "request_meta": str(request_meta_path),
            "close_compare": str(close_compare_path),
            "external_validation_points": str(validation_path),
            "external_validation_summary": str(external_summary_path),
            "internal_checks": str(checks_path),
        },
        "rows": {
            "long_raw": int(len(long_raw)),
            "combined_prices": int(len(combined_prices)),
            "rsi_long": int(len(rsi_long)),
            "rsi_output": int(len(rsi_output)),
        },
        "date_range": {
            "rsi_long_first": int(rsi_long["trade_date"].min()),
            "rsi_long_latest": int(rsi_long["trade_date"].max()),
            "rsi_output_first": int(rsi_output["trade_date"].min()),
            "rsi_output_latest": int(rsi_output["trade_date"].max()),
        },
        "latest": {
            "trade_date": int(rsi_output["trade_date"].iloc[-1]),
            "spy_close": float(rsi_output["spy_close"].iloc[-1]),
            "spy_wilder_rsi14": float(rsi_output["spy_wilder_rsi14"].iloc[-1]),
        },
        "internal_checks_pass": internal_pass,
        "strict_external_validation_pass": strict_external_pass,
        "strict_external_validation_failed": strict_external_failed,
        "strict_external_validation_pending": strict_external_pending,
        "WILDER_RSI14_REPAIR_V2_BUILD_PASS": build_pass,
        "note": (
            "Strict external validation fails only strict rows with entered external RSI, entered/matching external close, "
            "and RSI diff beyond tolerance. Diagnostic rows are documented but not build blockers."
        ),
    }

    manifest_path = audit_dir / f"spy_wilder_rsi14_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"RSI parquet:              {rsi_path}")
    print(f"RSI long history:         {rsi_long_path}")
    print(f"RSI output CSV:           {rsi_output_csv_path}")
    print(f"Request meta:             {request_meta_path}")
    print(f"Close compare:            {close_compare_path}")
    print(f"Validation template/file: {validation_path}")
    print(f"Validation summary:       {external_summary_path}")
    print(f"Internal checks:          {checks_path}")
    print(f"Manifest:                 {manifest_path}")

    banner("Final result")
    print(f"internal_checks_pass:                    {internal_pass}")
    print(f"strict_external_validation_pass:         {strict_external_pass}")
    print(f"strict_external_validation_failed:       {strict_external_failed}")
    print(f"strict_external_validation_pending:      {strict_external_pending}")
    print(f"WILDER_RSI14_REPAIR_V2_BUILD_PASS:       {build_pass}")

    if not build_pass:
        raise RuntimeError("WILDER_RSI14_REPAIR_V2_BUILD_PASS is False.")

    print("DONE — Wilder RSI14 v2 long-warmup repair complete.")


if __name__ == "__main__":
    main()
