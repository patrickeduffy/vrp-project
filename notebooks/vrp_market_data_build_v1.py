"""
VRP market data build v1 — canonical SPY EOD + realized-vol / RV21D layer.

Purpose
-------
Build the production market-data source of truth used by the VRP pipeline:
    1. Pull SPY EOD OHLCV from local ThetaData Terminal REST API v3.
    2. Pull in chunks because ThetaData v3 limits date windows.
    3. Normalize ThetaData list-style payloads.
    4. Drop and audit invalid OHLC rows and weekend rows.
    5. Store canonical SPY EOD prices.
    6. Compute realized-vol / realized-variance history, including rv21d_vol_pct.
    7. Build a clean Corsi/HAR support input panel.
    8. Write validation reports, dropped-row reports, raw history, and a manifest.

This script intentionally does NOT:
    - build VIX-style implied variance
    - fit or refit the Corsi/FDS forecast model
    - compute final VRP z-scores
    - apply Core/Secondary signal rules
    - apply sizing
    - update Streamlit directly

ThetaData endpoint used by default:
    http://127.0.0.1:25503/v3/stock/history/eod

Example
-------
py vrp_market_data_build_v1.py --project-root "C:\\Users\\patri\\vrp_project" --symbol SPY --start-date 20180101 --end-date 20260701 --force-full-refresh
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests


# ======================================================================================
# Configuration
# ======================================================================================

RAW_SUBDIR = Path("data/raw/thetadata/spy_eod_prices")
PROCESSED_SUBDIR = Path("data/processed/market_data")
AUDIT_SUBDIR = Path("data/audit/market_data")

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_THETADATA_URL = "http://127.0.0.1:25503/v3/stock/history/eod"
DEFAULT_SYMBOL = "SPY"
DEFAULT_START_DATE = "20180101"
DEFAULT_CHUNK_DAYS = 350
DEFAULT_SLEEP_SECONDS = 0.25

PRICE_OUTPUT_FILE = "spy_eod_prices_v1.parquet"
RV_OUTPUT_FILE = "spy_realized_vol_history_v1.parquet"
CORSI_OUTPUT_FILE = "spy_corsi_har_input_panel_v1.parquet"


# ======================================================================================
# Data classes
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    symbol: str
    start_date: str
    end_date: str
    thetadata_url: str
    chunk_days: int
    sleep_seconds: float
    force_full_refresh: bool

    @property
    def raw_dir(self) -> Path:
        return self.project_root / RAW_SUBDIR

    @property
    def processed_dir(self) -> Path:
        return self.project_root / PROCESSED_SUBDIR

    @property
    def audit_dir(self) -> Path:
        return self.project_root / AUDIT_SUBDIR


# ======================================================================================
# Helpers
# ======================================================================================

def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_dirs(cfg: Config) -> None:
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)
    cfg.processed_dir.mkdir(parents=True, exist_ok=True)
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)


def yyyymmdd(ts: pd.Timestamp) -> str:
    return pd.Timestamp(ts).strftime("%Y%m%d")


def parse_yyyymmdd(value: str) -> pd.Timestamp:
    try:
        parsed = pd.to_datetime(str(value), format="%Y%m%d", errors="raise")
        return pd.Timestamp(parsed).normalize()
    except Exception as exc:
        raise ValueError(f"Date must be YYYYMMDD. Got {value!r}") from exc

def make_chunks(start_date: str, end_date: str, chunk_days: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    if chunk_days <= 0 or chunk_days > 365:
        raise ValueError("chunk_days must be between 1 and 365")

    start = parse_yyyymmdd(start_date)
    end = parse_yyyymmdd(end_date)
    if end < start:
        raise ValueError(f"end_date {end_date} is before start_date {start_date}")

    chunks: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + pd.Timedelta(days=chunk_days), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + pd.Timedelta(days=1)
    return chunks


def is_list_like_cell(x: Any) -> bool:
    return isinstance(x, (list, tuple, np.ndarray, pd.Series))


def unwrap_cell(x: Any) -> Any:
    if is_list_like_cell(x):
        return x[0] if len(x) > 0 else pd.NA
    return x


def theta_payload_to_frame(payload: Any) -> pd.DataFrame:
    """Normalize ThetaData v3 JSON into one row per trading observation.

    ThetaData v3 may return either:
      - a dict of list-valued fields,
      - a list of row dicts,
      - a wrapper dict with data/response.
    In the one-day case it may return one row whose cells are one-item lists.
    """
    if isinstance(payload, dict):
        if "response" in payload:
            payload = payload["response"]
        elif "data" in payload:
            payload = payload["data"]

    raw = pd.DataFrame(payload)
    if raw.empty:
        return raw

    # Case: one row with full columns as arrays/lists.
    if len(raw) == 1 and any(is_list_like_cell(raw.iloc[0][c]) for c in raw.columns):
        max_len = 1
        for c in raw.columns:
            value = raw.iloc[0][c]
            if is_list_like_cell(value):
                max_len = max(max_len, len(value))

        expanded: Dict[str, Sequence[Any]] = {}
        for c in raw.columns:
            value = raw.iloc[0][c]
            if is_list_like_cell(value):
                values = list(value)
                if len(values) == max_len:
                    expanded[c] = values
                elif len(values) == 1:
                    expanded[c] = values * max_len
                else:
                    raise ValueError(f"Column {c!r} had list length {len(values)} but max length is {max_len}")
            else:
                expanded[c] = [value] * max_len
        raw = pd.DataFrame(expanded)

    # Case: cells are still one-item lists.
    for col in raw.columns:
        raw[col] = raw[col].apply(unwrap_cell)

    return raw


def fetch_thetadata_eod(cfg: Config, timestamp: str) -> Tuple[pd.DataFrame, Path, List[Dict[str, Any]]]:
    chunks = make_chunks(cfg.start_date, cfg.end_date, cfg.chunk_days)
    frames: List[pd.DataFrame] = []
    chunk_meta: List[Dict[str, Any]] = []

    print("=" * 100)
    print("Fetching ThetaData SPY EOD history in chunks")
    print("=" * 100)
    print(f"URL:        {cfg.thetadata_url}")
    print(f"Symbol:     {cfg.symbol}")
    print(f"Start:      {cfg.start_date}")
    print(f"End:        {cfg.end_date}")
    print(f"Chunks:     {len(chunks)}")
    print(f"Chunk days: {cfg.chunk_days}")

    for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        s = yyyymmdd(chunk_start)
        e = yyyymmdd(chunk_end)
        params = {
            "symbol": cfg.symbol,
            "start_date": s,
            "end_date": e,
            "format": "json",
        }

        print(f"\nChunk {i}/{len(chunks)}: {s} to {e}")
        response = requests.get(cfg.thetadata_url, params=params, timeout=300)
        print(f"HTTP status: {response.status_code}")

        if response.status_code != 200:
            print(response.text[:2000])
            raise RuntimeError(f"ThetaData request failed for chunk {s} to {e}")

        payload = response.json()
        frame = theta_payload_to_frame(payload)
        print(f"Rows returned after normalize: {len(frame)}")

        chunk_meta.append({
            "chunk_number": i,
            "request_start_date": s,
            "request_end_date": e,
            "http_status": response.status_code,
            "rows_after_normalize": int(len(frame)),
        })

        if not frame.empty:
            frame["request_start_date"] = s
            frame["request_end_date"] = e
            frames.append(frame)

        if cfg.sleep_seconds > 0:
            time.sleep(cfg.sleep_seconds)

    if not frames:
        raise RuntimeError("ThetaData returned zero rows across all chunks")

    raw = pd.concat(frames, ignore_index=True)
    raw_path = cfg.raw_dir / f"{cfg.symbol}_EOD_raw_chunked_{cfg.start_date}_{cfg.end_date}_{timestamp}.parquet"
    raw.to_parquet(raw_path, index=False)

    print("\nCombined raw shape:", raw.shape)
    print("Combined raw columns:", list(raw.columns))

    return raw, raw_path, chunk_meta


def normalize_prices(raw: pd.DataFrame, cfg: Config, timestamp: str) -> pd.DataFrame:
    df = raw.copy()
    date_col = "last_trade" if "last_trade" in df.columns else "created" if "created" in df.columns else None
    if date_col is None:
        raise RuntimeError(f"No usable date column found. Columns={list(df.columns)}")

    required = ["open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required OHLCV columns: {missing}. Columns={list(df.columns)}")

    df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    prices = (
        df[["trade_date", "open", "high", "low", "close", "volume"]]
        .rename(columns={
            "open": "spy_open",
            "high": "spy_high",
            "low": "spy_low",
            "close": "spy_close",
            "volume": "spy_volume",
        })
        .dropna(subset=["trade_date"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    prices["source"] = "ThetaData v3 stock/history/eod"
    prices["source_symbol"] = cfg.symbol
    prices["source_date_column"] = date_col
    prices["source_run_timestamp"] = timestamp

    return prices


def audit_and_clean_prices(prices: pd.DataFrame, cfg: Config, timestamp: str) -> Tuple[pd.DataFrame, Path, Path]:
    prices_start = prices.copy()

    bad_rows = prices_start[
        (prices_start["spy_open"] <= 0)
        | (prices_start["spy_high"] <= 0)
        | (prices_start["spy_low"] <= 0)
        | (prices_start["spy_close"] <= 0)
        | prices_start[["spy_open", "spy_high", "spy_low", "spy_close"]].isna().any(axis=1)
    ].copy()
    bad_rows["drop_reason"] = "non_positive_or_missing_ohlc"

    bad_rows_path = cfg.audit_dir / f"spy_eod_dropped_bad_rows_{timestamp}.csv"
    bad_rows.to_csv(bad_rows_path, index=False)

    prices_no_bad = prices_start[~prices_start.index.isin(bad_rows.index)].copy()
    prices_no_bad = prices_no_bad.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)
    prices_no_bad["clean_run_timestamp"] = timestamp

    weekend_rows = prices_no_bad[prices_no_bad["trade_date"].dt.dayofweek.isin([5, 6])].copy()
    weekend_rows["drop_reason"] = "weekend_non_trading_date"

    weekend_rows_path = cfg.audit_dir / f"spy_eod_dropped_weekend_rows_{timestamp}.csv"
    weekend_rows.to_csv(weekend_rows_path, index=False)

    prices_final = prices_no_bad[~prices_no_bad["trade_date"].dt.dayofweek.isin([5, 6])].copy()
    prices_final = prices_final.sort_values("trade_date").drop_duplicates("trade_date", keep="last").reset_index(drop=True)
    prices_final["final_clean_run_timestamp"] = timestamp

    print("\nDropped invalid OHLC rows:", len(bad_rows))
    if len(bad_rows):
        print(bad_rows[["trade_date", "spy_open", "spy_high", "spy_low", "spy_close", "spy_volume", "drop_reason"]].to_string(index=False))

    print("Dropped weekend rows:", len(weekend_rows))
    if len(weekend_rows):
        print(weekend_rows[["trade_date", "spy_open", "spy_high", "spy_low", "spy_close", "spy_volume", "drop_reason"]].to_string(index=False))

    return prices_final, bad_rows_path, weekend_rows_path


def build_realized_vol(prices: pd.DataFrame) -> pd.DataFrame:
    rv = prices.copy()
    rv["spy_log_return"] = np.log(rv["spy_close"] / rv["spy_close"].shift(1))

    for w in [1, 5, 10, 21, 63]:
        if w == 1:
            rv[f"spy_rv_{w}d"] = rv["spy_log_return"] ** 2 * 252.0
        else:
            rv[f"spy_rv_{w}d"] = rv["spy_log_return"].pow(2).rolling(w).mean() * 252.0
        rv[f"spy_vol_{w}d_pct"] = np.sqrt(rv[f"spy_rv_{w}d"]) * 100.0

    rv["rv21d_vol_pct"] = rv["spy_log_return"].rolling(21).std() * np.sqrt(252.0) * 100.0
    rv["rv21d_source"] = (
        "ThetaData SPY close; zero/missing OHLC rows and weekend rows removed; "
        "rolling 21 trading-day std(log return) * sqrt(252) * 100"
    )
    return rv


def build_corsi_support_panel(rv: pd.DataFrame) -> pd.DataFrame:
    corsi = rv[[
        "trade_date",
        "spy_close",
        "spy_log_return",
        "spy_rv_1d",
        "spy_rv_5d",
        "spy_rv_10d",
        "spy_rv_21d",
        "spy_rv_63d",
        "spy_vol_5d_pct",
        "spy_vol_10d_pct",
        "spy_vol_21d_pct",
        "spy_vol_63d_pct",
        "rv21d_vol_pct",
    ]].copy()

    eps = 1e-12
    for c in ["spy_rv_1d", "spy_rv_5d", "spy_rv_10d", "spy_rv_21d", "spy_rv_63d"]:
        corsi[f"log_{c}"] = np.log(corsi[c].clip(lower=eps))
        corsi[f"{c}_lag1"] = corsi[c].shift(1)
        corsi[f"log_{c}_lag1"] = np.log(corsi[f"{c}_lag1"].clip(lower=eps))

    corsi["rv21d_vol_pct_lag1"] = corsi["rv21d_vol_pct"].shift(1)
    return corsi


def validate_outputs(prices: pd.DataFrame, rv: pd.DataFrame, cfg: Config, timestamp: str) -> Tuple[pd.DataFrame, Path]:
    validation: List[Dict[str, str]] = []

    def add_check(check: str, status: str, detail: str) -> None:
        validation.append({"check": check, "status": status, "detail": detail})

    add_check("rows_after_final_clean", "PASS" if len(prices) > 0 else "FAIL", f"rows={len(prices)}")
    add_check("unique_trade_dates", "PASS" if prices["trade_date"].is_unique else "FAIL", "trade_date unique check")

    add_check(
        "positive_prices",
        "PASS" if (prices[["spy_open", "spy_high", "spy_low", "spy_close"]] > 0).all().all() else "FAIL",
        "OHLC prices must be positive",
    )

    add_check(
        "ohlc_relationships",
        "PASS" if (
            (prices["spy_high"] >= prices[["spy_open", "spy_close", "spy_low"]].max(axis=1)).all()
            and (prices["spy_low"] <= prices[["spy_open", "spy_close", "spy_high"]].min(axis=1)).all()
        ) else "FAIL",
        "high must be >= O/C/L and low must be <= O/C/H",
    )

    add_check(
        "finite_log_returns_after_first_row",
        "PASS" if np.isfinite(rv.loc[rv.index >= 1, "spy_log_return"]).all() else "FAIL",
        "log returns should be finite after first row",
    )

    add_check(
        "rv21d_column_created",
        "PASS" if "rv21d_vol_pct" in rv.columns else "FAIL",
        "rv21d_vol_pct exists",
    )

    add_check(
        "rv21d_non_null_after_warmup",
        "PASS" if rv.loc[rv.index >= 21, "rv21d_vol_pct"].notna().all() else "FAIL",
        "rv21d should be non-null after 21-row warmup",
    )

    add_check(
        "no_weekend_trade_dates",
        "PASS" if not prices["trade_date"].dt.dayofweek.isin([5, 6]).any() else "FAIL",
        "No Saturday/Sunday trade dates",
    )

    validation_df = pd.DataFrame(validation)
    validation_path = cfg.audit_dir / f"spy_market_data_validation_final_cleaned_{timestamp}.csv"
    validation_df.to_csv(validation_path, index=False)
    return validation_df, validation_path


def write_outputs(
    prices: pd.DataFrame,
    rv: pd.DataFrame,
    corsi: pd.DataFrame,
    raw_path: Path,
    bad_rows_path: Path,
    weekend_rows_path: Path,
    validation_path: Path,
    chunk_meta: List[Dict[str, Any]],
    cfg: Config,
    timestamp: str,
) -> Dict[str, Path]:
    prices_path = cfg.processed_dir / PRICE_OUTPUT_FILE
    rv_path = cfg.processed_dir / RV_OUTPUT_FILE
    corsi_path = cfg.processed_dir / CORSI_OUTPUT_FILE

    prices.to_parquet(prices_path, index=False)
    rv.to_parquet(rv_path, index=False)
    corsi.to_parquet(corsi_path, index=False)

    manifest = {
        "timestamp": timestamp,
        "config": {
            **asdict(cfg),
            "project_root": str(cfg.project_root),
        },
        "process": "thetadata_v3_chunked_spy_eod_market_data_build_v1",
        "chunk_meta": chunk_meta,
        "rows": {
            "prices_final": int(len(prices)),
            "rv": int(len(rv)),
            "corsi": int(len(corsi)),
        },
        "date_range": {
            "min_trade_date": str(prices["trade_date"].min().date()) if len(prices) else None,
            "max_trade_date": str(prices["trade_date"].max().date()) if len(prices) else None,
        },
        "latest": {
            "trade_date": str(rv["trade_date"].iloc[-1].date()) if len(rv) else None,
            "spy_close": float(rv["spy_close"].iloc[-1]) if len(rv) else None,
            "rv21d_vol_pct": float(rv["rv21d_vol_pct"].iloc[-1]) if len(rv) else None,
        },
        "outputs": {
            "raw_path": str(raw_path),
            "bad_rows_path": str(bad_rows_path),
            "weekend_rows_path": str(weekend_rows_path),
            "prices_path": str(prices_path),
            "rv_path": str(rv_path),
            "corsi_path": str(corsi_path),
            "validation_path": str(validation_path),
        },
    }

    manifest_path = cfg.audit_dir / f"spy_market_data_manifest_final_cleaned_{timestamp}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)

    return {
        "prices_path": prices_path,
        "rv_path": rv_path,
        "corsi_path": corsi_path,
        "manifest_path": manifest_path,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    parser = argparse.ArgumentParser(description="Build canonical SPY EOD + realized-vol/RV21D layer from ThetaData v3.")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT), help="Project root. Default: C:\\Users\\patri\\vrp_project")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Ticker symbol. Default: SPY")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Start date YYYYMMDD. Default: 20180101")
    parser.add_argument("--end-date", required=True, help="End date YYYYMMDD")
    parser.add_argument("--thetadata-url", default=DEFAULT_THETADATA_URL, help="ThetaData v3 stock EOD endpoint")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="Calendar days per request. Must be <= 365. Default: 350")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS, help="Sleep between chunk requests. Default: 0.25")
    parser.add_argument("--force-full-refresh", action="store_true", help="Accepted for explicit full rebuild. Current script always rebuilds requested range.")

    args = parser.parse_args(argv)
    return Config(
        project_root=Path(args.project_root),
        symbol=args.symbol.upper(),
        start_date=args.start_date,
        end_date=args.end_date,
        thetadata_url=args.thetadata_url,
        chunk_days=args.chunk_days,
        sleep_seconds=args.sleep_seconds,
        force_full_refresh=bool(args.force_full_refresh),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    timestamp = now_stamp()
    ensure_dirs(cfg)

    print("=" * 100)
    print("VRP market data build v1")
    print("=" * 100)
    print(f"Project root: {cfg.project_root}")
    print(f"ThetaData URL: {cfg.thetadata_url}")

    if "25510" in cfg.thetadata_url or "/v2/" in cfg.thetadata_url:
        raise RuntimeError(
            "This script is for ThetaData v3. URL still points to old v2/25510 endpoint: "
            f"{cfg.thetadata_url}"
        )

    raw, raw_path, chunk_meta = fetch_thetadata_eod(cfg, timestamp)
    prices_raw = normalize_prices(raw, cfg, timestamp)
    prices_final, bad_rows_path, weekend_rows_path = audit_and_clean_prices(prices_raw, cfg, timestamp)
    rv_final = build_realized_vol(prices_final)
    corsi_final = build_corsi_support_panel(rv_final)
    validation_df, validation_path = validate_outputs(prices_final, rv_final, cfg, timestamp)
    paths = write_outputs(
        prices=prices_final,
        rv=rv_final,
        corsi=corsi_final,
        raw_path=raw_path,
        bad_rows_path=bad_rows_path,
        weekend_rows_path=weekend_rows_path,
        validation_path=validation_path,
        chunk_meta=chunk_meta,
        cfg=cfg,
        timestamp=timestamp,
    )

    print("\nValidation:")
    print(validation_df.to_string(index=False))

    print("\nLatest realized-vol row:")
    cols = ["trade_date", "spy_close", "spy_log_return", "spy_rv_1d", "spy_rv_5d", "spy_rv_21d", "spy_vol_21d_pct", "rv21d_vol_pct"]
    print(rv_final[cols].tail(1).to_string(index=False))

    print("\nSaved files:")
    print(paths["prices_path"])
    print(paths["rv_path"])
    print(paths["corsi_path"])
    print(validation_path)
    print(paths["manifest_path"])

    if (validation_df["status"] == "FAIL").any():
        raise RuntimeError("Validation failed. Review validation table above.")

    print("\nDONE — final clean canonical SPY market data layer rebuilt.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
