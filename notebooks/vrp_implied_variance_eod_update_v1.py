#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_implied_variance_eod_update_v1.py

Production updater wrapper for the SPX VIX-style implied-variance term structure.

Purpose
-------
Use the existing v0.7 VIX-style term-structure notebook functions to update the canonical
implied-variance surface only when completed EOD dates are missing.

This script is intentionally conservative:
    - Default mode is check-only.
    - It never silently falls back to stale data.
    - It writes a new production copy under data/processed/implied_variance by default.
    - It can optionally overwrite/update the repaired canonical file only with --write-canonical.
    - Every run writes audit files and a manifest.

Canonical seed implied-variance file:
    data/processed/vix_term_structure_history_v0_7_1_repaired_total_variance.parquet

Primary source notebook for VIX functions:
    notebooks v0/old/01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb

Examples
--------
Check what is missing:
    py vrp_implied_variance_eod_update_v1.py --project-root "C:\\Users\\patri\\vrp_project" --check-only

Test one date without overwriting canonical repaired file:
    py vrp_implied_variance_eod_update_v1.py --project-root "C:\\Users\\patri\\vrp_project" --single-date 2026-07-02

Update only missing completed EOD dates, safety-capped to 3 dates:
    py vrp_implied_variance_eod_update_v1.py --project-root "C:\\Users\\patri\\vrp_project" --update-missing --max-dates 3
"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import math
import os
import pickle
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

try:
    import pandas_market_calendars as mcal  # type: ignore
except Exception:  # pragma: no cover - optional dependency on user machine
    mcal = None

try:
    import exchange_calendars as xcals  # type: ignore
except Exception:  # pragma: no cover - optional fallback
    xcals = None


# ======================================================================================
# Defaults
# ======================================================================================

DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_THETADATA_BASE_URL = "http://127.0.0.1:25503/v3"
DEFAULT_TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

DEFAULT_SOURCE_NOTEBOOK_REL = Path(
    r"notebooks v0\old\01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb"
)

DEFAULT_SEED_REL = Path(r"data\processed\vix_term_structure_history_v0_7_1_repaired_total_variance.parquet")
DEFAULT_SEED_CSV_REL = Path(r"data\processed\vix_term_structure_history_v0_7_1_repaired_total_variance.csv")

DEFAULT_OUTPUT_DIR_REL = Path(r"data\processed\implied_variance")
DEFAULT_AUDIT_DIR_REL = Path(r"data\audit\implied_variance")
DEFAULT_CHAIN_CACHE_REL = Path(r"data\raw\thetadata_chains")
DEFAULT_EXTERNAL_DIR_REL = Path(r"data\external")

OUTPUT_SURFACE_FILE = "spx_vix_style_implied_variance_surface_v1.parquet"
OUTPUT_SURFACE_CSV_FILE = "spx_vix_style_implied_variance_surface_v1.csv"
OUTPUT_LATEST_FILE = "spx_vix_style_implied_variance_latest_snapshot_v1.parquet"
OUTPUT_LATEST_CSV_FILE = "spx_vix_style_implied_variance_latest_snapshot_v1.csv"

UPDATER_VERSION = "vrp_implied_variance_eod_update_v1"
SOURCE_METHODOLOGY_VERSION = "v0_7_1_repaired_total_variance_seed_plus_v0_7_market_close_update"

EXPECTED_MIN_FUNCTIONS = [
    "calculate_vix_term_structure_for_date_v7_cached",
    "find_missing_trade_dates_v7",
    "upsert_term_structure_history",
]


# ======================================================================================
# Config / helpers
# ======================================================================================

@dataclass(frozen=True)
class Config:
    project_root: Path
    source_notebook_path: Path
    seed_path: Path
    output_dir: Path
    audit_dir: Path
    chain_cache_dir: Path
    external_dir: Path
    thetadata_base_url: str
    target_tenors: List[int]
    run_timestamp: str
    check_only: bool
    update_missing: bool
    single_date: Optional[pd.Timestamp]
    start_date: Optional[pd.Timestamp]
    end_date: Optional[pd.Timestamp]
    max_dates: Optional[int]
    force_refresh: bool
    write_canonical: bool
    max_workers: int
    require_thetadata: bool
    quote_time: Optional[str]



def _coerce_yyyymmdd_date(value: Any) -> pd.Timestamp:
    """Normalize YYYYMMDD/date-like inputs to a naive midnight Timestamp."""
    if isinstance(value, int) or (
        isinstance(value, str) and value.isdigit() and len(value) == 8
    ):
        return pd.Timestamp(pd.to_datetime(str(value), format="%Y%m%d", errors="raise")).normalize()
    return pd.Timestamp(pd.to_datetime(value, errors="raise")).normalize()


@lru_cache(maxsize=1)
def _production_xnys_close_minutes_by_date() -> Dict[int, int]:
    """Build the XNYS close-minute lookup once for the supported expiration range."""
    start_date = "2009-01-01"
    end_date = "2035-12-31"
    if mcal is not None:
        cal = mcal.get_calendar("XNYS")
        schedule = cal.schedule(start_date=start_date, end_date=end_date)
        rows = schedule["market_close"].items()
    elif xcals is not None:
        cal = xcals.get_calendar("XNYS")
        sessions = cal.sessions_in_range(start_date, end_date)
        rows = ((session, cal.session_close(session)) for session in sessions)
    else:
        raise RuntimeError(
            "An XNYS calendar package is required for expiration-aware SPXW settlement timing."
        )

    result: Dict[int, int] = {}
    for session, close_ts in rows:
        close_ts = pd.Timestamp(close_ts)
        if close_ts.tzinfo is None:
            close_ts = close_ts.tz_localize("UTC")
        close_et = close_ts.tz_convert("America/New_York")
        session_key = int(pd.Timestamp(session).strftime("%Y%m%d"))
        result[session_key] = int(close_et.hour * 60 + close_et.minute)
    return result


def production_spxw_settlement_minutes(expiration_date: Any) -> int:
    """Return the official XNYS close minute for a PM-settled SPXW expiration.

    Normal sessions settle at 16:00 ET. Early-close expirations settle at the
    actual exchange close (generally 13:00 ET). A missing calendar dependency or
    non-session expiration is a hard error; production must not silently assume
    a normal close.
    """
    exp_ts = _coerce_yyyymmdd_date(expiration_date)
    expiration_key = int(exp_ts.strftime("%Y%m%d"))
    close_minutes = _production_xnys_close_minutes_by_date()
    if expiration_key not in close_minutes:
        raise RuntimeError(
            f"SPXW expiration {exp_ts.date()} is not an XNYS trading session "
            "inside the supported 2009-2035 calendar range."
        )
    return close_minutes[expiration_key]


def production_settlement_minutes_after_midnight_et(root: str, expiration_date: Any) -> int:
    """Settlement minute used by the repaired VIX-style expiration clock."""
    root_text = str(root).upper()
    if root_text == "SPX":
        return 9 * 60 + 30
    if root_text == "SPXW":
        return production_spxw_settlement_minutes(expiration_date)
    raise ValueError(f"Unknown option root: {root!r}")


def production_minutes_to_expiry_vix_method(
    trade_date: Any,
    exp_yyyymmdd: Any,
    root: str,
    calc_time_ms: int,
) -> int:
    """Minutes from quote time to expiration using the expiration's actual schedule."""
    trade_ts = _coerce_yyyymmdd_date(trade_date)
    expiration_ts = _coerce_yyyymmdd_date(exp_yyyymmdd)
    calculation_minutes = int(calc_time_ms // 60000)
    settlement_minutes = production_settlement_minutes_after_midnight_et(
        root, exp_yyyymmdd
    )
    calendar_days = int((expiration_ts - trade_ts).days)
    return calendar_days * 24 * 60 + settlement_minutes - calculation_minutes


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def print_header(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def parse_date(value: Optional[str], label: str) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return pd.Timestamp(datetime.strptime(s, fmt)).normalize()
        except Exception:
            pass
    raise ValueError(f"{label} must be YYYY-MM-DD or YYYYMMDD. Got {value!r}")


def parse_trade_date_series(series: pd.Series) -> pd.Series:
    """Robust parser for datetime, YYYY-MM-DD, YYYYMMDD string, and numeric YYYYMMDD."""
    s = series.copy()
    if pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, errors="raise").dt.normalize()

    non_null = s.dropna()
    if len(non_null) == 0:
        return pd.to_datetime(s, errors="coerce").dt.normalize()

    if pd.api.types.is_numeric_dtype(non_null):
        vals = pd.to_numeric(s, errors="coerce")
        rounded = vals.round()
        numeric_non_null = vals.dropna()
        rounded_non_null = rounded.dropna()
        integer_like = len(numeric_non_null) == len(rounded_non_null) and (
            (numeric_non_null - rounded_non_null).abs() < 1e-6
        ).all()
        if integer_like:
            as_str = rounded.astype("Int64").astype("string").str.zfill(8)
            if as_str.dropna().str.fullmatch(r"\d{8}").all():
                return pd.to_datetime(as_str, format="%Y%m%d", errors="raise").dt.normalize()

    as_str = s.astype("string").str.strip()
    if as_str.dropna().str.fullmatch(r"\d{8}").all():
        return pd.to_datetime(as_str, format="%Y%m%d", errors="raise").dt.normalize()
    return pd.to_datetime(s, errors="raise").dt.normalize()


def select_prior_rate_record(
    rates: pd.DataFrame, trade_date: Any, symbol: str = "SOFR"
) -> Dict[str, Any]:
    """Select the latest rate observation strictly before trade_date."""
    td = pd.Timestamp(trade_date)
    if isinstance(trade_date, int) or (
        isinstance(trade_date, str) and trade_date.isdigit() and len(trade_date) == 8
    ):
        td = pd.to_datetime(str(trade_date), format="%Y%m%d")
    td = pd.Timestamp(td).normalize()
    work = rates.copy()
    if "trade_date" not in work.columns or "rate_decimal" not in work.columns:
        raise KeyError("Rate history requires trade_date and rate_decimal columns.")
    work["trade_date"] = parse_trade_date_series(work["trade_date"])
    work["rate_decimal"] = pd.to_numeric(work["rate_decimal"], errors="coerce")
    work = work.loc[work["trade_date"].lt(td) & work["rate_decimal"].notna()].copy()
    if work.empty:
        raise ValueError(f"No {symbol} observation exists strictly before {td.date()}")
    selected = work.sort_values("trade_date").iloc[-1]
    rate_decimal = float(selected["rate_decimal"])
    return {
        "rate_observation_date": pd.Timestamp(selected["trade_date"]).normalize(),
        "rate_decimal": rate_decimal,
        "rate_pct": rate_decimal * 100.0,
        "rate_selection_rule": "latest_observation_strictly_before_trade_date",
    }


def date_str(ts: Any) -> str:
    if pd.isna(ts):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d")


def yyyymmdd_int(ts: Any) -> int:
    return int(pd.Timestamp(ts).strftime("%Y%m%d"))


def yyyymmdd_str(ts: Any) -> str:
    return pd.Timestamp(ts).strftime("%Y%m%d")


def ensure_dirs(cfg: Config) -> None:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)
    cfg.chain_cache_dir.mkdir(parents=True, exist_ok=True)


# ======================================================================================
# Calendar / missing-date logic
# ======================================================================================


def get_xnys_trading_dates(start: pd.Timestamp, end: pd.Timestamp) -> List[pd.Timestamp]:
    start = pd.Timestamp(start).normalize()
    end = pd.Timestamp(end).normalize()
    if end < start:
        return []

    if mcal is not None:
        cal = mcal.get_calendar("XNYS")
        sched = cal.schedule(start_date=start.date(), end_date=end.date())
        return [pd.Timestamp(x).normalize() for x in sched.index]

    # Fallback: weekdays only. Explicit warning is emitted by caller.
    days = pd.date_range(start, end, freq="B")
    return [pd.Timestamp(x).normalize() for x in days]


def latest_completed_xnys_date(now_et: Optional[datetime] = None) -> pd.Timestamp:
    """Return latest completed NYSE trading date, using XNYS close + 30 min buffer when possible."""
    tz = ZoneInfo("America/New_York")
    now_et = now_et or datetime.now(tz)
    today = pd.Timestamp(now_et.date()).normalize()
    lookback_start = today - pd.Timedelta(days=14)

    if mcal is not None:
        cal = mcal.get_calendar("XNYS")
        sched = cal.schedule(start_date=lookback_start.date(), end_date=today.date())
        if sched.empty:
            raise RuntimeError("Could not determine latest completed XNYS date: empty calendar schedule.")
        sched_local = sched.copy()
        sched_local["market_close"] = sched_local["market_close"].dt.tz_convert("America/New_York")
        completed = sched_local[sched_local["market_close"] + pd.Timedelta(minutes=30) <= pd.Timestamp(now_et)]
        if completed.empty:
            # Before first close in the lookback range, use previous scheduled date if available.
            prior = sched_local.iloc[:-1]
            if prior.empty:
                raise RuntimeError("Could not determine latest completed XNYS date before today.")
            return pd.Timestamp(prior.index[-1]).normalize()
        return pd.Timestamp(completed.index[-1]).normalize()

    # Fallback: weekdays and 17:00 ET cutoff.
    cutoff = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    candidate = today if now_et >= cutoff and today.weekday() < 5 else today - pd.Timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= pd.Timedelta(days=1)
    return candidate.normalize()


def determine_target_dates(cfg: Config, existing_dates: Sequence[pd.Timestamp]) -> Tuple[List[pd.Timestamp], Dict[str, Any]]:
    existing = sorted({pd.Timestamp(x).normalize() for x in existing_dates})
    existing_max = max(existing) if existing else None

    mode_detail: Dict[str, Any] = {
        "mode": "unknown",
        "existing_max_date": date_str(existing_max) if existing_max is not None else None,
        "latest_completed_eod_date": None,
        "candidate_date_count_before_cap": None,
        "candidate_date_count_after_cap": None,
        "calendar_source": "XNYS/pandas_market_calendars" if mcal is not None else "weekday_fallback",
    }

    if cfg.single_date is not None:
        dates = [cfg.single_date]
        mode_detail["mode"] = "single_date"
    else:
        if cfg.start_date is not None and cfg.end_date is not None:
            start = cfg.start_date
            end = cfg.end_date
            mode_detail["mode"] = "explicit_window"
        elif cfg.start_date is not None and cfg.end_date is None:
            end = latest_completed_xnys_date()
            start = cfg.start_date
            mode_detail["mode"] = "start_to_latest_completed"
        elif cfg.update_missing or cfg.check_only:
            end = latest_completed_xnys_date()
            if existing_max is None:
                raise RuntimeError("No existing dates found in seed/output; provide --start-date for initial build.")
            start = existing_max + pd.Timedelta(days=1)
            mode_detail["mode"] = "missing_after_existing_max"
        else:
            end = latest_completed_xnys_date()
            if existing_max is None:
                raise RuntimeError("No existing dates found; provide --single-date or --start-date/--end-date.")
            start = existing_max + pd.Timedelta(days=1)
            mode_detail["mode"] = "default_missing_after_existing_max"

        mode_detail["latest_completed_eod_date"] = date_str(end)
        candidates = get_xnys_trading_dates(start, end)
        existing_set = set(existing)
        if cfg.force_refresh or cfg.single_date is not None or cfg.start_date is not None:
            dates = candidates
        else:
            dates = [d for d in candidates if d not in existing_set]

    mode_detail["candidate_date_count_before_cap"] = len(dates)
    if cfg.max_dates is not None:
        dates = dates[: int(cfg.max_dates)]
    mode_detail["candidate_date_count_after_cap"] = len(dates)
    return dates, mode_detail


# ======================================================================================
# Canonical surface load / normalization
# ======================================================================================


def find_existing_or_seed_surface(cfg: Config) -> Path:
    production_surface = cfg.output_dir / OUTPUT_SURFACE_FILE
    if production_surface.exists():
        return production_surface
    return cfg.seed_path


def normalize_surface(df: pd.DataFrame, label: str) -> pd.DataFrame:
    out = df.copy()

    if "trade_date" not in out.columns:
        raise ValueError(f"{label} missing required trade_date column")
    out["trade_date"] = parse_trade_date_series(out["trade_date"])

    if "tenor" not in out.columns:
        if "target_days" in out.columns:
            out["tenor"] = out["target_days"]
        else:
            raise ValueError(f"{label} missing tenor/target_days column")

    out["tenor"] = pd.to_numeric(out["tenor"], errors="raise").astype(int)
    if "target_days" not in out.columns:
        out["target_days"] = out["tenor"]
    else:
        out["target_days"] = pd.to_numeric(out["target_days"], errors="coerce").fillna(out["tenor"]).astype(int)

    if "implied_variance" not in out.columns:
        raise ValueError(f"{label} missing implied_variance column")
    out["implied_variance"] = pd.to_numeric(out["implied_variance"], errors="raise")

    if "vix_style_vol" not in out.columns:
        out["vix_style_vol"] = np.sqrt(out["implied_variance"]) * 100.0
    else:
        out["vix_style_vol"] = pd.to_numeric(out["vix_style_vol"], errors="raise")

    return out


def load_surface(cfg: Config) -> Tuple[pd.DataFrame, Path]:
    source_path = find_existing_or_seed_surface(cfg)
    if not source_path.exists():
        raise FileNotFoundError(f"Missing implied-variance seed/source file: {source_path}")
    df = pd.read_parquet(source_path)
    return normalize_surface(df, "surface"), source_path


# ======================================================================================
# VIX notebook function loading
# ======================================================================================


def read_ipynb_code_cells(path: Path) -> List[str]:
    raw = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    cells: List[str] = []
    for cell in raw.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(src)
        cells.append(str(src))
    return cells


def safe_source_segment(source: str, node: ast.AST) -> Optional[str]:
    try:
        return ast.get_source_segment(source, node)
    except Exception:
        return None


def extract_top_level_defs_from_cell(source: str) -> Tuple[List[str], List[str]]:
    """Return source code for top-level function/class defs from one cell, plus warnings."""
    warnings: List[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        # Regex fallback: capture top-level def/class blocks roughly.
        warnings.append(f"cell_parse_warning={exc}")
        lines = source.splitlines()
        starts: List[int] = []
        for i, line in enumerate(lines):
            if line.startswith("def ") or line.startswith("class "):
                starts.append(i)
        blocks: List[str] = []
        for idx, start in enumerate(starts):
            end = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
            blocks.append("\n".join(lines[start:end]))
        return blocks, warnings

    blocks = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            seg = safe_source_segment(source, node)
            if seg:
                blocks.append(seg)
    return blocks, warnings


def build_vix_namespace(cfg: Config) -> Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """
    Load canonical v0.7 VIX functions from the generated source dump.

    Uses AST extraction so multi-line function signatures are preserved.
    Also defines historical notebook globals needed by default arguments.
    """
    import ast

    source_dump_candidates = sorted(
        (cfg.project_root / "data" / "audit" / "production_inventory").glob(
            "vrp_vix_function_source_dump_*_source_dump.py"
        ),
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )

    if not source_dump_candidates:
        raise FileNotFoundError(
            "No vrp_vix_function_source_dump_*_source_dump.py found under "
            f"{cfg.project_root / 'data' / 'audit' / 'production_inventory'}"
        )

    source_dump_path = source_dump_candidates[0]

    ns: Dict[str, Any] = {
        "pd": pd,
        "np": np,
        "math": math,
        "os": os,
        "re": re,
        "json": json,
        "pickle": pickle,
        "time": time,
        "requests": requests,
        "Path": Path,
        "datetime": datetime,
        "date": date,
        "timedelta": timedelta,
        "dt_time": dt_time,
        "ZoneInfo": ZoneInfo,
        "ThreadPoolExecutor": ThreadPoolExecutor,
        "as_completed": as_completed,
        "Any": Any,
        "Dict": Dict,
        "Iterable": Iterable,
        "List": List,
        "Optional": Optional,
        "Sequence": Sequence,
        "Tuple": Tuple,
        "mcal": mcal,

        # Historical notebook globals / aliases.
        "PROJECT_ROOT": cfg.project_root,
        "PROJECT_DIR": cfg.project_root,
        "ROOT_DIR": cfg.project_root,
        "BASE_URL": cfg.thetadata_base_url,
        "THETADATA_BASE_URL": cfg.thetadata_base_url,
        "V3_BASE_URL": cfg.thetadata_base_url,
        "DATA_DIR": cfg.project_root / "data",
        "RAW_DIR": cfg.project_root / "data" / "raw",
        "PROCESSED_DIR": cfg.project_root / "data" / "processed",
        "AUDIT_DIR": cfg.project_root / "data" / "audit",
        "EXTERNAL_DIR": cfg.external_dir,
        "CHAIN_CACHE_DIR": cfg.chain_cache_dir,
        "THETADATA_CHAIN_CACHE_DIR": cfg.chain_cache_dir,
        "RAW_CHAIN_CACHE_DIR": cfg.chain_cache_dir,
        "RAW_THETADATA_CHAINS_DIR": cfg.chain_cache_dir,

        # Critical historical constants needed in default function arguments.
        "TARGET_TENORS": cfg.target_tenors,
        "TARGET_DAYS": cfg.target_tenors,
        "TARGET_TENOR_DAYS": cfg.target_tenors,
        "TARGET_TENOR_GRID": cfg.target_tenors,
        "TARGET_DTE_GRID": cfg.target_tenors,
        "CALC_TIME_MS": 16 * 60 * 60 * 1000,
        "DEFAULT_RATE_SYMBOL": "SOFR",

        # Common v0.7 paths.
        "FRED_SOFR_PATH": cfg.external_dir / "fred_sofr_history.csv",
        "FRED_SOFR_HISTORY_PATH": cfg.external_dir / "fred_sofr_history.csv",
        "SOFR_HISTORY_PATH": cfg.external_dir / "fred_sofr_history.csv",
        "SOFR_CACHE_PATH": cfg.external_dir / "fred_sofr_history.csv",
        "SPX_TRADING_DATES_PATH": cfg.external_dir / "spx_trading_dates.csv",
        "SPX_TRADING_DATES_CACHE_PATH": cfg.external_dir / "spx_trading_dates.csv",
        "TERM_STRUCTURE_PATH": cfg.seed_path,
        "TERM_STRUCTURE_HISTORY_PATH": cfg.seed_path,
        "TERM_STRUCTURE_CSV_PATH": cfg.project_root / DEFAULT_SEED_CSV_REL,
        "TERM_STRUCTURE_HISTORY_CSV_PATH": cfg.project_root / DEFAULT_SEED_CSV_REL,
        "NY_TZ": ZoneInfo("America/New_York"),
        "EASTERN_TZ": ZoneInfo("America/New_York"),
    }

    rows: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    source_dump_text = source_dump_path.read_text(encoding="utf-8", errors="ignore")

    try:
        tree = ast.parse(source_dump_text)
    except Exception as exc:
        raise RuntimeError(f"Could not parse VIX source dump with AST: {source_dump_path}; error={exc!r}") from exc

    for idx, node in enumerate(tree.body):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue

        name = node.name
        block = ast.get_source_segment(source_dump_text, node)

        if not block:
            rows.append({
                "source": "source_dump_ast_loader",
                "source_path": str(source_dump_path),
                "block_index": idx,
                "name": name,
                "status": "missing_source_segment",
                "line_count": None,
            })
            continue

        before = set(ns.keys())

        try:
            exec(compile(block, f"{source_dump_path}::{name}", "exec"), ns, ns)

            # The source dump contains multiple definitions with the same public name:
            #   calculate_vix_term_structure_for_date_v7_cached
            # The first definition is the fixed-time calculator. Later definitions are
            # market-close wrappers that call:
            #   _calculate_vix_term_structure_for_date_v7_cached_fixed_time
            # Preserve the first loaded version under that expected private alias.
            if (
                name == "calculate_vix_term_structure_for_date_v7_cached"
                and "_calculate_vix_term_structure_for_date_v7_cached_fixed_time" not in ns
            ):
                ns["_calculate_vix_term_structure_for_date_v7_cached_fixed_time"] = ns[name]

            after = set(ns.keys())

            rows.append({
                "source": "source_dump_ast_loader",
                "source_path": str(source_dump_path),
                "block_index": idx,
                "name": name,
                "status": "loaded",
                "line_count": block.count("\n") + 1,
                "new_globals": ",".join(sorted(after - before)),
            })

        except Exception as exc:
            rows.append({
                "source": "source_dump_ast_loader",
                "source_path": str(source_dump_path),
                "block_index": idx,
                "name": name,
                "status": "load_failed",
                "line_count": block.count("\n") + 1,
                "error": repr(exc),
            })
            warnings.append({
                "source": "source_dump_ast_loader",
                "source_path": str(source_dump_path),
                "function_name": name,
                "warning": repr(exc),
            })

    # Runtime helper required by the v0.7 market-close VIX function.
    # Returns XNYS market close time in milliseconds after midnight ET.
    # Normal close = 16:00 ET; early closes are pulled from pandas_market_calendars.
    def get_market_close_time_for_trade_date(trade_date):
        """
        Return XNYS market close as:
            (HHMMSS label, milliseconds after midnight ET)

        Normal close = ("160000", 57_600_000)
        Early close examples = ("130000", 46_800_000)
        """
        ts = pd.Timestamp(trade_date)

        # Handle YYYYMMDD int/string inputs.
        if isinstance(trade_date, int) or (
            isinstance(trade_date, str) and trade_date.isdigit() and len(trade_date) == 8
        ):
            ts = pd.to_datetime(str(trade_date), format="%Y%m%d")

        ts = pd.Timestamp(ts).normalize()

        try:
            cal = mcal.get_calendar("XNYS")
            sched = cal.schedule(start_date=ts.date(), end_date=ts.date())

            if sched.empty:
                close_hour, close_minute, close_second = 16, 0, 0
            else:
                close_ts = sched.iloc[0]["market_close"]

                # Convert to America/New_York.
                if close_ts.tzinfo is None:
                    close_ts = close_ts.tz_localize("UTC")

                close_et = close_ts.tz_convert("America/New_York")

                close_hour = int(close_et.hour)
                close_minute = int(close_et.minute)
                close_second = int(close_et.second)

        except Exception:
            # Safe default for normal trading days.
            close_hour, close_minute, close_second = 16, 0, 0

        close_label = f"{close_hour:02d}{close_minute:02d}{close_second:02d}"
        close_ms = int(
            close_hour * 60 * 60 * 1000
            + close_minute * 60 * 1000
            + close_second * 1000
        )

        return close_label, close_ms

    ns["get_market_close_time_for_trade_date"] = get_market_close_time_for_trade_date

    def ms_to_time_label(ms):
        """
        Convert milliseconds after midnight into HHMMSS label.

        Examples:
            57_600_000 -> "160000"
            57_540_000 -> "155900"
        """
        ms = int(ms)
        total_seconds = ms // 1000

        hour = total_seconds // 3600
        minute = (total_seconds % 3600) // 60
        second = total_seconds % 60

        return f"{hour:02d}{minute:02d}{second:02d}"

    ns["ms_to_time_label"] = ms_to_time_label

    def _parse_rate_date_series(s):
        raw = s.copy()

        numeric = pd.to_numeric(raw, errors="coerce")
        looks_yyyymmdd = numeric.notna() & numeric.between(19000101, 21001231)

        out = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

        if looks_yyyymmdd.any():
            out.loc[looks_yyyymmdd] = pd.to_datetime(
                numeric.loc[looks_yyyymmdd].astype("Int64").astype(str),
                format="%Y%m%d",
                errors="coerce",
            )

        remaining = out.isna()
        if remaining.any():
            out.loc[remaining] = pd.to_datetime(raw.loc[remaining], errors="coerce")

        return out.dt.normalize()

    def get_interest_rate_history_eod_v3(symbol="SOFR"):
        """
        Robust local SOFR history reader for the v0.7 VIX updater.

        Returns a normalized dataframe with:
            trade_date
            rate_decimal
            rate_pct
        """
        canonical_path = cfg.external_dir / "fred_sofr_history.csv"
        if not canonical_path.exists():
            raise FileNotFoundError(
                "Canonical SOFR cache is required for implied variance: "
                f"{canonical_path}"
            )
        deduped = [canonical_path]

        last_error = None

        for path in deduped:
            try:
                df = pd.read_csv(path)

                if df.empty:
                    continue

                cols_lower = {str(c).lower(): c for c in df.columns}

                date_col = None
                for cand in ["trade_date", "date", "observation_date", "timestamp", "datetime"]:
                    if cand in cols_lower:
                        date_col = cols_lower[cand]
                        break

                if date_col is None:
                    # Fall back to first column if it parses like dates.
                    date_col = df.columns[0]

                value_col = None
                for cand in [
                    "rate_decimal",
                    "sofr_decimal",
                    "rate",
                    "rate_pct",
                    "sofr",
                    "value",
                    "close",
                    "adj_close",
                ]:
                    if cand in cols_lower:
                        value_col = cols_lower[cand]
                        break

                if value_col is None:
                    numeric_candidates = []
                    for col in df.columns:
                        if col == date_col:
                            continue
                        ser = pd.to_numeric(df[col], errors="coerce")
                        if ser.notna().sum() > 0:
                            numeric_candidates.append((col, ser.notna().sum()))

                    if not numeric_candidates:
                        continue

                    value_col = sorted(numeric_candidates, key=lambda x: x[1], reverse=True)[0][0]

                out = pd.DataFrame({
                    "trade_date": _parse_rate_date_series(df[date_col]),
                    "rate_raw": pd.to_numeric(df[value_col], errors="coerce"),
                })

                out = out.dropna(subset=["trade_date", "rate_raw"]).copy()

                if out.empty:
                    continue

                # FRED SOFR is usually in percent form, e.g. 5.32.
                # If already decimal, leave it alone.
                out["rate_decimal"] = np.where(
                    out["rate_raw"].abs() > 1.0,
                    out["rate_raw"] / 100.0,
                    out["rate_raw"],
                )
                out["rate_pct"] = out["rate_decimal"] * 100.0

                out = (
                    out[["trade_date", "rate_decimal", "rate_pct"]]
                    .drop_duplicates(subset=["trade_date"], keep="last")
                    .sort_values("trade_date")
                    .reset_index(drop=True)
                )

                return out

            except Exception as exc:
                last_error = exc
                continue

        raise FileNotFoundError(
            "Could not load the canonical SOFR history for implied variance. "
            f"Required path={cfg.external_dir / 'fred_sofr_history.csv'}. "
            f"Last error: {last_error!r}"
        )

    def get_interest_rate_record_for_date_v3(symbol="SOFR", trade_date=None):
        """Return the latest SOFR observation strictly before trade_date.

        This is the production point-in-time convention. A same-date fixing is not
        available at that date's market close, so the observation date must satisfy
        observation_date < trade_date. The latest prior observation naturally carries
        across weekends and market holidays.
        """
        if trade_date is None:
            raise ValueError("trade_date is required")

        td = pd.Timestamp(trade_date)
        if isinstance(trade_date, int) or (
            isinstance(trade_date, str) and trade_date.isdigit() and len(trade_date) == 8
        ):
            td = pd.to_datetime(str(trade_date), format="%Y%m%d")
        td = pd.Timestamp(td).normalize()

        rates = get_interest_rate_history_eod_v3(symbol=symbol).copy()
        return select_prior_rate_record(rates, td, symbol=symbol)

    def get_interest_rate_for_date_v3(symbol="SOFR", trade_date=None):
        """Compatibility wrapper returning the selected T-1 rate in decimal form."""
        return float(
            get_interest_rate_record_for_date_v3(symbol=symbol, trade_date=trade_date)[
                "rate_decimal"
            ]
        )

    ns["get_interest_rate_history_eod_v3"] = get_interest_rate_history_eod_v3
    ns["get_interest_rate_record_for_date_v3"] = get_interest_rate_record_for_date_v3
    ns["get_interest_rate_for_date_v3"] = get_interest_rate_for_date_v3

    # Historical naming compatibility.
    # Some v0.7 source blocks call get_friday_expiration_candidates, while the loaded
    # function name from the source dump is get_friday_cycle_expiration_candidates.
    if (
        "get_friday_expiration_candidates" not in ns
        and "get_friday_cycle_expiration_candidates" in ns
    ):
        ns["get_friday_expiration_candidates"] = ns["get_friday_cycle_expiration_candidates"]

    def _date_to_yyyymmdd_int(x):
        if isinstance(x, int):
            return int(x)

        if isinstance(x, str) and x.isdigit() and len(x) == 8:
            return int(x)

        return int(pd.Timestamp(x).strftime("%Y%m%d"))

    def load_spx_trading_dates():
        """
        Return sorted SPX/XNYS trading dates as YYYYMMDD integers.

        Historical v0.7 expiration-selection functions use this for holiday adjustment.
        Prefer a cached local CSV if present; otherwise build from pandas_market_calendars.
        """
        candidate_paths = [
            cfg.external_dir / "spx_trading_dates.csv",
            cfg.project_root / "data" / "external" / "spx_trading_dates.csv",
            cfg.project_root / "data" / "raw" / "spx_trading_dates.csv",
            cfg.project_root / "data" / "processed" / "spx_trading_dates.csv",
        ]

        for base in [
            cfg.project_root / "data" / "external",
            cfg.project_root / "data" / "raw",
            cfg.project_root / "data" / "processed",
        ]:
            if base.exists():
                candidate_paths.extend(sorted(base.glob("*trading*date*.csv")))
                candidate_paths.extend(sorted(base.glob("*spx*calendar*.csv")))

        seen = set()
        deduped = []
        for path in candidate_paths:
            key = str(path).lower()
            if key not in seen:
                deduped.append(path)
                seen.add(key)

        for path in deduped:
            if not path.exists():
                continue

            try:
                df = pd.read_csv(path)

                if df.empty:
                    continue

                cols_lower = {str(c).lower(): c for c in df.columns}

                date_col = None
                for cand in ["trade_date", "date", "trading_date", "market_date", "session"]:
                    if cand in cols_lower:
                        date_col = cols_lower[cand]
                        break

                if date_col is None:
                    date_col = df.columns[0]

                parsed = pd.to_datetime(df[date_col], errors="coerce")

                # Handle numeric YYYYMMDD columns that pandas might parse as nanoseconds.
                numeric = pd.to_numeric(df[date_col], errors="coerce")
                looks_yyyymmdd = numeric.notna() & numeric.between(19000101, 21001231)

                if looks_yyyymmdd.any():
                    parsed.loc[looks_yyyymmdd] = pd.to_datetime(
                        numeric.loc[looks_yyyymmdd].astype("Int64").astype(str),
                        format="%Y%m%d",
                        errors="coerce",
                    )

                dates = sorted(
                    {
                        int(pd.Timestamp(x).strftime("%Y%m%d"))
                        for x in parsed.dropna().dt.normalize()
                    }
                )

                if dates:
                    return dates

            except Exception:
                continue

        # Fallback: build from XNYS calendar.
        cal = mcal.get_calendar("XNYS")
        sched = cal.schedule(start_date="2010-01-01", end_date="2035-12-31")

        return sorted(
            int(pd.Timestamp(x).strftime("%Y%m%d"))
            for x in pd.to_datetime(sched.index).normalize()
        )

    ns["load_spx_trading_dates"] = load_spx_trading_dates

    def _coerce_expiration_to_int(x):
        if x is None:
            return None

        try:
            if pd.isna(x):
                return None
        except Exception:
            pass

        s = str(x).strip()

        # Common case: YYYYMMDD.
        if s.isdigit() and len(s) == 8:
            value = int(s)
            if 19000101 <= value <= 21001231:
                return value

        # Numeric values that may be read as float.
        try:
            f = float(s)
            if np.isfinite(f):
                value = int(f)
                if 19000101 <= value <= 21001231:
                    return value
        except Exception:
            pass

        # Date string fallback.
        try:
            ts = pd.to_datetime(s, errors="coerce")
            if pd.notna(ts):
                return int(pd.Timestamp(ts).strftime("%Y%m%d"))
        except Exception:
            pass

        return None

    def _normalize_expiration_values(raw):
        values = []

        def add(v):
            out = _coerce_expiration_to_int(v)
            if out is not None:
                values.append(out)

        if isinstance(raw, pd.DataFrame):
            cols_lower = {str(c).lower(): c for c in raw.columns}
            candidate_cols = []

            for key in ["expiration", "expiry", "exp", "date", "expiration_date"]:
                for low, col in cols_lower.items():
                    if key == low or key in low:
                        candidate_cols.append(col)

            if not candidate_cols and len(raw.columns) > 0:
                candidate_cols = [raw.columns[0]]

            for col in candidate_cols:
                for v in raw[col].tolist():
                    add(v)

        elif isinstance(raw, pd.Series):
            for v in raw.tolist():
                add(v)

        elif isinstance(raw, dict):
            preferred_keys = ["expirations", "expiration", "expiry", "data", "response", "results"]

            used = False
            for key in preferred_keys:
                if key in raw:
                    values.extend(_normalize_expiration_values(raw[key]))
                    used = True

            if not used:
                for v in raw.values():
                    values.extend(_normalize_expiration_values(v))

        elif isinstance(raw, (list, tuple, set, np.ndarray)):
            for item in raw:
                if isinstance(item, dict):
                    keys = ["expiration", "expiry", "exp", "date", "expiration_date"]
                    matched = False

                    for key in keys:
                        if key in item:
                            add(item[key])
                            matched = True

                    if not matched:
                        for v in item.values():
                            add(v)

                elif isinstance(item, (list, tuple)) and len(item) > 0:
                    # ThetaData-style list payloads often put the payload value first.
                    for v in item:
                        add(v)

                else:
                    add(item)

        else:
            add(raw)

        return sorted(set(values))

    def _call_list_expirations_v3(root):
        if "list_expirations_v3" not in ns:
            return []

        fn = ns["list_expirations_v3"]

        attempts = [
            lambda: fn(root=root),
            lambda: fn(symbol=root),
            lambda: fn(root),
            lambda: fn(),
        ]

        for attempt in attempts:
            try:
                raw = attempt()
                exps = _normalize_expiration_values(raw)
                if exps:
                    return exps
            except Exception:
                continue

        return []

    # Initialize historical notebook expiration globals.
    #
    # Do NOT hard-fail if ThetaData's expiration-list endpoint is not usable during
    # namespace setup. Build a guarded Friday-cycle calendar instead.
    #
    # Legacy semantics:
    #   spx_exps  = standard monthly SPX expirations, proxied by third-Friday cycle
    #   spxw_exps = other Friday-cycle weekly expirations
    #   all_spx_exps / combined_spx_exps = union
    # Build a full XNYS trading calendar through 2035 for expiration holiday adjustment.
    # Do not rely only on any local cached spx_trading_dates file, because that file may
    # stop near the current date and expiration selection needs future expirations.
    cached_trading_ints = []
    try:
        cached_trading_ints = [
            int(x)
            for x in load_spx_trading_dates()
            if 20090101 <= int(x) <= 20351231
        ]
    except Exception:
        cached_trading_ints = []

    cal = mcal.get_calendar("XNYS")
    sched = cal.schedule(start_date="2009-01-01", end_date="2035-12-31")
    calendar_trading_ints = [
        int(pd.Timestamp(x).strftime("%Y%m%d"))
        for x in pd.to_datetime(sched.index).normalize()
    ]

    trading_ints = sorted(set(cached_trading_ints) | set(calendar_trading_ints))

    if not trading_ints:
        raise RuntimeError("Could not initialize XNYS trading calendar for expiration globals.")

    trading_set = set(trading_ints)

    def previous_trading_int(nominal_date):
        ts = pd.Timestamp(nominal_date).normalize()

        # Guard against infinite stepping. Holiday adjustments should only need
        # a few calendar days.
        for _ in range(10):
            value = int(ts.strftime("%Y%m%d"))

            if value in trading_set:
                return value

            ts = ts - pd.Timedelta(days=1)

        raise RuntimeError(f"Could not find previous trading day for nominal expiration {nominal_date}")

    first_trading_ts = pd.to_datetime(str(min(trading_ints)), format="%Y%m%d")

    # Start on the first Friday on/after the first available trading date.
    # This avoids trying to holiday-adjust 2010-01-01 backward when the trading
    # calendar itself does not include 2009 dates.
    friday_dates = pd.date_range(first_trading_ts, "2035-12-31", freq="W-FRI")

    spx_only_exps = []
    spxw_only_exps = []

    for friday in friday_dates:
        exp_int = previous_trading_int(friday)

        # Standard monthly third-Friday cycle.
        is_third_friday = (
            friday.weekday() == 4
            and 15 <= friday.day <= 21
        )

        if is_third_friday:
            spx_only_exps.append(exp_int)
        else:
            spxw_only_exps.append(exp_int)

    spx_only_exps = sorted(set(spx_only_exps))
    spxw_only_exps = sorted(set(spxw_only_exps))
    combined_spx_exps = sorted(set(spx_only_exps) | set(spxw_only_exps))

    ns["spx_exps"] = spx_only_exps
    ns["spxw_exps"] = spxw_only_exps
    ns["all_spx_exps"] = combined_spx_exps
    ns["combined_spx_exps"] = combined_spx_exps

    rows.append({
        "source": "guarded_calendar_expiration_globals",
        "source_path": "XNYS/pandas_market_calendars",
        "name": "spx_exps/spxw_exps",
        "status": "loaded",
        "line_count": None,
        "new_globals": "spx_exps,spxw_exps,all_spx_exps,combined_spx_exps",
        "spx_count": len(spx_only_exps),
        "spxw_count": len(spxw_only_exps),
        "combined_count": len(combined_spx_exps),
        "min_expiration": min(combined_spx_exps),
        "max_expiration": max(combined_spx_exps),
    })

    def is_friday_cycle_expiration_v6(expiration=None, *args, **kwargs):
        """
        Compatibility helper for legacy v0.7 expiration selection.

        Returns True when expiration is part of the Friday-cycle universe:
          - standard monthly SPX expirations in spx_exps
          - weekly Friday-cycle SPXW expirations in spxw_exps
          - holiday-adjusted previous trading day for a Friday expiration
        """
        if expiration is None:
            for key in ["expiration", "exp", "expiry", "expiration_date"]:
                if key in kwargs:
                    expiration = kwargs[key]
                    break

        if expiration is None and args:
            expiration = args[0]

        exp_int = _coerce_expiration_to_int(expiration)

        if exp_int is None:
            return False

        all_friday_cycle = set(ns.get("spx_exps", [])) | set(ns.get("spxw_exps", [])) | set(ns.get("all_spx_exps", []))

        if exp_int in all_friday_cycle:
            return True

        # Fallback: direct Friday date check.
        try:
            ts = pd.to_datetime(str(exp_int), format="%Y%m%d")
            if ts.weekday() == 4:
                return True

            # Holiday-adjusted Friday expiration can settle on the prior trading day.
            next_day = ts + pd.Timedelta(days=1)
            if next_day.weekday() == 4:
                return True

        except Exception:
            return False

        return False

    ns["is_friday_cycle_expiration_v6"] = is_friday_cycle_expiration_v6
    ns["is_friday_cycle_expiration"] = is_friday_cycle_expiration_v6

    def preferred_root_for_expiration_v6(expiration=None, *args, **kwargs):
        """
        Compatibility helper for legacy v0.7 root selection.

        Returns:
            "SPX"  for standard monthly SPX expirations
            "SPXW" for weekly Friday-cycle expirations
        """
        if expiration is None:
            for key in ["expiration", "exp", "expiry", "expiration_date"]:
                if key in kwargs:
                    expiration = kwargs[key]
                    break

        if expiration is None and args:
            expiration = args[0]

        exp_int = _coerce_expiration_to_int(expiration)

        if exp_int is None:
            raise ValueError(f"Could not parse expiration for root selection: {expiration!r}")

        if exp_int in set(ns.get("spx_exps", [])):
            return "SPX"

        if exp_int in set(ns.get("spxw_exps", [])):
            return "SPXW"

        # Holiday-adjusted / direct fallback:
        # third-Friday cycle -> SPX, other Friday-cycle expirations -> SPXW.
        ts = pd.to_datetime(str(exp_int), format="%Y%m%d", errors="coerce")

        if pd.isna(ts):
            raise ValueError(f"Could not parse expiration for root selection: {expiration!r}")

        # If the actual expiration is a Thursday because Friday is a holiday,
        # classify based on the nominal next day.
        nominal = ts
        if ts.weekday() == 3:
            nominal = ts + pd.Timedelta(days=1)

        is_third_friday = (
            nominal.weekday() == 4
            and 15 <= nominal.day <= 21
        )

        return "SPX" if is_third_friday else "SPXW"

    ns["preferred_root_for_expiration_v6"] = preferred_root_for_expiration_v6

    # If any older source block asks for the unversioned name, support that too.
    ns["preferred_root_for_expiration"] = preferred_root_for_expiration_v6

    # Legacy VIX notebook constants used by minutes-to-expiry / interpolation helpers.
    # Needed only for function definition defaults. In production calls,
    # calc_single_term_variance receives r explicitly from get_interest_rate_for_date_v3.
    ns["DEFAULT_RISK_FREE_RATE"] = 0.0

    ns["MINUTES_PER_DAY"] = 24 * 60
    ns["SECONDS_PER_DAY"] = 24 * 60 * 60
    ns["MILLISECONDS_PER_DAY"] = 24 * 60 * 60 * 1000
    ns["DAYS_PER_YEAR"] = 365
    ns["MINUTES_PER_YEAR"] = 365 * 24 * 60
    ns["SECONDS_PER_YEAR"] = 365 * 24 * 60 * 60
    ns["MILLISECONDS_PER_YEAR"] = 365 * 24 * 60 * 60 * 1000

    # Legacy ThetaData base-url aliases used by older notebook cells.
    ns["BASE_URL_V3"] = cfg.thetadata_base_url
    ns["V3_BASE_URL"] = cfg.thetadata_base_url
    ns["BASE_URL"] = cfg.thetadata_base_url
    ns["THETADATA_BASE_URL"] = cfg.thetadata_base_url




    # Second-pass loader:
    # Some source-dump functions may fail during the first AST pass because legacy
    # constants/helpers are only restored later in this function. After all compatibility
    # globals above are available, retry any missing def/class blocks.
    second_pass_loaded = []

    for idx, node in enumerate(tree.body):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue

        name = node.name

        if name in ns:
            continue

        block = ast.get_source_segment(source_dump_text, node)

        if not block:
            continue

        before = set(ns.keys())

        try:
            exec(compile(block, f"{source_dump_path}::{name}::second_pass", "exec"), ns, ns)
            after = set(ns.keys())

            second_pass_loaded.append(name)

            rows.append({
                "source": "source_dump_ast_second_pass",
                "source_path": str(source_dump_path),
                "block_index": idx,
                "name": name,
                "status": "loaded",
                "line_count": block.count("\n") + 1,
                "new_globals": ",".join(sorted(after - before)),
            })

            if (
                name == "calculate_vix_term_structure_for_date_v7_cached"
                and "_calculate_vix_term_structure_for_date_v7_cached_fixed_time" not in ns
            ):
                ns["_calculate_vix_term_structure_for_date_v7_cached_fixed_time"] = ns[name]

        except Exception as exc:
            rows.append({
                "source": "source_dump_ast_second_pass",
                "source_path": str(source_dump_path),
                "block_index": idx,
                "name": name,
                "status": "load_failed",
                "line_count": block.count("\n") + 1,
                "error": repr(exc),
            })
            warnings.append({
                "source": "source_dump_ast_second_pass",
                "source_path": str(source_dump_path),
                "function_name": name,
                "warning": repr(exc),
            })

    # Hard check the next known required runtime function.
    if "calc_single_term_variance" not in ns:
        raise RuntimeError(
            "calc_single_term_variance is still missing after second-pass source-dump loading."
        )

    # Repair the expiration clock after all source-dump passes. The notebook
    # implementation accepts only the root and therefore hardcodes SPXW to 16:00.
    # Production must use the actual XNYS close on the expiration date so that
    # early-close SPXW expirations settle at 13:00 rather than 16:00.
    ns["settlement_minutes_after_midnight_et"] = production_settlement_minutes_after_midnight_et
    ns["minutes_to_expiry_vix_method"] = production_minutes_to_expiry_vix_method
    rows.append({
        "source": "production_calendar_override",
        "source_path": str(Path(__file__).resolve()),
        "block_index": None,
        "name": "settlement_minutes_after_midnight_et/minutes_to_expiry_vix_method",
        "status": "loaded",
        "line_count": None,
        "new_globals": "expiration-aware SPXW settlement clock",
    })

    loaded_df = pd.DataFrame(rows)
    warnings_df = pd.DataFrame(warnings)

    return ns, loaded_df, warnings_df
def theta_health_check(cfg: Config) -> Dict[str, Any]:
    url = cfg.thetadata_base_url.rstrip("/")
    # Use a lightweight generic endpoint if possible. ThetaData Terminal usually responds quickly
    # on API endpoints, but exact endpoints vary. We only test local connectivity here.
    result: Dict[str, Any] = {"url": url, "reachable": False, "status_code": None, "error": None}
    try:
        resp = requests.get(url, timeout=5)
        result["status_code"] = int(resp.status_code)
        result["reachable"] = resp.status_code < 500
    except Exception as exc:
        result["error"] = repr(exc)
    return result


# ======================================================================================
# Dynamic function invocation
# ======================================================================================


def build_kwargs_for_signature(sig: inspect.Signature, cfg: Config, trade_date_value: Any) -> Tuple[Dict[str, Any], List[str]]:
    kwargs: Dict[str, Any] = {}
    missing_required: List[str] = []

    # Precompute common values.
    quote_time_value = cfg.quote_time
    trading_dates = get_xnys_trading_dates(pd.Timestamp(trade_date_value) - pd.Timedelta(days=120), pd.Timestamp(trade_date_value) + pd.Timedelta(days=120)) if not isinstance(trade_date_value, int) else None

    for name, p in sig.parameters.items():
        lname = name.lower()
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue

        value_set = True
        value: Any = None

        if lname in {"trade_date", "calc_date", "asof_date", "date", "d"} or ("trade" in lname and "date" in lname):
            value = trade_date_value
        elif lname in {"target_tenors", "tenors", "target_days", "target_days_list", "target_dtes", "target_dte_list"} or "target_tenor" in lname:
            value = cfg.target_tenors
        elif lname in {"base_url", "thetadata_base_url", "v3_base_url", "url"} or ("base" in lname and "url" in lname):
            value = cfg.thetadata_base_url
        elif "cache" in lname and ("dir" in lname or "path" in lname):
            value = cfg.chain_cache_dir
        elif lname in {"chain_cache_dir", "raw_chain_cache_dir", "thetadata_chain_cache_dir"}:
            value = cfg.chain_cache_dir
        elif lname in {"external_dir", "data_external_dir"}:
            value = cfg.external_dir
        elif lname in {"project_root", "root_dir"}:
            value = cfg.project_root
        elif lname in {"max_workers", "workers", "n_workers"}:
            value = cfg.max_workers
        elif lname in {"force_refresh", "refresh", "overwrite", "force"}:
            value = cfg.force_refresh
        elif "quote_time" in lname or "snapshot_time" in lname or "calc_time" in lname:
            if quote_time_value is None and p.default is not inspect._empty:
                value_set = False
            else:
                value = quote_time_value
        elif lname in {"trading_dates", "spx_trading_dates", "trade_dates"} and trading_dates is not None:
            value = trading_dates
        elif "source" in lname and "version" in lname:
            value = SOURCE_METHODOLOGY_VERSION
        else:
            value_set = False

        if value_set:
            kwargs[name] = value
        elif p.default is inspect._empty:
            missing_required.append(name)

    return kwargs, missing_required


def normalize_calculation_output(result: Any, trade_date_ts: pd.Timestamp, cfg: Config) -> pd.DataFrame:
    """Convert the historical function output into a normalized long DataFrame."""
    if isinstance(result, pd.DataFrame):
        df = result.copy()
    elif isinstance(result, dict):
        # Common cases:
        #   v0.7 calculator returns {"results_df": df, "required_by_tenor": ..., ...}
        #   older helpers may return {"term_structure": df/list}
        for key in ("results_df", "term_structure", "term_structure_df", "result", "rows", "data"):
            if key in result:
                value = result[key]
                if isinstance(value, pd.DataFrame):
                    df = value.copy()
                    break
                df = pd.DataFrame(value)
                break
        else:
            # Try records from dict values.
            if all(isinstance(v, dict) for v in result.values()):
                records = []
                for k, v in result.items():
                    rec = dict(v)
                    if "target_days" not in rec and "tenor" not in rec:
                        rec["target_days"] = k
                    records.append(rec)
                df = pd.DataFrame(records)
            else:
                df = pd.DataFrame([result])
    elif isinstance(result, (list, tuple)):
        # If tuple contains a DataFrame, use the first such DataFrame.
        dataframes = [x for x in result if isinstance(x, pd.DataFrame)]
        if dataframes:
            df = dataframes[0].copy()
        else:
            df = pd.DataFrame(result)
    else:
        raise TypeError(f"Unsupported calculation result type: {type(result)}")

    if df.empty:
        raise ValueError("calculation returned an empty DataFrame")

    # Normalize key columns.
    rename_map = {}
    if "target_days" not in df.columns and "tenor" in df.columns:
        rename_map["tenor"] = "target_days"
    if "target_tenor" in df.columns and "target_days" not in df.columns:
        rename_map["target_tenor"] = "target_days"
    if "target_dte" in df.columns and "target_days" not in df.columns:
        rename_map["target_dte"] = "target_days"
    if "variance" in df.columns and "implied_variance" not in df.columns:
        rename_map["variance"] = "implied_variance"
    if "vix" in df.columns and "vix_style_vol" not in df.columns:
        rename_map["vix"] = "vix_style_vol"
    if "vol" in df.columns and "vix_style_vol" not in df.columns:
        rename_map["vol"] = "vix_style_vol"
    df = df.rename(columns=rename_map)

    if "trade_date" not in df.columns:
        df["trade_date"] = trade_date_ts
    df["trade_date"] = parse_trade_date_series(df["trade_date"])

    if "target_days" not in df.columns:
        raise ValueError(f"calculation result missing target_days/tenor column; columns={list(df.columns)}")
    df["target_days"] = pd.to_numeric(df["target_days"], errors="raise").astype(int)
    df["tenor"] = df["target_days"]

    if "implied_variance" not in df.columns:
        raise ValueError(f"calculation result missing implied_variance column; columns={list(df.columns)}")
    df["implied_variance"] = pd.to_numeric(df["implied_variance"], errors="raise")

    if "vix_style_vol" not in df.columns:
        df["vix_style_vol"] = np.sqrt(df["implied_variance"]) * 100.0
    else:
        df["vix_style_vol"] = pd.to_numeric(df["vix_style_vol"], errors="raise")

    # Standard metadata columns if absent.
    if "methodology_version" not in df.columns:
        df["methodology_version"] = SOURCE_METHODOLOGY_VERSION
    if "source_methodology_version" not in df.columns:
        df["source_methodology_version"] = SOURCE_METHODOLOGY_VERSION
    df["updater_version"] = UPDATER_VERSION
    df["run_timestamp"] = cfg.run_timestamp

    # Retain target tenors only and sort.
    df = df[df["tenor"].isin(cfg.target_tenors)].copy()
    df = df.sort_values(["trade_date", "tenor"]).reset_index(drop=True)
    return df


def call_calculation_function(ns: Dict[str, Any], trade_date_ts: pd.Timestamp, cfg: Config) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    func_name = "calculate_vix_term_structure_for_date_v7_cached"
    if func_name not in ns or not callable(ns[func_name]):
        raise RuntimeError(f"Loaded namespace missing callable {func_name}")

    func = ns[func_name]
    sig = inspect.signature(func)

    # Try several trade-date representations because historical notebook functions may expect date/int/str.
    trade_date_variants: List[Tuple[str, Any]] = [
        ("python_date", trade_date_ts.date()),
        ("pd_timestamp", trade_date_ts),
        ("yyyymmdd_int", yyyymmdd_int(trade_date_ts)),
        ("yyyymmdd_str", yyyymmdd_str(trade_date_ts)),
        ("iso_str", date_str(trade_date_ts)),
    ]

    attempts: List[Dict[str, Any]] = []
    last_exc: Optional[BaseException] = None

    for variant_name, trade_date_value in trade_date_variants:
        kwargs, missing = build_kwargs_for_signature(sig, cfg, trade_date_value)
        attempt = {
            "function_name": func_name,
            "signature": str(sig),
            "trade_date_variant": variant_name,
            "kwargs_keys": ",".join(sorted(kwargs.keys())),
            "missing_required": ",".join(missing),
            "status": "not_run",
            "error": None,
        }
        if missing:
            attempt["status"] = "skipped_missing_required"
            attempts.append(attempt)
            continue
        try:
            result = func(**kwargs)
            df = normalize_calculation_output(result, trade_date_ts, cfg)
            attempt["status"] = "success"
            attempt["rows"] = len(df)
            attempts.append(attempt)
            return df, {"attempts": attempts, "successful_variant": variant_name, "signature": str(sig)}
        except BaseException as exc:  # noqa: BLE001 - capture and try alternate date forms
            last_exc = exc
            attempt["status"] = "failed"
            attempt["error"] = repr(exc)
            attempts.append(attempt)

    detail = {
        "attempts": attempts,
        "signature": str(sig),
        "last_error": repr(last_exc) if last_exc else None,
    }
    raise RuntimeError(f"Could not call {func_name}; attempts={json.dumps(detail, default=str)[:4000]}")


# ======================================================================================
# Validation / write outputs
# ======================================================================================


def upsert_rows(existing: pd.DataFrame, new_rows: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """Replace matching date x tenor keys explicitly, then append the new rows.

    Refresh precedence must not depend on nullable timestamps or sort behavior. Every
    key present in new_rows removes the existing canonical key before concatenation.
    """
    if new_rows.empty:
        return existing.copy()

    old = existing.copy()
    new = new_rows.copy()
    for frame in (old, new):
        frame["trade_date"] = parse_trade_date_series(frame["trade_date"])
        frame["tenor"] = pd.to_numeric(frame["tenor"], errors="raise").astype(int)
        frame["target_days"] = pd.to_numeric(
            frame.get("target_days", frame["tenor"]), errors="coerce"
        ).fillna(frame["tenor"]).astype(int)

    if new.duplicated(["trade_date", "tenor"]).any():
        duplicate_keys = new.loc[
            new.duplicated(["trade_date", "tenor"], keep=False), ["trade_date", "tenor"]
        ].drop_duplicates()
        raise RuntimeError(
            "New implied-variance rows are duplicated by date x tenor: "
            f"{duplicate_keys.head(20).to_dict(orient='records')}"
        )

    replacement_keys = pd.MultiIndex.from_frame(new[["trade_date", "tenor"]])
    old_keys = pd.MultiIndex.from_frame(old[["trade_date", "tenor"]])
    retained_old = old.loc[~old_keys.isin(replacement_keys)].copy()
    combined = pd.concat([retained_old, new], ignore_index=True, sort=False)
    if combined.duplicated(["trade_date", "tenor"]).any():
        raise RuntimeError("Explicit implied-variance upsert left duplicate date x tenor keys.")
    return combined.sort_values(["trade_date", "tenor"]).reset_index(drop=True)


def validate_surface(df: pd.DataFrame, cfg: Config, target_dates: Optional[Sequence[pd.Timestamp]] = None) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    def add(check: str, passed: bool, detail: str = "", severity: str = "hard") -> None:
        rows.append({"check": check, "severity": severity, "passed": bool(passed), "detail": detail})

    if df.empty:
        add("surface_non_empty", False, "rows=0")
        return pd.DataFrame(rows)

    key_dupes = int(df.duplicated(["trade_date", "tenor"]).sum())
    add("no_duplicate_trade_date_tenor", key_dupes == 0, f"duplicate_rows={key_dupes}")

    positive = bool((pd.to_numeric(df["implied_variance"], errors="coerce") > 0).all())
    add("positive_implied_variance", positive, f"non_positive_count={int((pd.to_numeric(df['implied_variance'], errors='coerce') <= 0).sum())}")

    recon = np.sqrt(pd.to_numeric(df["implied_variance"], errors="coerce")) * 100.0
    vol = pd.to_numeric(df["vix_style_vol"], errors="coerce")
    max_abs = float(np.nanmax(np.abs(recon - vol))) if len(df) else float("nan")
    add("vix_style_vol_reconstruction", max_abs < 1e-10, f"max_abs_diff={max_abs}")

    latest = df["trade_date"].max()
    latest_rows = df.loc[df["trade_date"].eq(latest)]
    latest_tenors = sorted(latest_rows["tenor"].dropna().astype(int).unique().tolist())
    add(
        "latest_snapshot_has_expected_tenors",
        latest_tenors == cfg.target_tenors,
        f"latest_date={date_str(latest)}; latest_tenors={latest_tenors}",
    )

    tenor_grid = df.groupby("trade_date")["tenor"].apply(lambda s: sorted(s.dropna().astype(int).unique().tolist()))
    bad_grid = tenor_grid[tenor_grid.apply(lambda x: x != cfg.target_tenors)]
    add("all_dates_have_expected_tenor_grid", len(bad_grid) == 0, f"bad_dates={len(bad_grid)}")

    # Bracketing check for newly calculated rows where columns exist.
    if all(c in df.columns for c in ["near_days", "next_days", "target_days"]):
        tmp = df.copy()
        tmp["near_days_num"] = pd.to_numeric(tmp["near_days"], errors="coerce")
        tmp["next_days_num"] = pd.to_numeric(tmp["next_days"], errors="coerce")
        tmp["target_days_num"] = pd.to_numeric(tmp["target_days"], errors="coerce")
        available = tmp[["near_days_num", "next_days_num", "target_days_num"]].notna().all(axis=1)
        if available.any():
            ok = (tmp.loc[available, "near_days_num"] <= tmp.loc[available, "target_days_num"]) & (
                tmp.loc[available, "target_days_num"] <= tmp.loc[available, "next_days_num"]
            )
            add("near_next_bracket_target_when_available", bool(ok.all()), f"bad_rows={int((~ok).sum())}; checked_rows={int(available.sum())}")
        else:
            add("near_next_bracket_target_when_available", True, "no complete near/next day metadata available", severity="info")
    else:
        add("near_next_bracket_target_when_available", True, "near/next metadata columns absent", severity="info")

    if target_dates:
        target_set = {pd.Timestamp(d).normalize() for d in target_dates}
        found_set = set(df.loc[df["trade_date"].isin(target_set), "trade_date"].dropna().map(pd.Timestamp).map(lambda x: x.normalize()))
        missing = sorted(target_set - found_set)
        add("target_dates_present_after_update", len(missing) == 0, f"missing={','.join(date_str(x) for x in missing)}")
        for d in sorted(target_set):
            tenors = sorted(df.loc[df["trade_date"].eq(d), "tenor"].dropna().astype(int).unique().tolist())
            add(
                f"target_date_{date_str(d)}_has_expected_tenors",
                tenors == cfg.target_tenors,
                f"tenors={tenors}",
            )

    return pd.DataFrame(rows)


def write_outputs(surface: pd.DataFrame, cfg: Config) -> Dict[str, Path]:
    out_surface = cfg.output_dir / OUTPUT_SURFACE_FILE
    out_surface_csv = cfg.output_dir / OUTPUT_SURFACE_CSV_FILE
    out_latest = cfg.output_dir / OUTPUT_LATEST_FILE
    out_latest_csv = cfg.output_dir / OUTPUT_LATEST_CSV_FILE

    latest_date = surface["trade_date"].max()
    latest_snapshot = surface.loc[surface["trade_date"].eq(latest_date)].copy().sort_values("tenor")

    surface.to_parquet(out_surface, index=False)
    surface.to_csv(out_surface_csv, index=False)
    latest_snapshot.to_parquet(out_latest, index=False)
    latest_snapshot.to_csv(out_latest_csv, index=False)

    outputs = {
        "surface_parquet": out_surface,
        "surface_csv": out_surface_csv,
        "latest_parquet": out_latest,
        "latest_csv": out_latest_csv,
    }

    if cfg.write_canonical:
        canonical_backup = cfg.seed_path.with_suffix(f".backup_{cfg.run_timestamp}.parquet")
        if cfg.seed_path.exists():
            cfg.seed_path.replace(canonical_backup)
        surface.to_parquet(cfg.seed_path, index=False)
        surface.to_csv(cfg.project_root / DEFAULT_SEED_CSV_REL, index=False)
        outputs["canonical_backup"] = canonical_backup
        outputs["canonical_parquet"] = cfg.seed_path
        outputs["canonical_csv"] = cfg.project_root / DEFAULT_SEED_CSV_REL

    return outputs


# ======================================================================================
# Main run
# ======================================================================================


def run(cfg: Config) -> int:
    ensure_dirs(cfg)

    print_header("VRP implied variance EOD update v1")
    print(f"Project root:              {cfg.project_root}")
    print(f"Run timestamp:             {cfg.run_timestamp}")
    print(f"Mode check-only:           {cfg.check_only}")
    print(f"Mode update-missing:       {cfg.update_missing}")
    print(f"Single date:               {date_str(cfg.single_date) if cfg.single_date is not None else None}")
    print(f"Start date:                {date_str(cfg.start_date) if cfg.start_date is not None else None}")
    print(f"End date:                  {date_str(cfg.end_date) if cfg.end_date is not None else None}")
    print(f"Max dates:                 {cfg.max_dates}")
    print(f"Force refresh:             {cfg.force_refresh}")
    print(f"Write canonical:           {cfg.write_canonical}")
    print(f"ThetaData base URL:        {cfg.thetadata_base_url}")
    print(f"Source notebook:           {cfg.source_notebook_path}")
    print(f"Seed/canonical path:       {cfg.seed_path}")
    print(f"Output dir:                {cfg.output_dir}")
    print(f"Audit dir:                 {cfg.audit_dir}")

    run_rows: List[Dict[str, Any]] = []
    calc_log_rows: List[Dict[str, Any]] = []

    print_header("File existence")
    files = {
        "source_notebook": cfg.source_notebook_path,
        "seed_surface": cfg.seed_path,
    }
    file_rows = []
    for label, path in files.items():
        exists = path.exists()
        print(f"{label:<18} exists={exists}  path={path}")
        file_rows.append({"label": label, "path": str(path), "exists": bool(exists)})
        if not exists:
            raise FileNotFoundError(f"Missing {label}: {path}")

    if mcal is None:
        print("WARNING: pandas_market_calendars is not installed; using weekday fallback calendar.")

    theta_status = theta_health_check(cfg)
    print_header("ThetaData connectivity")
    print(json.dumps(theta_status, indent=2, default=str))
    if cfg.require_thetadata and not theta_status.get("reachable"):
        raise RuntimeError(f"ThetaData base URL is not reachable: {theta_status}")

    print_header("Loading implied-variance surface")
    surface, surface_source = load_surface(cfg)
    print(f"Surface source: {surface_source}")
    print(f"Rows:           {len(surface):,}")
    print(f"Date range:     {date_str(surface['trade_date'].min())} to {date_str(surface['trade_date'].max())}")
    print(f"Unique dates:   {surface['trade_date'].nunique():,}")
    print(f"Tenors:         {sorted(surface['tenor'].dropna().astype(int).unique().tolist())}")

    target_dates, mode_detail = determine_target_dates(cfg, surface["trade_date"].unique())
    print_header("Missing / target date plan")
    print(json.dumps(mode_detail, indent=2, default=str))
    print("Target dates:")
    if target_dates:
        for d in target_dates:
            print(f"  {date_str(d)}")
    else:
        print("  None")

    if cfg.check_only and cfg.single_date is None and not cfg.update_missing:
        print_header("Check-only result")
        if target_dates:
            print(f"MISSING_DATES: {len(target_dates)}")
        else:
            print("UP_TO_DATE: no missing completed EOD dates found")

    loaded_df = pd.DataFrame()
    load_warnings_df = pd.DataFrame()
    new_rows_all = pd.DataFrame()

    if target_dates and not cfg.check_only:
        print_header("Loading v0.7 VIX function namespace")
        ns, loaded_df, load_warnings_df = build_vix_namespace(cfg)
        loaded_ok = loaded_df.loc[loaded_df["status"].eq("loaded"), "name"].dropna().astype(str).tolist() if not loaded_df.empty else []
        print(f"Definitions loaded: {len(loaded_ok)}")
        missing_min = [name for name in EXPECTED_MIN_FUNCTIONS if name not in ns or not callable(ns[name])]
        print(f"Minimum required functions missing: {missing_min}")
        if missing_min:
            raise RuntimeError(f"Cannot update implied variance; missing required source functions: {missing_min}")

        new_frames: List[pd.DataFrame] = []
        print_header("Calculating target dates")
        for d in target_dates:
            print(f"Calculating {date_str(d)} ...")
            calculation_started = time.perf_counter()
            row_base = {"trade_date": date_str(d), "status": "started", "rows": None, "error": None}
            try:
                calc_df, detail = call_calculation_function(ns, d, cfg)
                rate_record = ns["get_interest_rate_record_for_date_v3"](
                    symbol="SOFR", trade_date=d
                )
                calc_df = calc_df.copy()
                calc_df["rate_observation_date"] = rate_record["rate_observation_date"]
                calc_df["rate_selection_rule"] = rate_record["rate_selection_rule"]
                calc_df["rate_decimal"] = rate_record["rate_decimal"]
                # Override any legacy same-date or source-generated rate metadata.
                calc_df["rate_pct"] = rate_record["rate_pct"]
                if len(calc_df) != len(cfg.target_tenors):
                    raise RuntimeError(f"Expected {len(cfg.target_tenors)} rows for {date_str(d)}, got {len(calc_df)}")
                new_frames.append(calc_df)
                row_base.update(
                    {
                        "status": "success",
                        "rows": len(calc_df),
                        "tenors": sorted(calc_df["tenor"].dropna().astype(int).unique().tolist()),
                        "signature": detail.get("signature"),
                        "successful_variant": detail.get("successful_variant"),
                    }
                )
                # Flatten attempt metadata for audit.
                for attempt in detail.get("attempts", []):
                    attempt_row = {"trade_date": date_str(d), **attempt}
                    calc_log_rows.append(attempt_row)
                elapsed_seconds = time.perf_counter() - calculation_started
                row_base["elapsed_seconds"] = elapsed_seconds
                print(
                    f"  OK rows={len(calc_df)} tenors={row_base['tenors']} elapsed={elapsed_seconds:.2f}s"
                )
            except Exception as exc:
                row_base.update({"status": "failed", "error": repr(exc)})
                print(f"  FAILED: {repr(exc)}")
                run_rows.append(row_base)
                break
            run_rows.append(row_base)

        failed = [r for r in run_rows if r.get("status") == "failed"]
        if failed:
            raise RuntimeError(f"One or more target-date calculations failed. First failure: {failed[0]}")

        new_rows_all = pd.concat(new_frames, ignore_index=True, sort=False) if new_frames else pd.DataFrame()
        surface = upsert_rows(surface, new_rows_all, cfg)

    validation_df = validate_surface(surface, cfg, target_dates if (target_dates and not cfg.check_only) else None)
    hard_failed = validation_df.loc[validation_df["severity"].eq("hard") & ~validation_df["passed"]]

    print_header("Validation summary")
    print(validation_df.to_string(index=False, max_rows=200))
    print(f"Hard checks failed: {len(hard_failed)}")

    outputs: Dict[str, Path] = {}
    if not cfg.check_only and target_dates:
        if len(hard_failed) > 0:
            raise RuntimeError("Validation failed; outputs were not written.")
        outputs = write_outputs(surface, cfg)
        print_header("Saved processed outputs")
        for label, path in outputs.items():
            print(f"{label:<20} {path}")
    else:
        print_header("Processed output write")
        print("No processed outputs written in check-only mode or because there were no target dates.")

    # Audit outputs always written.
    prefix = f"spx_vix_style_implied_variance_eod_update_{cfg.run_timestamp}"
    audit_paths = {
        "validation": cfg.audit_dir / f"{prefix}_validation.csv",
        "files": cfg.audit_dir / f"{prefix}_files.csv",
        "date_plan": cfg.audit_dir / f"{prefix}_date_plan.csv",
        "calculation_log": cfg.audit_dir / f"{prefix}_calculation_log.csv",
        "function_load": cfg.audit_dir / f"{prefix}_function_load.csv",
        "function_load_warnings": cfg.audit_dir / f"{prefix}_function_load_warnings.csv",
        "new_rows": cfg.audit_dir / f"{prefix}_new_rows.csv",
        "manifest": cfg.audit_dir / f"{prefix}_manifest.json",
    }

    validation_df.to_csv(audit_paths["validation"], index=False)
    pd.DataFrame(file_rows).to_csv(audit_paths["files"], index=False)
    pd.DataFrame([{**mode_detail, "target_dates": ",".join(date_str(d) for d in target_dates)}]).to_csv(audit_paths["date_plan"], index=False)
    pd.DataFrame(calc_log_rows).to_csv(audit_paths["calculation_log"], index=False)
    loaded_df.to_csv(audit_paths["function_load"], index=False)
    load_warnings_df.to_csv(audit_paths["function_load_warnings"], index=False)
    new_rows_all.to_csv(audit_paths["new_rows"], index=False)

    manifest = {
        "config": {k: str(v) if isinstance(v, Path) else v for k, v in asdict(cfg).items()},
        "surface_source": str(surface_source),
        "theta_status": theta_status,
        "mode_detail": mode_detail,
        "target_dates": [date_str(d) for d in target_dates],
        "processed_outputs": {k: str(v) for k, v in outputs.items()},
        "audit_outputs": {k: str(v) for k, v in audit_paths.items()},
        "hard_failed_count": int(len(hard_failed)),
        "latest_surface_date_after_run": date_str(surface["trade_date"].max()) if not surface.empty else None,
        "surface_rows_after_run": int(len(surface)),
    }
    audit_paths["manifest"].write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print_header("Saved audit outputs")
    for label, path in audit_paths.items():
        print(f"{label:<24} {path}")

    if len(hard_failed) > 0:
        print("\nDONE — implied variance EOD update failed validation.")
        return 1

    if cfg.check_only:
        print("\nDONE — implied variance EOD check complete.")
    elif target_dates:
        print("\nDONE — implied variance EOD update complete.")
    else:
        print("\nDONE — implied variance EOD update not needed; already up to date.")
    return 0


# ======================================================================================
# CLI
# ======================================================================================


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update SPX VIX-style implied variance surface for missing EOD dates.")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--source-notebook", default=None, help="Optional explicit path to v0.7 source notebook.")
    parser.add_argument("--seed-path", default=None, help="Optional explicit seed/canonical repaired IV parquet path.")
    parser.add_argument("--thetadata-base-url", default=DEFAULT_THETADATA_BASE_URL)
    parser.add_argument("--target-tenors", default=",".join(str(x) for x in DEFAULT_TARGET_TENORS))

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-only", action="store_true", help="Only report missing dates; write audit files but no processed outputs. Default if no update flag/date is supplied.")
    mode.add_argument("--update-missing", action="store_true", help="Update only missing completed EOD dates.")
    mode.add_argument("--single-date", default=None, help="Calculate/update one date: YYYY-MM-DD or YYYYMMDD.")

    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--max-dates", type=int, default=None)
    parser.add_argument("--force-refresh", action="store_true", help="Recalculate target date rows and replace existing rows for those dates.")
    parser.add_argument("--write-canonical", action="store_true", help="Also update/overwrite the seed canonical repaired file, with timestamped backup. Use only after validation.")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--no-require-thetadata", action="store_true", help="Do not hard-fail if simple ThetaData connectivity probe fails.")
    parser.add_argument("--quote-time", default=None, help="Optional quote time override passed to source functions when they accept quote_time/snapshot_time.")
    return parser


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    args = build_parser().parse_args(argv)
    project_root = Path(args.project_root)
    source_notebook = Path(args.source_notebook) if args.source_notebook else project_root / DEFAULT_SOURCE_NOTEBOOK_REL
    seed_path = Path(args.seed_path) if args.seed_path else project_root / DEFAULT_SEED_REL

    target_tenors = [int(x.strip()) for x in str(args.target_tenors).split(",") if x.strip()]
    target_tenors = sorted(target_tenors)

    single_date = parse_date(args.single_date, "--single-date")
    start_date = parse_date(args.start_date, "--start-date")
    end_date = parse_date(args.end_date, "--end-date")

    # Safe default: check-only unless an explicit update/single-date/window action is requested.
    check_only = bool(args.check_only)
    update_missing = bool(args.update_missing)
    if not check_only and not update_missing and single_date is None and start_date is None:
        check_only = True

    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError("--end-date cannot be before --start-date")

    return Config(
        project_root=project_root,
        source_notebook_path=source_notebook,
        seed_path=seed_path,
        output_dir=project_root / DEFAULT_OUTPUT_DIR_REL,
        audit_dir=project_root / DEFAULT_AUDIT_DIR_REL,
        chain_cache_dir=project_root / DEFAULT_CHAIN_CACHE_REL,
        external_dir=project_root / DEFAULT_EXTERNAL_DIR_REL,
        thetadata_base_url=str(args.thetadata_base_url).rstrip("/"),
        target_tenors=target_tenors,
        run_timestamp=now_stamp(),
        check_only=check_only,
        update_missing=update_missing,
        single_date=single_date,
        start_date=start_date,
        end_date=end_date,
        max_dates=args.max_dates,
        force_refresh=bool(args.force_refresh),
        write_canonical=bool(args.write_canonical),
        max_workers=int(args.max_workers),
        require_thetadata=not bool(args.no_require_thetadata),
        quote_time=args.quote_time,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        cfg = parse_args(argv)
        return run(cfg)
    except Exception as exc:
        print("\nERROR:", repr(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
