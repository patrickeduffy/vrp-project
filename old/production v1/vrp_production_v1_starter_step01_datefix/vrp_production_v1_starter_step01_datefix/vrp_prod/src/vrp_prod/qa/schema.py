from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

REPAIRED_TERM_STRUCTURE_REQUIRED_COLUMNS = [
    "trade_date",
    "target_days",
    "implied_variance",
    "vix_style_vol",
    "near_expiration",
    "near_days",
    "near_variance",
    "next_expiration",
    "next_days",
    "next_variance",
    "methodology_version",
    "quote_time_used",
    "raw_implied_variance",
    "raw_vix_style_vol",
    "is_repaired",
]


@dataclass
class DataFrameCheckResult:
    file: str
    exists: bool
    rows: int | None = None
    latest_date: str | None = None
    earliest_date: str | None = None
    missing_required_columns: List[str] | None = None
    duplicate_date_tenor_rows: int | None = None
    dates_with_not_9_tenors: int | None = None
    invalid_variance_rows: int | None = None
    notes: List[str] | None = None


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file type: {path}")


def first_existing(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def parse_project_date_series(s: pd.Series) -> pd.Series:
    """Parse project date columns safely.

    Several historical files store dates as integers like 20260625 or strings like
    "2026-06-25". A plain pd.to_datetime(integer_series) interprets integers as
    nanoseconds since 1970, which makes the inventory report incorrectly show
    1970-01-01. This helper detects YYYYMMDD-style numeric/string dates first.
    """
    if s.empty:
        return pd.to_datetime(s, errors="coerce")

    non_null = s.dropna()
    if non_null.empty:
        return pd.to_datetime(s, errors="coerce")

    # Numeric columns such as 20260625 need explicit YYYYMMDD parsing.
    if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
        txt = s.astype("Int64").astype(str).str.replace("<NA>", "", regex=False)
        compact = txt.str.fullmatch(r"\d{8}", na=False)
        if compact.mean() >= 0.90:
            return pd.to_datetime(txt.where(compact), format="%Y%m%d", errors="coerce")

    # Object/string columns may also contain compact YYYYMMDD dates.
    txt = s.astype(str).str.strip()
    compact = txt.str.fullmatch(r"\d{8}", na=False)
    if compact.mean() >= 0.90:
        return pd.to_datetime(txt.where(compact), format="%Y%m%d", errors="coerce")

    return pd.to_datetime(s, errors="coerce")


def check_repaired_term_structure(path: Path) -> DataFrameCheckResult:
    if not path.exists():
        return DataFrameCheckResult(file=str(path), exists=False, notes=["File not found"])

    df = read_table(path)
    notes: List[str] = []
    missing = [c for c in REPAIRED_TERM_STRUCTURE_REQUIRED_COLUMNS if c not in df.columns]

    latest = earliest = None
    duplicate_rows = dates_with_not_9 = invalid_var = None

    if "trade_date" in df.columns:
        dates = parse_project_date_series(df["trade_date"])
        latest = dates.max().date().isoformat() if dates.notna().any() else None
        earliest = dates.min().date().isoformat() if dates.notna().any() else None

    if {"trade_date", "target_days"}.issubset(df.columns):
        duplicate_rows = int(df.duplicated(["trade_date", "target_days"]).sum())
        counts = df.groupby("trade_date")["target_days"].nunique(dropna=True)
        dates_with_not_9 = int((counts != len(TARGET_TENORS)).sum())
        observed_tenors = sorted(pd.to_numeric(df["target_days"], errors="coerce").dropna().astype(int).unique().tolist())
        if observed_tenors != TARGET_TENORS:
            notes.append(f"Observed tenor set differs from target: {observed_tenors}")

    if "implied_variance" in df.columns:
        iv = pd.to_numeric(df["implied_variance"], errors="coerce")
        invalid_var = int((iv.isna() | (iv <= 0)).sum())

    return DataFrameCheckResult(
        file=str(path),
        exists=True,
        rows=int(len(df)),
        latest_date=latest,
        earliest_date=earliest,
        missing_required_columns=missing,
        duplicate_date_tenor_rows=duplicate_rows,
        dates_with_not_9_tenors=dates_with_not_9,
        invalid_variance_rows=invalid_var,
        notes=notes,
    )


def latest_date_in_table(path: Path, candidate_date_columns: Sequence[str] = ("trade_date", "date", "observation_date")) -> Dict[str, str | int | bool | None]:
    if not path.exists():
        return {"file": str(path), "exists": False, "rows": None, "date_column": None, "latest_date": None, "earliest_date": None}
    try:
        df = read_table(path)
    except Exception as exc:
        return {"file": str(path), "exists": True, "rows": None, "date_column": None, "latest_date": None, "earliest_date": None, "error": str(exc)}

    date_col = next((c for c in candidate_date_columns if c in df.columns), None)
    if date_col is None:
        return {"file": str(path), "exists": True, "rows": int(len(df)), "date_column": None, "latest_date": None, "earliest_date": None}

    dates = parse_project_date_series(df[date_col])
    return {
        "file": str(path),
        "exists": True,
        "rows": int(len(df)),
        "date_column": date_col,
        "latest_date": dates.max().date().isoformat() if dates.notna().any() else None,
        "earliest_date": dates.min().date().isoformat() if dates.notna().any() else None,
    }
