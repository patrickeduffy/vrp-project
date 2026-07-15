#!/usr/bin/env python3
"""Read-only inventory of tenor-level trade outcome sources for VRP research.

Purpose
-------
Find existing parquet/CSV files that may contain realized trade outcomes by
trade date and tenor. The script does not construct trades, change signals,
run a parameter sweep, size trades, or modify any source file.

Default project root:
    C:\\Users\\patri\\vrp_project

Default scan roots:
    data\\processed
    data\\audit

Outputs are timestamped audit files under:
    data\\audit\\wilder_rsi_trade_outcome_source_inventory
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - pandas fallback remains available
    pq = None


SCRIPT_NAME = "vrp_wilder_rsi_trade_outcome_source_inventory_v1.py"
SCRIPT_VERSION = "1.0.0"
DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
DEFAULT_SCAN_RELS = (
    Path("data/processed"),
    Path("data/audit"),
)
DEFAULT_AUDIT_REL = Path("data/audit/wilder_rsi_trade_outcome_source_inventory")
TARGET_TENORS = (12, 15, 18, 21, 24, 27, 30, 33)
INACTIVE_TENOR = 9
SUPPORTED_EXTENSIONS = {".parquet", ".csv"}


class InventoryError(RuntimeError):
    """Raised when the inventory cannot complete its required checks."""


@dataclass(frozen=True)
class FileRecord:
    file_path: str
    relative_path: str
    file_name: str
    extension: str
    file_size_bytes: int
    modified_time: str
    row_count_metadata: int | None
    column_count: int | None
    filename_keyword_hits: str
    schema_keyword_hits: str
    plausibility_score: int
    plausible_candidate: bool
    inspection_error: str


ROLE_ORDER = (
    "trade_date",
    "tenor",
    "expiration",
    "trade_id",
    "layer",
    "selection",
    "held_to_expiration",
    "outcome",
    "expected_metric",
    "win_indicator",
    "max_risk",
    "entry_credit",
    "short_strike",
    "long_strike",
    "terminal_value",
    "structure",
)

FILENAME_TERMS: Mapping[str, int] = {
    "trade": 3,
    "trades": 3,
    "outcome": 5,
    "outcomes": 5,
    "return": 4,
    "returns": 4,
    "pnl": 5,
    "payoff": 5,
    "expiration": 3,
    "expiry": 3,
    "holding": 2,
    "spread": 2,
    "premium": 2,
    "credit": 2,
    "tenor": 3,
    "sizing": 2,
    "naked_atm_put": 4,
    "selected_trades": -3,
    "latest_snapshot": -4,
    "manifest": -3,
    "inventory": -5,
}

EXACT_ROLE_NAMES: Mapping[str, set[str]] = {
    "trade_date": {
        "trade_date", "entry_date", "signal_date", "quote_date", "asof_date",
        "date", "trade_dt", "entry_dt",
    },
    "tenor": {
        "tenor", "tenor_days", "dte", "days_to_expiry", "days_to_expiration",
        "target_dte", "actual_dte", "trade_tenor", "expiry_dte",
    },
    "expiration": {
        "expiration", "expiration_date", "expiry", "expiry_date", "exp_date",
        "maturity", "maturity_date",
    },
    "trade_id": {
        "trade_id", "trade_key", "position_id", "strategy_trade_id",
    },
    "layer": {
        "layer", "signal_layer", "selected_layer", "sleeve", "sleeve_id",
        "selected_sleeve_id", "core_secondary", "tier",
    },
    "selection": {
        "selected", "is_selected", "selection_rank", "selected_tenor",
        "selected_layer", "selected_sleeve_id",
    },
    "held_to_expiration": {
        "held_to_expiration", "hold_to_expiry", "hold_to_expiration",
        "expired", "is_expired", "settled_at_expiry",
    },
    "win_indicator": {
        "win", "is_win", "winner", "profitable", "win_flag", "won",
    },
    "max_risk": {
        "max_loss", "max_risk", "maximum_loss", "risk_amount", "max_loss_amount",
        "spread_width", "capital_at_risk", "initial_max_loss",
    },
    "entry_credit": {
        "entry_credit", "net_credit", "credit", "premium", "entry_premium",
        "premium_received", "initial_credit",
    },
    "short_strike": {
        "short_strike", "strike_short", "sold_strike",
    },
    "long_strike": {
        "long_strike", "strike_long", "bought_strike",
    },
    "terminal_value": {
        "expiration_spot", "expiry_spot", "terminal_spot", "settlement_price",
        "expiration_settlement", "expiry_settlement", "terminal_price",
        "underlying_at_expiry", "spy_expiry_close", "spx_settlement",
    },
    "structure": {
        "structure", "strategy", "option_type", "put_call", "right",
        "spread_type", "trade_type",
    },
}

# Expected/estimated loss fields are not accepted as realized trade outcomes.
EXPECTED_METRIC_PATTERNS = (
    r"(^|_)expected(_|$)",
    r"(^|_)estimated(_|$)",
    r"(^|_)forecast(_|$)",
    r"(^|_)expected_loss(_|$)",
)

STRONG_OUTCOME_NAMES = {
    "trade_return", "trade_return_pct", "return_on_max_risk", "return_on_risk",
    "realized_return", "realized_return_pct", "expiration_return",
    "expiry_return", "trade_pnl", "net_pnl", "gross_pnl", "realized_pnl",
    "expiration_pnl", "expiry_pnl", "payoff", "trade_payoff", "pnl",
    "return_pct", "romr", "return_max_loss",
}

OUTCOME_PATTERNS = (
    r"(^|_)(realized_)?pnl($|_)",
    r"(^|_)trade_return($|_)",
    r"(^|_)realized_return($|_)",
    r"(^|_)return_on_(max_)?risk($|_)",
    r"(^|_)expiration_return($|_)",
    r"(^|_)expiry_return($|_)",
    r"(^|_)payoff($|_)",
    r"(^|_)profit_loss($|_)",
    r"(^|_)net_profit($|_)",
)

SCHEMA_KEYWORDS = {
    "trade", "tenor", "dte", "expiration", "expiry", "return", "pnl",
    "payoff", "profit", "loss", "premium", "credit", "strike", "settlement",
    "winner", "win", "max_risk", "max_loss",
}


# --------------------------------------------------------------------------------------
# General helpers
# --------------------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help=f"Project root. Default: {DEFAULT_PROJECT_ROOT}",
    )
    parser.add_argument(
        "--scan-dir",
        action="append",
        type=Path,
        default=None,
        help=(
            "Directory to scan. May be repeated. Relative paths are resolved under "
            "the project root. Defaults to data/processed and data/audit."
        ),
    )
    parser.add_argument(
        "--audit-dir",
        type=Path,
        default=None,
        help="Audit output directory. Relative paths resolve under project root.",
    )
    return parser.parse_args()


def normalize_path(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def resolve_path(project_root: Path, path: Path) -> Path:
    return normalize_path(path if path.is_absolute() else project_root / path)


def safe_iso_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def normalize_name(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    return re.sub(r"_+", "_", text)


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and not np.isfinite(value):
            return None
        return value
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    if pd.isna(value):
        return None
    return str(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def print_section(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temporary, index=False)
    os.replace(temporary, path)


def atomic_write_json(payload: Mapping[str, Any], path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(json_safe(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temporary, path)


# --------------------------------------------------------------------------------------
# Schema inspection and semantic detection
# --------------------------------------------------------------------------------------

def filename_hits(path: Path) -> tuple[list[str], int]:
    name = normalize_name(path.stem)
    hits: list[str] = []
    score = 0
    for term, weight in FILENAME_TERMS.items():
        normalized_term = normalize_name(term)
        if normalized_term and normalized_term in name:
            hits.append(term)
            score += weight
    return sorted(set(hits)), score


def detect_roles(column: str) -> set[str]:
    normalized = normalize_name(column)
    roles: set[str] = set()

    for role, exact_names in EXACT_ROLE_NAMES.items():
        if normalized in exact_names:
            roles.add(role)

    if any(re.search(pattern, normalized) for pattern in EXPECTED_METRIC_PATTERNS):
        roles.add("expected_metric")
    else:
        if normalized in STRONG_OUTCOME_NAMES or any(
            re.search(pattern, normalized) for pattern in OUTCOME_PATTERNS
        ):
            roles.add("outcome")

    # Controlled broader matches, after excluding expected/forecast metrics.
    tokens = set(normalized.split("_"))
    if "trade_date" not in roles and (
        normalized.endswith("_date") or normalized.startswith("date_")
    ):
        if tokens & {"trade", "entry", "signal", "quote", "asof"}:
            roles.add("trade_date")
    if "tenor" in tokens or normalized.endswith("_dte") or normalized.startswith("dte_"):
        roles.add("tenor")
    if tokens & {"expiration", "expiry", "maturity"} and "date" in tokens:
        roles.add("expiration")
    if "selected" in tokens or "selection" in tokens:
        roles.add("selection")
    if "sleeve" in tokens or "layer" in tokens:
        roles.add("layer")
    if "short" in tokens and "strike" in tokens:
        roles.add("short_strike")
    if "long" in tokens and "strike" in tokens:
        roles.add("long_strike")
    if tokens & {"credit", "premium"} and not tokens & {"expected", "forecast"}:
        roles.add("entry_credit")
    if "max" in tokens and tokens & {"loss", "risk"}:
        roles.add("max_risk")
    if tokens & {"settlement", "terminal"} and not tokens & {"date"}:
        roles.add("terminal_value")
    if normalized.startswith("is_win") or normalized.endswith("_win_flag"):
        roles.add("win_indicator")

    return roles


def schema_hits(columns: Sequence[str]) -> tuple[list[str], int, dict[str, list[str]]]:
    hits: set[str] = set()
    roles_to_columns: dict[str, list[str]] = {role: [] for role in ROLE_ORDER}
    score = 0

    for column in columns:
        normalized = normalize_name(column)
        for keyword in SCHEMA_KEYWORDS:
            if keyword in normalized:
                hits.add(keyword)
        for role in detect_roles(column):
            roles_to_columns.setdefault(role, []).append(column)

    if roles_to_columns.get("trade_date"):
        score += 3
    if roles_to_columns.get("tenor"):
        score += 5
    if roles_to_columns.get("outcome"):
        score += 8
    if roles_to_columns.get("expiration"):
        score += 2
    if roles_to_columns.get("max_risk"):
        score += 2
    if roles_to_columns.get("entry_credit"):
        score += 1
    if roles_to_columns.get("short_strike") and roles_to_columns.get("long_strike"):
        score += 2
    if roles_to_columns.get("selection") and not roles_to_columns.get("outcome"):
        score -= 3
    if roles_to_columns.get("expected_metric") and not roles_to_columns.get("outcome"):
        score -= 3

    return sorted(hits), score, roles_to_columns


def inspect_parquet_schema(path: Path) -> tuple[list[str], list[str], int | None]:
    if pq is not None:
        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow
        columns = list(schema.names)
        dtypes = [str(schema.field(name).type) for name in columns]
        row_count = int(parquet_file.metadata.num_rows)
        return columns, dtypes, row_count

    frame = pd.read_parquet(path)
    return list(frame.columns), [str(dtype) for dtype in frame.dtypes], len(frame)


def inspect_csv_schema(path: Path) -> tuple[list[str], list[str], int | None]:
    sample = pd.read_csv(path, nrows=200, low_memory=False)
    return list(sample.columns), [str(dtype) for dtype in sample.dtypes], None


def inspect_schema(path: Path) -> tuple[list[str], list[str], int | None]:
    if path.suffix.lower() == ".parquet":
        return inspect_parquet_schema(path)
    if path.suffix.lower() == ".csv":
        return inspect_csv_schema(path)
    raise InventoryError(f"Unsupported file extension: {path}")


def candidate_rule(
    filename_score: int,
    schema_score: int,
    roles: Mapping[str, Sequence[str]],
) -> bool:
    has_date = bool(roles.get("trade_date"))
    has_tenor = bool(roles.get("tenor"))
    has_outcome = bool(roles.get("outcome"))
    has_trade_shape = bool(
        roles.get("expiration")
        or roles.get("entry_credit")
        or roles.get("short_strike")
        or roles.get("max_risk")
    )

    return bool(
        (has_date and has_tenor and (has_outcome or has_trade_shape))
        or (has_tenor and has_outcome)
        or (filename_score >= 6 and schema_score >= 3)
    )


# --------------------------------------------------------------------------------------
# Candidate data analysis
# --------------------------------------------------------------------------------------

def choose_column(columns: Sequence[str], role: str) -> str | None:
    candidates = [column for column in columns if role in detect_roles(column)]
    if not candidates:
        return None

    preferred = list(EXACT_ROLE_NAMES.get(role, set()))
    preferred_order = {name: index for index, name in enumerate(preferred)}

    def sort_key(column: str) -> tuple[int, int, str]:
        normalized = normalize_name(column)
        exact_rank = preferred_order.get(normalized, 10_000)
        return (exact_rank, len(normalized), normalized)

    return sorted(candidates, key=sort_key)[0]


def read_candidate_columns(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    selected = list(dict.fromkeys(columns))
    if not selected:
        return pd.DataFrame()
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path, columns=selected)
    return pd.read_csv(path, usecols=selected, low_memory=False)


def normalize_trade_date(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed = pd.to_datetime(series, errors="coerce")
    elif pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        integer = numeric.round().astype("Int64")
        parsed = pd.to_datetime(integer.astype("string"), format="%Y%m%d", errors="coerce")
    else:
        text = series.astype("string").str.strip()
        compact = text.str.fullmatch(r"\d{8}", na=False)
        parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        if compact.any():
            parsed.loc[compact] = pd.to_datetime(
                text.loc[compact], format="%Y%m%d", errors="coerce"
            )
        if (~compact).any():
            parsed.loc[~compact] = pd.to_datetime(text.loc[~compact], errors="coerce")
    return parsed.dt.normalize()


def numeric_tenor(series: pd.Series) -> pd.Series:
    if pd.api.types.is_timedelta64_dtype(series):
        return series.dt.days.astype("Float64")
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return numeric
    extracted = series.astype("string").str.extract(r"(\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(extracted, errors="coerce")


def infer_outcome_unit(column: str) -> tuple[str, str]:
    normalized = normalize_name(column)
    tokens = set(normalized.split("_"))
    if tokens & {"pct", "percent", "percentage"}:
        return "percent", "column_name"
    if "bps" in tokens or "basis_points" in normalized:
        return "basis_points", "column_name"
    if tokens & {"dollar", "dollars", "usd"} or normalized.endswith("_pnl_usd"):
        return "dollars", "column_name"
    if normalized in {"return_on_max_risk", "return_on_risk", "romr", "return_max_loss"}:
        return "fraction_or_percent_of_max_risk", "column_name_requires_scale_check"
    if "premium_multiple" in normalized or "multiple_of_premium" in normalized:
        return "premium_multiple", "column_name"
    if "pnl" in tokens or "payoff" in tokens:
        return "unknown_pnl_unit", "ambiguous"
    if "return" in tokens:
        return "unknown_return_scale", "ambiguous"
    return "unknown", "ambiguous"


def field_strength(column: str) -> int:
    normalized = normalize_name(column)
    if normalized in STRONG_OUTCOME_NAMES:
        return 3
    if any(re.search(pattern, normalized) for pattern in OUTCOME_PATTERNS):
        return 2
    return 1


def boolish_true_fraction(series: pd.Series) -> float | None:
    if series.empty:
        return None
    normalized = series.astype("string").str.strip().str.lower()
    mapping = normalized.map(
        {
            "true": True, "1": True, "yes": True, "y": True, "held": True,
            "false": False, "0": False, "no": False, "n": False,
        }
    )
    valid = mapping.dropna()
    if valid.empty:
        return None
    return float(valid.astype(bool).mean())


def recommend_trade_key(
    frame: pd.DataFrame,
    date_col: str | None,
    tenor_col: str | None,
    trade_id_col: str | None,
    structure_cols: Sequence[str],
) -> tuple[str, bool | None, int | None]:
    if trade_id_col and trade_id_col in frame.columns:
        duplicates = int(frame[trade_id_col].duplicated(keep=False).sum())
        return trade_id_col, duplicates == 0, duplicates

    key_cols = [column for column in (date_col, tenor_col) if column]
    if len(key_cols) < 2:
        return "UNRESOLVED", None, None

    duplicate_rows = int(frame.duplicated(key_cols, keep=False).sum())
    if duplicate_rows == 0:
        return " × ".join(key_cols), True, 0

    for extra_count in range(1, min(3, len(structure_cols)) + 1):
        candidate = key_cols + list(structure_cols[:extra_count])
        candidate_duplicates = int(frame.duplicated(candidate, keep=False).sum())
        if candidate_duplicates == 0:
            return " × ".join(candidate), True, 0

    return " × ".join(key_cols), False, duplicate_rows


def classification_for_candidate(metrics: Mapping[str, Any]) -> tuple[str, str, int]:
    date_col = metrics.get("trade_date_field")
    tenor_col = metrics.get("tenor_field")
    outcome_cols = metrics.get("outcome_fields_list", [])
    target_coverage = int(metrics.get("target_tenor_count", 0) or 0)
    multiple_tenors = bool(metrics.get("multiple_tenors_same_date"))
    likely_one_per_day = bool(metrics.get("likely_one_trade_per_day"))
    all_outcomes_null = bool(metrics.get("all_outcome_fields_all_null"))
    strong_outcome = int(metrics.get("best_outcome_field_strength", 0) or 0) >= 2
    output_null_rate = metrics.get("best_outcome_null_rate")
    selected_name = "selected" in normalize_name(str(metrics.get("file_name", "")))
    held_evidence = str(metrics.get("held_to_expiration_evidence", ""))

    score = int(metrics.get("plausibility_score", 0) or 0)
    score += target_coverage * 2
    score += 6 if multiple_tenors else 0
    score += 5 if strong_outcome else 0
    score += 3 if metrics.get("expiration_field") else 0
    score += 2 if metrics.get("max_risk_fields") else 0
    score += 2 if metrics.get("entry_credit_fields") else 0
    score += 2 if "confirmed" in held_evidence.lower() else 0
    score -= 10 if likely_one_per_day else 0
    score -= 5 if selected_name else 0
    score -= 10 if all_outcomes_null else 0
    if output_null_rate is not None and output_null_rate > 0.05:
        score -= 3

    if not date_col or not tenor_col:
        return "NOT_SUITABLE", "Missing a confirmed trade-date or tenor field.", score
    if not outcome_cols or all_outcomes_null:
        return "NOT_SUITABLE", "No usable realized outcome field was identified.", score
    if likely_one_per_day or (selected_name and not multiple_tenors):
        return (
            "NOT_SUITABLE",
            "Appears to have prior one-trade-per-day/selected-trade suppression.",
            score,
        )
    if target_coverage == 0:
        return "NOT_SUITABLE", "No target tenors were found.", score

    unresolved: list[str] = []
    if target_coverage < len(TARGET_TENORS):
        unresolved.append("does not contain all target tenors")
    if not multiple_tenors:
        unresolved.append("multiple tenors per date were not demonstrated")
    if not strong_outcome:
        unresolved.append("outcome-field meaning is weak or ambiguous")
    if metrics.get("outcome_unit") in {"unknown", "unknown_pnl_unit", "unknown_return_scale"}:
        unresolved.append("outcome unit/scale is unresolved")
    if "confirmed" not in held_evidence.lower():
        unresolved.append("held-to-expiration status is not confirmed")
    if metrics.get("holiday_adjusted_expiration_methodology") != "CONFIRMED":
        unresolved.append("holiday-adjusted expiration methodology is not confirmed")

    if not unresolved:
        return (
            "SUITABLE",
            "Contains all target tenors, multiple tenors per date, a strong realized outcome, and confirmed expiration treatment.",
            score,
        )

    return "POSSIBLY_SUITABLE", "; ".join(unresolved) + ".", score


def analyze_candidate(
    path: Path,
    columns: Sequence[str],
    dtypes: Sequence[str],
    row_count_metadata: int | None,
    plausibility_score: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    role_map: dict[str, list[str]] = {role: [] for role in ROLE_ORDER}
    for column in columns:
        for role in detect_roles(column):
            role_map.setdefault(role, []).append(column)

    date_col = choose_column(columns, "trade_date")
    tenor_col = choose_column(columns, "tenor")
    expiration_col = choose_column(columns, "expiration")
    trade_id_col = choose_column(columns, "trade_id")
    held_col = choose_column(columns, "held_to_expiration")

    outcome_cols = sorted(
        role_map.get("outcome", []),
        key=lambda column: (-field_strength(column), len(normalize_name(column)), normalize_name(column)),
    )
    expected_cols = sorted(role_map.get("expected_metric", []))
    structure_cols = list(
        dict.fromkeys(
            role_map.get("structure", [])
            + role_map.get("short_strike", [])
            + role_map.get("long_strike", [])
        )
    )

    load_columns = list(
        dict.fromkeys(
            [column for column in (date_col, tenor_col, expiration_col, trade_id_col, held_col) if column]
            + outcome_cols
            + expected_cols
            + role_map.get("win_indicator", [])
            + role_map.get("max_risk", [])
            + role_map.get("entry_credit", [])
            + role_map.get("short_strike", [])
            + role_map.get("long_strike", [])
            + role_map.get("terminal_value", [])
            + role_map.get("layer", [])
            + role_map.get("selection", [])
            + role_map.get("structure", [])
        )
    )

    frame = read_candidate_columns(path, load_columns)
    actual_row_count = len(frame)

    null_counts = {column: int(frame[column].isna().sum()) for column in load_columns}
    null_rates = {
        column: (float(frame[column].isna().mean()) if actual_row_count else None)
        for column in load_columns
    }

    parsed_dates = normalize_trade_date(frame[date_col]) if date_col else pd.Series(dtype="datetime64[ns]")
    tenors = numeric_tenor(frame[tenor_col]) if tenor_col else pd.Series(dtype="float64")

    valid_tenors = sorted(
        {
            int(round(value))
            for value in tenors.dropna().tolist()
            if np.isfinite(value) and abs(value - round(value)) < 1e-8
        }
    )
    target_present = [tenor for tenor in TARGET_TENORS if tenor in valid_tenors]
    missing_target = [tenor for tenor in TARGET_TENORS if tenor not in valid_tenors]

    unique_dates = int(parsed_dates.nunique()) if date_col else 0
    min_date = parsed_dates.min() if date_col and parsed_dates.notna().any() else None
    max_date = parsed_dates.max() if date_col and parsed_dates.notna().any() else None

    max_unique_tenors_per_date: int | None = None
    median_unique_tenors_per_date: float | None = None
    pct_dates_multiple_tenors: float | None = None
    multiple_tenors_same_date = False
    likely_one_trade_per_day = False

    if date_col and tenor_col:
        temporary = pd.DataFrame({"date": parsed_dates, "tenor": tenors}).dropna()
        if not temporary.empty:
            per_date = temporary.groupby("date", sort=False)["tenor"].nunique()
            max_unique_tenors_per_date = int(per_date.max())
            median_unique_tenors_per_date = float(per_date.median())
            pct_dates_multiple_tenors = float((per_date > 1).mean())
            multiple_tenors_same_date = bool((per_date > 1).any())
            likely_one_trade_per_day = bool(max_unique_tenors_per_date <= 1)

    outcome_null_counts = {column: null_counts[column] for column in outcome_cols}
    outcome_null_rates = {column: null_rates[column] for column in outcome_cols}
    best_outcome = outcome_cols[0] if outcome_cols else None
    best_outcome_strength = field_strength(best_outcome) if best_outcome else 0
    best_outcome_null_rate = outcome_null_rates.get(best_outcome) if best_outcome else None
    all_outcome_fields_all_null = bool(
        outcome_cols and all(outcome_null_counts[column] == actual_row_count for column in outcome_cols)
    )

    outcome_unit, outcome_unit_basis = (
        infer_outcome_unit(best_outcome) if best_outcome else ("unknown", "no_outcome_field")
    )

    held_evidence = "UNCONFIRMED"
    if held_col:
        true_fraction = boolish_true_fraction(frame[held_col])
        if true_fraction is not None and true_fraction == 1.0:
            held_evidence = f"CONFIRMED_BY_FIELD:{held_col}=100% true"
        elif true_fraction is not None:
            held_evidence = f"MIXED_BY_FIELD:{held_col} true_fraction={true_fraction:.6f}"
        else:
            held_evidence = f"FIELD_PRESENT_BUT_UNRESOLVED:{held_col}"
    elif expiration_col and (role_map.get("terminal_value") or any("expir" in normalize_name(c) for c in outcome_cols)):
        held_evidence = "SUPPORTED_BY_EXPIRATION_AND_TERMINAL_FIELDS_NOT_FULLY_CONFIRMED"

    expiration_methodology = "UNCONFIRMED"
    expiration_methodology_evidence = "A data file alone does not prove the project holiday-adjusted expiration-selection method."
    if any("holiday_adjust" in normalize_name(column) for column in columns):
        expiration_methodology = "CONFIRMED"
        expiration_methodology_evidence = "Explicit holiday-adjusted expiration field found."

    trade_key, trade_key_unique, duplicate_rows = recommend_trade_key(
        frame=frame,
        date_col=date_col,
        tenor_col=tenor_col,
        trade_id_col=trade_id_col,
        structure_cols=structure_cols,
    )

    layer_fields = role_map.get("layer", [])
    selection_fields = role_map.get("selection", [])
    core_secondary_interpretation = "UNRESOLVED"
    if layer_fields:
        core_secondary_interpretation = "LAYER_OR_SLEEVE_FIELD_PRESENT"
    else:
        core_secondary_interpretation = (
            "LIKELY_SAME_TENOR_OUTCOME_WITH_SIGNAL_LAYER_APPLIED_LATER; verify trade construction"
        )

    metrics: dict[str, Any] = {
        "file_path": str(path),
        "file_name": path.name,
        "extension": path.suffix.lower(),
        "file_size_bytes": path.stat().st_size,
        "modified_time": safe_iso_timestamp(path.stat().st_mtime),
        "row_count_metadata": row_count_metadata,
        "row_count_loaded": actual_row_count,
        "column_count": len(columns),
        "plausibility_score": plausibility_score,
        "trade_date_field": date_col,
        "tenor_field": tenor_col,
        "expiration_field": expiration_col,
        "trade_id_field": trade_id_col,
        "outcome_fields": " | ".join(outcome_cols),
        "outcome_fields_list": outcome_cols,
        "expected_or_forecast_metric_fields": " | ".join(expected_cols),
        "best_outcome_field": best_outcome,
        "best_outcome_field_strength": best_outcome_strength,
        "best_outcome_null_rate": best_outcome_null_rate,
        "all_outcome_fields_all_null": all_outcome_fields_all_null,
        "outcome_null_counts_json": json.dumps(outcome_null_counts, sort_keys=True),
        "outcome_null_rates_json": json.dumps(json_safe(outcome_null_rates), sort_keys=True),
        "outcome_unit": outcome_unit,
        "outcome_unit_basis": outcome_unit_basis,
        "win_indicator_fields": " | ".join(role_map.get("win_indicator", [])),
        "max_risk_fields": " | ".join(role_map.get("max_risk", [])),
        "entry_credit_fields": " | ".join(role_map.get("entry_credit", [])),
        "short_strike_fields": " | ".join(role_map.get("short_strike", [])),
        "long_strike_fields": " | ".join(role_map.get("long_strike", [])),
        "terminal_value_fields": " | ".join(role_map.get("terminal_value", [])),
        "layer_or_sleeve_fields": " | ".join(layer_fields),
        "selection_fields": " | ".join(selection_fields),
        "structure_fields": " | ".join(structure_cols),
        "date_min": min_date,
        "date_max": max_date,
        "unique_dates": unique_dates,
        "tenors_present": " | ".join(map(str, valid_tenors)),
        "target_tenors_present": " | ".join(map(str, target_present)),
        "missing_target_tenors": " | ".join(map(str, missing_target)),
        "target_tenor_count": len(target_present),
        "contains_9d": INACTIVE_TENOR in valid_tenors,
        "max_unique_tenors_per_date": max_unique_tenors_per_date,
        "median_unique_tenors_per_date": median_unique_tenors_per_date,
        "pct_dates_with_multiple_tenors": pct_dates_multiple_tenors,
        "multiple_tenors_same_date": multiple_tenors_same_date,
        "likely_one_trade_per_day": likely_one_trade_per_day,
        "likely_already_signal_or_selection_filtered": bool(selection_fields)
        or "selected" in normalize_name(path.stem),
        "proposed_trade_key": trade_key,
        "proposed_trade_key_unique": trade_key_unique,
        "duplicate_rows_under_proposed_base_key": duplicate_rows,
        "held_to_expiration_evidence": held_evidence,
        "holiday_adjusted_expiration_methodology": expiration_methodology,
        "expiration_methodology_evidence": expiration_methodology_evidence,
        "core_secondary_outcome_interpretation": core_secondary_interpretation,
        "null_counts_for_inspected_fields_json": json.dumps(null_counts, sort_keys=True),
        "inspection_error": "",
    }

    classification, reason, final_score = classification_for_candidate(metrics)
    metrics["classification"] = classification
    metrics["classification_reason"] = reason
    metrics["candidate_quality_score"] = final_score

    dtype_map = dict(zip(columns, dtypes))
    schema_rows: list[dict[str, Any]] = []
    for column in columns:
        roles = sorted(detect_roles(column))
        schema_rows.append(
            {
                "file_path": str(path),
                "file_name": path.name,
                "column_name": column,
                "dtype": dtype_map.get(column, ""),
                "semantic_roles": " | ".join(roles),
                "loaded_for_candidate_analysis": column in load_columns,
                "null_count": null_counts.get(column),
                "null_rate": null_rates.get(column),
            }
        )

    # Remove internal list before CSV output; the classification has already used it.
    metrics.pop("outcome_fields_list", None)
    return metrics, schema_rows


# --------------------------------------------------------------------------------------
# Main inventory
# --------------------------------------------------------------------------------------

def collect_files(scan_roots: Sequence[Path], audit_dir: Path) -> list[Path]:
    files: list[Path] = []
    audit_resolved = audit_dir.resolve()

    for root in scan_roots:
        if not root.exists():
            raise InventoryError(f"Scan directory does not exist: {root}")
        if not root.is_dir():
            raise InventoryError(f"Scan path is not a directory: {root}")
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue
            try:
                path.resolve().relative_to(audit_resolved)
                continue
            except ValueError:
                pass
            files.append(path)

    return sorted(set(files), key=lambda path: str(path).lower())


def main() -> int:
    args = parse_args()
    project_root = normalize_path(args.project_root)
    scan_inputs = args.scan_dir if args.scan_dir else list(DEFAULT_SCAN_RELS)
    scan_roots = [resolve_path(project_root, path) for path in scan_inputs]
    audit_dir = resolve_path(project_root, args.audit_dir or DEFAULT_AUDIT_REL)
    audit_dir.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now()
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")

    print_section("Trade outcome source inventory — read only")
    print(f"Script:             {SCRIPT_NAME} v{SCRIPT_VERSION}")
    print(f"Project root:       {project_root}")
    for index, root in enumerate(scan_roots, start=1):
        print(f"Scan root {index}:        {root}")
    print(f"Audit directory:    {audit_dir}")
    print(f"Target tenors:      {list(TARGET_TENORS)}")
    print("9D status:          inventory only; remains inactive for optimization")

    source_files = collect_files(scan_roots, audit_dir)
    if not source_files:
        raise InventoryError("No parquet or CSV files were found in the scan roots.")

    source_stats_before = {
        str(path): (path.stat().st_size, path.stat().st_mtime_ns) for path in source_files
    }

    print_section("Inspecting file schemas")
    file_records: list[FileRecord] = []
    candidate_inputs: list[tuple[Path, list[str], list[str], int | None, int]] = []

    for index, path in enumerate(source_files, start=1):
        relative = None
        try:
            relative = path.relative_to(project_root)
        except ValueError:
            relative = path

        name_hits, name_score = filename_hits(path)
        error = ""
        columns: list[str] = []
        dtypes: list[str] = []
        row_count: int | None = None
        schema_keyword_list: list[str] = []
        schema_score = 0
        roles: dict[str, list[str]] = {}
        plausible = False

        try:
            columns, dtypes, row_count = inspect_schema(path)
            schema_keyword_list, schema_score, roles = schema_hits(columns)
            plausible = candidate_rule(name_score, schema_score, roles)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        total_score = name_score + schema_score
        file_records.append(
            FileRecord(
                file_path=str(path),
                relative_path=str(relative),
                file_name=path.name,
                extension=path.suffix.lower(),
                file_size_bytes=path.stat().st_size,
                modified_time=safe_iso_timestamp(path.stat().st_mtime),
                row_count_metadata=row_count,
                column_count=len(columns) if columns else None,
                filename_keyword_hits=" | ".join(name_hits),
                schema_keyword_hits=" | ".join(schema_keyword_list),
                plausibility_score=total_score,
                plausible_candidate=plausible,
                inspection_error=error,
            )
        )

        if plausible and not error:
            candidate_inputs.append((path, columns, dtypes, row_count, total_score))

        if index % 250 == 0:
            print(f"Inspected {index:,} / {len(source_files):,} files...")

    print(f"Files scanned:              {len(source_files):,}")
    print(f"Plausible candidates found: {len(candidate_inputs):,}")

    print_section("Analyzing plausible candidates")
    candidate_rows: list[dict[str, Any]] = []
    schema_rows: list[dict[str, Any]] = []

    for index, (path, columns, dtypes, row_count, score) in enumerate(candidate_inputs, start=1):
        try:
            metrics, rows = analyze_candidate(path, columns, dtypes, row_count, score)
        except Exception as exc:
            metrics = {
                "file_path": str(path),
                "file_name": path.name,
                "extension": path.suffix.lower(),
                "file_size_bytes": path.stat().st_size,
                "modified_time": safe_iso_timestamp(path.stat().st_mtime),
                "row_count_metadata": row_count,
                "row_count_loaded": None,
                "column_count": len(columns),
                "plausibility_score": score,
                "classification": "NOT_SUITABLE",
                "classification_reason": "Candidate analysis failed.",
                "candidate_quality_score": score - 20,
                "inspection_error": f"{type(exc).__name__}: {exc}",
            }
            rows = [
                {
                    "file_path": str(path),
                    "file_name": path.name,
                    "column_name": column,
                    "dtype": dtype,
                    "semantic_roles": " | ".join(sorted(detect_roles(column))),
                    "loaded_for_candidate_analysis": False,
                    "null_count": None,
                    "null_rate": None,
                }
                for column, dtype in zip(columns, dtypes)
            ]

        candidate_rows.append(metrics)
        schema_rows.extend(rows)
        print(
            f"[{index:>3}/{len(candidate_inputs)}] {metrics.get('classification', 'UNKNOWN'):<19} "
            f"score={metrics.get('candidate_quality_score', '')!s:<4} {path.name}"
        )

    candidate_df = pd.DataFrame(candidate_rows)
    if not candidate_df.empty:
        classification_rank = {
            "SUITABLE": 0,
            "POSSIBLY_SUITABLE": 1,
            "NOT_SUITABLE": 2,
        }
        candidate_df["_classification_rank"] = candidate_df["classification"].map(
            classification_rank
        ).fillna(9)
        candidate_df = candidate_df.sort_values(
            ["_classification_rank", "candidate_quality_score", "file_name"],
            ascending=[True, False, True],
            kind="stable",
        ).drop(columns="_classification_rank")

    file_df = pd.DataFrame([asdict(record) for record in file_records])
    schema_df = pd.DataFrame(schema_rows)

    suitable_count = int((candidate_df.get("classification") == "SUITABLE").sum()) if not candidate_df.empty else 0
    possible_count = int((candidate_df.get("classification") == "POSSIBLY_SUITABLE").sum()) if not candidate_df.empty else 0
    rejected_count = int((candidate_df.get("classification") == "NOT_SUITABLE").sum()) if not candidate_df.empty else 0

    recommended: dict[str, Any] | None = None
    confirmed_source = False
    if not candidate_df.empty:
        eligible = candidate_df[
            candidate_df["classification"].isin(["SUITABLE", "POSSIBLY_SUITABLE"])
        ]
        if not eligible.empty:
            recommended = eligible.iloc[0].to_dict()
            confirmed_source = recommended.get("classification") == "SUITABLE"

    # Verify source files were not modified by this read-only analysis.
    modified_sources: list[str] = []
    for path in source_files:
        current = (path.stat().st_size, path.stat().st_mtime_ns)
        if current != source_stats_before[str(path)]:
            modified_sources.append(str(path))
    if modified_sources:
        raise InventoryError(
            "One or more source files changed during the inventory: "
            + "; ".join(modified_sources[:10])
        )

    file_output = audit_dir / f"wilder_rsi_trade_outcome_file_inventory_{timestamp}.csv"
    schema_output = audit_dir / f"wilder_rsi_trade_outcome_schema_inventory_{timestamp}.csv"
    candidate_output = audit_dir / f"wilder_rsi_trade_outcome_candidate_summary_{timestamp}.csv"
    manifest_output = audit_dir / f"wilder_rsi_trade_outcome_source_manifest_{timestamp}.json"

    atomic_write_csv(file_df, file_output)
    atomic_write_csv(schema_df, schema_output)
    atomic_write_csv(candidate_df, candidate_output)

    unresolved_questions: list[str] = []
    if recommended:
        if recommended.get("outcome_unit") in {"unknown", "unknown_pnl_unit", "unknown_return_scale"}:
            unresolved_questions.append("Confirm the unit/scale of the recommended realized outcome field.")
        if "confirmed" not in str(recommended.get("held_to_expiration_evidence", "")).lower():
            unresolved_questions.append("Confirm that recommended outcomes are held to expiration.")
        if recommended.get("holiday_adjusted_expiration_methodology") != "CONFIRMED":
            unresolved_questions.append(
                "Confirm the source uses the project holiday-adjusted expiration-calendar methodology."
            )
        if recommended.get("proposed_trade_key_unique") is not True:
            unresolved_questions.append("Resolve the exact unique trade key.")
        if int(recommended.get("target_tenor_count", 0) or 0) < len(TARGET_TENORS):
            unresolved_questions.append("Locate missing target-tenor outcomes or combine validated sources.")
    else:
        unresolved_questions.append("No candidate currently supports a tenor-isolated outcome join.")

    completed_at = datetime.now()
    result_code = (
        "CONFIRMED_OUTCOME_SOURCE"
        if confirmed_source
        else "CANDIDATE_OUTCOME_SOURCE_REQUIRES_REVIEW"
        if recommended
        else "NO_CONFIRMED_OUTCOME_SOURCE"
    )

    manifest = {
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "script_sha256": sha256_file(Path(__file__).resolve()),
        "run_started_at": started_at,
        "run_completed_at": completed_at,
        "project_root": project_root,
        "scan_roots": scan_roots,
        "audit_dir": audit_dir,
        "target_tenors": TARGET_TENORS,
        "inactive_tenor": INACTIVE_TENOR,
        "files_scanned": len(source_files),
        "schema_inspection_errors": int(file_df["inspection_error"].astype(bool).sum()),
        "plausible_candidates": len(candidate_inputs),
        "suitable_candidates": suitable_count,
        "possibly_suitable_candidates": possible_count,
        "rejected_candidates": rejected_count,
        "result_code": result_code,
        "recommended_source": recommended,
        "unresolved_questions": unresolved_questions,
        "source_files_modified": modified_sources,
        "outputs": {
            "file_inventory": file_output,
            "schema_inventory": schema_output,
            "candidate_summary": candidate_output,
            "manifest": manifest_output,
        },
        "scope_exclusions": [
            "No trade construction",
            "No signal changes",
            "No parameter sweep",
            "No sizing",
            "No one-trade-per-day selection",
            "No source-file writes",
        ],
    }
    atomic_write_json(manifest, manifest_output)

    print_section("Inventory result")
    print(f"Suitable candidates:          {suitable_count:,}")
    print(f"Possibly suitable candidates: {possible_count:,}")
    print(f"Rejected candidates:          {rejected_count:,}")
    print(f"Result code:                  {result_code}")

    if recommended:
        print(f"Top recommended source:       {recommended.get('file_path')}")
        print(f"Classification:               {recommended.get('classification')}")
        print(f"Trade date field:             {recommended.get('trade_date_field')}")
        print(f"Tenor field:                  {recommended.get('tenor_field')}")
        print(f"Best outcome field:           {recommended.get('best_outcome_field')}")
        print(f"Outcome unit:                 {recommended.get('outcome_unit')}")
        print(f"Proposed trade key:           {recommended.get('proposed_trade_key')}")
        print(f"Target tenors present:        {recommended.get('target_tenors_present')}")
        print(f"Multiple tenors per date:     {recommended.get('multiple_tenors_same_date')}")
    else:
        print("Top recommended source:       NONE")

    print("Unresolved questions:")
    if unresolved_questions:
        for question in unresolved_questions:
            print(f"  - {question}")
    else:
        print("  - None")

    print_section("Audit outputs")
    print(f"Wrote: {file_output}")
    print(f"Wrote: {schema_output}")
    print(f"Wrote: {candidate_output}")
    print(f"Wrote: {manifest_output}")

    print_section("PASS")
    print("Read-only trade outcome source inventory completed.")
    print("No source data files were modified and no parameter sweep was run.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except InventoryError as exc:
        print_section("FAIL")
        print(str(exc))
        raise SystemExit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        raise SystemExit(130)
