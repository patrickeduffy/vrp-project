#!/usr/bin/env python3
"""Build the parallel VRP signal base using repaired SPY Wilder RSI14.

Scope
-----
* Replaces all legacy RSI fields with the accepted repaired Wilder RSI source.
* Preserves existing implied variance, forecast variance, VRP, z-score, RV21D,
  SPY-close, model, and other non-RSI signal-base fields.
* Recomputes independent Core and Secondary pass flags from the approved
  starting thresholds.
* Does not optimize parameters, size trades, select one trade per day, create a
  selected-trades file, or overwrite canonical production signal files.

Default project root:
    C:\\Users\\patri\\vrp_project
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCRIPT_NAME = "vrp_repaired_wilder_rsi_signal_base_v1.py"
SCRIPT_VERSION = "1.0.2"
DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")

SIGNAL_INPUT_REL = Path(
    "data/processed/vrp_final_signal/vrp_final_corsi_signal_base_panel_v1.parquet"
)
RSI_INPUT_REL = Path(
    "data/processed/market_data/spy_wilder_rsi14_history_v1.parquet"
)
OUTPUT_DIR_REL = Path("data/processed/vrp_repaired_wilder_rsi_signal")
AUDIT_DIR_REL = Path("data/audit/repaired_wilder_rsi_signal")

BASE_OUTPUT_NAME = "vrp_repaired_wilder_rsi_signal_base_v1.parquet"
SNAPSHOT_OUTPUT_NAME = "vrp_repaired_wilder_rsi_latest_snapshot_v1.parquet"

ACCEPTED_RSI_VERSION = "wilder_rsi14_spy_close_v2_long_warmup"
TARGET_TENORS = (9, 12, 15, 18, 21, 24, 27, 30, 33)

# Existing downstream portfolio-selection and sizing fields are intentionally
# excluded. They are stale after RSI replacement and are outside Step 1 scope.
STALE_PORTFOLIO_EXACT_COLUMNS = {
    "selected",
    "selection_rank",
}
STALE_PORTFOLIO_PREFIXES = (
    "selected_",
)

# Logical required fields. Exact canonical names are preferred. The limited
# aliases exist only to support known project naming variants; ambiguity fails.
REQUIRED_SIGNAL_COLUMNS: Mapping[str, Sequence[str]] = {
    "trade_date": ("trade_date",),
    "tenor": ("tenor",),
    "tenor_bucket": ("tenor_bucket",),
    "implied_variance": ("implied_variance_final", "implied_variance"),
    "forecast_variance": ("forecast_variance_final", "forecast_variance"),
    "model_vrp_log": ("model_vrp_log_final", "model_vrp_log"),
    "z_3m": ("z_3m_final", "model_vrp_z_3m", "z_3m"),
    "z_1y": ("z_1y_final", "model_vrp_z_1y", "z_1y"),
    "rv21d_vol_pct": ("rv21d_vol_pct_final", "rv21d_vol_pct"),
    "spy_close": ("spy_close",),
}

REQUIRED_RSI_COLUMNS: Mapping[str, Sequence[str]] = {
    "trade_date": ("trade_date",),
    "spy_close": ("spy_close",),
    "spy_wilder_rsi14": ("spy_wilder_rsi14",),
    "rsi_formula_version": ("rsi_formula_version",),
}


@dataclass(frozen=True)
class Rule:
    layer: str
    bucket: str
    tenors: tuple[int, ...]
    vrp_log_gt: float
    z_3m_gt: float
    z_1y_gt: float
    rsi_lt: float
    rv21d_gt: float


RULES: tuple[Rule, ...] = (
    Rule("Core", "Middle", (21, 24), 0.65, 0.70, 0.70, 70.0, 8.5),
    Rule("Core", "Back", (27, 30, 33), 0.70, 0.70, 0.70, 70.0, 8.5),
    Rule("Secondary", "Front", (12, 15, 18), 0.65, 0.20, 0.20, 75.0, 7.0),
    Rule("Secondary", "Middle", (21, 24), 0.65, 0.20, 0.20, 76.0, 7.0),
    Rule("Secondary", "Back", (27, 30, 33), 0.65, 0.00, 0.00, 77.0, 6.5),
)


class BuildError(RuntimeError):
    """Raised when a required Step 1 check fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help=f"Project root. Default: {DEFAULT_PROJECT_ROOT}",
    )
    parser.add_argument(
        "--signal-input",
        type=Path,
        default=None,
        help="Optional explicit current final Corsi signal-base input path.",
    )
    parser.add_argument(
        "--rsi-input",
        type=Path,
        default=None,
        help="Optional explicit repaired Wilder RSI history input path.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional explicit parallel processed-output directory.",
    )
    parser.add_argument(
        "--audit-dir",
        type=Path,
        default=None,
        help="Optional explicit audit-output directory.",
    )
    parser.add_argument(
        "--allowed-rsi-version",
        action="append",
        default=None,
        help=(
            "Accepted repaired RSI version. May be repeated. If omitted, only "
            f"{ACCEPTED_RSI_VERSION!r} is accepted."
        ),
    )
    return parser.parse_args()


def resolve_path(project_root: Path, explicit: Path | None, relative: Path) -> Path:
    if explicit is None:
        return project_root / relative
    return explicit if explicit.is_absolute() else project_root / explicit


def normalize_path(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def ensure_file(path: Path, label: str) -> None:
    if not path.exists():
        raise BuildError(f"Missing {label}: {path}")
    if not path.is_file():
        raise BuildError(f"Expected {label} to be a file: {path}")


def resolve_columns(
    df: pd.DataFrame,
    requirements: Mapping[str, Sequence[str]],
    dataset_label: str,
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for logical_name, candidates in requirements.items():
        present = [name for name in candidates if name in df.columns]
        if not present:
            raise BuildError(
                f"{dataset_label} is missing required field {logical_name!r}. "
                f"Accepted column names: {list(candidates)}"
            )

        preferred = candidates[0]
        if preferred in present:
            mapping[logical_name] = preferred
        elif len(present) == 1:
            mapping[logical_name] = present[0]
        else:
            raise BuildError(
                f"{dataset_label} has ambiguous columns for {logical_name!r}: {present}"
            )
    return mapping


def normalize_trade_date(series: pd.Series, label: str) -> pd.Series:
    """Return normalized midnight timestamps without changing the source column."""
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed = pd.to_datetime(series, errors="coerce")
    elif pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        as_int = numeric.round().astype("Int64")
        parsed = pd.to_datetime(as_int.astype("string"), format="%Y%m%d", errors="coerce")
    else:
        text = series.astype("string").str.strip()
        compact_mask = text.str.fullmatch(r"\d{8}", na=False)
        parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        if compact_mask.any():
            parsed.loc[compact_mask] = pd.to_datetime(
                text.loc[compact_mask], format="%Y%m%d", errors="coerce"
            )
        remaining = ~compact_mask
        if remaining.any():
            parsed.loc[remaining] = pd.to_datetime(
                text.loc[remaining], errors="coerce"
            )

    # Normalize timezone-aware values safely if they appear.
    try:
        if getattr(parsed.dt, "tz", None) is not None:
            parsed = parsed.dt.tz_convert(None)
    except (AttributeError, TypeError):
        pass

    parsed = parsed.dt.normalize()
    bad = parsed.isna()
    if bad.any():
        examples = series.loc[bad].head(10).tolist()
        raise BuildError(
            f"Could not parse {int(bad.sum()):,} {label} values as trade dates. "
            f"Examples: {examples}"
        )
    return parsed


def numeric_series(df: pd.DataFrame, column: str, label: str) -> pd.Series:
    values = pd.to_numeric(df[column], errors="coerce")
    non_numeric = values.isna() & df[column].notna()
    if non_numeric.any():
        examples = df.loc[non_numeric, column].head(10).tolist()
        raise BuildError(
            f"Non-numeric values found in {label} column {column!r}. Examples: {examples}"
        )
    return values


def require_no_nulls(df: pd.DataFrame, columns: Iterable[str], dataset_label: str) -> dict[str, int]:
    null_counts = {column: int(df[column].isna().sum()) for column in columns}
    failures = {column: count for column, count in null_counts.items() if count > 0}
    if failures:
        raise BuildError(f"Missing required values in {dataset_label}: {failures}")
    return null_counts


def validate_prior_only_zscore_warmup(
    df: pd.DataFrame,
    *,
    date_col: str,
    tenor_col: str,
    z_col: str,
    window: int,
    label: str,
) -> dict[str, int]:
    """Allow z-score nulls only in each tenor's prior-only warmup rows."""
    work = pd.DataFrame(
        {
            "_date": df[date_col],
            "_tenor": numeric_series(df, tenor_col, "tenor").astype(int),
            "_z": numeric_series(df, z_col, label),
        },
        index=df.index,
    ).sort_values(["_tenor", "_date"], kind="mergesort")
    work["_position"] = work.groupby("_tenor", sort=False).cumcount()

    missing_after_warmup = work["_z"].isna() & work["_position"].ge(window)
    if missing_after_warmup.any():
        examples = work.loc[
            missing_after_warmup, ["_date", "_tenor", "_position"]
        ].head(20)
        raise BuildError(
            f"{label} has missing values after the expected {window}-observation "
            "prior-only warmup. Examples:\n"
            + examples.to_string(index=False)
        )

    warmup_rows = work["_position"].lt(window)
    return {
        "window": int(window),
        "total_nulls": int(work["_z"].isna().sum()),
        "warmup_rows": int(warmup_rows.sum()),
        "nulls_within_warmup": int((work["_z"].isna() & warmup_rows).sum()),
        "non_nulls_within_warmup": int((work["_z"].notna() & warmup_rows).sum()),
        "missing_after_warmup": int(missing_after_warmup.sum()),
    }


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def atomic_write_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def is_legacy_rsi_column(column: str) -> bool:
    """Identify actual RSI fields without matching unrelated words like ``version``.

    Examples matched: RSI, RSI14, rsi14_final, old_rsi14, rsi_formula_version.
    Examples not matched: final_signal_version, model_version.
    """
    normalized = re.sub(r"[^a-z0-9]+", "_", str(column).lower()).strip("_")
    tokens = normalized.split("_") if normalized else []
    return any(token in {"rsi", "rsi14"} or re.fullmatch(r"rsi\d+", token) for token in tokens)


def is_stale_portfolio_column(column: str) -> bool:
    lowered = column.lower()
    return (
        lowered in STALE_PORTFOLIO_EXACT_COLUMNS
        or any(lowered.startswith(prefix) for prefix in STALE_PORTFOLIO_PREFIXES)
    )


def tenor_bucket_from_tenor(tenor: int) -> str:
    if tenor in (9, 12, 15, 18):
        return "Front"
    if tenor in (21, 24):
        return "Middle"
    if tenor in (27, 30, 33):
        return "Back"
    return "Unexpected"


def validate_tenor_grid(
    df: pd.DataFrame,
    date_key_col: str,
    tenor_col: str,
    dataset_label: str,
) -> dict[str, Any]:
    tenor_num = numeric_series(df, tenor_col, f"{dataset_label} tenor")
    non_integer = tenor_num.notna() & (tenor_num % 1 != 0)
    if non_integer.any():
        examples = df.loc[non_integer, tenor_col].head(10).tolist()
        raise BuildError(f"Non-integer tenors in {dataset_label}: {examples}")

    tenor_int = tenor_num.astype(int)
    found = tuple(sorted(tenor_int.unique().tolist()))
    unexpected = sorted(set(found) - set(TARGET_TENORS))
    missing_global = sorted(set(TARGET_TENORS) - set(found))
    if unexpected or missing_global:
        raise BuildError(
            f"Unexpected tenor grid in {dataset_label}. Found={list(found)}, "
            f"missing={missing_global}, unexpected={unexpected}"
        )

    working = pd.DataFrame({"date": df[date_key_col], "tenor": tenor_int})
    sets_by_date = working.groupby("date", sort=True)["tenor"].agg(
        lambda values: tuple(sorted(set(values.tolist())))
    )
    target_tuple = tuple(TARGET_TENORS)
    full_grid_dates = int((sets_by_date == target_tuple).sum())
    incomplete = sets_by_date[sets_by_date != target_tuple]

    return {
        "tenors_found": list(found),
        "date_count": int(sets_by_date.shape[0]),
        "full_grid_date_count": full_grid_dates,
        "incomplete_grid_date_count": int(incomplete.shape[0]),
        "incomplete_grid_examples": {
            date.strftime("%Y-%m-%d"): list(tenors)
            for date, tenors in incomplete.head(10).items()
        },
    }


def apply_rules(
    df: pd.DataFrame,
    tenor_col: str,
    vrp_col: str,
    z3_col: str,
    z1_col: str,
    rsi_col: str,
    rv_col: str,
) -> pd.DataFrame:
    result = df.copy()
    tenor = numeric_series(result, tenor_col, "tenor").astype(int)
    vrp = numeric_series(result, vrp_col, "model VRP log")
    z3 = numeric_series(result, z3_col, "3m z-score")
    z1 = numeric_series(result, z1_col, "1y z-score")
    rsi = numeric_series(result, rsi_col, "repaired Wilder RSI14")
    rv = numeric_series(result, rv_col, "RV21D volatility percent")

    core_pass = pd.Series(False, index=result.index, dtype=bool)
    secondary_pass = pd.Series(False, index=result.index, dtype=bool)

    for rule in RULES:
        mask = (
            tenor.isin(rule.tenors)
            & (vrp > rule.vrp_log_gt)
            & (z3 > rule.z_3m_gt)
            & (z1 > rule.z_1y_gt)
            & (rsi < rule.rsi_lt)
            & (rv > rule.rv21d_gt)
        )
        if rule.layer == "Core":
            core_pass |= mask
        elif rule.layer == "Secondary":
            secondary_pass |= mask
        else:  # Defensive: RULES is internal and fixed.
            raise BuildError(f"Unsupported rule layer: {rule.layer}")

    result["core_pass"] = core_pass
    result["secondary_pass"] = secondary_pass
    return result


def build_pass_counts(
    df: pd.DataFrame,
    tenor_col: str,
    bucket_col: str,
) -> pd.DataFrame:
    tenor = numeric_series(df, tenor_col, "tenor").astype(int)
    rows: list[dict[str, Any]] = []

    rule_lookup: dict[tuple[str, int], Rule] = {}
    for rule in RULES:
        for tenor_value in rule.tenors:
            rule_lookup[(rule.layer, tenor_value)] = rule

    for tenor_value in TARGET_TENORS:
        tenor_mask = tenor.eq(tenor_value)
        row_count = int(tenor_mask.sum())
        source_buckets = sorted(
            df.loc[tenor_mask, bucket_col].dropna().astype(str).unique().tolist()
        )
        source_bucket = "|".join(source_buckets)
        expected_bucket = tenor_bucket_from_tenor(tenor_value)

        for layer, flag_col in (("Core", "core_pass"), ("Secondary", "secondary_pass")):
            rule = rule_lookup.get((layer, tenor_value))
            pass_count = int(df.loc[tenor_mask, flag_col].sum()) if row_count else 0
            rows.append(
                {
                    "tenor": tenor_value,
                    "source_tenor_bucket": source_bucket,
                    "expected_tenor_bucket": expected_bucket,
                    "layer": layer,
                    "active": rule is not None,
                    "row_count": row_count,
                    "pass_count": pass_count,
                    "pass_rate": (pass_count / row_count) if row_count else np.nan,
                    "vrp_log_gt": rule.vrp_log_gt if rule else np.nan,
                    "z_3m_gt": rule.z_3m_gt if rule else np.nan,
                    "z_1y_gt": rule.z_1y_gt if rule else np.nan,
                    "rsi_lt": rule.rsi_lt if rule else np.nan,
                    "rv21d_gt": rule.rv21d_gt if rule else np.nan,
                }
            )

    return pd.DataFrame(rows)


def safe_min_date(series: pd.Series) -> str:
    return pd.Timestamp(series.min()).strftime("%Y-%m-%d")


def safe_max_date(series: pd.Series) -> str:
    return pd.Timestamp(series.max()).strftime("%Y-%m-%d")


def print_header(title: str) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def run(args: argparse.Namespace) -> int:
    started_at = datetime.now()
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")

    project_root = normalize_path(args.project_root)
    signal_input = normalize_path(resolve_path(project_root, args.signal_input, SIGNAL_INPUT_REL))
    rsi_input = normalize_path(resolve_path(project_root, args.rsi_input, RSI_INPUT_REL))
    output_dir = normalize_path(resolve_path(project_root, args.output_dir, OUTPUT_DIR_REL))
    audit_dir = normalize_path(resolve_path(project_root, args.audit_dir, AUDIT_DIR_REL))

    base_output = output_dir / BASE_OUTPUT_NAME
    snapshot_output = output_dir / SNAPSHOT_OUTPUT_NAME
    manifest_output = audit_dir / f"repaired_wilder_rsi_signal_manifest_{timestamp}.json"
    snapshot_audit_output = (
        audit_dir / f"repaired_wilder_rsi_signal_latest_snapshot_{timestamp}.csv"
    )
    pass_counts_output = (
        audit_dir / f"repaired_wilder_rsi_signal_pass_counts_{timestamp}.csv"
    )

    allowed_versions = tuple(args.allowed_rsi_version or [ACCEPTED_RSI_VERSION])

    print_header("Step 1 — Repaired Wilder RSI signal base")
    print(f"Script:             {SCRIPT_NAME} v{SCRIPT_VERSION}")
    print(f"Project root:       {project_root}")
    print(f"Signal input:       {signal_input}")
    print(f"RSI input:          {rsi_input}")
    print(f"Base output:        {base_output}")
    print(f"Snapshot output:    {snapshot_output}")
    print(f"Audit directory:    {audit_dir}")
    print(f"Allowed RSI version(s): {list(allowed_versions)}")

    ensure_file(signal_input, "current final Corsi signal base")
    ensure_file(rsi_input, "repaired Wilder RSI history")

    # Explicitly protect canonical production files and input files.
    protected_paths = {
        signal_input,
        rsi_input,
        normalize_path(project_root / SIGNAL_INPUT_REL),
        normalize_path(
            project_root
            / "data/processed/vrp_final_signal/vrp_final_corsi_latest_snapshot_v1.parquet"
        ),
        normalize_path(
            project_root
            / "data/processed/vrp_final_signal/vrp_final_corsi_selected_trades_v1.parquet"
        ),
    }
    for candidate in (base_output, snapshot_output):
        if candidate in protected_paths:
            raise BuildError(f"Refusing to overwrite protected canonical/input file: {candidate}")
    canonical_production_dir = normalize_path(project_root / "data/processed/vrp_final_signal")
    if output_dir == canonical_production_dir or canonical_production_dir in output_dir.parents:
        raise BuildError(
            f"Output directory must be separate from canonical production: {output_dir}"
        )

    print_header("Loading inputs")
    signal_raw = pd.read_parquet(signal_input)
    rsi_raw = pd.read_parquet(rsi_input)
    print(f"Signal rows/columns: {len(signal_raw):,} / {len(signal_raw.columns):,}")
    print(f"RSI rows/columns:    {len(rsi_raw):,} / {len(rsi_raw.columns):,}")

    if signal_raw.empty:
        raise BuildError("Current final Corsi signal base is empty.")
    if rsi_raw.empty:
        raise BuildError("Repaired Wilder RSI history is empty.")

    signal_cols = resolve_columns(signal_raw, REQUIRED_SIGNAL_COLUMNS, "signal base")
    rsi_cols = resolve_columns(rsi_raw, REQUIRED_RSI_COLUMNS, "RSI history")

    signal = signal_raw.copy()
    rsi = rsi_raw.copy()
    signal["_trade_date_key"] = normalize_trade_date(
        signal[signal_cols["trade_date"]], "signal-base trade_date"
    )
    rsi["_trade_date_key"] = normalize_trade_date(
        rsi[rsi_cols["trade_date"]], "RSI-history trade_date"
    )

    duplicate_signal = signal.duplicated(
        ["_trade_date_key", signal_cols["tenor"]], keep=False
    )
    if duplicate_signal.any():
        examples = signal.loc[
            duplicate_signal, [signal_cols["trade_date"], signal_cols["tenor"]]
        ].head(20)
        raise BuildError(
            "Signal base is not unique by trade_date × tenor. Examples:\n"
            + examples.to_string(index=False)
        )

    duplicate_rsi = rsi.duplicated(["_trade_date_key"], keep=False)
    if duplicate_rsi.any():
        examples = rsi.loc[
            duplicate_rsi, [rsi_cols["trade_date"], rsi_cols["spy_wilder_rsi14"]]
        ].head(20)
        raise BuildError(
            "RSI history is not unique by trade_date. Examples:\n"
            + examples.to_string(index=False)
        )

    source_grid = validate_tenor_grid(
        signal, "_trade_date_key", signal_cols["tenor"], "source signal base"
    )

    # The prior-only z-scores are expected to be null during each tenor's
    # initial 63/252 observation warmup, and must be populated thereafter.
    source_null_counts_before_patch = {
        logical: int(signal[physical].isna().sum())
        for logical, physical in signal_cols.items()
        if logical not in {"trade_date", "tenor", "tenor_bucket"}
    }
    zscore_warmup_checks = {
        "z_3m": validate_prior_only_zscore_warmup(
            signal,
            date_col="_trade_date_key",
            tenor_col=signal_cols["tenor"],
            z_col=signal_cols["z_3m"],
            window=63,
            label="3m prior-only z-score",
        ),
        "z_1y": validate_prior_only_zscore_warmup(
            signal,
            date_col="_trade_date_key",
            tenor_col=signal_cols["tenor"],
            z_col=signal_cols["z_1y"],
            window=252,
            label="1y prior-only z-score",
        ),
    }

    rsi_required_value_columns = [
        rsi_cols["spy_close"],
        rsi_cols["spy_wilder_rsi14"],
        rsi_cols["rsi_formula_version"],
    ]
    rsi_null_counts = require_no_nulls(rsi, rsi_required_value_columns, "RSI history")

    # Preserve every existing signal-base SPY close. Fill only missing values
    # from the accepted repaired-RSI history, which uses the canonical
    # ThetaData SPY EOD close source.
    signal_spy_close = numeric_series(
        signal, signal_cols["spy_close"], "source signal-base SPY close"
    )
    rsi_spy_close = numeric_series(rsi, rsi_cols["spy_close"], "RSI-history SPY close")
    close_lookup = pd.Series(
        rsi_spy_close.to_numpy(), index=rsi["_trade_date_key"]
    )
    missing_spy_close_before = signal_spy_close.isna()
    patched_close_values = signal.loc[missing_spy_close_before, "_trade_date_key"].map(
        close_lookup
    )
    signal.loc[missing_spy_close_before, signal_cols["spy_close"]] = patched_close_values
    missing_spy_close_after = int(signal[signal_cols["spy_close"]].isna().sum())
    if missing_spy_close_after:
        missing_dates = (
            signal.loc[signal[signal_cols["spy_close"]].isna(), "_trade_date_key"]
            .drop_duplicates()
            .sort_values()
            .dt.strftime("%Y-%m-%d")
            .head(20)
            .tolist()
        )
        raise BuildError(
            f"SPY close remains missing for {missing_spy_close_after:,} signal rows "
            f"after the canonical RSI-history patch. First dates: {missing_dates}"
        )
    spy_close_patch_count = int(missing_spy_close_before.sum())
    spy_close_patch_dates = int(
        signal.loc[missing_spy_close_before, "_trade_date_key"].nunique()
    )

    required_signal_value_columns = [
        signal_cols["implied_variance"],
        signal_cols["forecast_variance"],
        signal_cols["model_vrp_log"],
        signal_cols["rv21d_vol_pct"],
        signal_cols["spy_close"],
    ]
    signal_null_counts = require_no_nulls(
        signal, required_signal_value_columns, "source signal base after SPY-close patch"
    )

    rsi_values = numeric_series(rsi, rsi_cols["spy_wilder_rsi14"], "Wilder RSI14")
    out_of_range = ~rsi_values.between(0.0, 100.0, inclusive="both")
    if out_of_range.any():
        examples = rsi.loc[
            out_of_range,
            [rsi_cols["trade_date"], rsi_cols["spy_wilder_rsi14"]],
        ].head(10)
        raise BuildError(
            "Repaired Wilder RSI14 contains values outside [0, 100]. Examples:\n"
            + examples.to_string(index=False)
        )

    version_values = sorted(
        rsi[rsi_cols["rsi_formula_version"]].dropna().astype(str).unique().tolist()
    )
    disallowed_versions = sorted(set(version_values) - set(allowed_versions))
    if disallowed_versions:
        raise BuildError(
            f"RSI history contains disallowed formula versions: {disallowed_versions}. "
            f"Allowed: {list(allowed_versions)}"
        )

    print("Resolved signal columns:")
    for logical, physical in signal_cols.items():
        print(f"  {logical:20s} -> {physical}")
    print("Resolved RSI columns:")
    for logical, physical in rsi_cols.items():
        print(f"  {logical:20s} -> {physical}")
    print("Expected prior-only z-score warmups:")
    print(
        f"  z_3m: {zscore_warmup_checks['z_3m']['total_nulls']:,} nulls; "
        f"missing after warmup={zscore_warmup_checks['z_3m']['missing_after_warmup']:,}"
    )
    print(
        f"  z_1y: {zscore_warmup_checks['z_1y']['total_nulls']:,} nulls; "
        f"missing after warmup={zscore_warmup_checks['z_1y']['missing_after_warmup']:,}"
    )
    print(
        f"SPY closes patched from RSI history: {spy_close_patch_count:,} rows "
        f"across {spy_close_patch_dates:,} dates"
    )

    # Remove every legacy RSI field and stale downstream selection/sizing field.
    legacy_rsi_columns = sorted(
        column for column in signal.columns if is_legacy_rsi_column(column)
    )
    stale_portfolio_columns = sorted(
        column for column in signal.columns if is_stale_portfolio_column(column)
    )
    recomputed_flag_columns = [
        column for column in ("core_pass", "secondary_pass") if column in signal.columns
    ]
    drop_columns = sorted(
        set(legacy_rsi_columns + stale_portfolio_columns + recomputed_flag_columns)
        - {"_trade_date_key"}
    )
    signal_clean = signal.drop(columns=drop_columns, errors="ignore")

    rsi_join = rsi[
        [
            "_trade_date_key",
            rsi_cols["spy_wilder_rsi14"],
            rsi_cols["rsi_formula_version"],
        ]
    ].copy()
    rsi_join = rsi_join.rename(
        columns={
            rsi_cols["spy_wilder_rsi14"]: "spy_wilder_rsi14",
            rsi_cols["rsi_formula_version"]: "rsi_formula_version",
        }
    )

    print_header("Joining repaired Wilder RSI")
    row_count_before = len(signal_clean)
    repaired = signal_clean.merge(
        rsi_join,
        on="_trade_date_key",
        how="left",
        validate="many_to_one",
        indicator="_rsi_merge_status",
    )
    if len(repaired) != row_count_before:
        raise BuildError(
            f"RSI join changed row count: before={row_count_before:,}, after={len(repaired):,}"
        )

    missing_rsi_join = repaired["_rsi_merge_status"].ne("both")
    if missing_rsi_join.any():
        missing_dates = (
            repaired.loc[missing_rsi_join, "_trade_date_key"]
            .drop_duplicates()
            .sort_values()
            .dt.strftime("%Y-%m-%d")
            .head(20)
            .tolist()
        )
        raise BuildError(
            f"Repaired RSI is missing for {int(missing_rsi_join.sum()):,} signal rows. "
            f"First missing dates: {missing_dates}"
        )
    repaired = repaired.drop(columns=["_rsi_merge_status"])

    repaired["rsi14_final"] = numeric_series(
        repaired, "spy_wilder_rsi14", "joined repaired Wilder RSI14"
    )
    require_no_nulls(
        repaired,
        ["spy_wilder_rsi14", "rsi14_final", "rsi_formula_version"],
        "repaired signal base after RSI join",
    )

    joined_versions = sorted(
        repaired["rsi_formula_version"].dropna().astype(str).unique().tolist()
    )
    if sorted(joined_versions) != sorted(version_values):
        raise BuildError(
            f"Joined RSI versions changed unexpectedly. Source={version_values}, "
            f"joined={joined_versions}"
        )

    repaired = apply_rules(
        repaired,
        tenor_col=signal_cols["tenor"],
        vrp_col=signal_cols["model_vrp_log"],
        z3_col=signal_cols["z_3m"],
        z1_col=signal_cols["z_1y"],
        rsi_col="rsi14_final",
        rv_col=signal_cols["rv21d_vol_pct"],
    )

    # Deterministic ordering. Keep original trade_date representation in output.
    repaired["_tenor_sort"] = numeric_series(
        repaired, signal_cols["tenor"], "tenor"
    ).astype(int)
    repaired = repaired.sort_values(
        ["_trade_date_key", "_tenor_sort"], kind="mergesort"
    ).reset_index(drop=True)

    output_grid = validate_tenor_grid(
        repaired, "_trade_date_key", signal_cols["tenor"], "repaired signal base"
    )
    if output_grid != source_grid:
        raise BuildError(
            "Tenor-grid summary changed after RSI replacement. "
            f"Source={source_grid}, output={output_grid}"
        )

    if repaired.duplicated(["_trade_date_key", signal_cols["tenor"]]).any():
        raise BuildError("Repaired signal base is not unique by trade_date × tenor.")

    source_latest = signal["_trade_date_key"].max()
    output_latest = repaired["_trade_date_key"].max()
    if output_latest != source_latest:
        raise BuildError(
            f"Latest date mismatch. Source={source_latest:%Y-%m-%d}, "
            f"output={output_latest:%Y-%m-%d}"
        )

    latest_snapshot = repaired.loc[repaired["_trade_date_key"].eq(output_latest)].copy()
    latest_tenors = tuple(
        sorted(
            numeric_series(latest_snapshot, signal_cols["tenor"], "latest snapshot tenor")
            .astype(int)
            .unique()
            .tolist()
        )
    )
    if latest_tenors != tuple(TARGET_TENORS):
        raise BuildError(
            f"Latest snapshot does not contain expected nine tenors. Found={list(latest_tenors)}"
        )
    if len(latest_snapshot) != len(TARGET_TENORS):
        raise BuildError(
            f"Latest snapshot row count must be {len(TARGET_TENORS)}, "
            f"found {len(latest_snapshot)}"
        )

    pass_counts = build_pass_counts(
        repaired,
        tenor_col=signal_cols["tenor"],
        bucket_col=signal_cols["tenor_bucket"],
    )

    # Internal helper columns never leave the script.
    repaired_output_df = repaired.drop(columns=["_trade_date_key", "_tenor_sort"])
    latest_snapshot_output_df = latest_snapshot.drop(
        columns=["_trade_date_key", "_tenor_sort"]
    )

    print(f"Rows preserved:      {len(repaired_output_df):,}")
    print(f"Date range:          {safe_min_date(repaired['_trade_date_key'])} to "
          f"{safe_max_date(repaired['_trade_date_key'])}")
    print(f"Latest date:         {output_latest:%Y-%m-%d}")
    print(f"Latest snapshot rows:{len(latest_snapshot_output_df):,}")
    print(f"Core passes:         {int(repaired_output_df['core_pass'].sum()):,}")
    print(f"Secondary passes:    {int(repaired_output_df['secondary_pass'].sum()):,}")
    print(f"Dropped legacy RSI fields: {legacy_rsi_columns}")
    print(f"Dropped stale selection/sizing fields: {stale_portfolio_columns}")

    print_header("Writing parallel repaired outputs")
    atomic_write_parquet(repaired_output_df, base_output)
    atomic_write_parquet(latest_snapshot_output_df, snapshot_output)

    # Re-open written Parquet files and repeat essential publication checks.
    base_reloaded = pd.read_parquet(base_output)
    snapshot_reloaded = pd.read_parquet(snapshot_output)
    if len(base_reloaded) != len(repaired_output_df):
        raise BuildError(
            f"Reloaded base row count mismatch: expected={len(repaired_output_df):,}, "
            f"actual={len(base_reloaded):,}"
        )
    if len(snapshot_reloaded) != len(latest_snapshot_output_df):
        raise BuildError(
            f"Reloaded snapshot row count mismatch: expected={len(latest_snapshot_output_df):,}, "
            f"actual={len(snapshot_reloaded):,}"
        )

    base_reloaded_date = normalize_trade_date(
        base_reloaded[signal_cols["trade_date"]], "reloaded base trade_date"
    )
    base_reloaded_tenor = numeric_series(
        base_reloaded, signal_cols["tenor"], "reloaded base tenor"
    ).astype(int)
    if pd.DataFrame(
        {"date": base_reloaded_date, "tenor": base_reloaded_tenor}
    ).duplicated(["date", "tenor"]).any():
        raise BuildError("Reloaded base is not unique by trade_date × tenor.")
    if base_reloaded_date.max() != output_latest:
        raise BuildError("Reloaded base latest date does not match expected latest date.")

    snapshot_reloaded_date = normalize_trade_date(
        snapshot_reloaded[signal_cols["trade_date"]], "reloaded snapshot trade_date"
    )
    snapshot_reloaded_tenor = numeric_series(
        snapshot_reloaded, signal_cols["tenor"], "reloaded snapshot tenor"
    ).astype(int)
    if not snapshot_reloaded_date.eq(output_latest).all():
        raise BuildError("Reloaded latest snapshot contains a non-latest date.")
    if tuple(sorted(snapshot_reloaded_tenor.unique().tolist())) != tuple(TARGET_TENORS):
        raise BuildError("Reloaded latest snapshot does not contain expected tenor rows.")

    completed_at = datetime.now()
    checks = {
        "all_signal_rows_have_repaired_rsi": True,
        "rsi_join_preserved_row_count": True,
        "date_tenor_unique": True,
        "target_tenor_grid_preserved": True,
        "required_non_z_signal_values_non_null": True,
        "prior_only_zscore_warmups_valid": True,
        "missing_spy_close_patched_from_canonical_rsi_history": True,
        "accepted_rsi_formula_version_only": True,
        "latest_date_aligned_to_source_signal": True,
        "latest_snapshot_has_expected_nine_tenors": True,
        "canonical_production_paths_not_overwritten": True,
        "written_parquet_reopen_checks_passed": True,
    }

    manifest: dict[str, Any] = {
        "status": "PASS",
        "script_name": SCRIPT_NAME,
        "script_version": SCRIPT_VERSION,
        "started_at_local": started_at.isoformat(timespec="seconds"),
        "completed_at_local": completed_at.isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "scope": {
            "replace_legacy_rsi_only": True,
            "recompute_core_secondary_flags": True,
            "parameter_optimization": False,
            "sizing": False,
            "one_trade_per_day_selection": False,
            "production_overwrite": False,
        },
        "inputs": {
            "signal_base": str(signal_input),
            "signal_base_sha256": sha256_file(signal_input),
            "rsi_history": str(rsi_input),
            "rsi_history_sha256": sha256_file(rsi_input),
        },
        "outputs": {
            "repaired_signal_base": str(base_output),
            "repaired_signal_base_sha256": sha256_file(base_output),
            "latest_snapshot": str(snapshot_output),
            "latest_snapshot_sha256": sha256_file(snapshot_output),
            "latest_snapshot_audit_csv": str(snapshot_audit_output),
            "pass_counts_audit_csv": str(pass_counts_output),
            "manifest_json": str(manifest_output),
        },
        "resolved_signal_columns": signal_cols,
        "resolved_rsi_columns": rsi_cols,
        "allowed_rsi_versions": list(allowed_versions),
        "observed_rsi_versions": joined_versions,
        "rows": {
            "source_signal": int(len(signal_raw)),
            "source_rsi_history": int(len(rsi_raw)),
            "repaired_signal": int(len(repaired_output_df)),
            "latest_snapshot": int(len(latest_snapshot_output_df)),
        },
        "date_range": {
            "first": safe_min_date(repaired["_trade_date_key"]),
            "latest": safe_max_date(repaired["_trade_date_key"]),
        },
        "tenor_grid": output_grid,
        "pass_totals": {
            "core_pass": int(repaired_output_df["core_pass"].sum()),
            "secondary_pass": int(repaired_output_df["secondary_pass"].sum()),
        },
        "rules": [asdict(rule) for rule in RULES],
        "inactive": {
            "tenor_9_all_layers": True,
            "core_front": True,
        },
        "dropped_columns": {
            "legacy_rsi": legacy_rsi_columns,
            "stale_portfolio_selection_or_sizing": stale_portfolio_columns,
            "recomputed_flags": recomputed_flag_columns,
        },
        "source_null_counts_before_patch": source_null_counts_before_patch,
        "source_null_counts_required_non_z_fields_after_patch": signal_null_counts,
        "zscore_warmup_checks": zscore_warmup_checks,
        "spy_close_patch": {
            "rows_patched": spy_close_patch_count,
            "dates_patched": spy_close_patch_dates,
            "source": str(rsi_input),
            "source_column": rsi_cols["spy_close"],
            "remaining_missing": missing_spy_close_after,
        },
        "rsi_source_null_counts_required_fields": rsi_null_counts,
        "checks": checks,
    }

    atomic_write_csv(latest_snapshot_output_df, snapshot_audit_output)
    atomic_write_csv(pass_counts, pass_counts_output)
    atomic_write_json(manifest, manifest_output)

    print(f"Wrote: {base_output}")
    print(f"Wrote: {snapshot_output}")
    print(f"Wrote: {snapshot_audit_output}")
    print(f"Wrote: {pass_counts_output}")
    print(f"Wrote: {manifest_output}")

    print_header("PASS")
    print("Repaired Wilder RSI signal base built successfully.")
    print("Canonical production final-signal files were not modified.")
    return 0


def main() -> int:
    try:
        return run(parse_args())
    except BuildError as exc:
        print_header("FAIL")
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # Preserve traceback for unexpected runtime failures.
        print_header("UNEXPECTED FAILURE")
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
