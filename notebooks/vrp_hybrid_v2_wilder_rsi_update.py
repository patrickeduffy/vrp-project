from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from vrp_hybrid_v2_common import (
    DEFAULT_PROJECT_ROOT,
    first_existing_column,
    normalize_dates,
    read_table,
    utc_now_iso,
    write_csv_atomic,
    write_json,
    write_parquet_atomic,
)

FORMULA_VERSION = "wilder_rsi14_spy_close_v3_clean_session_rebuild"
LEGACY_SEED_VERSIONS = {
    "wilder_rsi14_spy_close_v2_long_warmup",
    FORMULA_VERSION,
}
DEFAULT_OUTPUT_START = "20180102"
DEFAULT_CANONICAL_PRICE_REL = Path("data/processed/market_data/spy_eod_prices_v1.parquet")
DEFAULT_OUTPUT_REL = Path("data/processed/market_data/spy_wilder_rsi14_history_v1.parquet")
DEFAULT_AUDIT_REL = Path("data/audit/vrp_hybrid_v2_eod/rsi_updates")
PERIOD = 14
CLOSE_TOLERANCE = 1e-10


@dataclass(frozen=True)
class Config:
    project_root: Path
    end_date: pd.Timestamp
    output_start: pd.Timestamp
    canonical_price_path: Path
    output_path: Path
    audit_dir: Path
    force_full_refresh: bool


def parse_date(value: str) -> pd.Timestamp:
    parsed = pd.to_datetime(str(value), errors="raise")
    return pd.Timestamp(parsed).normalize()


def parse_args(argv: Sequence[str] | None = None) -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Extend the accepted long-warmup SPY Wilder RSI14 history from its stored recursive state. "
            "No historical ThetaData stock subscription is required."
        )
    )
    parser.add_argument("--project-root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument("--end-date", required=True, help="Target completed EOD date, YYYYMMDD or YYYY-MM-DD.")
    parser.add_argument("--output-start", default=DEFAULT_OUTPUT_START)
    parser.add_argument("--canonical-price-path", type=Path, default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--audit-dir", type=Path, default=None)
    parser.add_argument(
        "--force-full-refresh",
        action="store_true",
        help=(
            "Recompute every canonical row after the earliest valid accepted RSI seed row. "
            "The accepted seed itself is preserved because it embodies the locked long-warmup history."
        ),
    )

    # Deprecated compatibility arguments from the initial dashboard package. They are accepted and ignored so
    # existing launchers or ad-hoc commands do not break, but this updater never calls the blocked stock-history API.
    parser.add_argument("--warmup-start", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--thetadata-url", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--chunk-days", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--sleep-seconds", type=float, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--cache-path", type=Path, default=None, help=argparse.SUPPRESS)

    args = parser.parse_args(argv)
    root = args.project_root.resolve()

    def resolve(value: Path | None, default: Path) -> Path:
        path = value if value is not None else default
        return path if path.is_absolute() else root / path

    return Config(
        project_root=root,
        end_date=parse_date(args.end_date),
        output_start=parse_date(args.output_start),
        canonical_price_path=resolve(args.canonical_price_path, DEFAULT_CANONICAL_PRICE_REL),
        output_path=resolve(args.output_path, DEFAULT_OUTPUT_REL),
        audit_dir=resolve(args.audit_dir, DEFAULT_AUDIT_REL),
        force_full_refresh=bool(args.force_full_refresh),
    )


def load_canonical_prices(path: Path, end_date: pd.Timestamp) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Canonical SPY EOD file missing: {path}")
    frame = read_table(path)
    date_col = first_existing_column(frame, ["trade_date", "date"], label="SPY EOD date")
    close_col = first_existing_column(frame, ["spy_close", "close"], label="SPY EOD close")
    out = pd.DataFrame({
        "trade_date": normalize_dates(frame[date_col]),
        "spy_close": pd.to_numeric(frame[close_col], errors="coerce"),
    })
    out = out.loc[out["trade_date"].notna() & out["trade_date"].le(end_date)].copy()
    if out["spy_close"].isna().any() or not out["spy_close"].gt(0).all():
        raise RuntimeError("Canonical SPY EOD contains non-positive or missing closes.")
    if out.duplicated("trade_date").any():
        raise RuntimeError("Canonical SPY EOD is not unique by trade_date.")
    out = out.sort_values("trade_date").reset_index(drop=True)
    if out.empty or out["trade_date"].max() != end_date:
        raise RuntimeError(
            f"Canonical SPY EOD does not reach target date: max={out['trade_date'].max() if len(out) else None}, "
            f"target={end_date.date()}"
        )
    return out


DUPLICATE_STATE_COLUMNS = [
    "spy_close",
    "spy_change",
    "wilder_avg_gain_14",
    "wilder_avg_loss_14",
    "spy_wilder_rsi14",
]
DUPLICATE_STATE_RTOL = 1e-10
DUPLICATE_STATE_ATOL = 1e-12


def _duplicate_row_is_valid(row: pd.Series) -> bool:
    numeric = pd.to_numeric(
        row[["spy_close", "wilder_avg_gain_14", "wilder_avg_loss_14", "spy_wilder_rsi14"]],
        errors="coerce",
    )
    return bool(
        np.isfinite(numeric.to_numpy(dtype=float)).all()
        and float(numeric["spy_close"]) > 0
        and float(numeric["wilder_avg_gain_14"]) >= 0
        and float(numeric["wilder_avg_loss_14"]) >= 0
        and 0 <= float(numeric["spy_wilder_rsi14"]) <= 100
        and str(row["rsi_formula_version"]) in LEGACY_SEED_VERSIONS
    )


def collapse_semantic_duplicate_dates(out: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse duplicate dates only when their accepted recursive states do not conflict.

    Duplicate source labels are harmless. A duplicate date with conflicting close, Wilder state, RSI,
    or formula version is a hard failure because silently choosing one row would mutate the locked signal.
    When a duplicate group contains one valid accepted row plus incomplete/invalid copies, the valid row is
    retained and the discarded rows are recorded in the audit output.
    """
    duplicate_dates = out.loc[out.duplicated("trade_date", keep=False), "trade_date"].drop_duplicates()
    if duplicate_dates.empty:
        return out.sort_values("trade_date").reset_index(drop=True), pd.DataFrame(columns=[
            "trade_date", "input_rows", "valid_rows", "discarded_rows", "action", "selected_source_name"
        ])

    retained: list[pd.Series] = []
    audits: list[dict[str, Any]] = []
    for trade_date, group in out.groupby("trade_date", sort=True, dropna=False):
        group = group.copy()
        if len(group) == 1:
            retained.append(group.iloc[0])
            continue

        valid_mask = group.apply(_duplicate_row_is_valid, axis=1)
        valid = group.loc[valid_mask].copy()
        if valid.empty:
            sample = group[["trade_date", *DUPLICATE_STATE_COLUMNS, "rsi_formula_version", "source_name"]]
            raise RuntimeError(
                "Accepted Wilder RSI duplicate date has no valid locked recursive state: "
                f"date={pd.Timestamp(trade_date).date()}\n{sample.to_string(index=False)}"
            )

        reference = valid.iloc[0]
        conflicts: list[str] = []
        for column in DUPLICATE_STATE_COLUMNS:
            values = pd.to_numeric(valid[column], errors="coerce").to_numpy(dtype=float)
            reference_value = float(pd.to_numeric(pd.Series([reference[column]]), errors="coerce").iloc[0])
            if not np.isclose(
                values,
                reference_value,
                rtol=DUPLICATE_STATE_RTOL,
                atol=DUPLICATE_STATE_ATOL,
                equal_nan=True,
            ).all():
                conflicts.append(column)
        versions = sorted(valid["rsi_formula_version"].dropna().astype(str).unique())
        if len(versions) != 1 or not set(versions).issubset(LEGACY_SEED_VERSIONS):
            conflicts.append("rsi_formula_version")

        if conflicts:
            sample = valid[["trade_date", *DUPLICATE_STATE_COLUMNS, "rsi_formula_version", "source_name"]]
            raise RuntimeError(
                "Accepted Wilder RSI history contains conflicting duplicate states. "
                f"date={pd.Timestamp(trade_date).date()}, conflicting_columns={conflicts}\n"
                f"{sample.to_string(index=False)}"
            )

        # Prefer the last valid row so an intentional later append wins only when its state is identical.
        selected = valid.iloc[-1]
        retained.append(selected)
        audits.append({
            "trade_date": pd.Timestamp(trade_date),
            "input_rows": int(len(group)),
            "valid_rows": int(len(valid)),
            "discarded_rows": int(len(group) - 1),
            "action": (
                "collapsed_identical_valid_rows"
                if len(valid) == len(group)
                else "retained_valid_state_discarded_invalid_duplicate_rows"
            ),
            "selected_source_name": str(selected.get("source_name", "")),
        })

    deduped = pd.DataFrame(retained).sort_values("trade_date").reset_index(drop=True)
    if deduped.duplicated("trade_date").any():
        raise RuntimeError("Internal error: accepted Wilder RSI dates remain duplicated after semantic collapse.")
    return deduped, pd.DataFrame(audits)


def load_accepted_state(path: Path, end_date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    if not path.exists():
        raise FileNotFoundError(
            "Accepted Wilder RSI history is missing. Restore the locked file before running the EOD pipeline: "
            f"{path}"
        )
    frame = read_table(path)
    date_col = first_existing_column(frame, ["trade_date", "date"], label="RSI date")
    close_col = first_existing_column(frame, ["spy_close", "close"], label="RSI close")
    change_col = first_existing_column(frame, ["spy_change", "change"], label="RSI close change")
    avg_gain_col = first_existing_column(frame, ["wilder_avg_gain_14", "avg_gain_14"], label="RSI average gain")
    avg_loss_col = first_existing_column(frame, ["wilder_avg_loss_14", "avg_loss_14"], label="RSI average loss")
    rsi_col = first_existing_column(frame, ["spy_wilder_rsi14", "rsi14"], label="RSI value")
    version_col = first_existing_column(frame, ["rsi_formula_version", "rsi_version"], label="RSI formula version")
    source_col = first_existing_column(frame, ["source_name", "source"], required=False)

    out = pd.DataFrame({
        "trade_date": normalize_dates(frame[date_col]),
        "spy_close": pd.to_numeric(frame[close_col], errors="coerce"),
        "spy_change": pd.to_numeric(frame[change_col], errors="coerce"),
        "wilder_avg_gain_14": pd.to_numeric(frame[avg_gain_col], errors="coerce"),
        "wilder_avg_loss_14": pd.to_numeric(frame[avg_loss_col], errors="coerce"),
        "spy_wilder_rsi14": pd.to_numeric(frame[rsi_col], errors="coerce"),
        "rsi_formula_version": frame[version_col].astype("string"),
        "source_name": frame[source_col].astype("string") if source_col else "accepted_long_warmup_wilder_state",
    })
    out = out.loc[out["trade_date"].notna() & out["trade_date"].le(end_date)].copy()
    input_rows = int(len(out))
    out, duplicate_audit = collapse_semantic_duplicate_dates(out)
    if out.empty:
        raise RuntimeError("Accepted Wilder RSI history is empty.")
    observed = sorted(out["rsi_formula_version"].dropna().astype(str).unique())
    if not observed or not set(observed).issubset(LEGACY_SEED_VERSIONS):
        raise RuntimeError(
            "Accepted Wilder RSI formula version cannot seed the clean-session rebuild: "
            f"observed={observed}, allowed={sorted(LEGACY_SEED_VERSIONS)}"
        )
    return out, duplicate_audit, input_rows


def valid_state_mask(frame: pd.DataFrame) -> pd.Series:
    numeric = frame[[
        "spy_close", "wilder_avg_gain_14", "wilder_avg_loss_14", "spy_wilder_rsi14"
    ]].apply(pd.to_numeric, errors="coerce")
    return (
        numeric["spy_close"].gt(0)
        & numeric["wilder_avg_gain_14"].ge(0)
        & numeric["wilder_avg_loss_14"].ge(0)
        & numeric["spy_wilder_rsi14"].between(0, 100)
        & np.isfinite(numeric).all(axis=1)
    )


def compute_rsi(avg_gain: float, avg_loss: float) -> float:
    if math.isclose(avg_loss, 0.0, abs_tol=1e-15):
        return 50.0 if math.isclose(avg_gain, 0.0, abs_tol=1e-15) else 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def find_recalc_start(
    canonical_window: pd.DataFrame,
    existing: pd.DataFrame,
    *,
    force_full_refresh: bool,
) -> tuple[pd.Timestamp | None, list[str]]:
    existing_idx = existing.set_index("trade_date")
    reasons: list[tuple[pd.Timestamp, str]] = []

    observed_versions = set(existing["rsi_formula_version"].dropna().astype(str).unique())
    migrate_formula = observed_versions != {FORMULA_VERSION}
    if force_full_refresh or migrate_formula:
        valid = existing.loc[valid_state_mask(existing)].copy()
        valid = valid.loc[valid["trade_date"].isin(canonical_window["trade_date"])]
        if valid.empty:
            raise RuntimeError("Full RSI rebuild requested, but no valid accepted seed row exists.")
        seed_date = valid["trade_date"].min()
        later = canonical_window.loc[canonical_window["trade_date"].gt(seed_date), "trade_date"]
        reasons = []
        if force_full_refresh:
            reasons.append(f"force_full_refresh_after_seed={seed_date.date()}")
        if migrate_formula:
            reasons.append(
                "formula_migration="
                + ",".join(sorted(observed_versions))
                + f"->{FORMULA_VERSION}"
            )
        return (later.min() if len(later) else None), reasons

    for row in canonical_window.itertuples(index=False):
        date = pd.Timestamp(row.trade_date)
        close = float(row.spy_close)
        if date not in existing_idx.index:
            reasons.append((date, "missing_rsi_row"))
            continue
        old = existing_idx.loc[date]
        if isinstance(old, pd.DataFrame):
            raise RuntimeError(f"Accepted RSI history has duplicate date after indexing: {date.date()}")
        if not np.isfinite(pd.to_numeric(pd.Series([old["spy_close"]]), errors="coerce").iloc[0]):
            reasons.append((date, "invalid_stored_close"))
            continue
        if abs(float(old["spy_close"]) - close) > CLOSE_TOLERANCE:
            reasons.append((date, "canonical_close_changed"))
            continue
        one = pd.DataFrame([old])
        if not bool(valid_state_mask(one).iloc[0]):
            reasons.append((date, "invalid_recursive_state"))

    if not reasons:
        return None, []
    earliest = min(date for date, _ in reasons)
    earliest_reasons = sorted({reason for date, reason in reasons if date == earliest})
    return earliest, earliest_reasons


def rebuild_from_seed(
    canonical_window: pd.DataFrame,
    existing: pd.DataFrame,
    recalc_start: pd.Timestamp | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    canonical_window = canonical_window.sort_values("trade_date").reset_index(drop=True)
    existing = existing.sort_values("trade_date").reset_index(drop=True)

    if recalc_start is None:
        output = canonical_window[["trade_date"]].merge(
            existing,
            on="trade_date",
            how="left",
            validate="one_to_one",
        )
        return output, {
            "mode": "no_change",
            "seed_date": str(output["trade_date"].max().date()),
            "recalc_start": None,
            "recalculated_rows": 0,
            "preserved_rows": int(len(output)),
        }

    valid = existing.loc[valid_state_mask(existing) & existing["trade_date"].lt(recalc_start)].copy()
    valid = valid.loc[valid["trade_date"].isin(canonical_window["trade_date"])]
    if valid.empty:
        raise RuntimeError(
            "Cannot extend the locked Wilder RSI history because there is no valid accepted recursive state "
            f"before recalc_start={recalc_start.date()}. Restore the accepted RSI parquet or its backup."
        )
    seed = valid.sort_values("trade_date").iloc[-1]
    seed_date = pd.Timestamp(seed["trade_date"])

    # Preserve only the earliest valid long-warmup seed. Every later observation is
    # rebuilt from the clean canonical XNYS-session SPY close sequence.
    preserved = pd.DataFrame([seed]).copy()
    preserved["rsi_formula_version"] = FORMULA_VERSION
    preserved["source_name"] = (
        "canonical_spy_eod_prices_v1; clean-session recursive rebuild from accepted initial seed"
    )
    canonical_seed_close = float(
        canonical_window.loc[canonical_window["trade_date"].eq(seed_date), "spy_close"].iloc[0]
    )
    if abs(float(seed["spy_close"]) - canonical_seed_close) > CLOSE_TOLERANCE:
        raise RuntimeError("The accepted initial RSI seed close does not match canonical SPY.")
    preserved["spy_close"] = canonical_seed_close

    prev_close = float(preserved.iloc[0]["spy_close"])
    prev_avg_gain = float(seed["wilder_avg_gain_14"])
    prev_avg_loss = float(seed["wilder_avg_loss_14"])
    new_rows: list[dict[str, Any]] = []

    recalc_prices = canonical_window.loc[canonical_window["trade_date"].gt(seed_date)].copy()
    for row in recalc_prices.itertuples(index=False):
        trade_date = pd.Timestamp(row.trade_date)
        close = float(row.spy_close)
        change = close - prev_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = (prev_avg_gain * (PERIOD - 1) + gain) / PERIOD
        avg_loss = (prev_avg_loss * (PERIOD - 1) + loss) / PERIOD
        rsi = compute_rsi(avg_gain, avg_loss)
        new_rows.append({
            "trade_date": trade_date,
            "spy_close": close,
            "spy_change": change,
            "wilder_avg_gain_14": avg_gain,
            "wilder_avg_loss_14": avg_loss,
            "spy_wilder_rsi14": rsi,
            "rsi_formula_version": FORMULA_VERSION,
            "source_name": "canonical_spy_eod_prices_v1; clean-session recursive rebuild from accepted initial seed",
        })
        prev_close = close
        prev_avg_gain = avg_gain
        prev_avg_loss = avg_loss

    recalculated = pd.DataFrame(new_rows)
    output = pd.concat([preserved, recalculated], ignore_index=True)
    output = canonical_window[["trade_date"]].merge(output, on="trade_date", how="left", validate="one_to_one")
    return output, {
        "mode": "clean_session_recursive_rebuild_from_initial_seed",
        "seed_date": str(seed_date.date()),
        "recalc_start": str(recalc_start.date()),
        "recalculated_rows": int(len(recalculated)),
        "preserved_rows": int(len(preserved)),
    }


def recurrence_diagnostics(output: pd.DataFrame) -> dict[str, Any]:
    """Independently verify close changes, Wilder state recursion, and RSI values."""
    work = output.sort_values("trade_date").reset_index(drop=True).copy()
    if len(work) < 2:
        return {
            "checked_rows": 0,
            "max_change_abs_diff": 0.0,
            "max_avg_gain_abs_diff": 0.0,
            "max_avg_loss_abs_diff": 0.0,
            "max_rsi_abs_diff": 0.0,
        }

    change_expected = work["spy_close"].diff()
    gain = change_expected.clip(lower=0.0)
    loss = (-change_expected).clip(lower=0.0)
    gain_expected = pd.Series(np.nan, index=work.index, dtype=float)
    loss_expected = pd.Series(np.nan, index=work.index, dtype=float)
    rsi_expected = pd.Series(np.nan, index=work.index, dtype=float)
    gain_expected.iloc[0] = float(work.loc[0, "wilder_avg_gain_14"])
    loss_expected.iloc[0] = float(work.loc[0, "wilder_avg_loss_14"])
    rsi_expected.iloc[0] = float(work.loc[0, "spy_wilder_rsi14"])
    for idx in range(1, len(work)):
        gain_expected.iloc[idx] = (
            gain_expected.iloc[idx - 1] * (PERIOD - 1) + float(gain.iloc[idx])
        ) / PERIOD
        loss_expected.iloc[idx] = (
            loss_expected.iloc[idx - 1] * (PERIOD - 1) + float(loss.iloc[idx])
        ) / PERIOD
        rsi_expected.iloc[idx] = compute_rsi(
            float(gain_expected.iloc[idx]), float(loss_expected.iloc[idx])
        )

    def max_diff(a: pd.Series, b: pd.Series) -> float:
        values = (pd.to_numeric(a, errors="coerce") - pd.to_numeric(b, errors="coerce")).abs()
        values = values.iloc[1:].dropna()
        return float(values.max()) if len(values) else 0.0

    return {
        "checked_rows": int(len(work) - 1),
        "max_change_abs_diff": max_diff(work["spy_change"], change_expected),
        "max_avg_gain_abs_diff": max_diff(work["wilder_avg_gain_14"], gain_expected),
        "max_avg_loss_abs_diff": max_diff(work["wilder_avg_loss_14"], loss_expected),
        "max_rsi_abs_diff": max_diff(work["spy_wilder_rsi14"], rsi_expected),
    }


def validate_output(
    output: pd.DataFrame,
    canonical_window: pd.DataFrame,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    numeric_cols = [
        "spy_close", "spy_change", "wilder_avg_gain_14", "wilder_avg_loss_14", "spy_wilder_rsi14"
    ]
    recurrence = recurrence_diagnostics(output)
    recurrence_tolerance = 1e-10
    checks = [
        {
            "check": "output_dates_match_canonical",
            "status": "PASS" if output["trade_date"].tolist() == canonical_window["trade_date"].tolist() else "FAIL",
            "detail": f"output_rows={len(output)} canonical_rows={len(canonical_window)}",
        },
        {
            "check": "latest_date_exact",
            "status": "PASS" if len(output) and output["trade_date"].max() == end_date else "FAIL",
            "detail": f"max={output['trade_date'].max() if len(output) else None} target={end_date.date()}",
        },
        {
            "check": "trade_dates_unique",
            "status": "PASS" if not output.duplicated("trade_date").any() else "FAIL",
            "detail": "one row per canonical date",
        },
        {
            "check": "all_recursive_fields_finite",
            "status": "PASS" if np.isfinite(output[numeric_cols].apply(pd.to_numeric, errors="coerce")).all().all() else "FAIL",
            "detail": ",".join(numeric_cols),
        },
        {
            "check": "positive_close_nonnegative_states",
            "status": "PASS" if (
                output["spy_close"].gt(0).all()
                and output["wilder_avg_gain_14"].ge(0).all()
                and output["wilder_avg_loss_14"].ge(0).all()
            ) else "FAIL",
            "detail": "close>0; average gains/losses>=0",
        },
        {
            "check": "rsi_bounds",
            "status": "PASS" if output["spy_wilder_rsi14"].between(0, 100).all() else "FAIL",
            "detail": "RSI must be in [0,100]",
        },
        {
            "check": "formula_version_exact",
            "status": "PASS" if output["rsi_formula_version"].astype(str).eq(FORMULA_VERSION).all() else "FAIL",
            "detail": FORMULA_VERSION,
        },
        {
            "check": "canonical_close_exact",
            "status": "PASS" if np.allclose(
                output["spy_close"].to_numpy(float), canonical_window["spy_close"].to_numpy(float), rtol=0, atol=CLOSE_TOLERANCE
            ) else "FAIL",
            "detail": f"absolute_tolerance={CLOSE_TOLERANCE}",
        },
        {
            "check": "spy_change_recurrence_exact",
            "status": "PASS" if recurrence["max_change_abs_diff"] <= recurrence_tolerance else "FAIL",
            "detail": f"max_abs_diff={recurrence['max_change_abs_diff']}; tolerance={recurrence_tolerance}",
        },
        {
            "check": "wilder_avg_gain_recurrence_exact",
            "status": "PASS" if recurrence["max_avg_gain_abs_diff"] <= recurrence_tolerance else "FAIL",
            "detail": f"max_abs_diff={recurrence['max_avg_gain_abs_diff']}; tolerance={recurrence_tolerance}",
        },
        {
            "check": "wilder_avg_loss_recurrence_exact",
            "status": "PASS" if recurrence["max_avg_loss_abs_diff"] <= recurrence_tolerance else "FAIL",
            "detail": f"max_abs_diff={recurrence['max_avg_loss_abs_diff']}; tolerance={recurrence_tolerance}",
        },
        {
            "check": "wilder_rsi_recurrence_exact",
            "status": "PASS" if recurrence["max_rsi_abs_diff"] <= recurrence_tolerance else "FAIL",
            "detail": f"max_abs_diff={recurrence['max_rsi_abs_diff']}; tolerance={recurrence_tolerance}",
        },
        {
            "check": "latest_rsi_finite",
            "status": "PASS" if np.isfinite(output.loc[output["trade_date"].eq(end_date), "spy_wilder_rsi14"]).all() else "FAIL",
            "detail": str(output.loc[output["trade_date"].eq(end_date), "spy_wilder_rsi14"].tolist()),
        },
    ]
    return pd.DataFrame(checks)


def main(argv: Sequence[str] | None = None) -> int:
    cfg = parse_args(argv)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cfg.audit_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 100)
    print("VRP Hybrid v2 Wilder RSI14 stateful EOD update")
    print("=" * 100)
    print(f"Project root:              {cfg.project_root}")
    print(f"Target date:              {cfg.end_date.date()}")
    print(f"Canonical price path:     {cfg.canonical_price_path}")
    print(f"Accepted RSI state path:  {cfg.output_path}")
    print(f"Formula version:          {FORMULA_VERSION}")
    print(f"Force refresh:            {cfg.force_full_refresh}")
    print("Historical stock API:     DISABLED - clean canonical-session rebuild uses accepted initial seed")

    canonical = load_canonical_prices(cfg.canonical_price_path, cfg.end_date)
    canonical_window = canonical.loc[canonical["trade_date"].ge(cfg.output_start)].copy().reset_index(drop=True)
    existing, duplicate_audit, accepted_input_rows = load_accepted_state(cfg.output_path, cfg.end_date)
    existing = existing.loc[existing["trade_date"].ge(cfg.output_start)].copy().reset_index(drop=True)

    recalc_start, recalc_reasons = find_recalc_start(
        canonical_window,
        existing,
        force_full_refresh=cfg.force_full_refresh,
    )
    output, update_meta = rebuild_from_seed(canonical_window, existing, recalc_start)
    output = output[[
        "trade_date", "spy_close", "spy_change", "wilder_avg_gain_14", "wilder_avg_loss_14",
        "spy_wilder_rsi14", "rsi_formula_version", "source_name",
    ]]

    validation = validate_output(output, canonical_window, cfg.end_date)
    failures = validation.loc[validation["status"].ne("PASS")]

    update_meta.update({
        "recalc_reasons": "|".join(recalc_reasons) if recalc_reasons else "none",
        "accepted_input_rows_before_duplicate_collapse": accepted_input_rows,
        "duplicate_dates_collapsed": int(len(duplicate_audit)),
        "duplicate_rows_discarded": int(duplicate_audit["discarded_rows"].sum()) if len(duplicate_audit) else 0,
        "existing_rows": int(len(existing)),
        "output_rows": int(len(output)),
        "target_date": str(cfg.end_date.date()),
        "historical_stock_api_called": False,
    })
    update_meta_df = pd.DataFrame([update_meta])

    validation_path = cfg.audit_dir / f"wilder_rsi_validation_{stamp}.csv"
    update_meta_path = cfg.audit_dir / f"wilder_rsi_update_meta_{stamp}.csv"
    duplicate_audit_path = cfg.audit_dir / f"wilder_rsi_duplicate_date_audit_{stamp}.csv"
    manifest_path = cfg.audit_dir / f"wilder_rsi_manifest_{stamp}.json"
    write_csv_atomic(validation, validation_path)
    write_csv_atomic(update_meta_df, update_meta_path)
    write_csv_atomic(duplicate_audit, duplicate_audit_path)

    print("\nAccepted-state duplicate audit")
    if duplicate_audit.empty:
        print("No duplicate trade dates detected.")
    else:
        print(duplicate_audit.to_string(index=False))
    print("\nUpdate plan")
    print(update_meta_df.to_string(index=False))
    print("\nValidation")
    print(validation.to_string(index=False))

    if len(failures):
        raise RuntimeError(f"Wilder RSI update failed validation:\n{failures.to_string(index=False)}")

    backup = None
    if cfg.output_path.exists():
        backup = cfg.output_path.with_name(f"{cfg.output_path.stem}_backup_{stamp}{cfg.output_path.suffix}")
        backup.write_bytes(cfg.output_path.read_bytes())
    write_parquet_atomic(output, cfg.output_path)

    latest = output.loc[output["trade_date"].eq(cfg.end_date)].iloc[0]
    manifest = {
        "process": "vrp_hybrid_v2_wilder_rsi_stateful_update",
        "generated_at": utc_now_iso(),
        "target_date": str(cfg.end_date.date()),
        "formula_version": FORMULA_VERSION,
        "rows": int(len(output)),
        "date_start": str(output["trade_date"].min().date()),
        "date_end": str(output["trade_date"].max().date()),
        "latest_rsi": float(latest["spy_wilder_rsi14"]),
        "seed_date": update_meta["seed_date"],
        "recalc_start": update_meta["recalc_start"],
        "recalculated_rows": int(update_meta["recalculated_rows"]),
        "historical_stock_api_called": False,
        "subscription_requirement": "No historical ThetaData stock subscription required; clean canonical-session rebuild uses the accepted initial seed.",
        "output": str(cfg.output_path),
        "backup": str(backup) if backup else None,
        "validation": str(validation_path),
        "update_meta": str(update_meta_path),
        "duplicate_date_audit": str(duplicate_audit_path),
        "duplicate_dates_collapsed": int(len(duplicate_audit)),
        "duplicate_rows_discarded": int(duplicate_audit["discarded_rows"].sum()) if len(duplicate_audit) else 0,
    }
    write_json(manifest_path, manifest)

    print(f"\nRSI output:   {cfg.output_path}")
    print(f"RSI backup:   {backup}")
    print(f"RSI manifest: {manifest_path}")
    print("WILDER_RSI14_HYBRID_V2_UPDATE_PASS: True")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
