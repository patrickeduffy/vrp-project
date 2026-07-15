#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vrp_sofr_eod_update_v1.py

Validated production updater for the canonical FRED SOFR cache.

Canonical file
--------------
data/external/fred_sofr_history.csv

Canonical schema
----------------
observation_date, SOFR

Behavior
--------
- Default mode is check-only.
- Downloads the official FRED SOFR CSV.
- Detects new and revised observations.
- Runs hard validation before any production write.
- In write mode, creates a timestamped backup, validates a temporary
  candidate, and atomically replaces the canonical cache.
- Writes a per-run audit directory and JSON manifest.
- Does not rewrite the canonical cache when there are no changes.

Examples
--------
Check only:
    python notebooks/vrp_sofr_eod_update_v1.py ^
        --project-root C:\\Users\\patri\\vrp_project ^
        --check-only

Publish a validated refresh:
    python notebooks/vrp_sofr_eod_update_v1.py ^
        --project-root C:\\Users\\patri\\vrp_project ^
        --write-canonical
"""

from __future__ import annotations

import argparse
import io
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
import requests


DEFAULT_PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
FRED_SERIES_ID = "SOFR"
FRED_CSV_URL = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={FRED_SERIES_ID}"
CANONICAL_REL = Path("data/external/fred_sofr_history.csv")
AUDIT_ROOT_REL = Path("data/audit/sofr_eod_update_v1")
UPDATER_VERSION = "vrp_sofr_eod_update_v1"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_safe(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def normalize_sofr_frame(raw: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Normalize a cache or FRED download to the canonical two-column schema."""
    if raw.empty:
        raise ValueError(f"{source_name} returned an empty table.")

    columns_by_lower = {str(col).strip().lower(): col for col in raw.columns}

    date_col = next(
        (
            columns_by_lower[name]
            for name in ("observation_date", "date")
            if name in columns_by_lower
        ),
        None,
    )
    value_col = next(
        (
            columns_by_lower[name]
            for name in ("sofr", "value")
            if name in columns_by_lower
        ),
        None,
    )

    if date_col is None or value_col is None:
        raise ValueError(
            f"{source_name} does not contain recognizable SOFR columns. "
            f"Columns received: {list(raw.columns)}"
        )

    out = raw[[date_col, value_col]].copy()
    out.columns = ["observation_date", "SOFR"]
    out["observation_date"] = pd.to_datetime(
        out["observation_date"],
        errors="coerce",
    ).dt.normalize()
    out["SOFR"] = pd.to_numeric(out["SOFR"], errors="coerce")

    return (
        out.dropna(subset=["observation_date", "SOFR"])
        .sort_values("observation_date")
        .drop_duplicates(subset=["observation_date"], keep="last")
        .reset_index(drop=True)
    )


def download_fred_sofr(timeout_seconds: int) -> pd.DataFrame:
    response = requests.get(
        FRED_CSV_URL,
        headers={
            "User-Agent": f"{UPDATER_VERSION}/1.0",
            "Accept": "text/csv,application/csv,*/*",
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    if not response.content:
        raise RuntimeError("FRED returned an empty response body.")

    raw = pd.read_csv(io.StringIO(response.text))
    return normalize_sofr_frame(raw, "FRED download")


def build_comparison(
    existing: pd.DataFrame,
    refreshed: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    comparison = existing.merge(
        refreshed,
        on="observation_date",
        how="outer",
        suffixes=("_old", "_fresh"),
        indicator=True,
    )

    new_rows = (
        comparison.loc[
            comparison["_merge"].eq("right_only"),
            ["observation_date", "SOFR_fresh"],
        ]
        .rename(columns={"SOFR_fresh": "SOFR"})
        .sort_values("observation_date")
        .reset_index(drop=True)
    )

    missing_from_fred = (
        comparison.loc[
            comparison["_merge"].eq("left_only"),
            ["observation_date", "SOFR_old"],
        ]
        .rename(columns={"SOFR_old": "SOFR"})
        .sort_values("observation_date")
        .reset_index(drop=True)
    )

    both_mask = comparison["_merge"].eq("both")
    revised_mask = both_mask & ~np.isclose(
        comparison["SOFR_old"],
        comparison["SOFR_fresh"],
        rtol=0.0,
        atol=1e-12,
        equal_nan=True,
    )

    revised_rows = (
        comparison.loc[
            revised_mask,
            ["observation_date", "SOFR_old", "SOFR_fresh"],
        ]
        .sort_values("observation_date")
        .reset_index(drop=True)
    )

    changed_dates = pd.concat(
        [
            new_rows[["observation_date"]],
            revised_rows[["observation_date"]],
        ],
        ignore_index=True,
    ).drop_duplicates()

    first_changed = (
        changed_dates["observation_date"].min()
        if not changed_dates.empty
        else pd.NaT
    )

    return comparison, new_rows, revised_rows, missing_from_fred, first_changed


def validate_sofr(
    existing: pd.DataFrame,
    refreshed: pd.DataFrame,
    missing_from_fred: pd.DataFrame,
) -> pd.DataFrame:
    finite_mask = np.isfinite(refreshed["SOFR"])
    return pd.DataFrame(
        [
            {
                "check": "canonical_columns_exact",
                "severity": "hard",
                "passed": list(refreshed.columns)
                == ["observation_date", "SOFR"],
                "detail": str(list(refreshed.columns)),
            },
            {
                "check": "download_not_empty",
                "severity": "hard",
                "passed": len(refreshed) > 0,
                "detail": f"rows={len(refreshed):,}",
            },
            {
                "check": "observation_dates_unique",
                "severity": "hard",
                "passed": refreshed["observation_date"].is_unique,
                "detail": (
                    f"duplicate_rows="
                    f"{int(refreshed['observation_date'].duplicated().sum()):,}"
                ),
            },
            {
                "check": "dates_monotonic",
                "severity": "hard",
                "passed": refreshed["observation_date"].is_monotonic_increasing,
                "detail": "sorted ascending",
            },
            {
                "check": "rates_finite",
                "severity": "hard",
                "passed": bool(finite_mask.all()),
                "detail": f"nonfinite_rows={int((~finite_mask).sum()):,}",
            },
            {
                "check": "rates_within_sanity_range",
                "severity": "hard",
                "passed": bool(refreshed["SOFR"].between(-5.0, 25.0).all()),
                "detail": (
                    f"min={refreshed['SOFR'].min():.6f}; "
                    f"max={refreshed['SOFR'].max():.6f}; units=percent"
                ),
            },
            {
                "check": "fresh_max_not_older_than_cache",
                "severity": "hard",
                "passed": (
                    refreshed["observation_date"].max()
                    >= existing["observation_date"].max()
                ),
                "detail": (
                    f"old_max={existing['observation_date'].max().date()}; "
                    f"fresh_max={refreshed['observation_date'].max().date()}"
                ),
            },
            {
                "check": "fresh_history_not_truncated",
                "severity": "hard",
                "passed": len(refreshed) >= len(existing),
                "detail": (
                    f"old_rows={len(existing):,}; "
                    f"fresh_rows={len(refreshed):,}"
                ),
            },
            {
                "check": "no_existing_dates_missing_from_fred",
                "severity": "hard",
                "passed": missing_from_fred.empty,
                "detail": f"missing_dates={len(missing_from_fred):,}",
            },
        ]
    )


def atomic_publish_csv(
    refreshed: pd.DataFrame,
    canonical_path: Path,
    backup_path: Path,
    run_timestamp: str,
) -> None:
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(canonical_path, backup_path)

    temp_path = canonical_path.with_name(
        f".{canonical_path.name}.{run_timestamp}.{os.getpid()}.tmp"
    )
    try:
        refreshed.to_csv(
            temp_path,
            index=False,
            date_format="%Y-%m-%d",
            lineterminator="\n",
        )

        temp_reloaded = normalize_sofr_frame(
            pd.read_csv(temp_path),
            "Temporary SOFR candidate",
        )
        if not temp_reloaded.equals(refreshed.reset_index(drop=True)):
            raise RuntimeError(
                "Temporary SOFR candidate did not exactly match the validated frame."
            )

        os.replace(temp_path, canonical_path)

        canonical_after = normalize_sofr_frame(
            pd.read_csv(canonical_path),
            "Canonical cache after publication",
        )
        if not canonical_after.equals(refreshed.reset_index(drop=True)):
            shutil.copy2(backup_path, canonical_path)
            raise RuntimeError(
                "Post-publication readback did not match. "
                "The prior canonical cache was restored."
            )
    finally:
        temp_path.unlink(missing_ok=True)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh and validate the canonical FRED SOFR cache."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check-only",
        action="store_true",
        help="Download, compare, and validate without changing production.",
    )
    mode.add_argument(
        "--write-canonical",
        action="store_true",
        help="Publish the validated refresh to the canonical cache.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30)
    return parser.parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    canonical_path = project_root / CANONICAL_REL
    audit_root = project_root / AUDIT_ROOT_REL
    run_timestamp = utc_stamp()
    run_dir = audit_root / run_timestamp
    run_dir.mkdir(parents=True, exist_ok=False)

    if not canonical_path.exists():
        raise FileNotFoundError(f"Canonical SOFR cache not found: {canonical_path}")

    check_only = not bool(args.write_canonical)

    print("=" * 100)
    print("VRP SOFR EOD update v1")
    print("=" * 100)
    print(f"Project root:       {project_root}")
    print(f"Canonical cache:    {canonical_path}")
    print(f"FRED source:        {FRED_CSV_URL}")
    print(f"Check only:         {check_only}")
    print(f"Audit directory:    {run_dir}")

    existing = normalize_sofr_frame(
        pd.read_csv(canonical_path),
        "Existing cache",
    )
    refreshed = download_fred_sofr(int(args.timeout_seconds))

    (
        comparison,
        new_rows,
        revised_rows,
        missing_from_fred,
        first_changed_sofr_date,
    ) = build_comparison(existing, refreshed)

    validations = validate_sofr(existing, refreshed, missing_from_fred)
    hard_checks_passed = bool(
        validations.loc[
            validations["severity"].eq("hard"),
            "passed",
        ].all()
    )
    changes_detected = bool(len(new_rows) or len(revised_rows))

    validation_path = run_dir / "sofr_validation.csv"
    new_rows_path = run_dir / "sofr_new_observations.csv"
    revised_rows_path = run_dir / "sofr_revised_observations.csv"
    missing_path = run_dir / "sofr_existing_dates_missing_from_fred.csv"
    refreshed_snapshot_path = run_dir / "fred_sofr_history_refreshed_snapshot.csv"
    manifest_path = run_dir / "sofr_update_manifest.json"

    validations.to_csv(validation_path, index=False)
    new_rows.to_csv(new_rows_path, index=False)
    revised_rows.to_csv(revised_rows_path, index=False)
    missing_from_fred.to_csv(missing_path, index=False)
    refreshed.to_csv(
        refreshed_snapshot_path,
        index=False,
        date_format="%Y-%m-%d",
    )

    print("\nComparison")
    print(f"Existing rows:          {len(existing):,}")
    print(f"Refreshed rows:         {len(refreshed):,}")
    print(f"Rows added:             {len(new_rows):,}")
    print(f"Rows revised:           {len(revised_rows):,}")
    print(f"Old dates absent FRED:  {len(missing_from_fred):,}")
    print(
        "First changed date:    "
        + (
            str(first_changed_sofr_date.date())
            if pd.notna(first_changed_sofr_date)
            else "None"
        )
    )
    print(
        f"Old max / fresh max:    "
        f"{existing['observation_date'].max().date()} / "
        f"{refreshed['observation_date'].max().date()}"
    )
    print(f"Hard checks passed:     {hard_checks_passed}")

    if not hard_checks_passed:
        status = "FAILED_VALIDATION"
        published = False
        backup_path = None
    elif check_only:
        status = "CHECK_ONLY"
        published = False
        backup_path = None
    elif not changes_detected:
        status = "NO_CHANGE"
        published = False
        backup_path = None
    else:
        backup_path = canonical_path.with_name(
            f"{canonical_path.stem}.backup_{run_timestamp}{canonical_path.suffix}"
        )
        atomic_publish_csv(
            refreshed=refreshed,
            canonical_path=canonical_path,
            backup_path=backup_path,
            run_timestamp=run_timestamp,
        )
        status = "PUBLISHED"
        published = True

    manifest = {
        "updater_version": UPDATER_VERSION,
        "status": status,
        "published": published,
        "source": "FRED_SOFR",
        "source_url": FRED_CSV_URL,
        "run_timestamp_utc": run_timestamp,
        "generated_at_utc": utc_now_iso(),
        "project_root": str(project_root),
        "canonical_path": str(canonical_path),
        "backup_path": str(backup_path) if backup_path else None,
        "audit_directory": str(run_dir),
        "old_rows": int(len(existing)),
        "new_rows_total": int(len(refreshed)),
        "rows_added": int(len(new_rows)),
        "rows_revised": int(len(revised_rows)),
        "old_dates_absent_from_fred": int(len(missing_from_fred)),
        "old_min_date": str(existing["observation_date"].min().date()),
        "old_max_date": str(existing["observation_date"].max().date()),
        "new_min_date": str(refreshed["observation_date"].min().date()),
        "new_max_date": str(refreshed["observation_date"].max().date()),
        "first_changed_sofr_date": (
            str(first_changed_sofr_date.date())
            if pd.notna(first_changed_sofr_date)
            else None
        ),
        "changes_detected": changes_detected,
        "hard_checks_passed": hard_checks_passed,
        "audit_files": {
            "validation": str(validation_path),
            "new_observations": str(new_rows_path),
            "revised_observations": str(revised_rows_path),
            "missing_from_fred": str(missing_path),
            "refreshed_snapshot": str(refreshed_snapshot_path),
        },
    }
    write_json(manifest_path, manifest)

    print("\n" + "=" * 100)
    print(f"SOFR UPDATE STATUS: {status}")
    print("=" * 100)
    print(f"Latest SOFR date:    {refreshed['observation_date'].max().date()}")
    print(f"Latest SOFR rate:    {refreshed.iloc[-1]['SOFR']:.4f}%")
    print(f"Manifest:            {manifest_path}")

    # Machine-readable markers consumed by the consolidated pipeline.
    print(f"SOFR_STATUS={status}")
    print(f"SOFR_MANIFEST_PATH={manifest_path}")
    print(
        "SOFR_FIRST_CHANGED_DATE="
        + (
            str(first_changed_sofr_date.date())
            if pd.notna(first_changed_sofr_date)
            else ""
        )
    )

    return 0 if hard_checks_passed else 2


def main() -> int:
    try:
        return run()
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
