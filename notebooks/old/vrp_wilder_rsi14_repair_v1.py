
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


RSI_FORMULA_VERSION = "wilder_rsi14_spy_close_v1"
PERIOD = 14


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


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


def load_spy_eod_prices(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "market_data" / "spy_eod_prices_v1.parquet"

    if not path.exists():
        raise FileNotFoundError(f"Missing canonical SPY EOD price file: {path}")

    raw = pd.read_parquet(path)

    required = ["trade_date", "spy_close"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise RuntimeError(f"SPY EOD price file missing required columns {missing}. Columns: {list(raw.columns)}")

    out = raw[required].copy()
    out["trade_date"] = normalize_trade_date_series(out["trade_date"]).astype("Int64")
    out["spy_close"] = pd.to_numeric(out["spy_close"], errors="coerce")

    out = (
        out.dropna(subset=["trade_date", "spy_close"])
        .sort_values("trade_date")
        .drop_duplicates("trade_date", keep="last")
        .reset_index(drop=True)
    )

    out["trade_date"] = out["trade_date"].astype(int)

    if out.empty:
        raise RuntimeError("Canonical SPY EOD price file has no usable rows after cleaning.")

    if not out["trade_date"].is_unique:
        raise RuntimeError("trade_date is not unique after cleaning.")

    if not np.isfinite(out["spy_close"].to_numpy(dtype=float)).all():
        raise RuntimeError("Non-finite SPY close found after cleaning.")

    if not (out["spy_close"].astype(float) > 0).all():
        raise RuntimeError("Non-positive SPY close found after cleaning.")

    return out, path


def compute_wilder_rsi14(prices: pd.DataFrame, source_price_file: Path) -> pd.DataFrame:
    df = prices[["trade_date", "spy_close"]].copy().sort_values("trade_date").reset_index(drop=True)

    close = df["spy_close"].astype(float)
    change = close.diff()
    gain = change.clip(lower=0.0)
    loss = (-change).clip(lower=0.0)

    avg_gain = pd.Series(np.nan, index=df.index, dtype=float)
    avg_loss = pd.Series(np.nan, index=df.index, dtype=float)

    if len(df) > PERIOD:
        first = PERIOD

        # First Wilder seed uses the simple average of the first 14 close-to-close changes.
        avg_gain.iloc[first] = gain.iloc[1:first + 1].mean()
        avg_loss.iloc[first] = loss.iloc[1:first + 1].mean()

        for i in range(first + 1, len(df)):
            avg_gain.iloc[i] = ((avg_gain.iloc[i - 1] * (PERIOD - 1)) + gain.iloc[i]) / PERIOD
            avg_loss.iloc[i] = ((avg_loss.iloc[i - 1] * (PERIOD - 1)) + loss.iloc[i]) / PERIOD

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))

    # Explicit zero-loss / flat-series handling.
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
    out["source_price_file"] = str(source_price_file)

    return out


def nearest_available_date(dates: pd.Series, requested: int) -> int | None:
    vals = sorted(int(x) for x in dates.dropna().astype(int).unique())
    if not vals:
        return None

    prior_or_equal = [x for x in vals if x <= int(requested)]
    if prior_or_equal:
        return int(prior_or_equal[-1])

    return int(vals[0])


def build_external_validation_template(rsi: pd.DataFrame) -> pd.DataFrame:
    latest = int(rsi["trade_date"].max())

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
        matched = nearest_available_date(rsi["trade_date"], requested)

        if matched is None:
            continue

        row = rsi[rsi["trade_date"].eq(matched)].tail(1)

        if row.empty:
            continue

        rr = row.iloc[0]

        rows.append({
            "validation_label": label,
            "requested_trade_date": int(requested),
            "matched_trade_date": int(matched),
            "spy_close": float(rr["spy_close"]),
            "our_wilder_rsi14": float(rr["spy_wilder_rsi14"]) if pd.notna(rr["spy_wilder_rsi14"]) else np.nan,
            "external_source_1": "",
            "external_rsi_1": np.nan,
            "external_diff_1": np.nan,
            "external_source_2": "",
            "external_rsi_2": np.nan,
            "external_diff_2": np.nan,
            "external_chart_config": "SPY daily close, RSI length 14, Wilder/RMA smoothing",
            "notes": "",
        })

    template = pd.DataFrame(rows).drop_duplicates("matched_trade_date", keep="first").reset_index(drop=True)
    return template


def summarize_external_validation(template: pd.DataFrame, tolerance: float) -> pd.DataFrame:
    rows = []

    if template.empty:
        return pd.DataFrame([{
            "check": "external_validation_rows",
            "status": "WARN",
            "detail": "No external validation template rows found.",
        }])

    for source_num in [1, 2]:
        ext_col = f"external_rsi_{source_num}"
        diff_col = f"external_diff_{source_num}"
        source_col = f"external_source_{source_num}"

        if ext_col not in template.columns:
            continue

        vals = pd.to_numeric(template[ext_col], errors="coerce")
        our = pd.to_numeric(template["our_wilder_rsi14"], errors="coerce")
        diffs = vals - our

        provided = vals.notna() & our.notna()

        if provided.any():
            max_abs = float(diffs[provided].abs().max())
            mean_abs = float(diffs[provided].abs().mean())
            status = "PASS" if max_abs <= float(tolerance) else "FAIL"
            detail = (
                f"source={source_num}; rows={int(provided.sum())}; "
                f"max_abs_diff={max_abs:.6f}; mean_abs_diff={mean_abs:.6f}; tolerance={tolerance}"
            )
        else:
            status = "PENDING"
            detail = f"source={source_num}; no external RSI values entered yet."

        rows.append({
            "check": f"external_source_{source_num}",
            "status": status,
            "detail": detail,
        })

    any_external = False
    for source_num in [1, 2]:
        ext_col = f"external_rsi_{source_num}"
        if ext_col in template.columns and pd.to_numeric(template[ext_col], errors="coerce").notna().any():
            any_external = True

    rows.append({
        "check": "external_validation_overall",
        "status": "PASS" if any_external and all(r["status"] in ["PASS", "PENDING"] for r in rows) else "PENDING",
        "detail": "At least one external source has values and no entered values failed tolerance."
        if any_external else
        "Fill external_rsi_1 and/or external_rsi_2 in the validation template, then rerun with --external-validation-file.",
    })

    return pd.DataFrame(rows)


def update_external_diffs(template: pd.DataFrame) -> pd.DataFrame:
    out = template.copy()

    for source_num in [1, 2]:
        ext_col = f"external_rsi_{source_num}"
        diff_col = f"external_diff_{source_num}"

        if ext_col in out.columns:
            out[ext_col] = pd.to_numeric(out[ext_col], errors="coerce")
            out[diff_col] = out[ext_col] - pd.to_numeric(out["our_wilder_rsi14"], errors="coerce")

    return out


def build_internal_checks(prices: pd.DataFrame, rsi: pd.DataFrame) -> pd.DataFrame:
    finite_rsi = rsi["spy_wilder_rsi14"].notna() & np.isfinite(pd.to_numeric(rsi["spy_wilder_rsi14"], errors="coerce"))

    first_finite_date = int(rsi.loc[finite_rsi, "trade_date"].min()) if finite_rsi.any() else None
    latest_date = int(rsi["trade_date"].max())
    latest_rsi = float(rsi.loc[rsi["trade_date"].eq(latest_date), "spy_wilder_rsi14"].iloc[-1])

    rows = [
        {
            "check": "price_rows_match_rsi_rows",
            "status": "PASS" if len(prices) == len(rsi) else "FAIL",
            "detail": f"prices={len(prices)}; rsi={len(rsi)}",
        },
        {
            "check": "trade_dates_unique",
            "status": "PASS" if rsi["trade_date"].is_unique else "FAIL",
            "detail": "trade_date unique check",
        },
        {
            "check": "close_positive_finite",
            "status": "PASS" if (np.isfinite(rsi["spy_close"].astype(float)).all() and (rsi["spy_close"].astype(float) > 0).all()) else "FAIL",
            "detail": "SPY close finite and positive",
        },
        {
            "check": "first_finite_rsi_after_warmup",
            "status": "PASS" if first_finite_date is not None else "FAIL",
            "detail": f"first_finite_date={first_finite_date}",
        },
        {
            "check": "latest_rsi_finite",
            "status": "PASS" if np.isfinite(latest_rsi) else "FAIL",
            "detail": f"latest_trade_date={latest_date}; latest_rsi={latest_rsi}",
        },
        {
            "check": "rsi_bounds",
            "status": "PASS" if rsi.loc[finite_rsi, "spy_wilder_rsi14"].between(0, 100).all() else "FAIL",
            "detail": "Finite RSI values must be between 0 and 100.",
        },
        {
            "check": "formula_version",
            "status": "PASS" if rsi["rsi_formula_version"].eq(RSI_FORMULA_VERSION).all() else "FAIL",
            "detail": RSI_FORMULA_VERSION,
        },
    ]

    return pd.DataFrame(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--external-validation-file", default="")
    p.add_argument("--external-tolerance", type=float, default=0.10)
    args = p.parse_args()

    project_root = Path(args.project_root)
    run_ts = now_stamp()

    processed_dir = project_root / "data" / "processed" / "market_data"
    audit_dir = project_root / "data" / "audit" / "rsi_repair_v1"
    processed_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("Wilder RSI14 repair v1")
    print(f"Project root:  {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Formula:       {RSI_FORMULA_VERSION}")
    print("Mode:          builds standalone RSI history and validation scaffold only")
    print("No final signal files, parameter files, sizing files, or production files are overwritten.")

    banner("Load canonical SPY EOD prices")
    prices, source_price_file = load_spy_eod_prices(project_root)

    print(f"Source file: {source_price_file}")
    print(f"Rows:        {len(prices)}")
    print(f"Date range:  {int(prices['trade_date'].min())} to {int(prices['trade_date'].max())}")
    print(f"Latest close:{float(prices['spy_close'].iloc[-1])}")

    banner("Compute Wilder RSI14")
    rsi = compute_wilder_rsi14(prices, source_price_file)

    show_cols = [
        "trade_date",
        "spy_close",
        "spy_change",
        "spy_gain",
        "spy_loss",
        "wilder_avg_gain_14",
        "wilder_avg_loss_14",
        "spy_wilder_rsi14",
    ]

    print(rsi[show_cols].tail(20).to_string(index=False))

    banner("Internal checks")
    checks = build_internal_checks(prices, rsi)
    print(checks.to_string(index=False))

    internal_pass = bool(checks["status"].eq("PASS").all())

    banner("External validation scaffold")
    if args.external_validation_file:
        ext_path = Path(args.external_validation_file)
        if not ext_path.exists():
            raise FileNotFoundError(f"External validation file not found: {ext_path}")

        template = pd.read_csv(ext_path)
        template = update_external_diffs(template)
        print(f"Loaded external validation file: {ext_path}")
    else:
        template = build_external_validation_template(rsi)
        print("Created new blank external validation template.")

    external_summary = summarize_external_validation(template, tolerance=float(args.external_tolerance))

    print("\nExternal validation template:")
    print(template.to_string(index=False))

    print("\nExternal validation summary:")
    print(external_summary.to_string(index=False))

    banner("Save outputs")
    rsi_path = processed_dir / "spy_wilder_rsi14_history_v1.parquet"

    internal_history_csv_path = audit_dir / f"spy_wilder_rsi14_internal_history_{run_ts}.csv"
    template_path = audit_dir / f"spy_wilder_rsi14_external_validation_points_{run_ts}.csv"
    external_summary_path = audit_dir / f"spy_wilder_rsi14_external_validation_summary_{run_ts}.csv"
    checks_path = audit_dir / f"spy_wilder_rsi14_internal_checks_{run_ts}.csv"

    rsi.to_parquet(rsi_path, index=False)
    rsi.to_csv(internal_history_csv_path, index=False)
    template.to_csv(template_path, index=False)
    external_summary.to_csv(external_summary_path, index=False)
    checks.to_csv(checks_path, index=False)

    external_statuses = set(external_summary["status"].astype(str).tolist())
    external_entered = any(s in external_statuses for s in ["PASS", "FAIL"])
    external_fail = "FAIL" in external_statuses

    build_pass = bool(internal_pass and not external_fail)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "formula_version": RSI_FORMULA_VERSION,
        "period": PERIOD,
        "source_price_file": str(source_price_file),
        "processed_output": str(rsi_path),
        "audit_outputs": {
            "internal_history_csv": str(internal_history_csv_path),
            "external_validation_template": str(template_path),
            "external_validation_summary": str(external_summary_path),
            "internal_checks": str(checks_path),
        },
        "rows": int(len(rsi)),
        "date_range": {
            "first_trade_date": int(rsi["trade_date"].min()),
            "latest_trade_date": int(rsi["trade_date"].max()),
        },
        "latest": {
            "trade_date": int(rsi["trade_date"].iloc[-1]),
            "spy_close": float(rsi["spy_close"].iloc[-1]),
            "spy_wilder_rsi14": float(rsi["spy_wilder_rsi14"].iloc[-1]),
        },
        "internal_checks_pass": internal_pass,
        "external_validation_entered": external_entered,
        "external_validation_failed": external_fail,
        "WILDER_RSI14_REPAIR_BUILD_PASS": build_pass,
        "note": (
            "External validation is allowed to be PENDING until values are entered. "
            "Any entered external value outside tolerance fails the build."
        ),
    }

    manifest_path = audit_dir / f"spy_wilder_rsi14_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"RSI parquet:          {rsi_path}")
    print(f"Internal history CSV: {internal_history_csv_path}")
    print(f"Validation template:  {template_path}")
    print(f"Validation summary:   {external_summary_path}")
    print(f"Internal checks:      {checks_path}")
    print(f"Manifest:             {manifest_path}")

    banner("Final result")
    print(f"internal_checks_pass:              {internal_pass}")
    print(f"external_validation_entered:       {external_entered}")
    print(f"external_validation_failed:        {external_fail}")
    print(f"WILDER_RSI14_REPAIR_BUILD_PASS:    {build_pass}")

    if not build_pass:
        raise RuntimeError("WILDER_RSI14_REPAIR_BUILD_PASS is False.")

    print("DONE — Wilder RSI14 history and external validation scaffold complete.")


if __name__ == "__main__":
    main()
