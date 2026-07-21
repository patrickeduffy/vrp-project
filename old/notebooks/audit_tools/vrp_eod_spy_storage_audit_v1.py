
from __future__ import annotations

import argparse
import io
import json
import math
import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests


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


def find_col(df: pd.DataFrame, preferred: list[str], contains_all: list[str] | None = None) -> str | None:
    lower = {str(c).lower(): c for c in df.columns}

    for p in preferred:
        if p.lower() in lower:
            return lower[p.lower()]

    if contains_all:
        for c in df.columns:
            cl = str(c).lower()
            if all(x.lower() in cl for x in contains_all):
                return c

    return None


def load_market_file(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {name}: {path}")

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    elif path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    else:
        raise RuntimeError(f"Unsupported file extension for {name}: {path}")

    if df.empty:
        raise RuntimeError(f"{name} is empty: {path}")

    return df


def standardize_market_panel(df: pd.DataFrame, name: str) -> pd.DataFrame:
    out = df.copy()

    date_col = find_col(out, ["trade_date", "date", "calc_date", "source_date_column"])
    close_col = find_col(out, ["spy_close", "close", "adjusted_close", "adj_close"], contains_all=["close"])

    if date_col is None:
        raise RuntimeError(f"{name}: could not identify trade_date column. Columns: {list(out.columns)}")

    if close_col is None:
        raise RuntimeError(f"{name}: could not identify SPY close column. Columns: {list(out.columns)}")

    out["_trade_date_int"] = normalize_trade_date_series(out[date_col]).astype("Int64")
    out["_spy_close"] = pd.to_numeric(out[close_col], errors="coerce")

    if "spy_log_return" in out.columns:
        out["_spy_log_return"] = pd.to_numeric(out["spy_log_return"], errors="coerce")
    else:
        out["_spy_log_return"] = np.nan

    out = (
        out.dropna(subset=["_trade_date_int", "_spy_close"])
        .sort_values("_trade_date_int")
        .reset_index(drop=True)
    )
    out["_trade_date_int"] = out["_trade_date_int"].astype(int)

    return out


def summarize_panel(df: pd.DataFrame, name: str, path: Path) -> dict:
    dates = df["_trade_date_int"].dropna().astype(int)
    duplicated_dates = int(df["_trade_date_int"].duplicated().sum())

    latest_date = int(dates.max())
    latest = df[df["_trade_date_int"].eq(latest_date)].tail(1).iloc[0]

    close_ok = bool(np.isfinite(float(latest["_spy_close"])) and float(latest["_spy_close"]) > 0)

    return {
        "file_name": name,
        "path": str(path),
        "rows": int(len(df)),
        "first_trade_date": int(dates.min()),
        "latest_trade_date": latest_date,
        "duplicate_trade_date_rows": duplicated_dates,
        "latest_spy_close": float(latest["_spy_close"]),
        "latest_spy_close_ok": close_ok,
        "has_spy_log_return": bool("_spy_log_return" in df.columns and df["_spy_log_return"].notna().any()),
        "latest_spy_log_return": float(latest["_spy_log_return"]) if pd.notna(latest["_spy_log_return"]) else np.nan,
    }


def inspect_script(path: Path, search_terms: list[str]) -> dict:
    result = {
        "script": str(path),
        "exists": path.exists(),
        "terms_found": "",
        "evidence_lines": "",
    }

    if not path.exists():
        return result

    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()

    terms_found = []
    evidence = []

    for term in search_terms:
        if term.lower() in text.lower():
            terms_found.append(term)

    for i, line in enumerate(lines, start=1):
        lo = line.lower()
        if any(term.lower() in lo for term in search_terms):
            evidence.append(f"L{i}: {line.strip()}")

    result["terms_found"] = "|".join(terms_found)
    result["evidence_lines"] = "\n".join(evidence[:80])

    return result


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


def fetch_thetadata_spy_eod(base_url: str, start_date: int, end_date: int) -> tuple[pd.DataFrame, dict]:
    url = base_url.rstrip("/") + "/stock/history/eod"
    params = {
        "symbol": "SPY",
        "start_date": str(int(start_date)),
        "end_date": str(int(end_date)),
        "format": "json",
    }

    meta = {
        "url": url,
        "params": json.dumps(params, sort_keys=True),
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
        r = requests.get(url, params=params, timeout=90)
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


def standardize_thetadata_eod(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = df.copy()

    close_col = find_col(out, ["close", "price", "last"], contains_all=["close"])
    if close_col is None:
        raise RuntimeError(f"ThetaData EOD response missing close column. Columns: {list(out.columns)}")

    # ThetaData v3 stock/history/eod may return no explicit date column.
    # In that case derive trade_date from last_trade or created timestamp.
    date_col = find_col(out, ["date", "trade_date", "timestamp"])

    if date_col is not None:
        out["_trade_date_int"] = normalize_trade_date_series(out[date_col]).astype("Int64")
    elif "last_trade" in out.columns:
        out["_trade_date_int"] = (
            pd.to_datetime(out["last_trade"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )
    elif "created" in out.columns:
        out["_trade_date_int"] = (
            pd.to_datetime(out["created"], errors="coerce")
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )
    else:
        raise RuntimeError(
            f"ThetaData EOD response missing usable date source. "
            f"Expected date/trade_date/timestamp/last_trade/created. Columns: {list(out.columns)}"
        )

    out["_spy_close_thetadata"] = pd.to_numeric(out[close_col], errors="coerce")

    out = (
        out.dropna(subset=["_trade_date_int", "_spy_close_thetadata"])
        .sort_values("_trade_date_int")
        .drop_duplicates("_trade_date_int", keep="last")
        .reset_index(drop=True)
    )

    out["_trade_date_int"] = out["_trade_date_int"].astype(int)
    return out


def compare_latest_files(eod: pd.DataFrame, rv: pd.DataFrame, corsi: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    rows = []

    latest_eod_date = int(eod["_trade_date_int"].max())
    latest_rv_date = int(rv["_trade_date_int"].max())
    latest_corsi_date = int(corsi["_trade_date_int"].max())

    latest_dates_match = latest_eod_date == latest_rv_date == latest_corsi_date

    for name, df in [("eod", eod), ("rv", rv), ("corsi", corsi)]:
        latest = df[df["_trade_date_int"].eq(int(df["_trade_date_int"].max()))].tail(1).iloc[0]
        rows.append({
            "source": name,
            "latest_trade_date": int(latest["_trade_date_int"]),
            "latest_spy_close": float(latest["_spy_close"]),
            "latest_spy_log_return": float(latest["_spy_log_return"]) if pd.notna(latest["_spy_log_return"]) else np.nan,
        })

    comp = pd.DataFrame(rows)

    latest_close_values = comp["latest_spy_close"].astype(float).values
    latest_close_max_abs_diff = float(np.nanmax(latest_close_values) - np.nanmin(latest_close_values))

    latest_return_values = comp["latest_spy_log_return"].astype(float).dropna().values
    latest_return_max_abs_diff = (
        float(np.nanmax(latest_return_values) - np.nanmin(latest_return_values))
        if len(latest_return_values) >= 2
        else np.nan
    )

    checks = {
        "latest_dates_match": bool(latest_dates_match),
        "latest_eod_date": latest_eod_date,
        "latest_rv_date": latest_rv_date,
        "latest_corsi_date": latest_corsi_date,
        "latest_close_max_abs_diff": latest_close_max_abs_diff,
        "latest_close_match": bool(latest_close_max_abs_diff <= 1e-10),
        "latest_return_max_abs_diff": latest_return_max_abs_diff,
        "latest_return_match": bool(np.isnan(latest_return_max_abs_diff) or latest_return_max_abs_diff <= 1e-12),
    }

    return comp, checks


def validate_log_return_consistency(df: pd.DataFrame, name: str, tolerance: float = 1e-12) -> dict:
    work = (
        df[["_trade_date_int", "_spy_close", "_spy_log_return"]]
        .dropna(subset=["_trade_date_int", "_spy_close"])
        .sort_values("_trade_date_int")
        .drop_duplicates("_trade_date_int", keep="last")
        .reset_index(drop=True)
    )

    work["_calc_log_return"] = np.log(work["_spy_close"] / work["_spy_close"].shift(1))
    mask = work["_spy_log_return"].notna() & work["_calc_log_return"].notna()

    if mask.sum() == 0:
        return {
            "source": name,
            "return_column_available": False,
            "rows_checked": 0,
            "max_abs_log_return_diff": np.nan,
            "latest_log_return_diff": np.nan,
            "log_return_consistent": True,
        }

    diff = work.loc[mask, "_spy_log_return"] - work.loc[mask, "_calc_log_return"]
    latest_masked = work.loc[mask].tail(1).copy()
    latest_diff = float(
        latest_masked["_spy_log_return"].iloc[0] - latest_masked["_calc_log_return"].iloc[0]
    )

    max_abs = float(diff.abs().max())

    return {
        "source": name,
        "return_column_available": True,
        "rows_checked": int(mask.sum()),
        "max_abs_log_return_diff": max_abs,
        "latest_log_return_diff": latest_diff,
        "log_return_consistent": bool(max_abs <= tolerance),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--as-of-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--base-url", default="http://127.0.0.1:25503/v3")
    p.add_argument("--skip-thetadata", action="store_true")
    p.add_argument("--thetadata-lookback-calendar-days", type=int, default=14)
    p.add_argument("--close-tolerance", type=float, default=1e-6)
    args = p.parse_args()

    project_root = Path(args.project_root)
    as_of_date = int(pd.to_datetime(str(args.as_of_date), format="%Y%m%d").strftime("%Y%m%d"))
    run_ts = now_stamp()

    audit_dir = project_root / "data" / "audit" / "eod_spy_storage"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP EOD SPY storage audit v1")
    print(f"Project root:  {project_root}")
    print(f"As-of date:    {as_of_date}")
    print(f"Run timestamp: {run_ts}")
    print(f"Audit dir:     {audit_dir}")
    print("Mode:          audit-only; no production files modified")

    paths = {
        "eod": project_root / "data" / "processed" / "market_data" / "spy_eod_prices_v1.parquet",
        "rv": project_root / "data" / "processed" / "market_data" / "spy_realized_vol_history_v1.parquet",
        "corsi": project_root / "data" / "processed" / "market_data" / "spy_corsi_har_input_panel_v1.parquet",
        "daily_wrapper": project_root / "notebooks" / "vrp_daily_production_update_v1.py",
        "market_builder": project_root / "notebooks" / "vrp_market_data_build_v1.py",
    }

    banner("Inspect production scripts")
    script_terms = [
        "spy_eod_prices_v1.parquet",
        "spy_realized_vol_history_v1.parquet",
        "spy_corsi_har_input_panel_v1.parquet",
        "vrp_market_data_build_v1.py",
        "market_data",
        "stock/history/eod",
        "symbol",
        "SPY",
    ]

    script_rows = [
        inspect_script(paths["daily_wrapper"], script_terms),
        inspect_script(paths["market_builder"], script_terms),
    ]
    script_audit = pd.DataFrame(script_rows)

    print(script_audit[["script", "exists", "terms_found"]].to_string(index=False))

    daily_wrapper_exists = bool(paths["daily_wrapper"].exists())
    market_builder_exists = bool(paths["market_builder"].exists())

    wrapper_mentions_market_builder = False
    wrapper_mentions_market_outputs = False

    if daily_wrapper_exists:
        wrapper_text = paths["daily_wrapper"].read_text(encoding="utf-8", errors="replace").lower()
        wrapper_mentions_market_builder = "vrp_market_data_build_v1.py" in wrapper_text or "market_data" in wrapper_text
        wrapper_mentions_market_outputs = (
            "spy_eod_prices_v1.parquet" in wrapper_text
            or "spy_realized_vol_history_v1.parquet" in wrapper_text
            or "spy_corsi_har_input_panel_v1.parquet" in wrapper_text
        )

    banner("Load stored EOD market files")
    eod_raw = load_market_file(paths["eod"], "spy_eod_prices")
    rv_raw = load_market_file(paths["rv"], "spy_realized_vol_history")
    corsi_raw = load_market_file(paths["corsi"], "spy_corsi_har_input_panel")

    eod = standardize_market_panel(eod_raw, "spy_eod_prices")
    rv = standardize_market_panel(rv_raw, "spy_realized_vol_history")
    corsi = standardize_market_panel(corsi_raw, "spy_corsi_har_input_panel")

    summaries = pd.DataFrame([
        summarize_panel(eod, "spy_eod_prices", paths["eod"]),
        summarize_panel(rv, "spy_realized_vol_history", paths["rv"]),
        summarize_panel(corsi, "spy_corsi_har_input_panel", paths["corsi"]),
    ])

    print(summaries[[
        "file_name",
        "rows",
        "first_trade_date",
        "latest_trade_date",
        "duplicate_trade_date_rows",
        "latest_spy_close",
        "latest_spy_close_ok",
        "latest_spy_log_return",
    ]].to_string(index=False))

    banner("Cross-file consistency")
    latest_compare, cross_checks = compare_latest_files(eod, rv, corsi)
    print(latest_compare.to_string(index=False))
    print(json.dumps(cross_checks, indent=2, default=str))

    return_checks = pd.DataFrame([
        validate_log_return_consistency(eod, "spy_eod_prices"),
        validate_log_return_consistency(rv, "spy_realized_vol_history"),
        validate_log_return_consistency(corsi, "spy_corsi_har_input_panel"),
    ])

    print("\nLog return consistency:")
    print(return_checks.to_string(index=False))

    banner("Optional independent ThetaData EOD comparison")
    thetadata_meta = {}
    thetadata_compare = pd.DataFrame()
    thetadata_check = {
        "thetadata_checked": False,
        "thetadata_available": False,
        "thetadata_latest_date": None,
        "stored_latest_date": int(eod["_trade_date_int"].max()),
        "stored_close_on_thetadata_latest_date": np.nan,
        "thetadata_close": np.nan,
        "close_abs_diff": np.nan,
        "stored_is_current_vs_thetadata": True,
        "stored_close_matches_thetadata": True,
        "thetadata_note": "",
    }

    if args.skip_thetadata:
        thetadata_check["thetadata_note"] = "Skipped by --skip-thetadata."
        print("Skipped by --skip-thetadata.")
    else:
        end_dt = pd.to_datetime(str(as_of_date), format="%Y%m%d")
        start_dt = end_dt - pd.Timedelta(days=int(args.thetadata_lookback_calendar_days))
        start_date = int(start_dt.strftime("%Y%m%d"))

        td_raw, thetadata_meta = fetch_thetadata_spy_eod(
            base_url=args.base_url,
            start_date=start_date,
            end_date=as_of_date,
        )

        thetadata_check["thetadata_checked"] = True

        print(json.dumps(thetadata_meta, indent=2, default=str))

        if td_raw.empty:
            thetadata_check["thetadata_note"] = "ThetaData EOD query returned no rows or non-OK response. This does not fail storage audit by itself."
        else:
            try:
                td = standardize_thetadata_eod(td_raw)

                if td.empty:
                    thetadata_check["thetadata_note"] = "ThetaData EOD response parsed but no usable rows."
                else:
                    td_latest = td.tail(1).iloc[0]
                    td_latest_date = int(td_latest["_trade_date_int"])
                    td_close = float(td_latest["_spy_close_thetadata"])

                    stored_on_td_date = eod[eod["_trade_date_int"].eq(td_latest_date)].copy()

                    thetadata_check["thetadata_available"] = True
                    thetadata_check["thetadata_latest_date"] = td_latest_date
                    thetadata_check["thetadata_close"] = td_close

                    if stored_on_td_date.empty:
                        thetadata_check["stored_is_current_vs_thetadata"] = False
                        thetadata_check["stored_close_matches_thetadata"] = False
                        thetadata_check["thetadata_note"] = (
                            f"ThetaData latest date {td_latest_date} is not present in stored EOD file."
                        )
                    else:
                        stored_close = float(stored_on_td_date.tail(1).iloc[0]["_spy_close"])
                        abs_diff = abs(stored_close - td_close)

                        thetadata_check["stored_close_on_thetadata_latest_date"] = stored_close
                        thetadata_check["close_abs_diff"] = abs_diff
                        thetadata_check["stored_is_current_vs_thetadata"] = True
                        thetadata_check["stored_close_matches_thetadata"] = bool(abs_diff <= float(args.close_tolerance))
                        thetadata_check["thetadata_note"] = (
                            "Compared stored close to latest available ThetaData EOD row."
                        )

                    thetadata_compare = td.copy()

            except Exception as exc:
                thetadata_check["thetadata_note"] = f"ThetaData parse/compare failed: {repr(exc)}. This does not fail storage audit by itself."

    print(json.dumps(thetadata_check, indent=2, default=str))

    banner("Final checks")
    duplicate_ok = bool((summaries["duplicate_trade_date_rows"] == 0).all())
    close_positive_ok = bool(summaries["latest_spy_close_ok"].all())
    latest_dates_match = bool(cross_checks["latest_dates_match"])
    latest_close_match = bool(cross_checks["latest_close_match"])
    latest_return_match = bool(cross_checks["latest_return_match"])
    log_return_consistent = bool(return_checks["log_return_consistent"].all())

    scripts_ok = bool(
        daily_wrapper_exists
        and market_builder_exists
        and wrapper_mentions_market_builder
    )

    thetadata_ok = bool(
        (not thetadata_check["thetadata_checked"])
        or (not thetadata_check["thetadata_available"])
        or (
            thetadata_check["stored_is_current_vs_thetadata"]
            and thetadata_check["stored_close_matches_thetadata"]
        )
    )

    audit_pass = bool(
        scripts_ok
        and duplicate_ok
        and close_positive_ok
        and latest_dates_match
        and latest_close_match
        and latest_return_match
        and log_return_consistent
        and thetadata_ok
    )

    checks = {
        "scripts_ok": scripts_ok,
        "daily_wrapper_exists": daily_wrapper_exists,
        "market_builder_exists": market_builder_exists,
        "wrapper_mentions_market_builder_or_market_data": wrapper_mentions_market_builder,
        "wrapper_mentions_market_outputs": wrapper_mentions_market_outputs,
        "duplicate_ok": duplicate_ok,
        "close_positive_ok": close_positive_ok,
        "latest_dates_match": latest_dates_match,
        "latest_close_match": latest_close_match,
        "latest_return_match": latest_return_match,
        "log_return_consistent": log_return_consistent,
        "thetadata_ok": thetadata_ok,
        "EOD_SPY_STORAGE_AUDIT_PASS": audit_pass,
    }

    print(json.dumps(checks, indent=2, default=str))

    banner("Save audit outputs")
    summary_path = audit_dir / f"eod_spy_storage_audit_summary_{as_of_date}_{run_ts}.csv"
    latest_rows_path = audit_dir / f"eod_spy_storage_audit_file_latest_rows_{as_of_date}_{run_ts}.csv"
    script_audit_path = audit_dir / f"eod_spy_storage_audit_script_evidence_{as_of_date}_{run_ts}.csv"
    return_check_path = audit_dir / f"eod_spy_storage_audit_return_checks_{as_of_date}_{run_ts}.csv"
    thetadata_compare_path = audit_dir / f"eod_spy_storage_audit_thetadata_rows_{as_of_date}_{run_ts}.csv"

    summaries.to_csv(summary_path, index=False)
    latest_compare.to_csv(latest_rows_path, index=False)
    script_audit.to_csv(script_audit_path, index=False)
    return_checks.to_csv(return_check_path, index=False)

    if not thetadata_compare.empty:
        thetadata_compare.to_csv(thetadata_compare_path, index=False)
    else:
        pd.DataFrame().to_csv(thetadata_compare_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "as_of_date": int(as_of_date),
        "inputs": {k: str(v) for k, v in paths.items()},
        "cross_file_checks": cross_checks,
        "thetadata_check": thetadata_check,
        "final_checks": checks,
        "audit_outputs": {
            "summary": str(summary_path),
            "latest_rows": str(latest_rows_path),
            "script_evidence": str(script_audit_path),
            "return_checks": str(return_check_path),
            "thetadata_rows": str(thetadata_compare_path),
        },
        "method_note": "Audit-only check that official EOD SPY files are stored, current, deduplicated, internally consistent, and optionally aligned with latest available ThetaData EOD.",
    }

    manifest_path = audit_dir / f"eod_spy_storage_audit_manifest_{as_of_date}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"summary:         {summary_path}")
    print(f"latest_rows:     {latest_rows_path}")
    print(f"script_evidence: {script_audit_path}")
    print(f"return_checks:   {return_check_path}")
    print(f"thetadata_rows:  {thetadata_compare_path}")
    print(f"manifest:        {manifest_path}")

    banner("Final result")
    print(f"EOD_SPY_STORAGE_AUDIT_PASS: {audit_pass}")

    if not audit_pass:
        raise RuntimeError("EOD_SPY_STORAGE_AUDIT_PASS is False.")

    print("DONE — EOD SPY storage audit complete.")


if __name__ == "__main__":
    main()
