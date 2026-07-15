
from __future__ import annotations

import argparse
import io
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import requests


COMMON_PORTS = [25503, 25510, 25511]
DEFAULT_TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def safe_name(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]+", "_", str(s))
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:120] or "sample"


def read_text_safe(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def scan_source_scripts(project_root: Path) -> pd.DataFrame:
    candidates = [
        project_root / "notebooks" / "vrp_implied_variance_eod_update_v1.py",
        project_root / "notebooks" / "vrp_market_data_build_v1.py",
        project_root / "notebooks" / "vrp_corsi_source_update_v1.py",
        project_root / "notebooks" / "vrp_corsi_spx_source_extension_audit_v1.py",
        project_root / "notebooks" / "vrp_intraday_source_lock_v1.py",
    ]

    rows = []
    endpoint_patterns = [
        r"https?://(?:127\.0\.0\.1|localhost):\d+[A-Za-z0-9_/\-?.=&%]*",
        r"(?:v[23]/[A-Za-z0-9_/\-]+)",
        r"(?:stock/history/[A-Za-z0-9_/\-]+)",
        r"(?:option/history/[A-Za-z0-9_/\-]+)",
        r"(?:option/snapshot/[A-Za-z0-9_/\-]+)",
        r"(?:bulk_snapshot/option/[A-Za-z0-9_/\-]+)",
    ]

    for path in candidates:
        text = read_text_safe(path)
        if not text:
            rows.append({
                "source_path": str(path),
                "exists": path.exists(),
                "matched_text": "",
                "context": "",
            })
            continue

        for pat in endpoint_patterns:
            for m in re.finditer(pat, text, flags=re.IGNORECASE):
                start = max(0, m.start() - 120)
                end = min(len(text), m.end() + 120)
                context = text[start:end].replace("\r", " ").replace("\n", " ")
                context = re.sub(r"\s+", " ", context).strip()
                rows.append({
                    "source_path": str(path),
                    "exists": path.exists(),
                    "matched_text": m.group(0),
                    "context": context,
                })

    return pd.DataFrame(rows)


def discovered_ports_from_sources(source_scan: pd.DataFrame) -> list[int]:
    ports = set(COMMON_PORTS)

    if not source_scan.empty:
        text = " ".join(source_scan["matched_text"].fillna("").astype(str).tolist())
        for p in re.findall(r"(?:127\.0\.0\.1|localhost):(\d+)", text):
            try:
                ports.add(int(p))
            except Exception:
                pass

    return sorted(ports)


def parse_json_table(obj) -> pd.DataFrame | None:
    if isinstance(obj, list):
        if not obj:
            return pd.DataFrame()
        if isinstance(obj[0], dict):
            return pd.DataFrame(obj)
        if isinstance(obj[0], list):
            return pd.DataFrame(obj)

    if isinstance(obj, dict):
        header = obj.get("header", {}) if isinstance(obj.get("header", {}), dict) else {}
        fmt = header.get("format") or header.get("columns") or header.get("fields")

        if isinstance(fmt, list):
            cols = []
            for item in fmt:
                if isinstance(item, dict):
                    cols.append(
                        item.get("name")
                        or item.get("field")
                        or item.get("column")
                        or item.get("header")
                        or str(item)
                    )
                else:
                    cols.append(str(item))
        else:
            cols = None

        for key in ["response", "data", "rows", "results"]:
            value = obj.get(key)
            if value is None:
                continue

            if isinstance(value, list):
                if not value:
                    return pd.DataFrame(columns=cols or [])
                if isinstance(value[0], dict):
                    return pd.DataFrame(value)
                if isinstance(value[0], list):
                    if cols and len(cols) == len(value[0]):
                        return pd.DataFrame(value, columns=cols)
                    return pd.DataFrame(value)

            if isinstance(value, dict):
                nested = parse_json_table(value)
                if nested is not None:
                    return nested

        # Fallback: one-row frame if scalar dict.
        scalar = {k: v for k, v in obj.items() if not isinstance(v, (list, dict))}
        if scalar:
            return pd.DataFrame([scalar])

    return None


def parse_response_table(resp: requests.Response) -> tuple[str, pd.DataFrame | None, str]:
    text = resp.text or ""
    text_head = text[:3000]

    # Try JSON first.
    try:
        obj = resp.json()
        df = parse_json_table(obj)
        if df is not None:
            return "json_table", df, text_head
    except Exception:
        pass

    # Try CSV / delimited table.
    try:
        if text.strip():
            df = pd.read_csv(io.StringIO(text))
            if df is not None:
                return "csv_table", df, text_head
    except Exception:
        pass

    # Some APIs may return TSV-like or semicolon-like text.
    try:
        if text.strip():
            df = pd.read_csv(io.StringIO(text), sep=None, engine="python")
            if df is not None:
                return "delimited_table", df, text_head
    except Exception:
        pass

    return "unparsed_text", None, text_head


def probe_endpoint(
    *,
    base_url: str,
    category: str,
    name: str,
    path: str,
    params: dict,
    timeout: float,
) -> tuple[dict, pd.DataFrame | None]:
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    start = time.time()

    result = {
        "category": category,
        "name": name,
        "base_url": base_url,
        "path": path,
        "params_json": json.dumps(params, sort_keys=True),
        "full_url": url + ("?" + urlencode(params) if params else ""),
        "status_code": None,
        "elapsed_sec": None,
        "request_ok": False,
        "parse_status": "",
        "rows": None,
        "cols": None,
        "columns": "",
        "sample_path": "",
        "error": "",
        "text_sample": "",
    }

    try:
        resp = requests.get(url, params=params, timeout=timeout)
        result["elapsed_sec"] = round(time.time() - start, 4)
        result["status_code"] = int(resp.status_code)
        result["request_ok"] = bool(resp.ok)

        parse_status, df, text_sample = parse_response_table(resp)
        result["parse_status"] = parse_status
        result["text_sample"] = re.sub(r"\s+", " ", (text_sample or "")[:1000]).strip()

        if df is not None:
            result["rows"] = int(len(df))
            result["cols"] = int(len(df.columns))
            result["columns"] = "|".join([str(c) for c in df.columns])

        return result, df

    except Exception as exc:
        result["elapsed_sec"] = round(time.time() - start, 4)
        result["error"] = str(exc)
        return result, None


def extract_expirations_from_text(text: str) -> list[int]:
    vals = []
    for m in re.findall(r"\b20\d{6}\b", str(text)):
        try:
            vals.append(int(m))
        except Exception:
            pass
    return vals


def extract_expirations_from_df(df: pd.DataFrame | None) -> list[int]:
    if df is None or df.empty:
        return []

    vals = set()
    for col in df.columns:
        c = str(col).lower()
        if any(x in c for x in ["exp", "expiration", "expiry", "date"]):
            for raw in df[col].dropna().astype(str).head(5000):
                raw_digits = re.sub(r"\D", "", raw)
                if re.fullmatch(r"20\d{6}", raw_digits):
                    vals.add(int(raw_digits))

    # Fallback: regex over first rows.
    text = df.head(500).to_csv(index=False)
    vals.update(extract_expirations_from_text(text))

    return sorted(vals)


def upcoming_friday_expirations(trade_date: pd.Timestamp, min_days: int = 5, max_days: int = 60) -> list[int]:
    out = []
    d = trade_date.normalize() + pd.Timedelta(days=1)
    end = trade_date.normalize() + pd.Timedelta(days=max_days)

    while d <= end:
        days = (d - trade_date.normalize()).days
        if d.weekday() == 4 and days >= min_days:
            out.append(int(d.strftime("%Y%m%d")))
        d += pd.Timedelta(days=1)

    return out


def column_quality_flags(df: pd.DataFrame | None, text_sample: str = "") -> dict:
    cols = [] if df is None else [str(c).lower() for c in df.columns]
    joined_cols = " ".join(cols)
    joined = joined_cols + " " + str(text_sample).lower()

    def has_any(words: list[str]) -> bool:
        return any(w.lower() in joined for w in words)

    return {
        "has_strike": has_any(["strike"]),
        "has_right": has_any(["right", "put_call", "option_type"]),
        "has_bid": has_any(["bid"]),
        "has_ask": has_any(["ask"]),
        "has_mid": has_any(["mid", "mark"]),
        "has_price": has_any(["price", "close", "last", "trade"]),
        "has_timestamp": has_any(["timestamp", "ms_of_day", "time", "date"]),
    }


def save_sample_if_any(
    *,
    df: pd.DataFrame | None,
    result: dict,
    audit_dir: Path,
    run_ts: str,
    sample_rows: int,
) -> None:
    if df is None:
        return

    sample_path = audit_dir / f"sample_{safe_name(result['category'])}_{safe_name(result['name'])}_{run_ts}.csv"
    df.head(sample_rows).to_csv(sample_path, index=False)
    result["sample_path"] = str(sample_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    parser.add_argument("--trade-date", default=None, help="YYYYMMDD. Defaults to local today.")
    parser.add_argument("--stock-root", default="SPY")
    parser.add_argument("--index-root", default="SPX")
    parser.add_argument("--option-roots", default="SPXW,SPX")
    parser.add_argument("--api-bases", default=None, help="Comma-separated base URLs. Default scans common local ports.")
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--sample-rows", type=int, default=100)
    parser.add_argument("--allow-root-snapshot", action="store_true", help="Also try root-wide option snapshots without expiration. May be large.")
    args = parser.parse_args()

    project_root = Path(args.project_root)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    trade_date_str = args.trade_date or now_yyyymmdd()
    trade_date = pd.to_datetime(trade_date_str, format="%Y%m%d", errors="raise")

    audit_dir = project_root / "data" / "audit" / "intraday_live_probe"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday ThetaData live probe v1")
    print(f"Project root: {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Trade date used for probes: {trade_date_str}")
    print(f"Audit dir: {audit_dir}")
    print("Read-only probe: no production signal files will be modified.")

    banner("Scan local source scripts for known ThetaData endpoints")
    source_scan = scan_source_scripts(project_root)
    source_scan_path = audit_dir / f"intraday_thetadata_live_probe_source_scan_{run_ts}.csv"
    source_scan.to_csv(source_scan_path, index=False)
    print(f"Saved source scan: {source_scan_path}")
    if not source_scan.empty:
        print(source_scan.head(25).to_string(index=False))

    if args.api_bases:
        api_bases = [x.strip().rstrip("/") for x in args.api_bases.split(",") if x.strip()]
    else:
        ports = discovered_ports_from_sources(source_scan)
        api_bases = [f"http://127.0.0.1:{p}" for p in ports]

    print(f"\nAPI bases to probe: {api_bases}")

    option_roots = [x.strip() for x in args.option_roots.split(",") if x.strip()]
    attempts: list[dict] = []

    def run(category: str, name: str, base_url: str, path: str, params: dict) -> tuple[dict, pd.DataFrame | None]:
        result, df = probe_endpoint(
            base_url=base_url,
            category=category,
            name=name,
            path=path,
            params=params,
            timeout=float(args.timeout),
        )
        save_sample_if_any(
            df=df,
            result=result,
            audit_dir=audit_dir,
            run_ts=run_ts,
            sample_rows=int(args.sample_rows),
        )
        attempts.append(result)
        return result, df

    banner("Probe terminal/base availability")
    base_paths = [
        ("base_root", "/"),
        ("v3_system_status", "/v3/system/status"),
        ("v2_system_status", "/v2/system/status"),
    ]

    for base_url in api_bases:
        for name, path_ in base_paths:
            run("terminal_health", name, base_url, path_, {})

    banner("Probe live/current stock or index price candidates")
    stock_price_endpoints = [
        ("v3_stock_snapshot_quote", "/v3/stock/snapshot/quote", {"root": args.stock_root}),
        ("v3_stock_snapshot_trade", "/v3/stock/snapshot/trade", {"root": args.stock_root}),
        ("v3_stock_snapshot_ohlc", "/v3/stock/snapshot/ohlc", {"root": args.stock_root}),
        ("v3_stock_history_eod_today", "/v3/stock/history/eod", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str}),
        ("v3_stock_history_ohlc_1m_today", "/v3/stock/history/ohlc", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str, "interval_size": 60000}),
        ("v3_stock_history_ohlc_5m_today", "/v3/stock/history/ohlc", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str, "interval_size": 300000}),
        ("v2_hist_stock_eod_today", "/v2/hist/stock/eod", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str, "use_csv": "true"}),
        ("v2_hist_stock_ohlc_1m_today", "/v2/hist/stock/ohlc", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str, "ivl": 60000, "use_csv": "true"}),
        ("v2_hist_stock_ohlc_5m_today", "/v2/hist/stock/ohlc", {"root": args.stock_root, "start_date": trade_date_str, "end_date": trade_date_str, "ivl": 300000, "use_csv": "true"}),
        ("v3_index_snapshot_price", "/v3/index/snapshot/price", {"root": args.index_root}),
        ("v3_index_snapshot_quote", "/v3/index/snapshot/quote", {"root": args.index_root}),
        ("v3_index_history_eod_today", "/v3/index/history/eod", {"root": args.index_root, "start_date": trade_date_str, "end_date": trade_date_str}),
    ]

    for base_url in api_bases:
        for name, path_, params in stock_price_endpoints:
            run("stock_index_price", name, base_url, path_, params)

    banner("Probe option expiration lists")
    expiration_candidates_by_root: dict[str, list[int]] = {}

    expiration_list_paths = [
        ("v3_option_list_expirations", "/v3/option/list/expirations"),
        ("v3_list_option_expirations", "/v3/list/option/expirations"),
        ("v3_list_expirations", "/v3/list/expirations"),
        ("v2_list_expirations", "/v2/list/expirations"),
    ]

    for root in option_roots:
        all_exps = set()

        for base_url in api_bases:
            for name, path_ in expiration_list_paths:
                result, df = run(
                    "option_expiration_list",
                    f"{name}_{root}",
                    base_url,
                    path_,
                    {"root": root, "use_csv": "true"},
                )
                all_exps.update(extract_expirations_from_df(df))
                all_exps.update(extract_expirations_from_text(result.get("text_sample", "")))

        if not all_exps:
            # Fallback for probing only; not a production expiration universe.
            all_exps.update(upcoming_friday_expirations(trade_date, min_days=5, max_days=60))

        usable = []
        for exp in sorted(all_exps):
            try:
                exp_dt = pd.to_datetime(str(exp), format="%Y%m%d")
                days = int((exp_dt.normalize() - trade_date.normalize()).days)
                if 5 <= days <= 60:
                    usable.append(exp)
            except Exception:
                pass

        expiration_candidates_by_root[root] = usable[:8]

    print("\nExpiration candidates selected for option quote probes:")
    for root, exps in expiration_candidates_by_root.items():
        print(f"{root}: {exps}")

    banner("Probe option quote snapshot / chain candidates")
    option_snapshot_paths = [
        ("v3_option_snapshot_quote_expiration", "/v3/option/snapshot/quote", "expiration"),
        ("v3_option_snapshot_quote_exp", "/v3/option/snapshot/quote", "exp"),
        ("v3_bulk_snapshot_option_quote_expiration", "/v3/bulk_snapshot/option/quote", "expiration"),
        ("v3_bulk_snapshot_option_quote_exp", "/v3/bulk_snapshot/option/quote", "exp"),
        ("v2_snapshot_option_quote_exp", "/v2/snapshot/option/quote", "exp"),
        ("v2_snapshot_option_quote_expiration", "/v2/snapshot/option/quote", "expiration"),
        ("v2_bulk_snapshot_option_quote_exp", "/v2/bulk_snapshot/option/quote", "exp"),
        ("v2_bulk_snapshot_option_quote_expiration", "/v2/bulk_snapshot/option/quote", "expiration"),
    ]

    # Limit chain attempts to keep runtime reasonable.
    for root in option_roots:
        exps = expiration_candidates_by_root.get(root, [])
        for exp in exps[:4]:
            for base_url in api_bases:
                for name, path_, exp_key in option_snapshot_paths:
                    params = {"root": root, exp_key: int(exp), "use_csv": "true"}
                    run("option_quote_snapshot", f"{name}_{root}_{exp}", base_url, path_, params)

    if args.allow_root_snapshot:
        root_snapshot_paths = [
            ("v3_option_snapshot_quote_root_only", "/v3/option/snapshot/quote"),
            ("v3_bulk_snapshot_option_quote_root_only", "/v3/bulk_snapshot/option/quote"),
            ("v2_bulk_snapshot_option_quote_root_only", "/v2/bulk_snapshot/option/quote"),
        ]
        for root in option_roots:
            for base_url in api_bases:
                for name, path_ in root_snapshot_paths:
                    run("option_root_snapshot_large", f"{name}_{root}", base_url, path_, {"root": root, "use_csv": "true"})

    banner("Assess probe results")
    attempts_df = pd.DataFrame(attempts)

    # Add quality flags from columns and text samples.
    quality_rows = []
    for _, row in attempts_df.iterrows():
        cols_text = str(row.get("columns", ""))
        text_sample = str(row.get("text_sample", ""))
        fake_df = pd.DataFrame(columns=cols_text.split("|") if cols_text else [])
        flags = column_quality_flags(fake_df, text_sample)
        quality_rows.append(flags)

    quality_df = pd.DataFrame(quality_rows)
    attempts_df = pd.concat([attempts_df.reset_index(drop=True), quality_df.reset_index(drop=True)], axis=1)

    terminal_reachable = bool(
        attempts_df["status_code"].notna().any()
        and attempts_df["error"].fillna("").astype(str).str.contains("connection", case=False).mean() < 1.0
    )

    def rows_numeric(s):
        return pd.to_numeric(s, errors="coerce").fillna(0)

    stock_candidates = attempts_df[
        attempts_df["category"].eq("stock_index_price")
        & attempts_df["request_ok"].eq(True)
        & rows_numeric(attempts_df["rows"]).gt(0)
    ].copy()

    stock_live_candidate = bool(
        len(stock_candidates) > 0
        and (
            stock_candidates["has_price"].any()
            or stock_candidates["has_bid"].any()
            or stock_candidates["has_ask"].any()
        )
    )

    exp_list_candidates = attempts_df[
        attempts_df["category"].eq("option_expiration_list")
        & attempts_df["request_ok"].eq(True)
        & (
            rows_numeric(attempts_df["rows"]).gt(0)
            | attempts_df["text_sample"].fillna("").astype(str).str.contains(r"20\d{6}", regex=True)
        )
    ].copy()

    expiration_list_found = bool(len(exp_list_candidates) > 0)

    option_candidates = attempts_df[
        attempts_df["category"].isin(["option_quote_snapshot", "option_root_snapshot_large"])
        & attempts_df["request_ok"].eq(True)
        & rows_numeric(attempts_df["rows"]).gt(20)
    ].copy()

    option_quote_fields_ok = bool(
        len(option_candidates) > 0
        and option_candidates["has_strike"].any()
        and option_candidates["has_bid"].any()
        and option_candidates["has_ask"].any()
    )

    live_probe_pass = bool(stock_live_candidate and option_quote_fields_ok)

    attempts_path = audit_dir / f"intraday_thetadata_live_probe_attempts_{run_ts}.csv"
    attempts_df.to_csv(attempts_path, index=False)

    successes_path = audit_dir / f"intraday_thetadata_live_probe_successes_{run_ts}.csv"
    successes = attempts_df[
        attempts_df["request_ok"].eq(True)
        & (rows_numeric(attempts_df["rows"]).gt(0) | attempts_df["text_sample"].fillna("").astype(str).str.len().gt(0))
    ].copy()
    successes.to_csv(successes_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "trade_date": trade_date_str,
        "api_bases": api_bases,
        "stock_root": args.stock_root,
        "index_root": args.index_root,
        "option_roots": option_roots,
        "source_scan_csv": str(source_scan_path),
        "attempts_csv": str(attempts_path),
        "successes_csv": str(successes_path),
        "terminal_reachable": terminal_reachable,
        "stock_live_candidate": stock_live_candidate,
        "expiration_list_found": expiration_list_found,
        "option_quote_fields_ok": option_quote_fields_ok,
        "live_probe_pass": live_probe_pass,
        "expiration_candidates_by_root": expiration_candidates_by_root,
        "method_note": "Read-only ThetaData live/current timestamp probe. No production signal files modified.",
    }

    manifest_path = audit_dir / f"intraday_thetadata_live_probe_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print("Terminal reachable candidate:", terminal_reachable)
    print("Stock/index live price candidate:", stock_live_candidate)
    print("Option expiration list found:", expiration_list_found)
    print("Option quote snapshot fields ok:", option_quote_fields_ok)
    print("THETADATA_LIVE_PROBE_PASS:", live_probe_pass)

    print("\nTop successful stock/index candidates:")
    if stock_candidates.empty:
        print("None")
    else:
        print(stock_candidates[["base_url", "name", "status_code", "rows", "cols", "columns", "sample_path"]].head(20).to_string(index=False))

    print("\nTop successful option candidates:")
    if option_candidates.empty:
        print("None")
    else:
        print(option_candidates[["base_url", "name", "status_code", "rows", "cols", "columns", "sample_path"]].head(20).to_string(index=False))

    banner("Saved audit outputs")
    print(f"source_scan: {source_scan_path}")
    print(f"attempts:    {attempts_path}")
    print(f"successes:   {successes_path}")
    print(f"manifest:    {manifest_path}")

    banner("Final result")
    print(f"THETADATA_LIVE_PROBE_PASS: {live_probe_pass}")
    print("DONE — read-only ThetaData live probe complete.")


if __name__ == "__main__":
    main()
