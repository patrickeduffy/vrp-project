
from __future__ import annotations

import argparse
import io
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def yyyymmdd_to_dash(x: str) -> str:
    ts = pd.to_datetime(str(x), format="%Y%m%d")
    return ts.strftime("%Y-%m-%d")


def parse_response(resp: requests.Response) -> pd.DataFrame:
    txt = resp.text or ""

    try:
        obj = resp.json()

        if isinstance(obj, list):
            return pd.DataFrame(obj)

        if isinstance(obj, dict):
            # ThetaData v3 often returns a dict-of-list payload:
            # {"close": [...], "timestamp": [...], ...}
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
                                # Uneven list length; preserve as object fallback.
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
                        return parse_dict_payload(val)

            return pd.DataFrame([obj])

    except Exception:
        pass

    try:
        return pd.read_csv(io.StringIO(txt))
    except Exception:
        return pd.DataFrame({"raw_text": [txt[:2000]]})


def parse_dict_payload(obj: dict) -> pd.DataFrame:
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

    return pd.DataFrame([obj])


def request_df(url: str, params: dict, timeout: int = 180) -> tuple[dict, pd.DataFrame]:
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
        resp = requests.get(url, params=params, timeout=timeout)
        meta["full_url"] = resp.url
        meta["status_code"] = int(resp.status_code)
        meta["ok"] = bool(resp.ok)
        meta["text_head"] = (resp.text or "")[:1000].replace("\n", " ")

        df = parse_response(resp)
        meta["rows"] = int(len(df))
        meta["cols"] = int(len(df.columns))
        meta["columns"] = "|".join(map(str, df.columns))
        return meta, df

    except Exception as exc:
        meta["error"] = repr(exc)
        return meta, pd.DataFrame()


def normalize_chain(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if out.empty:
        return out

    if "symbol" in out.columns and "root" not in out.columns:
        out["root"] = out["symbol"]

    if "expiration" in out.columns:
        out["expiration_norm"] = pd.to_datetime(out["expiration"], errors="coerce").dt.strftime("%Y%m%d")

    if "right" in out.columns:
        out["right_norm"] = out["right"].astype(str).str.upper().replace({"CALL": "C", "PUT": "P"})

    for c in ["strike", "bid", "ask"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "bid" in out.columns and "ask" in out.columns:
        out["mid"] = (out["bid"] + out["ask"]) / 2.0

    return out


def chain_quality(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "has_required_chain_fields": False,
            "call_rows": 0,
            "put_rows": 0,
            "unique_strikes": 0,
            "positive_bid_rows": 0,
        }

    cols = set(df.columns)
    required = {"strike", "bid", "ask"}

    right_col = "right_norm" if "right_norm" in cols else "right" if "right" in cols else None

    has_required = required.issubset(cols) and right_col is not None

    if right_col is not None:
        rights = df[right_col].astype(str).str.upper()
        call_rows = int(rights.isin(["C", "CALL"]).sum())
        put_rows = int(rights.isin(["P", "PUT"]).sum())
    else:
        call_rows = 0
        put_rows = 0

    return {
        "has_required_chain_fields": bool(has_required),
        "call_rows": call_rows,
        "put_rows": put_rows,
        "unique_strikes": int(df["strike"].nunique()) if "strike" in cols else 0,
        "positive_bid_rows": int((pd.to_numeric(df["bid"], errors="coerce") > 0).sum()) if "bid" in cols else 0,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--base-url", default="http://127.0.0.1:25503/v3")
    p.add_argument("--trade-date", default=datetime.now().strftime("%Y%m%d"))
    p.add_argument("--quote-time", default="14:30:00.000")
    p.add_argument("--stock-symbol", default="SPY")
    p.add_argument("--option-symbols", default="SPXW,SPX")
    p.add_argument("--expirations", default="", help="Comma-separated YYYYMMDD. If omitted, expiration list is used.")
    args = p.parse_args()

    project_root = Path(args.project_root)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    audit_dir = project_root / "data" / "audit" / "intraday_live_probe"
    audit_dir.mkdir(parents=True, exist_ok=True)

    base = args.base_url.rstrip("/")
    trade_date_dash = yyyymmdd_to_dash(args.trade_date)
    option_symbols = [x.strip().upper() for x in args.option_symbols.split(",") if x.strip()]

    banner("VRP ThetaData working endpoint probe v1")
    print("Project root:", project_root)
    print("Run timestamp:", run_ts)
    print("Base URL:", base)
    print("Trade date:", args.trade_date, trade_date_dash)
    print("Quote time:", args.quote_time)
    print("Audit dir:", audit_dir)

    rows = []
    sample_paths = []

    # 1. Confirm known-working stock EOD endpoint with symbol param.
    banner("Probe stock EOD endpoint with working symbol= parameter")
    stock_eod_url = base + "/stock/history/eod"
    stock_eod_params = {
        "symbol": args.stock_symbol,
        "start_date": args.trade_date,
        "end_date": args.trade_date,
        "format": "json",
    }
    meta, df = request_df(stock_eod_url, stock_eod_params, timeout=60)
    meta["probe"] = "stock_history_eod_symbol_param"
    rows.append(meta)
    if not df.empty:
        sample = audit_dir / f"sample_stock_history_eod_symbol_param_{run_ts}.csv"
        df.head(100).to_csv(sample, index=False)
        sample_paths.append(sample)
    print(json.dumps(meta, indent=2))

    # 2. Try stock OHLC intraday endpoint with symbol param, because earlier probe used root=.
    banner("Probe stock OHLC intraday endpoint with symbol= parameter")
    stock_ohlc_url = base + "/stock/history/ohlc"
    stock_ohlc_param_sets = [
        {
            "symbol": args.stock_symbol,
            "start_date": args.trade_date,
            "end_date": args.trade_date,
            "interval": "1m",
            "format": "json",
        },
        {
            "symbol": args.stock_symbol,
            "start_date": args.trade_date,
            "end_date": args.trade_date,
            "interval": "5m",
            "format": "json",
        },
        {
            "symbol": args.stock_symbol,
            "start_date": args.trade_date,
            "end_date": args.trade_date,
            "interval": 60000,
            "format": "json",
        },
        {
            "symbol": args.stock_symbol,
            "start_date": args.trade_date,
            "end_date": args.trade_date,
            "ivl": 60000,
            "format": "json",
        },
    ]

    for i, params in enumerate(stock_ohlc_param_sets, start=1):
        meta, df = request_df(stock_ohlc_url, params, timeout=60)
        meta["probe"] = f"stock_history_ohlc_symbol_param_attempt_{i}"
        rows.append(meta)
        if meta["ok"] and not df.empty:
            sample = audit_dir / f"sample_stock_history_ohlc_attempt_{i}_{run_ts}.csv"
            df.head(100).to_csv(sample, index=False)
            sample_paths.append(sample)
        print(json.dumps(meta, indent=2))

    # 3. Expiration list using exact legacy route/param.
    banner("Probe option expiration list with symbol= parameter")
    expirations_by_symbol = {}

    for sym in option_symbols:
        url = base + "/option/list/expirations"
        params = {"symbol": sym}
        meta, df = request_df(url, params, timeout=60)
        meta["probe"] = f"option_list_expirations_{sym}"
        rows.append(meta)

        if not df.empty:
            sample = audit_dir / f"sample_option_list_expirations_{sym}_{run_ts}.csv"
            df.head(100).to_csv(sample, index=False)
            sample_paths.append(sample)

        exps = []
        if "expiration" in df.columns:
            exps = (
                pd.to_datetime(df["expiration"], errors="coerce")
                .dropna()
                .dt.strftime("%Y%m%d")
                .astype(int)
                .tolist()
            )

        expirations_by_symbol[sym] = sorted(set(exps))
        print(sym, json.dumps(meta, indent=2))
        print("Expiration count:", len(expirations_by_symbol[sym]))
        print("First/last:", (expirations_by_symbol[sym][:3], expirations_by_symbol[sym][-3:] if expirations_by_symbol[sym] else []))

    # 4. Pick expirations near target tenors or use explicit.
    if args.expirations.strip():
        selected_exps = [int(x.strip()) for x in args.expirations.split(",") if x.strip()]
    else:
        trade_ts = pd.to_datetime(args.trade_date, format="%Y%m%d")
        selected_exps = []
        for sym, exps in expirations_by_symbol.items():
            for exp in exps:
                exp_ts = pd.to_datetime(str(exp), format="%Y%m%d")
                dte = int((exp_ts - trade_ts).days)
                if 5 <= dte <= 45:
                    selected_exps.append(exp)
        selected_exps = sorted(set(selected_exps))[:8]

    print("\nSelected expirations for chain probes:", selected_exps)

    # 5. Probe actual working option history quote endpoint.
    banner("Probe option/history/quote with exact legacy params")
    chain_success_rows = []

    for sym in option_symbols:
        sym_exps = set(expirations_by_symbol.get(sym, []))

        for exp in selected_exps:
            # Only test symbol if expiration exists there, unless list failed.
            if sym_exps and exp not in sym_exps:
                continue

            url = base + "/option/history/quote"
            params = {
                "symbol": sym,
                "expiration": yyyymmdd_to_dash(str(exp)),
                "strike": "*",
                "right": "both",
                "start_date": trade_date_dash,
                "end_date": trade_date_dash,
                "start_time": args.quote_time,
                "end_time": args.quote_time,
                "interval": "1m",
                "format": "json",
            }

            meta, df = request_df(url, params, timeout=180)
            meta["probe"] = f"option_history_quote_{sym}_{exp}"
            chain = normalize_chain(df)
            quality = chain_quality(chain)
            meta.update(quality)
            rows.append(meta)

            if meta["ok"] and not chain.empty:
                sample = audit_dir / f"sample_option_history_quote_{sym}_{exp}_{run_ts}.csv"
                chain.head(300).to_csv(sample, index=False)
                sample_paths.append(sample)
                meta["sample_path"] = str(sample)

            print(sym, exp, json.dumps({k: meta[k] for k in ["status_code", "ok", "rows", "cols", "columns", "has_required_chain_fields", "call_rows", "put_rows", "unique_strikes", "positive_bid_rows", "error", "text_head"]}, indent=2))

            if meta.get("ok") and quality["has_required_chain_fields"] and quality["call_rows"] > 0 and quality["put_rows"] > 0:
                chain_success_rows.append(meta)

    attempts = pd.DataFrame(rows)
    attempts_path = audit_dir / f"intraday_thetadata_working_endpoint_probe_attempts_{run_ts}.csv"
    attempts.to_csv(attempts_path, index=False)

    chain_ok = len(chain_success_rows) > 0
    stock_eod_ok = bool(
        len(attempts[(attempts["probe"] == "stock_history_eod_symbol_param") & (attempts["ok"] == True) & (attempts["rows"] > 0)]) > 0
    )
    stock_ohlc_ok = bool(
        len(attempts[attempts["probe"].astype(str).str.startswith("stock_history_ohlc") & (attempts["ok"] == True) & (attempts["rows"] > 0)]) > 0
    )
    expiration_ok = bool(
        len(attempts[attempts["probe"].astype(str).str.startswith("option_list_expirations") & (attempts["ok"] == True) & (attempts["rows"] > 0)]) > 0
    )

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "base_url": base,
        "trade_date": args.trade_date,
        "quote_time": args.quote_time,
        "stock_symbol": args.stock_symbol,
        "option_symbols": option_symbols,
        "selected_expirations": selected_exps,
        "attempts_csv": str(attempts_path),
        "sample_paths": [str(p) for p in sample_paths],
        "stock_eod_ok": stock_eod_ok,
        "stock_ohlc_intraday_ok": stock_ohlc_ok,
        "expiration_list_ok": expiration_ok,
        "option_history_quote_chain_ok": chain_ok,
        "THETADATA_WORKING_ENDPOINT_PROBE_PASS": bool(chain_ok and expiration_ok),
        "method_note": "Read-only probe of actual known-working ThetaData v3 endpoint patterns from legacy VIX notebook.",
    }

    manifest_path = audit_dir / f"intraday_thetadata_working_endpoint_probe_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    banner("Final assessment")
    print("stock_eod_ok:", stock_eod_ok)
    print("stock_ohlc_intraday_ok:", stock_ohlc_ok)
    print("expiration_list_ok:", expiration_ok)
    print("option_history_quote_chain_ok:", chain_ok)
    print("THETADATA_WORKING_ENDPOINT_PROBE_PASS:", bool(chain_ok and expiration_ok))
    print()
    print("attempts:", attempts_path)
    print("manifest:", manifest_path)
    print("DONE — working endpoint probe complete.")


if __name__ == "__main__":
    main()
