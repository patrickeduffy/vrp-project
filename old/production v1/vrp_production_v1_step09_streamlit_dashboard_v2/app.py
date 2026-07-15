from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st


MODEL_LABEL = "locked_2621_win_band_25bps_conditional"
TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
WIN_BAND_BPS = 25
WIN_BAND_DECIMAL = WIN_BAND_BPS / 10000.0

PANEL_FILES = {
    "Term structure": "data/processed/vix_term_structure_history_v0_7_1_repaired_total_variance.parquet",
    "Realized variance": "data/processed/realized_variance_panel_v0_1.parquet",
    "VRP panel": "data/processed/vrp_panel_v0_1.parquet",
    "Feature panel": "data/processed/production_feature_panel_v0_1.parquet",
}

SCRIPT_HINTS = {
    "step08": ("run_step08_daily_pipeline.py", ["step08_daily_pipeline_v2", "step08_daily_pipeline"]),
    "step07": ("run_step07_signal_snapshot.py", ["step07_signal_snapshot"]),
    "step01": ("run_step01_inventory.py", ["datefix", "starter"]),
}

CORE_THRESHOLDS = {
    "front": {"vrp_log": 0.60, "vrp_z_3m": 0.55, "vrp_z_1y": 0.65, "rsi14_cap": 70.0, "rv21d_floor": 8.5},
    "middle": {"vrp_log": 0.65, "vrp_z_3m": 0.75, "vrp_z_1y": 0.65, "rsi14_cap": 68.0, "rv21d_floor": 8.5},
    "back": {"vrp_log": 0.70, "vrp_z_3m": 0.75, "vrp_z_1y": 0.75, "rsi14_cap": 66.0, "rv21d_floor": 8.5},
}

SECONDARY_THRESHOLDS = {
    "front": {"vrp_log": 0.60, "vrp_z_3m": 0.50, "vrp_z_1y": 0.40, "rsi14_cap": 74.0, "rv21d_floor": 6.5},
    "middle": {"vrp_log": 0.60, "vrp_z_3m": 0.50, "vrp_z_1y": 0.50, "rsi14_cap": 70.0, "rv21d_floor": 6.5},
    "back": {"vrp_log": 0.70, "vrp_z_3m": 0.50, "vrp_z_1y": 0.50, "rsi14_cap": 68.0, "rv21d_floor": 6.5},
}

SIZING = {
    ("Core", "back"): 0.0500,
    ("Core", "middle"): 0.0485,
    ("Core", "front"): 0.0175,
    ("Secondary", "back"): 0.0450,
    ("Secondary", "middle"): 0.0325,
    ("Secondary", "front"): 0.0175,
}

# Fallback only. If the exact locked metrics file exists under data/audit, the app uses that instead.
EMBEDDED_GROUP_METRICS = {
    ("Core", "back"): {"conditional_win_probability": 0.8884, "conditional_avg_pnl_per_day": 470.13, "aggregate_pnl_per_day": 470.13},
    ("Core", "middle"): {"conditional_win_probability": 0.8615, "conditional_avg_pnl_per_day": 495.01, "aggregate_pnl_per_day": 495.01},
    ("Core", "front"): {"conditional_win_probability": 0.7938, "conditional_avg_pnl_per_day": 436.10, "aggregate_pnl_per_day": 436.10},
    ("Secondary", "back"): {"conditional_win_probability": 0.7805, "conditional_avg_pnl_per_day": 197.15, "aggregate_pnl_per_day": 197.15},
    ("Secondary", "middle"): {"conditional_win_probability": 0.8000, "conditional_avg_pnl_per_day": 239.42, "aggregate_pnl_per_day": 239.42},
    ("Secondary", "front"): {"conditional_win_probability": 0.7677, "conditional_avg_pnl_per_day": 328.23, "aggregate_pnl_per_day": 328.23},
}

LONG_TABLE_DEFAULT_COLS = [
    "date", "trade_date", "spx_close", "spx_log_return", "rate_pct", "tenor", "tenor_group",
    "vix_style_vol", "implied_variance", "forecast_vol", "forecast_variance",
    "trailing_realized_vol", "trailing_realized_variance", "vrp_log", "vrp_z_3m", "vrp_z_1y",
    "rv21d", "rsi14", "core_pass", "secondary_pass", "selected", "selected_layer", "risk_fraction",
    "portfolio_status", "is_repaired", "near_expiration", "next_expiration", "quote_time_used",
]

DAILY_TABLE_COLS = [
    "date", "trade_date", "spx_close", "spx_log_return", "rate_pct", "rv21d", "rsi14",
    "selected_trade_flag", "selected_layer", "selected_tenor", "selected_bucket", "risk_fraction",
    "portfolio_status", "core_candidates", "secondary_candidates",
]

CURVE_METRIC_CHOICES = {
    "VIX-style vol": "vix_style_vol",
    "Forecast vol": "forecast_vol",
    "Trailing realized vol": "trailing_realized_vol",
    "VRP log": "vrp_log",
    "3m VRP z-score": "vrp_z_3m",
    "1y VRP z-score": "vrp_z_1y",
    "Implied variance": "implied_variance",
    "Forecast variance": "forecast_variance",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    known, _ = parser.parse_known_args()
    return known


def normalize_project_root(value: str) -> Path:
    return Path(value).expanduser().resolve()


def yyyymmdd_to_iso(x: Any) -> Optional[str]:
    if x is None or pd.isna(x):
        return None
    try:
        s = str(int(x))
        if len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    except Exception:
        pass
    try:
        return str(pd.to_datetime(x).date())
    except Exception:
        return str(x)


def iso_to_yyyymmdd(s: str) -> str:
    return s.replace("-", "")


def normalize_bucket(value: Any, tenor: Optional[int] = None) -> str:
    if pd.notna(value):
        s = str(value).strip().lower()
        if s in {"front", "middle", "back"}:
            return s
    if tenor is None or pd.isna(tenor):
        raise ValueError(f"Cannot determine tenor group from value={value!r}, tenor={tenor!r}")
    t = int(tenor)
    if t in (9, 12, 15):
        return "front"
    if t in (18, 21, 24):
        return "middle"
    if t in (27, 30, 33):
        return "back"
    raise ValueError(f"Unsupported tenor for locked 2621 universe: {t}")


def parse_pct_or_float(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        cleaned = s.astype(str).str.replace("%", "", regex=False).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip()
        out = pd.to_numeric(cleaned, errors="coerce")
        if out.dropna().gt(1.0).any() and out.dropna().le(100).all():
            out = out / 100.0
        return out
    out = pd.to_numeric(s, errors="coerce")
    if out.dropna().gt(1.0).any() and out.dropna().le(100).all():
        out = out / 100.0
    return out


def find_col(columns: List[str], options: List[str]) -> Optional[str]:
    lower_map = {c.lower().strip(): c for c in columns}
    for opt in options:
        if opt.lower() in lower_map:
            return lower_map[opt.lower()]
    for c in columns:
        cl = c.lower().strip()
        for opt in options:
            if opt.lower() in cl:
                return c
    return None


@st.cache_data(show_spinner=False)
def read_parquet_cached(path_str: str, mtime: float) -> pd.DataFrame:
    # mtime is included only to invalidate cache when the local file changes.
    return pd.read_parquet(path_str)


def read_panel(project_root: Path, rel_path: str) -> pd.DataFrame:
    p = project_root / rel_path
    return read_parquet_cached(str(p), p.stat().st_mtime)


def read_panel_info(project_root: Path, rel_path: str) -> Dict[str, Any]:
    p = project_root / rel_path
    info = {"path": str(p), "exists": p.exists(), "rows": None, "latest": None, "error": None}
    if not p.exists():
        return info
    try:
        df = read_panel(project_root, rel_path)
        info["rows"] = int(len(df))
        if "trade_date" in df.columns and len(df):
            info["latest"] = yyyymmdd_to_iso(pd.to_numeric(df["trade_date"], errors="coerce").max())
        elif "date" in df.columns and len(df):
            info["latest"] = yyyymmdd_to_iso(df["date"].max())
    except Exception as e:
        info["error"] = str(e)
    return info


def load_feature_panel(project_root: Path) -> pd.DataFrame:
    df = read_panel(project_root, PANEL_FILES["Feature panel"]).copy()
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    else:
        df["date"] = pd.to_datetime(df["date"])
    df["trade_date"] = pd.to_numeric(df["trade_date"], errors="coerce").astype("Int64")
    df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")
    if "tenor_group" not in df.columns:
        df["tenor_group"] = [normalize_bucket(None, int(t)) for t in df["tenor"]]
    else:
        df["tenor_group"] = [normalize_bucket(g, int(t)) for g, t in zip(df["tenor_group"], df["tenor"])]
    return df.sort_values(["date", "tenor"]).reset_index(drop=True)


def check_threshold_fast(row: pd.Series, thresholds: Dict[str, float]) -> Tuple[bool, str]:
    failed: List[str] = []
    try:
        if float(row["vrp_log"]) < thresholds["vrp_log"]:
            failed.append(f"vrp_log {row['vrp_log']:.4f} < {thresholds['vrp_log']:.2f}")
        if float(row["vrp_z_3m"]) < thresholds["vrp_z_3m"]:
            failed.append(f"vrp_z_3m {row['vrp_z_3m']:.4f} < {thresholds['vrp_z_3m']:.2f}")
        if float(row["vrp_z_1y"]) < thresholds["vrp_z_1y"]:
            failed.append(f"vrp_z_1y {row['vrp_z_1y']:.4f} < {thresholds['vrp_z_1y']:.2f}")
        if float(row["rsi14"]) > thresholds["rsi14_cap"]:
            failed.append(f"rsi14 {row['rsi14']:.2f} > {thresholds['rsi14_cap']:.1f}")
        if float(row["rv21d"]) < thresholds["rv21d_floor"]:
            failed.append(f"rv21d {row['rv21d']:.2f} < {thresholds['rv21d_floor']:.1f}")
    except Exception as e:
        failed.append(f"threshold eval error: {e}")
    return len(failed) == 0, "; ".join(failed) if failed else "PASS"


def load_ranking_metrics(project_root: Path) -> Tuple[pd.DataFrame, str, bool]:
    candidates: List[Path] = []
    audit_dir = project_root / "data" / "audit"
    processed_dir = project_root / "data" / "processed"
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*metrics*.csv")))
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*summary*.csv")))
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*tenor*.csv")))
    candidates.extend(sorted(processed_dir.glob("locked_2621_win_band_25bps_conditional*metrics*.parquet")))
    candidates.extend(sorted(processed_dir.glob("locked_2621_win_band_25bps_conditional*summary*.parquet")))

    for path in candidates:
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        cols = list(df.columns)
        layer_col = find_col(cols, ["layer", "selected_layer", "signal_layer", "tier"])
        group_col = find_col(cols, ["tenor_group", "group", "bucket", "sizing_group"])
        tenor_col = find_col(cols, ["tenor", "target_days", "entry_tenor"])
        win_col = find_col(cols, ["conditional_win_probability", "conditional_win_prob", "win_rate", "win probability"])
        avg_col = find_col(cols, ["conditional_avg_pnl_per_day", "avg_pnl_per_day", "average_pnl_day", "Avg P&L/day"])
        agg_col = find_col(cols, ["aggregate_pnl_per_day", "agg_pnl_per_day", "aggregate P&L/day", "avg_pnl_per_day"])
        if not (layer_col and (group_col or tenor_col) and win_col and avg_col):
            continue
        out = pd.DataFrame()
        out["layer"] = df[layer_col].astype(str).str.strip().str.capitalize()
        if tenor_col:
            out["tenor"] = pd.to_numeric(df[tenor_col], errors="coerce")
        else:
            out["tenor"] = np.nan
        if group_col:
            out["tenor_group"] = df[group_col].astype(str).str.lower().str.strip()
            out["tenor_group"] = out["tenor_group"].str.replace("core ", "", regex=False).str.replace("secondary ", "", regex=False)
        else:
            out["tenor_group"] = out["tenor"].apply(lambda x: normalize_bucket(None, int(x)) if pd.notna(x) else np.nan)
        out["conditional_win_probability"] = parse_pct_or_float(df[win_col])
        out["conditional_avg_pnl_per_day"] = pd.to_numeric(df[avg_col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
        if agg_col:
            out["aggregate_pnl_per_day"] = pd.to_numeric(df[agg_col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False), errors="coerce")
        else:
            out["aggregate_pnl_per_day"] = out["conditional_avg_pnl_per_day"]
        out = out[out["layer"].isin(["Core", "Secondary"])]
        out = out[out["tenor_group"].isin(["front", "middle", "back"])]
        if not out.empty and out["conditional_win_probability"].notna().any():
            return out, str(path), True

    rows = []
    for (layer, group), vals in EMBEDDED_GROUP_METRICS.items():
        rows.append({"layer": layer, "tenor": np.nan, "tenor_group": group, **vals})
    return pd.DataFrame(rows), "embedded_group_level_locked_report_fallback", False


def lookup_metric(metrics: pd.DataFrame, layer: str, tenor: int, group: str, col: str) -> float:
    by_tenor = metrics[(metrics["layer"] == layer) & (metrics["tenor"].notna()) & (metrics["tenor"].astype(float) == float(tenor))]
    if not by_tenor.empty and pd.notna(by_tenor.iloc[0].get(col)):
        return float(by_tenor.iloc[0][col])
    by_group = metrics[(metrics["layer"] == layer) & (metrics["tenor_group"] == group)]
    if not by_group.empty and pd.notna(by_group.iloc[0].get(col)):
        return float(by_group.iloc[0][col])
    return np.nan


@st.cache_data(show_spinner=False)
def build_signal_history_cached(feature_path: str, feature_mtime: float, metrics_path: str, metrics_exact: bool, metrics_records_json: str) -> Tuple[pd.DataFrame, pd.DataFrame, str, bool]:
    # Read feature from path inside cached function to avoid passing the full dataframe into the cache key.
    df = pd.read_parquet(feature_path).copy()
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    else:
        df["date"] = pd.to_datetime(df["date"])
    df["trade_date"] = pd.to_numeric(df["trade_date"], errors="coerce").astype("Int64")
    df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")
    df["tenor_group"] = [normalize_bucket(g if "tenor_group" in df.columns else None, int(t)) for g, t in zip(df.get("tenor_group", pd.Series([None]*len(df))), df["tenor"])]

    signal_cols = ["vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"]
    for c in signal_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    core_pass, secondary_pass, core_failed, secondary_failed = [], [], [], []
    for _, row in df.iterrows():
        group = row["tenor_group"]
        cp, cf = check_threshold_fast(row, CORE_THRESHOLDS[group])
        sp, sf = check_threshold_fast(row, SECONDARY_THRESHOLDS[group])
        core_pass.append(cp)
        secondary_pass.append(sp)
        core_failed.append(cf)
        secondary_failed.append(sf)
    df["core_pass"] = core_pass
    df["secondary_pass"] = secondary_pass
    df["core_failed_checks"] = core_failed
    df["secondary_failed_checks"] = secondary_failed
    df["selected"] = False
    df["selected_layer"] = None
    df["risk_fraction"] = np.nan
    df["portfolio_status"] = "NO_TRADE"

    metrics = pd.DataFrame(json.loads(metrics_records_json))
    daily_rows: List[Dict[str, Any]] = []
    selected_updates: List[Tuple[int, int, str, float]] = []

    for trade_date, day in df.groupby("trade_date", sort=True):
        day = day.sort_values("tenor")
        core = day[day["core_pass"]]
        secondary = day[day["secondary_pass"]]
        selected_trade = False
        sel_layer = None
        sel_tenor = None
        sel_bucket = None
        risk_fraction = np.nan
        active = pd.DataFrame()
        if not core.empty:
            sel_layer = "Core"
            active = core.copy()
        elif not secondary.empty:
            sel_layer = "Secondary"
            active = secondary.copy()

        if not active.empty:
            metric_rows = []
            for _, r in active.iterrows():
                t = int(r["tenor"])
                g = str(r["tenor_group"])
                metric_rows.append({
                    "index": r.name,
                    "tenor": t,
                    "tenor_group": g,
                    "conditional_win_probability": lookup_metric(metrics, sel_layer, t, g, "conditional_win_probability"),
                    "conditional_avg_pnl_per_day": lookup_metric(metrics, sel_layer, t, g, "conditional_avg_pnl_per_day"),
                    "aggregate_pnl_per_day": lookup_metric(metrics, sel_layer, t, g, "aggregate_pnl_per_day"),
                })
            m = pd.DataFrame(metric_rows)
            if m["conditional_win_probability"].notna().all():
                best_win = float(m["conditional_win_probability"].max())
                near = m[m["conditional_win_probability"] >= best_win - WIN_BAND_DECIMAL].copy()
                near = near.sort_values(
                    ["conditional_avg_pnl_per_day", "aggregate_pnl_per_day", "conditional_win_probability", "tenor"],
                    ascending=[False, False, False, False],
                )
                selected = near.iloc[0]
                selected_trade = True
                sel_tenor = int(selected["tenor"])
                sel_bucket = str(selected["tenor_group"])
                risk_fraction = SIZING[(sel_layer, sel_bucket)]
                selected_updates.append((int(trade_date), sel_tenor, sel_layer, risk_fraction))

        first = day.iloc[0]
        daily_rows.append({
            "date": first["date"].date(),
            "trade_date": int(trade_date),
            "spx_close": first.get("spx_close", np.nan),
            "spx_log_return": first.get("spx_log_return", np.nan),
            "rate_pct": first.get("rate_pct", np.nan),
            "rv21d": first.get("rv21d", np.nan),
            "rsi14": first.get("rsi14", np.nan),
            "selected_trade_flag": selected_trade,
            "selected_layer": sel_layer,
            "selected_tenor": sel_tenor,
            "selected_bucket": sel_bucket,
            "risk_fraction": None if pd.isna(risk_fraction) else float(risk_fraction),
            "portfolio_status": "MANUAL_REVIEW_REQUIRED" if selected_trade else "NO_TRADE",
            "core_candidates": core["tenor"].astype(int).tolist(),
            "secondary_candidates": secondary["tenor"].astype(int).tolist(),
        })

    for td, tenor, layer, risk in selected_updates:
        mask = (df["trade_date"].astype(int) == td) & (df["tenor"].astype(int) == tenor)
        df.loc[mask, "selected"] = True
        df.loc[mask, "selected_layer"] = layer
        df.loc[mask, "risk_fraction"] = risk
        df.loc[df["trade_date"].astype(int) == td, "portfolio_status"] = "MANUAL_REVIEW_REQUIRED"

    daily = pd.DataFrame(daily_rows).sort_values("date").reset_index(drop=True)
    df = df.sort_values(["date", "tenor"]).reset_index(drop=True)
    return df, daily, metrics_path, metrics_exact


def build_signal_history(project_root: Path) -> Tuple[pd.DataFrame, pd.DataFrame, str, bool]:
    feature_path = project_root / PANEL_FILES["Feature panel"]
    metrics, metrics_path, metrics_exact = load_ranking_metrics(project_root)
    metrics_records_json = json.dumps(metrics.to_dict(orient="records"), default=str)
    return build_signal_history_cached(str(feature_path), feature_path.stat().st_mtime, metrics_path, metrics_exact, metrics_records_json)


def latest_signal_summary(project_root: Path) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    signal_dir = project_root / "data" / "processed" / "signals"
    if not signal_dir.exists():
        return None, None
    candidates = sorted(signal_dir.glob("locked_2621_eod_signal_summary_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None, None
    p = candidates[0]
    try:
        return p, json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return p, None


def latest_signal_snapshot(project_root: Path) -> Tuple[Optional[Path], Optional[pd.DataFrame]]:
    signal_dir = project_root / "data" / "processed" / "signals"
    if not signal_dir.exists():
        return None, None
    candidates = sorted(signal_dir.glob("locked_2621_eod_signal_snapshot_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None, None
    p = candidates[0]
    try:
        return p, pd.read_csv(p)
    except Exception:
        return p, None


def score_script(step: str, p: Path) -> Tuple[int, str]:
    _, hints = SCRIPT_HINTS[step]
    s = str(p).lower()
    score = 0
    for i, hint in enumerate(hints):
        if hint.lower() in s:
            score += 100 - i
    return score, str(p)


def find_script(project_root: Path, step: str) -> Optional[Path]:
    script_name, _ = SCRIPT_HINTS[step]
    prod = project_root / "production v1"
    roots = [prod, project_root]
    found = []
    for root in roots:
        if root.exists():
            found.extend(root.rglob(script_name))
    unique = sorted(set(found), key=lambda x: str(x).lower())
    if not unique:
        return None
    unique.sort(key=lambda p: score_script(step, p), reverse=True)
    return unique[0]


def run_command(cmd: list[str], cwd: Optional[Path] = None) -> Tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode, proc.stdout


def status_badge(label: str, good: bool) -> str:
    color = "#137333" if good else "#b3261e"
    bg = "#e6f4ea" if good else "#fce8e6"
    return f"<span style='background:{bg}; color:{color}; padding:4px 10px; border-radius:999px; font-weight:600'>{label}</span>"


def render_health(project_root: Path) -> Dict[str, Dict[str, Any]]:
    infos = {name: read_panel_info(project_root, rel) for name, rel in PANEL_FILES.items()}
    latest_values = [v.get("latest") for v in infos.values() if v.get("exists") and not v.get("error")]
    all_same = len(set(latest_values)) == 1 if latest_values else False

    st.subheader("Data health")
    cols = st.columns(4)
    for col, (name, info) in zip(cols, infos.items()):
        with col:
            st.metric(name, info.get("latest") or "missing", f"{info.get('rows') or 0:,} rows" if info.get("rows") else None)
            if info.get("error"):
                st.error(info["error"])
            elif not info.get("exists"):
                st.warning("File missing")

    st.markdown(status_badge("All core panel dates match" if all_same else "Panel dates do not all match", all_same), unsafe_allow_html=True)
    return infos


def render_signal(project_root: Path) -> None:
    st.subheader("Latest locked 2621 signal")
    summary_path, summary = latest_signal_summary(project_root)
    snapshot_path, snapshot = latest_signal_snapshot(project_root)

    if summary_path is None:
        st.info("No signal summary found yet. Run the daily pipeline or signal snapshot.")
        return
    if summary is None:
        st.warning(f"Found summary but could not read it: {summary_path}")
        return

    c1, c2, c3, c4 = st.columns(4)
    trade_flag = bool(summary.get("selected_trade_flag"))
    portfolio_status = summary.get("portfolio_approval_status") or summary.get("portfolio_status") or "UNKNOWN"
    signal_date = summary.get("signal_date_iso") or summary.get("signal_date") or summary.get("date") or "UNKNOWN"

    c1.metric("Signal date", str(signal_date))
    c2.metric("Decision", "TRADE" if trade_flag else "NO TRADE")
    c3.metric("Layer / tenor", f"{summary.get('selected_layer') or '-'} / {summary.get('selected_tenor') or '-'}")
    c4.metric("Max risk", str(summary.get("max_risk_dollars") or "-"))

    good = portfolio_status in {"NO_TRADE", "MANUAL_REVIEW_REQUIRED"}
    st.markdown(status_badge(str(portfolio_status), good), unsafe_allow_html=True)

    if snapshot is not None:
        show_cols = [
            "tenor", "tenor_group", "vix_style_vol", "forecast_vol", "vrp_log", "vrp_z_3m", "vrp_z_1y",
            "rv21d", "rsi14", "core_pass", "secondary_pass", "selected", "selected_layer", "risk_fraction",
            "core_failed_checks", "secondary_failed_checks",
        ]
        cols = [c for c in show_cols if c in snapshot.columns]
        st.dataframe(snapshot[cols], use_container_width=True, hide_index=True)
        with st.expander("Signal output files"):
            st.write(f"Summary: `{summary_path}`")
            if snapshot_path:
                st.write(f"Snapshot: `{snapshot_path}`")


def render_data_explorer(project_root: Path) -> None:
    st.subheader("One-year input / calculation / signal table")
    st.caption("This is the audit table you asked for: SPX, SOFR/rate, calculated VIX-style vols, forecast vols, RSI, VRP z-scores, and locked signal flags.")

    try:
        long_df, daily_df, ranking_source, ranking_exact = build_signal_history(project_root)
    except Exception as e:
        st.error(f"Could not build data explorer table: {e}")
        return

    latest_date = pd.to_datetime(long_df["date"]).max().date()
    min_date = pd.to_datetime(long_df["date"]).min().date()
    default_start = max(min_date, latest_date - timedelta(days=365))

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    start = c1.date_input("Start date", value=default_start, min_value=min_date, max_value=latest_date)
    end = c2.date_input("End date", value=latest_date, min_value=min_date, max_value=latest_date)
    table_mode = c3.selectbox("Table", ["Long tenor audit table", "Daily signal summary"])
    signal_filter = c4.selectbox("Signal filter", ["All rows", "Selected rows only", "Trade dates only", "No-trade dates only"])

    if start > end:
        st.warning("Start date is after end date.")
        return

    long_f = long_df[(pd.to_datetime(long_df["date"]).dt.date >= start) & (pd.to_datetime(long_df["date"]).dt.date <= end)].copy()
    daily_f = daily_df[(pd.to_datetime(daily_df["date"]).dt.date >= start) & (pd.to_datetime(daily_df["date"]).dt.date <= end)].copy()

    if signal_filter == "Selected rows only":
        long_f = long_f[long_f["selected"]]
        daily_f = daily_f[daily_f["selected_trade_flag"]]
    elif signal_filter == "Trade dates only":
        trade_dates = set(daily_f.loc[daily_f["selected_trade_flag"], "trade_date"].astype(int))
        long_f = long_f[long_f["trade_date"].astype(int).isin(trade_dates)]
        daily_f = daily_f[daily_f["trade_date"].astype(int).isin(trade_dates)]
    elif signal_filter == "No-trade dates only":
        no_trade_dates = set(daily_f.loc[~daily_f["selected_trade_flag"], "trade_date"].astype(int))
        long_f = long_f[long_f["trade_date"].astype(int).isin(no_trade_dates)]
        daily_f = daily_f[daily_f["trade_date"].astype(int).isin(no_trade_dates)]

    st.info(f"Ranking metrics source: {ranking_source} | exact external file: {ranking_exact}")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Dates shown", f"{daily_f['trade_date'].nunique():,}")
    c6.metric("Long rows shown", f"{len(long_f):,}")
    c7.metric("Trade dates", f"{int(daily_f['selected_trade_flag'].sum()) if len(daily_f) else 0:,}")
    c8.metric("Latest date shown", str(end))

    if table_mode == "Long tenor audit table":
        available = list(long_f.columns)
        defaults = [c for c in LONG_TABLE_DEFAULT_COLS if c in available]
        selected_cols = st.multiselect("Columns to show", options=available, default=defaults)
        show = long_f[selected_cols].copy() if selected_cols else long_f.copy()
        st.dataframe(show, use_container_width=True, hide_index=True, height=520)
        csv_bytes = show.to_csv(index=False).encode("utf-8")
        st.download_button("Download shown long table as CSV", data=csv_bytes, file_name=f"vrp_long_audit_table_{start}_{end}.csv", mime="text/csv")
    else:
        available = list(daily_f.columns)
        defaults = [c for c in DAILY_TABLE_COLS if c in available]
        selected_cols = st.multiselect("Columns to show", options=available, default=defaults)
        show = daily_f[selected_cols].copy() if selected_cols else daily_f.copy()
        st.dataframe(show, use_container_width=True, hide_index=True, height=520)
        csv_bytes = show.to_csv(index=False).encode("utf-8")
        st.download_button("Download shown daily summary as CSV", data=csv_bytes, file_name=f"vrp_daily_signal_summary_{start}_{end}.csv", mime="text/csv")

    st.divider()
    st.subheader("Quick curves")
    curve_date = st.selectbox("Curve date", options=sorted(long_f["date"].dt.date.unique(), reverse=True), index=0 if len(long_f) else None)
    metric_label = st.selectbox("Metric", options=list(CURVE_METRIC_CHOICES.keys()), index=0)
    metric_col = CURVE_METRIC_CHOICES[metric_label]
    curve = long_f[pd.to_datetime(long_f["date"]).dt.date == curve_date][["tenor", metric_col]].dropna().sort_values("tenor")
    if not curve.empty:
        st.line_chart(curve.set_index("tenor"), use_container_width=True)
    else:
        st.info("No curve data for that date/metric.")


def render_pipeline_controls(project_root: Path) -> None:
    st.subheader("Run controls")
    step08 = find_script(project_root, "step08")
    step07 = find_script(project_root, "step07")
    step01 = find_script(project_root, "step01")

    with st.expander("Detected script paths", expanded=False):
        st.write(f"Step 01: `{step01}`")
        st.write(f"Step 07: `{step07}`")
        st.write(f"Step 08: `{step08}`")

    c1, c2, c3 = st.columns([1, 1, 1])
    as_of = c1.date_input("As-of date", value=date.today())
    nav = c2.number_input("NAV for max-risk dollars", min_value=0.0, value=1_000_000.0, step=10_000.0)
    force_downstream = c3.checkbox("Force downstream rebuild", value=False)

    run_daily = st.button("Run daily EOD pipeline", type="primary", use_container_width=True)
    if run_daily:
        if step08 is None:
            st.error("Could not find Step 08 script under the project folder.")
        else:
            cmd = [sys.executable, str(step08), "--project-root", str(project_root), "--as-of", str(as_of), "--nav", str(float(nav))]
            if force_downstream:
                cmd.append("--force-downstream")
            with st.spinner("Running daily pipeline. This can take several minutes if new ThetaData pulls are needed..."):
                rc, out = run_command(cmd, cwd=step08.parent)
            st.code(out, language="text")
            if rc == 0:
                st.success("Daily pipeline finished.")
                st.cache_data.clear()
            else:
                st.error(f"Daily pipeline failed with exit code {rc}.")

    run_signal = st.button("Run latest signal only", use_container_width=True)
    if run_signal:
        if step07 is None:
            st.error("Could not find Step 07 script under the project folder.")
        else:
            feature_info = read_panel_info(project_root, PANEL_FILES["Feature panel"])
            latest = feature_info.get("latest")
            if not latest:
                st.error("Could not determine latest feature-panel date.")
            else:
                signal_date = iso_to_yyyymmdd(latest)
                cmd = [sys.executable, str(step07), "--project-root", str(project_root), "--signal-date", signal_date, "--nav", str(float(nav))]
                with st.spinner("Running signal snapshot..."):
                    rc, out = run_command(cmd, cwd=step07.parent)
                st.code(out, language="text")
                if rc == 0:
                    st.success("Signal snapshot finished.")
                    st.cache_data.clear()
                else:
                    st.error(f"Signal snapshot failed with exit code {rc}.")

    run_inventory = st.button("Run inventory check", use_container_width=True)
    if run_inventory:
        if step01 is None:
            st.error("Could not find Step 01 inventory script under the project folder.")
        else:
            cmd = [sys.executable, str(step01), "--project-root", str(project_root), "--as-of", str(as_of)]
            with st.spinner("Running inventory check..."):
                rc, out = run_command(cmd, cwd=step01.parent)
            st.code(out, language="text")
            if rc == 0:
                st.success("Inventory check finished.")
                st.cache_data.clear()
            else:
                st.error(f"Inventory check failed with exit code {rc}.")


def render_latest_reports(project_root: Path) -> None:
    st.subheader("Recent audit reports")
    audit = project_root / "data" / "audit" / "production_v1"
    if not audit.exists():
        st.info("No production_v1 audit folder found yet.")
        return
    reports = sorted(audit.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:20]
    rows = []
    for p in reports:
        rows.append({"report": p.name, "modified": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"), "path": str(p)})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No Markdown reports found.")


def main() -> None:
    args = parse_args()
    st.set_page_config(page_title="VRP Production Control Center", layout="wide")
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.5rem; padding-bottom: 2rem;}
        h1 {font-size: 2.1rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("VRP Production Control Center")
    st.caption("Local dashboard for data health, one-year data audit tables, daily EOD pipeline runs, and locked 2621 signal review.")

    with st.sidebar:
        st.header("Settings")
        root_text = st.text_input("Project root", value=args.project_root)
        project_root = normalize_project_root(root_text)
        st.write(f"Using: `{project_root}`")
        if st.button("Refresh dashboard"):
            st.cache_data.clear()
            st.rerun()

    if not project_root.exists():
        st.error(f"Project root does not exist: {project_root}")
        return

    tab_health, tab_signal, tab_data, tab_run, tab_reports = st.tabs(["Health", "Signal", "Data Explorer", "Run", "Reports"])

    with tab_health:
        render_health(project_root)
        st.divider()
        st.markdown(
            "Use this page before trusting a signal. The core panel latest dates should match. "
            "The daily pipeline button will run the full audited update when new EOD dates are missing."
        )

    with tab_signal:
        render_signal(project_root)

    with tab_data:
        render_data_explorer(project_root)

    with tab_run:
        render_pipeline_controls(project_root)

    with tab_reports:
        render_latest_reports(project_root)


if __name__ == "__main__":
    main()
