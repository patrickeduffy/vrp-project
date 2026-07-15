from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import streamlit as st


MODEL_LABEL = "locked_2621_win_band_25bps_conditional"

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


def read_panel_info(project_root: Path, rel_path: str) -> Dict[str, Any]:
    p = project_root / rel_path
    info = {"path": str(p), "exists": p.exists(), "rows": None, "latest": None, "error": None}
    if not p.exists():
        return info
    try:
        df = pd.read_parquet(p)
        info["rows"] = int(len(df))
        if "trade_date" in df.columns and len(df):
            info["latest"] = yyyymmdd_to_iso(df["trade_date"].max())
        elif "date" in df.columns and len(df):
            info["latest"] = yyyymmdd_to_iso(df["date"].max())
    except Exception as e:
        info["error"] = str(e)
    return info


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

    st.markdown(
        status_badge("All core panel dates match" if all_same else "Panel dates do not all match", all_same),
        unsafe_allow_html=True,
    )
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
    signal_date = summary.get("signal_date") or summary.get("date") or "UNKNOWN"

    c1.metric("Signal date", str(signal_date))
    c2.metric("Decision", "TRADE" if trade_flag else "NO TRADE")
    c3.metric("Layer / tenor", f"{summary.get('selected_layer') or '-'} / {summary.get('selected_tenor') or '-'}")
    c4.metric("Max risk", str(summary.get("max_risk_dollars") or "-"))

    good = portfolio_status in {"NO_TRADE", "MANUAL_REVIEW_REQUIRED"}
    st.markdown(status_badge(str(portfolio_status), good), unsafe_allow_html=True)

    if snapshot is not None:
        show_cols = [
            "tenor",
            "tenor_group",
            "vix_style_vol",
            "forecast_vol",
            "vrp_log",
            "vrp_z_3m",
            "vrp_z_1y",
            "rv21d",
            "rsi14",
            "core_pass",
            "secondary_pass",
            "selected",
            "selected_layer",
            "risk_fraction",
            "core_failed_checks",
            "secondary_failed_checks",
        ]
        cols = [c for c in show_cols if c in snapshot.columns]
        st.dataframe(snapshot[cols], use_container_width=True, hide_index=True)
        with st.expander("Signal output files"):
            st.write(f"Summary: `{summary_path}`")
            if snapshot_path:
                st.write(f"Snapshot: `{snapshot_path}`")


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
            else:
                st.error(f"Inventory check failed with exit code {rc}.")


def render_latest_reports(project_root: Path) -> None:
    st.subheader("Recent audit reports")
    audit = project_root / "data" / "audit" / "production_v1"
    if not audit.exists():
        st.info("No production_v1 audit folder found yet.")
        return
    reports = sorted(audit.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:15]
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
    st.caption("Local dashboard for data health, daily EOD pipeline runs, and locked 2621 signal review.")

    with st.sidebar:
        st.header("Settings")
        root_text = st.text_input("Project root", value=args.project_root)
        project_root = normalize_project_root(root_text)
        st.write(f"Using: `{project_root}`")
        if st.button("Refresh dashboard"):
            st.rerun()

    if not project_root.exists():
        st.error(f"Project root does not exist: {project_root}")
        return

    tab_health, tab_signal, tab_run, tab_reports = st.tabs(["Health", "Signal", "Run", "Reports"])

    with tab_health:
        render_health(project_root)
        st.divider()
        st.markdown(
            "Use this page before trusting a signal. The core panel latest dates should match. "
            "The daily pipeline button will run the full audited update when new EOD dates are missing."
        )

    with tab_signal:
        render_signal(project_root)

    with tab_run:
        render_pipeline_controls(project_root)

    with tab_reports:
        render_latest_reports(project_root)


if __name__ == "__main__":
    main()
