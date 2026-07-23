from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

from vrp.orchestration.eod import terminate_process_tree
from vrp_hybrid_v2_common import DEFAULT_PROJECT_ROOT, load_json, load_runtime_config, resolve_path

SHADOW_FAILURE_EXIT_CODE = 3


st.set_page_config(
    page_title="VRP Hybrid v2 EOD",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
    div.stButton > button:first-child {height: 3.4rem; font-size: 1.05rem; font-weight: 700;}
    .signal-card {border: 1px solid rgba(128,128,128,.35); border-radius: 12px; padding: 1rem 1.2rem; margin-bottom: .8rem;}
    .signal-title {font-size: 1.65rem; font-weight: 750; margin-bottom: .25rem;}
    .muted {opacity: .72; font-size: .92rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


def read_parquet_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        st.error(f"Could not read {path}: {exc}")
        return pd.DataFrame()


def read_json_safe(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return load_json(path)
    except Exception as exc:
        st.error(f"Could not read {path}: {exc}")
        return {}


def run_pipeline(
    project_root: Path,
    runtime_path: Path,
    approved_nav: float,
    target_date: str | None,
    force_recalculate: bool,
    skip_upstream: bool,
    shadow_write: bool,
) -> tuple[int, str]:
    script = project_root / "scripts/run_eod.py"
    command = [
        sys.executable,
        "-u",
        str(script),
        "--project-root",
        str(project_root),
        "--runtime-config",
        str(runtime_path),
        "--approved-nav",
        str(approved_nav),
    ]
    if target_date:
        command += ["--target-date", target_date]
    if force_recalculate:
        command.append("--force-recalculate")
    if skip_upstream:
        command.append("--skip-upstream")
    if shadow_write:
        command.append("--shadow-write")

    progress_bar = st.progress(0, text="Starting production refresh…")
    status_line = st.empty()
    console = st.empty()
    lines: list[str] = []
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=project_root,
        creationflags=(
            subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        ),
        start_new_session=os.name != "nt",
    )
    assert process.stdout is not None
    try:
        for raw in process.stdout:
            line = raw.rstrip("\n")
            lines.append(line)
            if line.startswith("VRP_PROGRESS|"):
                parts = line.split("|", 3)
                if len(parts) == 4:
                    _, step, pct, message = parts
                    try:
                        progress_bar.progress(
                            max(0, min(100, int(pct))),
                            text=f"{step}: {message}",
                        )
                    except ValueError:
                        pass
                    status_line.info(f"{step}: {message}")
            elif line.startswith("VRP_SHADOW|"):
                parts = line.split("|", 2)
                if len(parts) == 3:
                    _, shadow_step, message = parts
                    progress_bar.progress(
                        99,
                        text=f"PostgreSQL shadow — {shadow_step.lower()}: {message}",
                    )
                    status_line.info(f"PostgreSQL shadow: {message}")
            console.code("\n".join(lines[-220:]), language="text")
        return_code = process.wait()
    except BaseException:
        terminate_process_tree(process)
        raise
    finally:
        process.stdout.close()
    output = "\n".join(lines)
    if return_code == 0:
        progress_bar.progress(100, text="Refresh completed and published.")
        if shadow_write:
            status_line.success(
                "PASS — latest EOD signal is published and its PostgreSQL "
                "shadow is recorded."
            )
        else:
            status_line.success("PASS — latest completed EOD signal is published.")
    elif return_code == SHADOW_FAILURE_EXIT_CODE:
        progress_bar.progress(
            100,
            text="File refresh published; PostgreSQL shadow recording failed.",
        )
        status_line.warning(
            "PUBLISHED — canonical files are healthy, but the non-authoritative "
            "PostgreSQL shadow needs attention."
        )
    else:
        progress_bar.progress(
            100,
            text="Refresh did not complete; prior canonical outputs remain in place.",
        )
        status_line.error(
            "REFRESH FAILED — review the run console and audit directory."
        )
    return return_code, output


def format_pct(value: Any, digits: int = 2) -> str:
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):.{digits}f}%"
    except Exception:
        return "—"


def format_num(value: Any, digits: int = 3) -> str:
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "—"


def environment_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def display_latest_signal(decisions: pd.DataFrame, snapshot: pd.DataFrame, approved_nav: float) -> None:
    st.subheader("Latest signal")
    if decisions.empty:
        st.warning("DATA NOT READY — no canonical decision history is available.")
        return
    date_col = "date" if "date" in decisions.columns else "trade_date"
    decisions = decisions.copy()
    decisions[date_col] = pd.to_datetime(decisions[date_col], errors="coerce")
    latest = decisions.sort_values(date_col).iloc[-1]
    status = str(latest.get("decision_status", "DATA_NOT_READY"))
    signal_date = pd.Timestamp(latest[date_col]).date() if pd.notna(latest[date_col]) else "—"
    if status == "TRADE":
        size = float(latest.get("size_pct_nav", 0.0))
        max_risk = approved_nav * size
        st.success(f"TRADE — {latest.get('layer')} {latest.get('bucket')} {int(latest.get('tenor'))}D")
        st.markdown(
            f"""
            <div class="signal-card">
              <div class="signal-title">{latest.get('layer')} {latest.get('bucket')} · {int(latest.get('tenor'))}D</div>
              <div><b>Signal date:</b> {signal_date} &nbsp; | &nbsp; <b>Locked size:</b> {size:.2%} of NAV &nbsp; | &nbsp; <b>Target max risk:</b> ${max_risk:,.0f}</div>
              <div class="muted">Completed-EOD model decision. Execution pricing and portfolio overlap approval are separate controls.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        cols = st.columns(7)
        metrics = [
            ("Implied vol", format_pct(latest.get("implied_vol_pct"))),
            ("Forecast vol", format_pct(latest.get("forecast_vol_pct"))),
            ("VRP log", format_num(latest.get("model_vrp_log"))),
            ("3M z", format_num(latest.get("z_3m"))),
            ("1Y z", format_num(latest.get("z_1y"))),
            ("RSI14", format_num(latest.get("rsi14"), 2)),
            ("RV21D", format_pct(latest.get("rv21d_vol_pct"))),
        ]
        for column, (label, value) in zip(cols, metrics):
            column.metric(label, value)
        threshold_cols = st.columns(5)
        threshold_metrics = [
            ("VRP >", format_num(latest.get("threshold_vrp"))),
            ("3M z >", format_num(latest.get("threshold_z3"))),
            ("1Y z >", format_num(latest.get("threshold_z1"))),
            ("RSI <", format_num(latest.get("threshold_rsi"), 2)),
            ("RV21D >", format_pct(latest.get("threshold_rv"))),
        ]
        for column, (label, value) in zip(threshold_cols, threshold_metrics):
            column.metric(label, value)
        st.caption(str(latest.get(
            "selection_reason",
            "Selection: highest locked size → Core on size tie → locked sleeve quality → research win rate → research worst-1% tail → longer DTE.",
        )))
    elif status == "NO_TRADE":
        st.info(f"NO TRADE — no active sleeve qualified on {signal_date}.")
    else:
        st.warning(f"{status} — latest decision is not ready for trading.")


def display_tenor_table(snapshot: pd.DataFrame) -> None:
    st.subheader("Latest tenor decision table")
    if snapshot.empty:
        st.info("Latest snapshot is not available.")
        return
    frame = snapshot.copy()
    date_col = "date" if "date" in frame.columns else "trade_date"
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    latest_date = frame[date_col].max()
    frame = frame.loc[frame[date_col].eq(latest_date)].sort_values("tenor")
    columns = [
        "tenor", "implied_vol_pct", "forecast_vol_pct", "model_vrp_log", "z_3m", "z_1y",
        "rsi14", "rv21d_vol_pct", "core_pass", "secondary_pass",
        "core_size_pct_nav", "secondary_size_pct_nav",
        "core_failure_reason", "secondary_failure_reason", "selected_trade", "selected_layer",
    ]
    columns = [c for c in columns if c in frame.columns]
    st.dataframe(frame[columns], use_container_width=True, hide_index=True)


def display_charts(history: pd.DataFrame) -> None:
    st.subheader("Term structures")
    if history.empty:
        st.info("Signal history is not available.")
        return
    frame = history.copy()
    date_col = "date" if "date" in frame.columns else "trade_date"
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    dates = sorted(frame[date_col].dropna().unique())
    if not dates:
        return
    selected_dates = [dates[-1]]
    if len(dates) >= 2:
        selected_dates.append(dates[-2])
    if len(dates) >= 6:
        selected_dates.append(dates[-6])
    selected_dates = list(dict.fromkeys(selected_dates))
    labels = {
        selected_dates[0]: "Latest",
        selected_dates[1] if len(selected_dates) > 1 else selected_dates[0]: "Prior session",
        selected_dates[2] if len(selected_dates) > 2 else selected_dates[0]: "Five sessions ago",
    }

    col1, col2 = st.columns(2)
    iv = frame.loc[frame[date_col].isin(selected_dates), [date_col, "tenor", "implied_vol_pct"]].copy()
    iv["series"] = iv[date_col].map(labels)
    iv_pivot = iv.pivot(index="tenor", columns="series", values="implied_vol_pct").sort_index()
    col1.caption("VIX-style volatility across tenors")
    col1.line_chart(iv_pivot, x_label="DTE", y_label="Volatility (%)")

    latest = frame.loc[frame[date_col].eq(dates[-1])].sort_values("tenor")
    fcols = [c for c in ["forecast_vol_pct", "rv21d_vol_pct"] if c in latest.columns]
    col2.caption("Forecast volatility and RV21D")
    col2.line_chart(latest.set_index("tenor")[fcols], x_label="DTE", y_label="Volatility (%)")

    st.caption("VRP log across tenors")
    st.line_chart(latest.set_index("tenor")[["model_vrp_log"]], x_label="DTE", y_label="Log variance premium")


def display_data_health(status: dict[str, Any]) -> None:
    st.subheader("Data health")
    if not status:
        st.info("No published data-health report is available.")
        return
    health = status.get("data_health", status)
    overall = health.get("overall_status", status.get("status", "UNKNOWN"))
    if overall == "PASS":
        st.success(f"PASS — data complete through {health.get('target_date', status.get('target_date', '—'))}")
    else:
        st.error(f"{overall} — inspect the component table and latest audit run.")
    components = pd.DataFrame(health.get("components", []))
    if not components.empty:
        columns = [
            "component", "status", "latest_date", "row_count", "missing_date_count",
            "interior_missing_count", "missing_tenor_cells", "detail",
        ]
        st.dataframe(components[[c for c in columns if c in components.columns]], use_container_width=True, hide_index=True)
    audit_dir = status.get("audit_dir")
    if audit_dir:
        st.caption(f"Latest audit directory: {audit_dir}")


def display_history(history: pd.DataFrame) -> None:
    st.subheader("Historical signal data")
    if history.empty:
        st.info("No signal history is available.")
        return
    frame = history.copy()
    date_col = "date" if "date" in frame.columns else "trade_date"
    frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
    latest = frame[date_col].max()
    default_start = latest - pd.Timedelta(days=365)
    c1, c2 = st.columns([1, 1])
    start = c1.date_input("History start", value=default_start.date(), max_value=latest.date())
    tenor_options = sorted(frame["tenor"].dropna().astype(int).unique())
    chosen_tenors = c2.multiselect("Tenors", tenor_options, default=tenor_options)
    filtered = frame.loc[
        frame[date_col].ge(pd.Timestamp(start)) & frame["tenor"].isin(chosen_tenors)
    ].sort_values([date_col, "tenor"], ascending=[False, True])
    preferred = [
        date_col, "tenor", "implied_vol_pct", "forecast_vol_pct", "model_vrp_log", "z_3m", "z_1y",
        "rsi14", "rv21d_vol_pct", "rsi_formula_version",
        "core_pass", "secondary_pass", "core_size_pct_nav", "secondary_size_pct_nav",
        "selected_trade", "selected_layer",
    ]
    columns = [c for c in preferred if c in filtered.columns]
    st.dataframe(filtered[columns], use_container_width=True, hide_index=True, height=520)
    st.download_button(
        "Download filtered history CSV",
        data=filtered[columns].to_csv(index=False).encode("utf-8"),
        file_name="vrp_hybrid_v2_eod_history.csv",
        mime="text/csv",
    )


st.title("VRP Hybrid v2 — EOD Production Dashboard")
st.caption("Locked signal, per-trade sizing, and one-trade-per-day selection. Portfolio overlap remains an external control.")

with st.sidebar:
    st.header("Production controls")
    project_root_text = st.text_input("Project root", value=str(DEFAULT_PROJECT_ROOT))
    project_root = Path(project_root_text).expanduser()
    runtime_path = project_root / "config/vrp_hybrid_v2_eod_runtime_config.json"
    approved_nav = st.number_input("Approved NAV ($)", min_value=100_000.0, value=1_000_000.0, step=50_000.0)
    with st.expander("Advanced"):
        manual_date_enabled = st.checkbox("Use manual target date", value=False)
        manual_date = st.date_input("Target date") if manual_date_enabled else None
        force_recalculate = st.checkbox("Force recalculate from earliest detected gap", value=False)
        skip_upstream = st.checkbox("Skip upstream backfill (diagnostic only)", value=False)
        shadow_write = st.checkbox(
            "Record non-authoritative PostgreSQL shadow",
            value=environment_flag("VRP_EOD_SHADOW_WRITE"),
            help=(
                "Runs only after successful canonical publication. PostgreSQL "
                "remains a reconciliation copy, not the signal source of record."
            ),
        )
    st.caption(f"Runtime config: {runtime_path}")

run_clicked = st.button(
    "Backfill Missing Data and Recalculate Through Latest EOD",
    type="primary",
    use_container_width=True,
)

if run_clicked:
    if not runtime_path.exists():
        st.error(f"Missing runtime config: {runtime_path}")
    else:
        target_text = manual_date.strftime("%Y%m%d") if manual_date_enabled and manual_date else None
        code, output = run_pipeline(
            project_root=project_root,
            runtime_path=runtime_path,
            approved_nav=float(approved_nav),
            target_date=target_text,
            force_recalculate=force_recalculate,
            skip_upstream=skip_upstream,
            shadow_write=shadow_write,
        )
        st.session_state["latest_run_console"] = output
        st.session_state["latest_run_code"] = code

if runtime_path.exists():
    runtime, _ = load_runtime_config(project_root, runtime_path)
    signal_path = resolve_path(project_root, runtime["canonical"]["signal_history"])
    snapshot_path = resolve_path(project_root, runtime["canonical"]["latest_snapshot"])
    decision_path = resolve_path(project_root, runtime["canonical"]["selected_decisions"])
    status_path = resolve_path(project_root, runtime["canonical"]["data_status"])
    assert signal_path and snapshot_path and decision_path and status_path
    history = read_parquet_safe(signal_path)
    snapshot = read_parquet_safe(snapshot_path)
    decisions = read_parquet_safe(decision_path)
    status = read_json_safe(status_path)
else:
    history = pd.DataFrame()
    snapshot = pd.DataFrame()
    decisions = pd.DataFrame()
    status = {}

left, right = st.columns([1.35, 1])
with left:
    display_latest_signal(decisions, snapshot, float(approved_nav))
with right:
    display_data_health(status)

display_tenor_table(snapshot)
display_charts(history)
display_history(history)

if "latest_run_console" in st.session_state:
    with st.expander("Latest run console", expanded=st.session_state.get("latest_run_code", 1) != 0):
        st.code(st.session_state["latest_run_console"], language="text")
