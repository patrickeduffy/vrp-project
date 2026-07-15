
from __future__ import annotations

import subprocess
import time
from datetime import datetime, date, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import streamlit as st


# =============================================================================
# VRP Streamlit Dashboard v1.2
#
# Fast/lazy version:
# - Does NOT load all large files on startup.
# - Sidebar page selector controls what gets loaded.
# - Home page loads only latest snapshot + latest health status.
# - Backfill page loads only what it needs and calls daily wrapper.
# - Charts page loads full final panel only when selected.
# - Audit page loads audit/log files only when selected.
#
# No model/threshold/sizing/denominator/signal changes.
# =============================================================================

PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

DAILY_WRAPPER = NOTEBOOKS_DIR / "vrp_daily_production_update_v1.py"

CANONICAL_FINAL = PROJECT_ROOT / r"data\processed\vrp_final_signal\vrp_final_corsi_signal_base_panel_v1.parquet"
CANONICAL_SNAPSHOT = PROJECT_ROOT / r"data\processed\vrp_final_signal\vrp_final_corsi_latest_snapshot_v1.parquet"
CANONICAL_SELECTED = PROJECT_ROOT / r"data\processed\vrp_final_signal\vrp_final_corsi_selected_trades_v1.parquet"

SPY_EOD = PROJECT_ROOT / r"data\processed\market_data\spy_eod_prices_v1.parquet"
SPY_RV_HISTORY = PROJECT_ROOT / r"data\processed\market_data\spy_realized_vol_history_v1.parquet"
CORSI_SUPPORT = PROJECT_ROOT / r"data\processed\market_data\spy_corsi_har_input_panel_v1.parquet"
IMPLIED_SURFACE = PROJECT_ROOT / r"data\processed\implied_variance\spx_vix_style_implied_variance_surface_v1.parquet"

DAILY_AUDIT_DIR = PROJECT_ROOT / r"data\audit\daily_production_update"
HEALTH_DIR = PROJECT_ROOT / r"data\audit\production_health"

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


# =============================================================================
# Streamlit setup
# =============================================================================

st.set_page_config(
    page_title="VRP Production Dashboard",
    page_icon="📈",
    layout="wide",
)

st.title("VRP Production Dashboard")
st.caption("Official EOD production monitor, one-click backfill, and signal viewer.")


# =============================================================================
# Utility functions
# =============================================================================

def parse_date_like(s: pd.Series) -> pd.Series:
    raw = pd.Series(s, index=s.index)

    if pd.api.types.is_datetime64_any_dtype(raw):
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    if len(as_str) and as_str.str.fullmatch(r"\d{8}").mean() > 0.80:
        return pd.to_datetime(as_str, format="%Y%m%d", errors="coerce").dt.normalize()

    return pd.to_datetime(raw, errors="coerce").dt.normalize()


def add_work_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["_work_date"] = parse_date_like(out["date"])
    elif "trade_date" in out.columns:
        out["_work_date"] = parse_date_like(out["trade_date"])
    else:
        out["_work_date"] = pd.NaT

    if "tenor" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["tenor"], errors="coerce")
    elif "target_days" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["target_days"], errors="coerce")
    else:
        out["_work_tenor"] = np.nan

    return out


def fmt_date(x) -> str:
    if x is None or pd.isna(x):
        return "NA"
    return pd.Timestamp(x).strftime("%Y-%m-%d")


def file_modified_time(path: Path) -> str:
    if not path.exists():
        return "missing"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def file_size_mb(path: Path) -> float:
    if not path.exists():
        return np.nan
    return round(path.stat().st_size / (1024 * 1024), 3)


@st.cache_data(ttl=30, show_spinner=False)
def read_parquet_cached(path_str: str) -> pd.DataFrame:
    p = Path(path_str)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


@st.cache_data(ttl=30, show_spinner=False)
def read_csv_cached(path_str: str) -> pd.DataFrame:
    p = Path(path_str)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


def latest_file(folder: Path, pattern: str) -> Path | None:
    if not folder.exists():
        return None
    files = list(folder.glob(pattern))
    if not files:
        return None
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def latest_daily_audit_dir() -> Path | None:
    if not DAILY_AUDIT_DIR.exists():
        return None
    dirs = [p for p in DAILY_AUDIT_DIR.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return sorted(dirs, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def latest_date_from_df(df: pd.DataFrame) -> pd.Timestamp | None:
    if df.empty:
        return None
    dfx = add_work_cols(df)
    mx = dfx["_work_date"].max()
    if pd.isna(mx):
        return None
    return pd.Timestamp(mx).normalize()


def latest_snapshot_rows() -> pd.DataFrame:
    snap = read_parquet_cached(str(CANONICAL_SNAPSHOT))
    if snap.empty:
        return pd.DataFrame()

    snap = add_work_cols(snap)
    latest_date = snap["_work_date"].max()
    latest = snap[snap["_work_date"].eq(latest_date)].copy()
    return latest.sort_values("_work_tenor")


def current_production_date() -> pd.Timestamp | None:
    snap = read_parquet_cached(str(CANONICAL_SNAPSHOT))
    dt = latest_date_from_df(snap)
    if dt is not None:
        return dt

    final = read_parquet_cached(str(CANONICAL_FINAL))
    return latest_date_from_df(final)


def nyse_trading_days(start_date: pd.Timestamp, end_date: pd.Timestamp) -> tuple[list[pd.Timestamp], str]:
    start_date = pd.Timestamp(start_date).normalize()
    end_date = pd.Timestamp(end_date).normalize()

    if end_date < start_date:
        return [], "none"

    try:
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar("XNYS")
        schedule = nyse.schedule(start_date=start_date, end_date=end_date)
        return [pd.Timestamp(x).normalize() for x in schedule.index], "XNYS/pandas_market_calendars"
    except Exception:
        days = []
        d = start_date
        while d <= end_date:
            if d.weekday() < 5:
                days.append(pd.Timestamp(d).normalize())
            d += pd.Timedelta(days=1)
        return days, "weekday fallback"


def last_completed_trading_day() -> tuple[pd.Timestamp, str]:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    today = pd.Timestamp(now_et.date()).normalize()

    # Conservative: only consider today's EOD complete after 6 PM ET.
    provisional_end = today if now_et.time() >= dtime(18, 0) else today - pd.Timedelta(days=1)

    days, source = nyse_trading_days(provisional_end - pd.Timedelta(days=14), provisional_end)
    if not days:
        return provisional_end, source

    return max(days), source


def latest_target_trading_day(target_date_value: date) -> tuple[pd.Timestamp | None, str]:
    target = pd.Timestamp(target_date_value).normalize()
    days, source = nyse_trading_days(target - pd.Timedelta(days=14), target)
    valid = [d for d in days if d <= target]
    if not valid:
        return None, source
    return max(valid), source


def missing_trading_dates(current_date: pd.Timestamp | None, target_date: pd.Timestamp) -> tuple[list[pd.Timestamp], str]:
    if current_date is None:
        return [], "no current production date"

    start = pd.Timestamp(current_date).normalize() + pd.Timedelta(days=1)
    target_date = pd.Timestamp(target_date).normalize()
    return nyse_trading_days(start, target_date)


def status_badge(label: str, status: str, detail: str = ""):
    txt = f"**{label}: {status}**"
    if detail:
        txt += f" — {detail}"

    s = status.upper()
    if s in ["PASS", "CURRENT", "OK"]:
        st.success(txt)
    elif s in ["STALE", "WARN", "WARNING"]:
        st.warning(txt)
    else:
        st.error(txt)


def load_latest_component_status() -> tuple[pd.DataFrame, Path | None]:
    p = latest_file(HEALTH_DIR, "vrp_production_health_component_status_*.csv")
    if p is None:
        return pd.DataFrame(), None
    return read_csv_cached(str(p)), p


def load_latest_health_validation() -> tuple[pd.DataFrame, Path | None]:
    p = latest_file(HEALTH_DIR, "vrp_production_health_check_*.csv")
    if p is None:
        return pd.DataFrame(), None
    return read_csv_cached(str(p)), p


def run_daily_wrapper_for_date(run_date: pd.Timestamp, python_command: str, output_placeholder) -> tuple[bool, int, str]:
    yyyymmdd = pd.Timestamp(run_date).strftime("%Y%m%d")

    cmd = [
        python_command,
        "-u",
        str(DAILY_WRAPPER),
        "--project-root",
        str(PROJECT_ROOT),
        "--end-date",
        yyyymmdd,
    ]

    output_lines = ["COMMAND:\n", " ".join(cmd) + "\n\n"]
    output_placeholder.code("".join(output_lines), language="text")

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(NOTEBOOKS_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        assert proc.stdout is not None

        for line in proc.stdout:
            output_lines.append(line)
            output_placeholder.code("".join(output_lines[-500:]), language="text")

        return_code = proc.wait()

    except Exception as exc:
        output_lines.append(f"\nSTREAMLIT SUBPROCESS ERROR: {exc}\n")
        output_placeholder.code("".join(output_lines[-500:]), language="text")
        return False, -999, "".join(output_lines)

    output_text = "".join(output_lines)
    ok = (
        return_code == 0
        and "DAILY PRODUCTION UPDATE PASS" in output_text
        and "PRODUCTION HEALTH: PASS" in output_text
    )

    return ok, return_code, output_text


def first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def table_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    if not keep:
        return pd.DataFrame()
    return df[keep].copy()


def get_num(row: pd.Series, col: str) -> float:
    if col not in row.index:
        return np.nan
    return float(pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0])


def tenor_bucket_for_rule(tenor: int) -> str | None:
    if tenor in [12, 15, 18]:
        return "Front"
    if tenor in [21, 24]:
        return "Middle"
    if tenor in [27, 30, 33]:
        return "Back"
    return None


def locked_signal_rule(layer: str, bucket: str | None, tenor: int) -> dict | None:
    if tenor == 9 or bucket is None:
        return None

    if layer == "Core":
        if bucket == "Middle":
            return {"vrp_min": 0.65, "z3_min": 0.70, "z1_min": 0.70, "rsi_max": 70.0, "rv_min": 8.5}
        if bucket == "Back":
            return {"vrp_min": 0.70, "z3_min": 0.70, "z1_min": 0.70, "rsi_max": 70.0, "rv_min": 8.5}
        return None

    if layer == "Secondary":
        if bucket == "Front":
            return {"vrp_min": 0.65, "z3_min": 0.20, "z1_min": 0.20, "rsi_max": 75.0, "rv_min": 7.0}
        if bucket == "Middle":
            return {"vrp_min": 0.65, "z3_min": 0.20, "z1_min": 0.20, "rsi_max": 76.0, "rv_min": 7.0}
        if bucket == "Back":
            return {"vrp_min": 0.65, "z3_min": 0.00, "z1_min": 0.00, "rsi_max": 77.0, "rv_min": 6.5}
        return None

    return None


LOCKED_SIZE_PCT_BY_LABEL = {
    "Core_Middle_21D": 3.50,
    "Core_Middle_24D": 4.25,
    "Core_Back_27D": 4.50,
    "Core_Back_30D": 4.75,
    "Core_Back_33D": 5.00,
    "Secondary_Front_12D": 1.50,
    "Secondary_Front_15D": 2.00,
    "Secondary_Front_18D": 2.75,
    "Secondary_Middle_21D": 3.50,
    "Secondary_Middle_24D": 3.75,
    "Secondary_Back_27D": 4.00,
    "Secondary_Back_30D": 4.25,
    "Secondary_Back_33D": 4.50,
}


def build_decision_explanation(latest: pd.DataFrame) -> pd.DataFrame:
    if latest.empty:
        return pd.DataFrame()

    rows = []

    for _, row in latest.iterrows():
        tenor = int(pd.to_numeric(pd.Series([row.get("tenor", row.get("_work_tenor"))]), errors="coerce").iloc[0])
        bucket = row.get("tenor_bucket", None)
        if pd.isna(bucket) or bucket is None:
            bucket = tenor_bucket_for_rule(tenor)

        vrp = get_num(row, "model_vrp_log_final")
        z3 = get_num(row, "z_3m_final")
        z1 = get_num(row, "z_1y_final")
        rsi = get_num(row, "rsi14_final")
        rv = get_num(row, "rv21d_vol_pct_final")

        for layer in ["Core", "Secondary"]:
            rule = locked_signal_rule(layer, bucket, tenor)
            if rule is None:
                continue

            sleeve = f"{layer}_{bucket}_{tenor}D"
            size_pct = LOCKED_SIZE_PCT_BY_LABEL.get(sleeve, np.nan)

            checks = {
                "vrp_pass": np.isfinite(vrp) and vrp > rule["vrp_min"],
                "z_3m_pass": np.isfinite(z3) and z3 > rule["z3_min"],
                "z_1y_pass": np.isfinite(z1) and z1 > rule["z1_min"],
                "rsi_pass": np.isfinite(rsi) and rsi < rule["rsi_max"],
                "rv21d_pass": np.isfinite(rv) and rv > rule["rv_min"],
            }

            blockers = []
            if not checks["vrp_pass"]:
                blockers.append(f"VRP {vrp:.3f} <= {rule['vrp_min']:.2f}" if np.isfinite(vrp) else "VRP missing")
            if not checks["z_3m_pass"]:
                blockers.append(f"3m z {z3:.3f} <= {rule['z3_min']:.2f}" if np.isfinite(z3) else "3m z missing")
            if not checks["z_1y_pass"]:
                blockers.append(f"1y z {z1:.3f} <= {rule['z1_min']:.2f}" if np.isfinite(z1) else "1y z missing")
            if not checks["rsi_pass"]:
                blockers.append(f"RSI {rsi:.2f} >= {rule['rsi_max']:.0f}" if np.isfinite(rsi) else "RSI missing")
            if not checks["rv21d_pass"]:
                blockers.append(f"RV21D {rv:.2f} <= {rule['rv_min']:.1f}" if np.isfinite(rv) else "RV21D missing")

            rows.append({
                "layer": layer,
                "bucket": bucket,
                "tenor": tenor,
                "sleeve": sleeve,
                "locked_size_pct": size_pct,
                "vrp": vrp,
                "vrp_min": rule["vrp_min"],
                "z_3m": z3,
                "z_3m_min": rule["z3_min"],
                "z_1y": z1,
                "z_1y_min": rule["z1_min"],
                "rsi14": rsi,
                "rsi14_max": rule["rsi_max"],
                "rv21d": rv,
                "rv21d_min": rule["rv_min"],
                **checks,
                "all_pass": all(checks.values()),
                "blockers": "; ".join(blockers) if blockers else "PASS",
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values(
        ["all_pass", "locked_size_pct", "layer", "tenor"],
        ascending=[False, False, True, True],
    )


def overlay_table(final_panel: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    if final_panel.empty or metric_col not in final_panel.columns:
        return pd.DataFrame()

    fp = add_work_cols(final_panel)
    fp = fp.dropna(subset=["_work_date", "_work_tenor"]).copy()

    dates = sorted(pd.to_datetime(fp["_work_date"].dropna().unique()))
    if not dates:
        return pd.DataFrame()

    selected = []
    labels = []

    selected.append(dates[-1])
    labels.append(f"{pd.Timestamp(dates[-1]).strftime('%Y-%m-%d')} latest")

    if len(dates) >= 2:
        selected.append(dates[-2])
        labels.append(f"{pd.Timestamp(dates[-2]).strftime('%Y-%m-%d')} prior")

    if len(dates) >= 6:
        selected.append(dates[-6])
        labels.append(f"{pd.Timestamp(dates[-6]).strftime('%Y-%m-%d')} 5d ago")

    pieces = []

    for d, label in zip(selected, labels):
        tmp = fp[fp["_work_date"].eq(d)][["_work_tenor", metric_col]].copy()
        tmp["_work_tenor"] = pd.to_numeric(tmp["_work_tenor"], errors="coerce")
        tmp[metric_col] = pd.to_numeric(tmp[metric_col], errors="coerce")
        tmp = tmp.dropna().sort_values("_work_tenor")
        pieces.append(tmp.rename(columns={metric_col: label}).set_index("_work_tenor"))

    if not pieces:
        return pd.DataFrame()

    out = pd.concat(pieces, axis=1)
    out.index.name = "tenor"
    return out


# =============================================================================
# Sidebar navigation
# =============================================================================

with st.sidebar:
    st.header("Navigation")

    page = st.radio(
        "Page",
        [
            "Home",
            "Backfill",
            "Latest Signal",
            "Decision Explanation",
            "Charts",
            "Recent History",
            "Audit / Files",
        ],
        index=0,
    )

    st.divider()

    python_command = st.text_input("Python command", value="py")

    if st.button("Refresh data cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("v1.2 lazy-loaded EOD dashboard. Intraday signal generation is not included.")


# =============================================================================
# Home
# =============================================================================

if page == "Home":
    st.subheader("Home / Production Status")

    with st.spinner("Loading latest snapshot and latest health status..."):
        latest = latest_snapshot_rows()
        prod_date = current_production_date()
        completed_day, cal_source = last_completed_trading_day()
        component_status, component_path = load_latest_component_status()

    if prod_date is not None and prod_date >= completed_day:
        status_badge("Freshness", "CURRENT", f"production={fmt_date(prod_date)}, completed={fmt_date(completed_day)}")
    elif prod_date is not None:
        miss, _ = missing_trading_dates(prod_date, completed_day)
        status_badge("Freshness", "STALE", f"{len(miss)} missing trading day(s)")
    else:
        status_badge("Freshness", "FAIL", "could not determine production date")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Official production date", fmt_date(prod_date))
    c2.metric("Latest completed trading day", fmt_date(completed_day))
    c3.metric("Calendar source", cal_source)
    c4.metric("Latest snapshot rows", len(latest))

    if not latest.empty:
        tenors = sorted(latest["_work_tenor"].dropna().astype(int).unique().tolist())
        if tenors == EXPECTED_TENORS:
            st.success(f"Latest snapshot has expected tenor grid: {tenors}")
        else:
            st.error(f"Latest snapshot tenor grid mismatch: {tenors}")

        selected_col = first_existing_col(latest, ["selected", "selected_trade_final"])
        if selected_col:
            selected_count = int(latest[selected_col].fillna(False).astype(bool).sum())
        else:
            selected_count = 0
        st.metric("Selected trades latest date", selected_count)

    if component_path:
        st.caption(f"Latest component status audit: `{component_path}`")

    if not component_status.empty:
        st.dataframe(component_status, use_container_width=True, hide_index=True)
    else:
        st.warning("No production health component-status audit found yet.")


# =============================================================================
# Backfill
# =============================================================================

elif page == "Backfill":
    st.subheader("One-Click EOD Backfill")

    st.write(
        "Choose a target completed EOD date. The app detects missing trading dates and runs "
        "`vrp_daily_production_update_v1.py` once per missing trading date, in chronological order."
    )

    with st.spinner("Checking current production date..."):
        prod_date = current_production_date()
        default_target, default_source = last_completed_trading_day()

    target_input = st.date_input(
        "Backfill through target date",
        value=default_target.date() if default_target is not None else date.today(),
    )

    target_official_date, target_source = latest_target_trading_day(target_input)

    if target_official_date is None:
        st.error("Could not resolve a target trading date.")
        missing_dates = []
    else:
        missing_dates, missing_source = missing_trading_dates(prod_date, target_official_date)

        b1, b2, b3 = st.columns(3)
        b1.metric("Current production", fmt_date(prod_date))
        b2.metric("Target official date", fmt_date(target_official_date))
        b3.metric("Missing trading dates", len(missing_dates))

        st.caption(f"Trading-calendar source: `{missing_source}`")

        if missing_dates:
            st.write("Dates to run:")
            st.code(", ".join(d.strftime("%Y%m%d") for d in missing_dates), language="text")
        else:
            st.success("No missing trading dates through the selected target.")

    clicked = st.button(
        "Check and Backfill to Target Date",
        type="primary",
        use_container_width=True,
        disabled=(target_official_date is None or prod_date is None or len(missing_dates) == 0),
    )

    if clicked:
        if not DAILY_WRAPPER.exists():
            st.error(f"Daily wrapper not found: {DAILY_WRAPPER}")
            st.stop()

        st.warning("Backfill started. Do not close this browser tab or interrupt Python until the run finishes.")

        run_summary = []
        all_ok = True

        for d in missing_dates:
            st.markdown(f"### Running production update for `{d.strftime('%Y-%m-%d')}`")
            output_box = st.empty()

            ok, return_code, output_text = run_daily_wrapper_for_date(
                run_date=d,
                python_command=python_command,
                output_placeholder=output_box,
            )

            run_summary.append({
                "date": d.strftime("%Y-%m-%d"),
                "return_code": return_code,
                "pass": ok,
            })

            if ok:
                st.success(f"{d.strftime('%Y-%m-%d')} passed.")
            else:
                st.error(f"{d.strftime('%Y-%m-%d')} failed. Stopping backfill.")
                all_ok = False
                break

        st.subheader("Backfill Summary")
        st.dataframe(pd.DataFrame(run_summary), use_container_width=True, hide_index=True)

        if all_ok:
            st.success("Backfill completed successfully. Refreshing dashboard data.")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
        else:
            st.error("Backfill stopped on first failure. Review command output and audit logs before rerunning.")


# =============================================================================
# Latest Signal
# =============================================================================

elif page == "Latest Signal":
    st.subheader("Latest Official 9-Tenor Signal")

    with st.spinner("Loading latest snapshot..."):
        latest = latest_snapshot_rows()

    if latest.empty:
        st.error(f"Latest snapshot missing or empty: {CANONICAL_SNAPSHOT}")
    else:
        latest_date = latest["_work_date"].max()
        tenors = sorted(latest["_work_tenor"].dropna().astype(int).unique().tolist())

        if tenors == EXPECTED_TENORS:
            st.success(f"Latest snapshot date {fmt_date(latest_date)} has all 9 expected tenors.")
        else:
            st.error(f"Latest tenor grid mismatch. Found {tenors}; expected {EXPECTED_TENORS}.")

        display_cols = [
            "trade_date",
            "tenor",
            "tenor_bucket",
            "vix_style_vol_final",
            "forecast_vol_final",
            "model_vrp_log_final",
            "z_3m_final",
            "z_1y_final",
            "rsi14_final",
            "rv21d_vol_pct_final",
            "core_pass",
            "secondary_pass",
            "selected",
            "selected_layer",
            "selected_tenor_bucket",
            "selected_size_pct",
        ]

        table = table_cols(latest, display_cols)
        st.dataframe(table, use_container_width=True, hide_index=True)

        st.download_button(
            label="Download latest 9-tenor table as CSV",
            data=table.to_csv(index=False).encode("utf-8"),
            file_name=f"vrp_latest_9_tenor_signal_{pd.Timestamp(latest_date).strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )


# =============================================================================
# Decision Explanation
# =============================================================================

elif page == "Decision Explanation":
    st.subheader("Selected Trade / No-Trade Explanation")

    with st.spinner("Loading latest snapshot and calculating sleeve checks..."):
        latest = latest_snapshot_rows()

    if latest.empty:
        st.error("No latest snapshot loaded.")
    else:
        latest_date = latest["_work_date"].max()

        selected_col = first_existing_col(latest, ["selected", "selected_trade_final"])
        if selected_col:
            selected_rows = latest[latest[selected_col].fillna(False).astype(bool)].copy()
        else:
            selected_rows = pd.DataFrame()

        if selected_rows.empty:
            st.info(f"No selected trade on {fmt_date(latest_date)}.")
        else:
            st.success(f"Selected trade on {fmt_date(latest_date)}")
            st.dataframe(selected_rows, use_container_width=True, hide_index=True)

        explanation = build_decision_explanation(latest)

        if explanation.empty:
            st.warning("Could not build decision explanation table.")
        else:
            pass_count = int(explanation["all_pass"].sum())

            e1, e2, e3 = st.columns(3)
            e1.metric("Candidate sleeves passing", pass_count)
            e2.metric("Active sleeve checks", len(explanation))
            e3.metric("Lowest active VRP threshold", "0.65")

            st.dataframe(explanation, use_container_width=True, hide_index=True)

            if pass_count == 0:
                st.write("Top blockers")
                st.dataframe(
                    explanation[["layer", "bucket", "tenor", "sleeve", "blockers"]].head(10),
                    use_container_width=True,
                    hide_index=True,
                )


# =============================================================================
# Charts
# =============================================================================

elif page == "Charts":
    st.subheader("Term Structure Charts")

    st.warning("This page loads the full final signal panel. It may take a few seconds.")

    with st.spinner("Loading full final panel for charts..."):
        final_panel = read_parquet_cached(str(CANONICAL_FINAL))

    if final_panel.empty:
        st.error(f"Final panel missing or empty: {CANONICAL_FINAL}")
    else:
        c1, c2 = st.columns(2)

        with c1:
            st.markdown("**Implied vol: latest / prior / 5d ago**")
            t = overlay_table(final_panel, "vix_style_vol_final")
            if t.empty:
                st.info("Required implied-vol column unavailable.")
            else:
                st.line_chart(t)

        with c2:
            st.markdown("**Forecast vol: latest / prior / 5d ago**")
            t = overlay_table(final_panel, "forecast_vol_final")
            if t.empty:
                st.info("Required forecast-vol column unavailable.")
            else:
                st.line_chart(t)

        c3, c4 = st.columns(2)

        with c3:
            st.markdown("**VRP log: latest / prior / 5d ago**")
            t = overlay_table(final_panel, "model_vrp_log_final")
            if t.empty:
                st.info("Required VRP column unavailable.")
            else:
                st.line_chart(t)

        with c4:
            st.markdown("**Latest 3m / 1y z-score term structure**")
            latest = latest_snapshot_rows()
            if latest.empty:
                st.info("Latest snapshot unavailable.")
            else:
                z_cols = [c for c in ["z_3m_final", "z_1y_final"] if c in latest.columns]
                if not z_cols:
                    st.info("Required z-score columns unavailable.")
                else:
                    z = latest[["tenor"] + z_cols].copy()
                    z["tenor"] = pd.to_numeric(z["tenor"], errors="coerce")
                    st.line_chart(z.set_index("tenor"))


# =============================================================================
# Recent History
# =============================================================================

elif page == "Recent History":
    st.subheader("Recent History")

    st.warning("This page loads the full final signal panel. It may take a few seconds.")

    with st.spinner("Loading full final panel..."):
        final_panel = read_parquet_cached(str(CANONICAL_FINAL))

    if final_panel.empty:
        st.error("Final panel unavailable.")
    else:
        fp = add_work_cols(final_panel)
        recent_dates = sorted(fp["_work_date"].dropna().unique())[-90:]
        recent = fp[fp["_work_date"].isin(recent_dates)].copy()

        selected_col = first_existing_col(recent, ["selected", "selected_trade_final"])
        if selected_col:
            daily_selected = (
                recent.groupby("_work_date")[selected_col]
                .apply(lambda x: bool(pd.Series(x).fillna(False).astype(bool).any()))
                .reset_index(name="selected_any")
            )
            daily_selected["selected_any_int"] = daily_selected["selected_any"].astype(int)

            st.markdown("**Recent selected-trade days**")
            st.line_chart(daily_selected.set_index("_work_date")["selected_any_int"])

        if "model_vrp_log_final" in recent.columns and "tenor" in recent.columns:
            tenor_choice = st.selectbox("Recent VRP history tenor", options=EXPECTED_TENORS, index=EXPECTED_TENORS.index(30))
            hist = recent[pd.to_numeric(recent["tenor"], errors="coerce").eq(tenor_choice)].copy()
            hist = hist.sort_values("_work_date")

            if not hist.empty:
                st.markdown(f"**Recent VRP log history: {tenor_choice}D**")
                st.line_chart(hist.set_index("_work_date")["model_vrp_log_final"])

    st.divider()
    st.subheader("Recent Selected Trades File")

    selected = read_parquet_cached(str(CANONICAL_SELECTED))
    if selected.empty:
        st.info("Selected trades file is empty or unavailable.")
    else:
        sel = add_work_cols(selected).sort_values("_work_date", ascending=False)
        drop_cols = [c for c in ["_work_date", "_work_tenor"] if c in sel.columns]
        st.dataframe(sel.head(50).drop(columns=drop_cols), use_container_width=True, hide_index=True)


# =============================================================================
# Audit / Files
# =============================================================================

elif page == "Audit / Files":
    st.subheader("Audit / Files")

    latest_audit = latest_daily_audit_dir()

    if latest_audit is None:
        st.info("No daily production audit folder found.")
    else:
        st.write("Latest daily audit folder")
        st.code(str(latest_audit), language="text")

        manifest_files = sorted(latest_audit.glob("*manifest*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        stdout_files = sorted(latest_audit.glob("*stdout*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
        stderr_files = sorted(latest_audit.glob("*stderr*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)

        a1, a2, a3 = st.columns(3)
        a1.metric("Manifest files", len(manifest_files))
        a2.metric("stdout logs", len(stdout_files))
        a3.metric("stderr logs", len(stderr_files))

        if manifest_files:
            st.write("Latest manifest")
            p = manifest_files[0]
            st.caption(f"`{p}`")
            try:
                st.json(p.read_text(encoding="utf-8"))
            except Exception:
                st.code(p.read_text(encoding="utf-8", errors="replace")[:12000], language="text")

        if stdout_files:
            st.write("Latest stdout tail")
            p = stdout_files[0]
            st.caption(f"`{p}`")
            txt = p.read_text(encoding="utf-8", errors="replace")
            st.code(txt[-12000:], language="text")

        if stderr_files:
            st.write("Latest stderr tail")
            p = stderr_files[0]
            st.caption(f"`{p}`")
            txt = p.read_text(encoding="utf-8", errors="replace")
            st.code(txt[-12000:], language="text")

    st.divider()

    file_rows = [
        ("canonical_final", CANONICAL_FINAL),
        ("canonical_snapshot", CANONICAL_SNAPSHOT),
        ("canonical_selected", CANONICAL_SELECTED),
        ("implied_surface", IMPLIED_SURFACE),
        ("spy_rv_history", SPY_RV_HISTORY),
        ("spy_eod", SPY_EOD),
        ("corsi_support", CORSI_SUPPORT),
        ("daily_wrapper", DAILY_WRAPPER),
    ]

    file_status = pd.DataFrame([
        {
            "name": name,
            "exists": p.exists(),
            "modified": file_modified_time(p),
            "size_mb": file_size_mb(p),
            "path": str(p),
        }
        for name, p in file_rows
    ])

    st.dataframe(file_status, use_container_width=True, hide_index=True)
