
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


REQUIRED_LOCKED_FEATURES = [
    "candidate_log_downside_rv_5d",
    "candidate_log_downside_rv_10d",
    "candidate_log_downside_rv_21d",
    "candidate_log_downside_rv_63d",
    "candidate_downside_share_5d",
    "candidate_downside_share_10d",
    "candidate_max_abs_return_3d",
    "candidate_max_abs_return_5d",
    "candidate_max_abs_return_10d",
]

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def read_text_safe(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def find_terms(text: str, terms: list[str]) -> list[str]:
    lo = text.lower()
    return [t for t in terms if t.lower() in lo]


def extract_context(text: str, terms: list[str], max_hits: int = 8, radius: int = 120) -> str:
    lo = text.lower()
    snippets = []

    for term in terms:
        idx = lo.find(term.lower())
        if idx >= 0:
            start = max(0, idx - radius)
            end = min(len(text), idx + len(term) + radius)
            snippet = text[start:end].replace("\n", " ").replace("\r", " ")
            snippet = re.sub(r"\s+", " ", snippet).strip()
            snippets.append(f"...{snippet}...")
        if len(snippets) >= max_hits:
            break

    return "\n".join(snippets)


def latest_file(folder: Path, pattern: str) -> Path | None:
    if not folder.exists():
        return None
    files = list(folder.glob(pattern))
    if not files:
        return None
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


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


def parquet_summary(path: Path) -> dict:
    out = {
        "exists": path.exists(),
        "path": str(path),
        "rows": None,
        "cols": None,
        "latest_date": None,
        "latest_tenors": None,
        "columns": [],
        "error": "",
    }

    if not path.exists():
        return out

    try:
        df = pd.read_parquet(path)
        out["rows"] = int(len(df))
        out["cols"] = int(len(df.columns))
        out["columns"] = list(df.columns)

        dfx = add_work_cols(df)
        latest = dfx["_work_date"].max()
        if pd.notna(latest):
            out["latest_date"] = str(pd.Timestamp(latest).date())
            tenors = sorted(
                dfx.loc[dfx["_work_date"].eq(latest), "_work_tenor"]
                .dropna()
                .astype(int)
                .unique()
                .tolist()
            )
            out["latest_tenors"] = tenors

    except Exception as exc:
        out["error"] = str(exc)

    return out


def csv_summary(path: Path) -> dict:
    out = {
        "exists": path.exists(),
        "path": str(path),
        "rows": None,
        "cols": None,
        "columns": [],
        "head": [],
        "error": "",
    }

    if not path.exists():
        return out

    try:
        df = pd.read_csv(path)
        out["rows"] = int(len(df))
        out["cols"] = int(len(df.columns))
        out["columns"] = list(df.columns)
        out["head"] = df.head(5).to_dict(orient="records")
    except Exception as exc:
        out["error"] = str(exc)

    return out


def source_row(
    *,
    component: str,
    canonical_source: Path | None,
    source_type: str,
    required_for_true_intraday: bool,
    status: str,
    intraday_support: str,
    evidence: str,
    blocker: str = "",
    notes: str = "",
) -> dict:
    return {
        "component": component,
        "canonical_source": str(canonical_source) if canonical_source else "",
        "source_type": source_type,
        "required_for_true_intraday": required_for_true_intraday,
        "status": status,
        "intraday_support": intraday_support,
        "evidence": evidence,
        "blocker": blocker,
        "notes": notes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    args = parser.parse_args()

    project_root = Path(args.project_root)
    notebooks = project_root / "notebooks"
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    audit_dir = project_root / "data" / "audit" / "intraday_source_inventory"
    audit_dir.mkdir(parents=True, exist_ok=True)

    # Known / intended production scripts.
    implied_script = notebooks / "vrp_implied_variance_eod_update_v1.py"
    market_script = notebooks / "vrp_market_data_build_v1.py"
    corsi_source_script = notebooks / "vrp_corsi_source_update_v1.py"
    cell4_script = notebooks / "vrp_locked_cell4_feature_panel_update_v1.py"
    forecast_script = notebooks / "vrp_locked_unified_fds_forecast_update_v1.py"
    final_signal_script = notebooks / "vrp_final_signal_panel_update_v1.py"
    daily_wrapper = notebooks / "vrp_daily_production_update_v1.py"

    # Canonical data / latest panels.
    implied_surface = project_root / r"data\processed\implied_variance\spx_vix_style_implied_variance_surface_v1.parquet"
    spy_eod = project_root / r"data\processed\market_data\spy_eod_prices_v1.parquet"
    rv21d = project_root / r"data\processed\market_data\spy_realized_vol_history_v1.parquet"
    corsi_support = project_root / r"data\processed\market_data\spy_corsi_har_input_panel_v1.parquet"
    canonical_final = project_root / r"data\processed\vrp_final_signal\vrp_final_corsi_signal_base_panel_v1.parquet"
    canonical_snapshot = project_root / r"data\processed\vrp_final_signal\vrp_final_corsi_latest_snapshot_v1.parquet"

    repair_dir = project_root / r"data\processed\vrp_front_middle_corsi_forecast_repair_v1"
    latest_feature_panel = latest_file(repair_dir, "04_front_middle_candidate_feature_panel_*.parquet")
    latest_forecast_panel = latest_file(repair_dir, "07A_unified_fds_no_min_return_oos_forecast_panel_*schema_repair.parquet")
    if latest_forecast_panel is None:
        latest_forecast_panel = latest_file(repair_dir, "07A_unified_fds_no_min_return_oos_forecast_panel_*.parquet")

    coefficient_artifact = project_root / r"data\audit\forecast_model_corsi_v1\corsi_cell6_ridge_coefficients_20200102_20260623_utp_cta_20260703_211350.csv"

    banner("VRP intraday source lock v1")
    print(f"Project root: {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Audit dir: {audit_dir}")

    rows = []
    excerpts = []

    # -------------------------------------------------------------------------
    # 1. VIX-style implied variance / option chain source
    # -------------------------------------------------------------------------
    implied_text = read_text_safe(implied_script)
    implied_terms = [
        "thetadata", "SPX", "SPXW", "option", "chain", "quote", "bid", "ask",
        "implied_variance", "vix_style", "target_days", "single-date", "160000",
        "snapshot", "ms_of_day", "expir"
    ]
    implied_hits = find_terms(implied_text, implied_terms)

    implied_data = parquet_summary(implied_surface)

    implied_intraday_terms = find_terms(implied_text, ["snapshot", "ms_of_day", "intraday", "quote_at_time", "interval", "time"])
    implied_support = "EOD_READY"
    implied_blocker = "Script appears to be EOD-oriented unless intraday quote-time parameters are confirmed."
    if implied_intraday_terms:
        implied_support = "POSSIBLE_INTRADAY_SUPPORT_REVIEW_REQUIRED"
        implied_blocker = ""

    rows.append(source_row(
        component="implied_variance_by_tenor",
        canonical_source=implied_script,
        source_type="script",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if implied_script.exists() else "MISSING",
        intraday_support=implied_support,
        evidence=f"script_exists={implied_script.exists()}; hits={implied_hits}; canonical_surface_latest={implied_data['latest_date']}; tenors={implied_data['latest_tenors']}",
        blocker=implied_blocker,
        notes="Need to confirm whether the ThetaData chain pull can request current/live timestamp T, not only 16:00 EOD.",
    ))

    excerpts.append({
        "component": "implied_variance_by_tenor",
        "source": str(implied_script),
        "excerpt": extract_context(implied_text, implied_hits),
    })

    # -------------------------------------------------------------------------
    # 2. Intraday / live SPX-SPY price source
    # -------------------------------------------------------------------------
    corsi_source_text = read_text_safe(corsi_source_script)
    market_text = read_text_safe(market_script)
    price_terms = [
        "intraday", "minute", "5min", "5-minute", "bar", "ohlc", "trade",
        "quote", "SPY", "SPX", "utp_cta", "cta", "price", "close"
    ]
    corsi_price_hits = find_terms(corsi_source_text, price_terms)
    market_price_hits = find_terms(market_text, price_terms)

    sample_intraday_files = sorted(
        (project_root / r"data\audit\forecast_model_corsi_v1").glob("spy_daily_intraday_rv_sample_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ) if (project_root / r"data\audit\forecast_model_corsi_v1").exists() else []

    support = "EOD_OR_DAILY_SOURCE_READY"
    blocker = "Need exact live timestamp T price endpoint or file path for current intraday SPX/SPY price."
    if "intraday" in [x.lower() for x in corsi_price_hits + market_price_hits] or sample_intraday_files:
        support = "HISTORICAL_INTRADAY_SOURCE_FOUND_REVIEW_REQUIRED"
        blocker = "Historical intraday source found, but live/current timestamp T availability still must be confirmed."

    rows.append(source_row(
        component="live_spx_spy_price_through_T",
        canonical_source=corsi_source_script if corsi_source_script.exists() else market_script,
        source_type="script",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if (corsi_source_script.exists() or market_script.exists()) else "MISSING",
        intraday_support=support,
        evidence=f"corsi_source_hits={corsi_price_hits}; market_hits={market_price_hits}; sample_intraday_files={len(sample_intraday_files)}",
        blocker=blocker,
        notes="Needed for live RSI14, RV21D, and intraday Corsi/FDS features.",
    ))

    excerpts.append({
        "component": "live_spx_spy_price_through_T",
        "source": str(corsi_source_script),
        "excerpt": extract_context(corsi_source_text, corsi_price_hits),
    })

    # -------------------------------------------------------------------------
    # 3. Corsi/FDS locked feature construction
    # -------------------------------------------------------------------------
    cell4_text = read_text_safe(cell4_script)
    feature_hits = find_terms(cell4_text, REQUIRED_LOCKED_FEATURES + ["spx_log_return", "source return fallback"])
    latest_feature_summary = parquet_summary(latest_feature_panel) if latest_feature_panel else {}

    feature_cols_found = []
    if latest_feature_summary:
        cols = latest_feature_summary.get("columns", [])
        feature_cols_found = [c for c in REQUIRED_LOCKED_FEATURES if c in cols]

    feature_ready = len(feature_cols_found) == len(REQUIRED_LOCKED_FEATURES)

    rows.append(source_row(
        component="locked_corsi_fds_feature_vector",
        canonical_source=cell4_script,
        source_type="script+latest_feature_panel",
        required_for_true_intraday=True,
        status="LOCKED_EOD_FEATURE_SOURCE_FOUND" if feature_ready else "PARTIAL_OR_MISSING",
        intraday_support="EOD_READY_NEEDS_INTRADAY_ADAPTATION",
        evidence=f"script_exists={cell4_script.exists()}; feature_hits={feature_hits}; latest_feature_panel={latest_feature_panel}; locked_features_found={feature_cols_found}",
        blocker="Need to define how each locked feature is recomputed using live price/returns through timestamp T.",
        notes="EOD feature contract is clear; intraday adaptation must preserve comparable feature definitions.",
    ))

    excerpts.append({
        "component": "locked_corsi_fds_feature_vector",
        "source": str(cell4_script),
        "excerpt": extract_context(cell4_text, feature_hits),
    })

    # -------------------------------------------------------------------------
    # 4. Forecast model scoring / coefficients
    # -------------------------------------------------------------------------
    forecast_text = read_text_safe(forecast_script)
    forecast_terms = [
        "StandardScaler", "Ridge", "Pipeline", "alpha", "fit_log",
        "predicted_log_variance_candidate", "forecast_variance_candidate",
        "candidate_forecast_vol_pct", "target_log_variance"
    ]
    forecast_hits = find_terms(forecast_text, forecast_terms)

    coef_summary = csv_summary(coefficient_artifact)
    coef_cols = coef_summary.get("columns", [])
    coef_has_basic = coefficient_artifact.exists() and any("coef" in c.lower() for c in coef_cols)
    coef_has_scaler = any(("scale" in c.lower() or "mean" in c.lower() or "std" in c.lower()) for c in coef_cols)
    coef_has_intercept = any("intercept" in c.lower() for c in coef_cols)

    latest_forecast_summary = parquet_summary(latest_forecast_panel) if latest_forecast_panel else {}
    forecast_cols = latest_forecast_summary.get("columns", []) if latest_forecast_summary else []
    forecast_required_cols = [
        "implied_variance",
        "forecast_variance_candidate",
        "predicted_log_variance_candidate",
        "candidate_forecast_vol_pct",
    ]
    forecast_cols_found = [c for c in forecast_required_cols if c in forecast_cols]

    if coef_has_basic and coef_has_scaler and coef_has_intercept:
        forecast_support = "SAVED_MODEL_ARTIFACT_POSSIBLY_USABLE"
        forecast_blocker = "Manual coefficient/scaler validation required against latest locked forecast output."
    else:
        forecast_support = "REPRODUCIBLE_EOD_FIT_SCORING_PATH_FOUND"
        forecast_blocker = "No complete standalone scaler+Ridge model artifact confirmed. Intraday v1 may need to refit/score using locked script logic or create a proper model artifact."

    rows.append(source_row(
        component="forecast_variance_intraday_scoring",
        canonical_source=forecast_script,
        source_type="script+optional_coefficients+latest_forecast_panel",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if forecast_script.exists() else "MISSING",
        intraday_support=forecast_support,
        evidence=f"forecast_script_hits={forecast_hits}; coefficient_artifact_exists={coefficient_artifact.exists()}; coefficient_columns={coef_cols}; latest_forecast_panel={latest_forecast_panel}; forecast_cols_found={forecast_cols_found}",
        blocker=forecast_blocker,
        notes="True intraday forecast requires applying locked Corsi/FDS model to live feature vector at timestamp T. Refit should stay EOD unless intentionally changed.",
    ))

    excerpts.append({
        "component": "forecast_variance_intraday_scoring",
        "source": str(forecast_script),
        "excerpt": extract_context(forecast_text, forecast_hits),
    })

    # -------------------------------------------------------------------------
    # 5. RSI14 / RV21D
    # -------------------------------------------------------------------------
    final_text = read_text_safe(final_signal_script)
    rsi_terms = ["rsi14", "RSI14", "avg_gain", "avg_loss", "wilder"]
    rv_terms = ["rv21d", "rv21d_vol_pct", "realized_vol_history", "rolling(21)", "sqrt(252)", "log_return"]

    rsi_hits = find_terms(final_text + "\n" + market_text, rsi_terms)
    rv_hits = find_terms(final_text + "\n" + market_text, rv_terms)

    rv_summary = parquet_summary(rv21d)

    rows.append(source_row(
        component="rsi14_intraday_formula",
        canonical_source=final_signal_script,
        source_type="script",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if rsi_hits else "MISSING_OR_NOT_EXPLICIT",
        intraday_support="NEEDS_LIVE_PRICE_ADAPTATION",
        evidence=f"hits={rsi_hits}",
        blocker="Need exact EOD RSI14 formula confirmed, then define intraday replacement of today's close with live price T.",
        notes="Use prior completed closes plus current live price as today's observation.",
    ))

    rows.append(source_row(
        component="rv21d_intraday_formula",
        canonical_source=market_script,
        source_type="script+rv_history",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if rv_hits else "MISSING_OR_NOT_EXPLICIT",
        intraday_support="NEEDS_LIVE_PRICE_ADAPTATION",
        evidence=f"hits={rv_hits}; rv_history_latest={rv_summary.get('latest_date')}; rv_history_rows={rv_summary.get('rows')}",
        blocker="Intraday v1 should use last 20 completed close-to-close log returns plus current live return from prior close to T.",
        notes="Keep comparable to EOD threshold calibration unless thresholds are retested.",
    ))

    # -------------------------------------------------------------------------
    # 6. Final signal / thresholds / sizing / selection
    # -------------------------------------------------------------------------
    signal_terms = [
        "core_pass", "secondary_pass", "selected", "selected_trade",
        "model_vrp_log_final", "z_3m_final", "z_1y_final",
        "selected_size_pct", "Core", "Secondary", "rv21d_vol_pct_final"
    ]
    signal_hits = find_terms(final_text, signal_terms)
    canonical_final_summary = parquet_summary(canonical_final)
    canonical_snapshot_summary = parquet_summary(canonical_snapshot)

    rows.append(source_row(
        component="final_signal_threshold_sizing_selection",
        canonical_source=final_signal_script,
        source_type="script+canonical_final",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if final_signal_script.exists() else "MISSING",
        intraday_support="REUSABLE_FOR_INTRADAY_WITH_FIELD_REMAP",
        evidence=f"signal_hits={signal_hits}; canonical_final_latest={canonical_final_summary.get('latest_date')}; snapshot_latest={canonical_snapshot_summary.get('latest_date')}; snapshot_tenors={canonical_snapshot_summary.get('latest_tenors')}",
        blocker="Need to map intraday field names to same threshold logic without writing into official EOD history.",
        notes="Intraday output must be separate from official EOD files.",
    ))

    excerpts.append({
        "component": "final_signal_threshold_sizing_selection",
        "source": str(final_signal_script),
        "excerpt": extract_context(final_text, signal_hits),
    })

    # -------------------------------------------------------------------------
    # 7. Prior-only z-score history
    # -------------------------------------------------------------------------
    rows.append(source_row(
        component="prior_only_zscore_history",
        canonical_source=canonical_final,
        source_type="canonical_final_panel",
        required_for_true_intraday=True,
        status="LOCK_CANDIDATE_FOUND" if canonical_final.exists() else "MISSING",
        intraday_support="READY_FOR_INTRADAY_NUMERATOR_WITH_PRIOR_EOD_HISTORY",
        evidence=f"canonical_final_latest={canonical_final_summary.get('latest_date')}; rows={canonical_final_summary.get('rows')}; tenors={canonical_final_summary.get('latest_tenors')}",
        blocker="Need to ensure current live observation is not included in rolling mean/std.",
        notes="Use completed EOD history for prior 63/252 observations; live VRP only in numerator.",
    ))

    # -------------------------------------------------------------------------
    # 8. Daily wrapper / dashboard separation
    # -------------------------------------------------------------------------
    wrapper_text = read_text_safe(daily_wrapper)
    wrapper_hits = find_terms(wrapper_text, ["DAILY PRODUCTION UPDATE PASS", "PRODUCTION HEALTH", "publish_canonical_final", "zscore", "repair"])
    rows.append(source_row(
        component="eod_production_wrapper_reference",
        canonical_source=daily_wrapper,
        source_type="script",
        required_for_true_intraday=False,
        status="REFERENCE_FOUND" if daily_wrapper.exists() else "MISSING",
        intraday_support="EOD_ONLY_REFERENCE",
        evidence=f"hits={wrapper_hits}",
        blocker="Not used directly for live intraday signal generation.",
        notes="Dashboard backfill should remain EOD-only. Intraday outputs should be separate.",
    ))

    # -------------------------------------------------------------------------
    # Build outputs
    # -------------------------------------------------------------------------
    source_lock_df = pd.DataFrame(rows)

    # Conservative readiness: all required components must have source found,
    # and true intraday must not depend on an unconfirmed live option/price path.
    blocking_statuses = {"MISSING", "MISSING_OR_NOT_EXPLICIT", "PARTIAL_OR_MISSING"}
    required_blockers = source_lock_df[
        source_lock_df["required_for_true_intraday"]
        & source_lock_df["status"].isin(blocking_statuses)
    ]

    # Separate "implementation blockers" that are not missing source, but still
    # must be resolved before production intraday.
    implementation_blockers = source_lock_df[
        source_lock_df["required_for_true_intraday"]
        & source_lock_df["blocker"].astype(str).str.len().gt(0)
    ]

    locked_source_map_ready = required_blockers.empty
    true_intraday_ready_to_code = locked_source_map_ready and implementation_blockers.empty

    source_lock_path = audit_dir / f"intraday_source_lock_v1_{run_ts}.csv"
    source_lock_df.to_csv(source_lock_path, index=False)

    excerpts_df = pd.DataFrame(excerpts)
    excerpts_path = audit_dir / f"intraday_source_lock_v1_excerpts_{run_ts}.csv"
    excerpts_df.to_csv(excerpts_path, index=False)

    data_summary = {
        "implied_surface": implied_data,
        "spy_eod": parquet_summary(spy_eod),
        "rv21d": rv_summary,
        "corsi_support": parquet_summary(corsi_support),
        "latest_feature_panel": latest_feature_summary,
        "latest_forecast_panel": latest_forecast_summary,
        "canonical_final": canonical_final_summary,
        "canonical_snapshot": canonical_snapshot_summary,
        "coefficient_artifact": coef_summary,
        "sample_intraday_files": [str(p) for p in sample_intraday_files[:20]],
    }

    data_summary_path = audit_dir / f"intraday_source_lock_v1_data_summary_{run_ts}.json"
    data_summary_path.write_text(json.dumps(data_summary, indent=2, default=str), encoding="utf-8")

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "audit_dir": str(audit_dir),
        "source_lock_csv": str(source_lock_path),
        "excerpts_csv": str(excerpts_path),
        "data_summary_json": str(data_summary_path),
        "locked_source_map_ready": bool(locked_source_map_ready),
        "true_intraday_ready_to_code": bool(true_intraday_ready_to_code),
        "required_missing_or_partial_components": required_blockers["component"].tolist(),
        "implementation_blocker_components": implementation_blockers["component"].tolist(),
        "method_note": "Read-only targeted source lock. No production files modified.",
    }

    manifest_path = audit_dir / f"intraday_source_lock_v1_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    banner("Source lock assessment")
    print(source_lock_df.to_string(index=False))
    print()
    print(f"LOCKED_INTRADAY_SOURCE_MAP_READY: {locked_source_map_ready}")
    print(f"TRUE_INTRADAY_READY_TO_CODE: {true_intraday_ready_to_code}")

    if not required_blockers.empty:
        print("\nMissing / partial required source components:")
        print(required_blockers[["component", "status", "blocker"]].to_string(index=False))

    if not implementation_blockers.empty:
        print("\nImplementation blockers requiring decision before true intraday code:")
        print(implementation_blockers[["component", "intraday_support", "blocker"]].to_string(index=False))

    banner("Saved audit outputs")
    print(f"source_lock:   {source_lock_path}")
    print(f"excerpts:      {excerpts_path}")
    print(f"data_summary:  {data_summary_path}")
    print(f"manifest:      {manifest_path}")

    banner("Final result")
    print(f"LOCKED_INTRADAY_SOURCE_MAP_READY: {locked_source_map_ready}")
    print(f"TRUE_INTRADAY_READY_TO_CODE: {true_intraday_ready_to_code}")
    print("DONE — targeted intraday source lock complete.")


if __name__ == "__main__":
    main()
