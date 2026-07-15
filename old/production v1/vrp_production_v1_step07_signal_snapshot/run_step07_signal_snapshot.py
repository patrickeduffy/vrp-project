from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

MODEL_LABEL = "locked_2621_win_band_25bps_conditional"
TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
WIN_BAND_BPS = 25
WIN_BAND_DECIMAL = WIN_BAND_BPS / 10000.0

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

# Embedded group-level summary from the locked methodology report.
# This is only a fallback for ranking when no exact priority metric file is supplied/found.
EMBEDDED_GROUP_METRICS = {
    ("Core", "back"): {"conditional_win_probability": 0.8884, "conditional_avg_pnl_per_day": 470.13, "aggregate_pnl_per_day": 470.13},
    ("Core", "middle"): {"conditional_win_probability": 0.8615, "conditional_avg_pnl_per_day": 495.01, "aggregate_pnl_per_day": 495.01},
    ("Core", "front"): {"conditional_win_probability": 0.7938, "conditional_avg_pnl_per_day": 436.10, "aggregate_pnl_per_day": 436.10},
    ("Secondary", "back"): {"conditional_win_probability": 0.7805, "conditional_avg_pnl_per_day": 197.15, "aggregate_pnl_per_day": 197.15},
    ("Secondary", "middle"): {"conditional_win_probability": 0.8000, "conditional_avg_pnl_per_day": 239.42, "aggregate_pnl_per_day": 239.42},
    ("Secondary", "front"): {"conditional_win_probability": 0.7677, "conditional_avg_pnl_per_day": 328.23, "aggregate_pnl_per_day": 328.23},
}

REQUIRED_FEATURE_COLS = [
    "trade_date", "tenor", "tenor_group", "spx_close", "implied_variance", "vix_style_vol",
    "forecast_variance", "forecast_vol", "vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14",
]

DISPLAY_COLS = [
    "trade_date", "tenor", "tenor_group", "spx_close", "vix_style_vol", "forecast_vol",
    "implied_variance", "forecast_variance", "vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14",
    "core_pass", "secondary_pass", "selected", "selected_layer", "risk_fraction", "max_risk_dollars",
    "core_failed_checks", "secondary_failed_checks",
]


def yyyymmdd_to_iso(x: int) -> str:
    return datetime.strptime(str(int(x)), "%Y%m%d").strftime("%Y-%m-%d")


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


def check_threshold(row: pd.Series, thresholds: Dict[str, float]) -> Tuple[bool, List[str]]:
    failed: List[str] = []
    checks = {
        "vrp_log": float(row["vrp_log"]) >= thresholds["vrp_log"],
        "vrp_z_3m": float(row["vrp_z_3m"]) >= thresholds["vrp_z_3m"],
        "vrp_z_1y": float(row["vrp_z_1y"]) >= thresholds["vrp_z_1y"],
        "rsi14": float(row["rsi14"]) <= thresholds["rsi14_cap"],
        "rv21d": float(row["rv21d"]) >= thresholds["rv21d_floor"],
    }
    if not checks["vrp_log"]:
        failed.append(f"vrp_log {row['vrp_log']:.4f} < {thresholds['vrp_log']:.2f}")
    if not checks["vrp_z_3m"]:
        failed.append(f"vrp_z_3m {row['vrp_z_3m']:.4f} < {thresholds['vrp_z_3m']:.2f}")
    if not checks["vrp_z_1y"]:
        failed.append(f"vrp_z_1y {row['vrp_z_1y']:.4f} < {thresholds['vrp_z_1y']:.2f}")
    if not checks["rsi14"]:
        failed.append(f"rsi14 {row['rsi14']:.2f} > {thresholds['rsi14_cap']:.1f}")
    if not checks["rv21d"]:
        failed.append(f"rv21d {row['rv21d']:.2f} < {thresholds['rv21d_floor']:.1f}")
    return all(checks.values()), failed


def find_col(columns: List[str], options: List[str]) -> Optional[str]:
    lower_map = {c.lower().strip(): c for c in columns}
    for opt in options:
        if opt.lower() in lower_map:
            return lower_map[opt.lower()]
    # fuzzy containment fallback
    for c in columns:
        cl = c.lower().strip()
        for opt in options:
            if opt.lower() in cl:
                return c
    return None


def parse_pct_or_float(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        cleaned = s.astype(str).str.replace("%", "", regex=False).str.replace("$", "", regex=False).str.replace(",", "", regex=False).str.strip()
        out = pd.to_numeric(cleaned, errors="coerce")
        # If looks like whole-percent win rate, convert to decimal.
        if out.dropna().gt(1.0).any() and out.dropna().le(100).all():
            out = out / 100.0
        return out
    out = pd.to_numeric(s, errors="coerce")
    if out.dropna().gt(1.0).any() and out.dropna().le(100).all():
        out = out / 100.0
    return out


def load_ranking_metrics(project_root: Path, explicit_path: Optional[str]) -> Tuple[pd.DataFrame, str, bool]:
    """Return ranking metrics by layer and tenor_group/tenor.

    Exact ranking would use the model's conditional win-probability and P&L/day by active layer/tenor.
    This loader supports an explicit metrics CSV/parquet if available. Otherwise it falls back to
    embedded group-level metrics from the locked report.
    """
    candidates: List[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    audit_dir = project_root / "data" / "audit"
    processed_dir = project_root / "data" / "processed"
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*metrics*.csv")))
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*summary*.csv")))
    candidates.extend(sorted(audit_dir.glob("locked_2621_win_band_25bps_conditional*tenor*.csv")))
    candidates.extend(sorted(processed_dir.glob("locked_2621_win_band_25bps_conditional*metrics*.parquet")))
    candidates.extend(sorted(processed_dir.glob("locked_2621_win_band_25bps_conditional*summary*.parquet")))

    tried: List[str] = []
    for path in candidates:
        if not path.exists():
            continue
        tried.append(str(path))
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
    fallback = pd.DataFrame(rows)
    source = "embedded_group_level_locked_report_fallback"
    return fallback, source, False


def attach_ranking_metrics(df: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["layer_for_ranking"] = out["candidate_layer"]

    by_tenor = metrics[metrics["tenor"].notna()].copy()
    by_group = metrics[metrics["tenor"].isna()].copy()
    if not by_tenor.empty:
        out = out.merge(
            by_tenor[["layer", "tenor", "conditional_win_probability", "conditional_avg_pnl_per_day", "aggregate_pnl_per_day"]],
            left_on=["candidate_layer", "tenor"], right_on=["layer", "tenor"], how="left", suffixes=("", "_metric")
        )
        out = out.drop(columns=[c for c in ["layer"] if c in out.columns])
    else:
        out["conditional_win_probability"] = np.nan
        out["conditional_avg_pnl_per_day"] = np.nan
        out["aggregate_pnl_per_day"] = np.nan

    missing = out["conditional_win_probability"].isna()
    if missing.any() and not by_group.empty:
        group_metrics = by_group[["layer", "tenor_group", "conditional_win_probability", "conditional_avg_pnl_per_day", "aggregate_pnl_per_day"]].copy()
        group_metrics = group_metrics.rename(columns={
            "conditional_win_probability": "group_conditional_win_probability",
            "conditional_avg_pnl_per_day": "group_conditional_avg_pnl_per_day",
            "aggregate_pnl_per_day": "group_aggregate_pnl_per_day",
        })
        out = out.merge(group_metrics, left_on=["candidate_layer", "tenor_group"], right_on=["layer", "tenor_group"], how="left")
        out["conditional_win_probability"] = out["conditional_win_probability"].fillna(out["group_conditional_win_probability"])
        out["conditional_avg_pnl_per_day"] = out["conditional_avg_pnl_per_day"].fillna(out["group_conditional_avg_pnl_per_day"])
        out["aggregate_pnl_per_day"] = out["aggregate_pnl_per_day"].fillna(out["group_aggregate_pnl_per_day"])
        out = out.drop(columns=[c for c in ["layer", "group_conditional_win_probability", "group_conditional_avg_pnl_per_day", "group_aggregate_pnl_per_day"] if c in out.columns])

    return out


def select_trade(df: pd.DataFrame, metrics: pd.DataFrame, ranking_source_is_exact: bool) -> Tuple[Optional[pd.Series], pd.DataFrame, Dict[str, Any]]:
    core = df[df["core_pass"]].copy()
    secondary = df[df["secondary_pass"]].copy()

    if not core.empty:
        active_layer = "Core"
        candidates = core.copy()
    elif not secondary.empty:
        active_layer = "Secondary"
        candidates = secondary.copy()
    else:
        info = {
            "selected_trade_flag": False,
            "active_layer": None,
            "core_candidates": [],
            "secondary_candidates": [],
            "selected_reason": "No Core or Secondary tenor passed all locked thresholds.",
            "ranking_exact": bool(ranking_source_is_exact),
        }
        return None, pd.DataFrame(), info

    candidates["candidate_layer"] = active_layer
    candidates = attach_ranking_metrics(candidates, metrics)

    if candidates["conditional_win_probability"].isna().any():
        raise ValueError("At least one candidate is missing ranking metrics. Provide --ranking-metrics-file or add locked summary files to data/audit.")

    best_win = float(candidates["conditional_win_probability"].max())
    near = candidates[candidates["conditional_win_probability"] >= best_win - WIN_BAND_DECIMAL].copy()
    near = near.sort_values(
        ["conditional_avg_pnl_per_day", "aggregate_pnl_per_day", "conditional_win_probability", "tenor"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    selected = near.iloc[0].copy()

    info = {
        "selected_trade_flag": True,
        "active_layer": active_layer,
        "core_candidates": core["tenor"].astype(int).tolist(),
        "secondary_candidates": secondary["tenor"].astype(int).tolist(),
        "active_candidates": candidates["tenor"].astype(int).tolist(),
        "near_win_band_candidates": near["tenor"].astype(int).tolist(),
        "best_conditional_win_probability": best_win,
        "win_band_bps": WIN_BAND_BPS,
        "selected_reason": (
            f"{active_layer} candidate selected using locked ranking: keep tenors within {WIN_BAND_BPS} bps "
            "of best conditional win probability, then choose highest conditional average P&L/day; "
            "remaining ties by aggregate P&L/day, win probability, then longer tenor."
        ),
        "ranking_exact": bool(ranking_source_is_exact),
    }
    return selected, candidates, info


def build_snapshot(project_root: Path, signal_date: Optional[int], nav: float, ranking_metrics_file: Optional[str]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    processed_dir = project_root / "data" / "processed"
    feature_path = processed_dir / "production_feature_panel_v0_1.parquet"
    if not feature_path.exists():
        raise FileNotFoundError(f"Feature panel not found: {feature_path}")
    df = pd.read_parquet(feature_path)
    missing_cols = [c for c in REQUIRED_FEATURE_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Feature panel missing required columns: {missing_cols}")

    if signal_date is None:
        signal_date = int(pd.to_numeric(df["trade_date"], errors="coerce").max())
    else:
        signal_date = int(signal_date)

    day = df[df["trade_date"].astype(int) == signal_date].copy()
    if day.empty:
        raise ValueError(f"No feature rows found for signal_date={signal_date}")
    day["tenor"] = pd.to_numeric(day["tenor"], errors="coerce").astype(int)
    day = day.sort_values("tenor").reset_index(drop=True)
    if day["tenor"].tolist() != TARGET_TENORS:
        raise ValueError(f"Expected tenors {TARGET_TENORS}; found {day['tenor'].tolist()}")

    day["tenor_group"] = [normalize_bucket(g, t) for g, t in zip(day["tenor_group"], day["tenor"])]
    # Use numeric versions for safety.
    for col in ["spx_close", "vix_style_vol", "forecast_vol", "implied_variance", "forecast_variance", "vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"]:
        day[col] = pd.to_numeric(day[col], errors="coerce")
    if day[["vrp_log", "vrp_z_3m", "vrp_z_1y", "rv21d", "rsi14"]].isna().any().any():
        raise ValueError("Signal feature rows contain NaN in required signal fields.")

    core_passes = []
    secondary_passes = []
    core_failed = []
    secondary_failed = []
    for _, row in day.iterrows():
        bucket = row["tenor_group"]
        cp, cf = check_threshold(row, CORE_THRESHOLDS[bucket])
        sp, sf = check_threshold(row, SECONDARY_THRESHOLDS[bucket])
        core_passes.append(cp)
        secondary_passes.append(sp)
        core_failed.append("; ".join(cf) if cf else "PASS")
        secondary_failed.append("; ".join(sf) if sf else "PASS")

    snapshot = day.copy()
    snapshot["core_pass"] = core_passes
    snapshot["secondary_pass"] = secondary_passes
    snapshot["core_failed_checks"] = core_failed
    snapshot["secondary_failed_checks"] = secondary_failed
    snapshot["selected"] = False
    snapshot["selected_layer"] = None
    snapshot["risk_fraction"] = np.nan
    snapshot["max_risk_dollars"] = np.nan
    snapshot["portfolio_approval_required"] = False
    snapshot["model_label"] = MODEL_LABEL

    metrics, ranking_source, ranking_exact = load_ranking_metrics(project_root, ranking_metrics_file)
    selected, active_candidates_df, info = select_trade(snapshot, metrics, ranking_exact)

    if selected is not None:
        sel_tenor = int(selected["tenor"])
        sel_layer = str(selected["candidate_layer"])
        sel_bucket = str(selected["tenor_group"])
        risk_fraction = SIZING[(sel_layer, sel_bucket)]
        snapshot.loc[snapshot["tenor"].astype(int) == sel_tenor, "selected"] = True
        snapshot.loc[snapshot["tenor"].astype(int) == sel_tenor, "selected_layer"] = sel_layer
        snapshot.loc[snapshot["tenor"].astype(int) == sel_tenor, "risk_fraction"] = risk_fraction
        snapshot.loc[snapshot["tenor"].astype(int) == sel_tenor, "max_risk_dollars"] = nav * risk_fraction
        snapshot.loc[snapshot["tenor"].astype(int) == sel_tenor, "portfolio_approval_required"] = True
        summary = {
            "selected_trade_flag": True,
            "selected_tenor": sel_tenor,
            "selected_bucket": sel_bucket,
            "selected_layer": sel_layer,
            "risk_fraction": risk_fraction,
            "max_risk_dollars": nav * risk_fraction,
            "conditional_win_probability": float(selected.get("conditional_win_probability", np.nan)),
            "conditional_avg_pnl_per_day": float(selected.get("conditional_avg_pnl_per_day", np.nan)),
            "aggregate_pnl_per_day": float(selected.get("aggregate_pnl_per_day", np.nan)),
        }
    else:
        summary = {
            "selected_trade_flag": False,
            "selected_tenor": None,
            "selected_bucket": None,
            "selected_layer": None,
            "risk_fraction": None,
            "max_risk_dollars": None,
            "conditional_win_probability": None,
            "conditional_avg_pnl_per_day": None,
            "aggregate_pnl_per_day": None,
        }

    summary.update({
        "run_type": "official_eod_signal_snapshot",
        "model_label": MODEL_LABEL,
        "signal_date": signal_date,
        "signal_date_iso": yyyymmdd_to_iso(signal_date),
        "nav": nav,
        "feature_panel_path": str(feature_path),
        "feature_panel_rows": int(len(df)),
        "feature_panel_latest_trade_date": int(pd.to_numeric(df["trade_date"], errors="coerce").max()),
        "snapshot_rows": int(len(snapshot)),
        "core_candidate_count": int(snapshot["core_pass"].sum()),
        "secondary_candidate_count": int(snapshot["secondary_pass"].sum()),
        "core_candidates": snapshot.loc[snapshot["core_pass"], "tenor"].astype(int).tolist(),
        "secondary_candidates": snapshot.loc[snapshot["secondary_pass"], "tenor"].astype(int).tolist(),
        "selected_reason": info["selected_reason"],
        "ranking_metrics_source": ranking_source,
        "ranking_exact_from_external_file": bool(ranking_exact),
        "portfolio_approval_status": "MANUAL_REVIEW_REQUIRED" if summary["selected_trade_flag"] else "NO_TRADE",
        "sizing_interpretation": "max-risk at inception; not premium, not margin, not expected loss, not portfolio-level cap",
        "all_checks_green": True,
    })

    return snapshot, active_candidates_df, summary


def write_outputs(project_root: Path, snapshot: pd.DataFrame, active_candidates: pd.DataFrame, summary: Dict[str, Any]) -> Tuple[Path, Path, Path, Path]:
    processed_dir = project_root / "data" / "processed"
    signal_dir = processed_dir / "signals"
    audit_dir = project_root / "data" / "audit" / "production_v1"
    signal_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    signal_date = int(summary["signal_date"])
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    snapshot_base = signal_dir / f"locked_2621_eod_signal_snapshot_{signal_date}"
    summary_base = signal_dir / f"locked_2621_eod_signal_summary_{signal_date}"
    candidate_base = signal_dir / f"locked_2621_eod_active_candidates_{signal_date}"

    snapshot_out = snapshot.copy()
    cols = [c for c in DISPLAY_COLS if c in snapshot_out.columns] + [c for c in snapshot_out.columns if c not in DISPLAY_COLS]
    snapshot_out = snapshot_out[cols]
    snapshot_csv = snapshot_base.with_suffix(".csv")
    snapshot_parquet = snapshot_base.with_suffix(".parquet")
    snapshot_out.to_csv(snapshot_csv, index=False)
    snapshot_out.to_parquet(snapshot_parquet, index=False)

    summary_csv = summary_base.with_suffix(".csv")
    summary_json = summary_base.with_suffix(".json")
    pd.DataFrame([summary]).to_csv(summary_csv, index=False)
    with open(summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    if not active_candidates.empty:
        active_candidates.to_csv(candidate_base.with_suffix(".csv"), index=False)
        active_candidates.to_parquet(candidate_base.with_suffix(".parquet"), index=False)

    md_path = audit_dir / f"step07_signal_snapshot_{run_id}.md"
    audit_json = audit_dir / f"step07_signal_snapshot_{run_id}.json"
    with open(audit_json, "w", encoding="utf-8") as f:
        payload = {"summary": summary, "snapshot_csv": str(snapshot_csv), "snapshot_parquet": str(snapshot_parquet), "summary_csv": str(summary_csv), "summary_json": str(summary_json)}
        json.dump(payload, f, indent=2, default=str)

    report_cols = [
        "tenor", "tenor_group", "vix_style_vol", "forecast_vol", "vrp_log", "vrp_z_3m", "vrp_z_1y",
        "rv21d", "rsi14", "core_pass", "secondary_pass", "selected", "selected_layer", "risk_fraction",
        "core_failed_checks", "secondary_failed_checks"
    ]
    report_df = snapshot_out[[c for c in report_cols if c in snapshot_out.columns]].copy()

    lines = []
    lines.append("# VRP Production v1 - Step 07 Locked 2621 EOD Signal Snapshot")
    lines.append("")
    lines.append(f"Run timestamp: `{run_id}`")
    lines.append(f"Signal date: `{summary['signal_date_iso']}` / `{summary['signal_date']}`")
    lines.append(f"Model label: `{MODEL_LABEL}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **selected_trade_flag**: `{summary['selected_trade_flag']}`")
    lines.append(f"- **selected_layer**: `{summary['selected_layer']}`")
    lines.append(f"- **selected_tenor**: `{summary['selected_tenor']}`")
    lines.append(f"- **selected_bucket**: `{summary['selected_bucket']}`")
    lines.append(f"- **core_candidates**: `{summary['core_candidates']}`")
    lines.append(f"- **secondary_candidates**: `{summary['secondary_candidates']}`")
    lines.append(f"- **risk_fraction**: `{summary['risk_fraction']}`")
    lines.append(f"- **max_risk_dollars**: `{summary['max_risk_dollars']}`")
    lines.append(f"- **portfolio_approval_status**: `{summary['portfolio_approval_status']}`")
    lines.append(f"- **ranking_metrics_source**: `{summary['ranking_metrics_source']}`")
    lines.append(f"- **ranking_exact_from_external_file**: `{summary['ranking_exact_from_external_file']}`")
    lines.append("")
    lines.append("## Selected reason")
    lines.append("")
    lines.append(str(summary["selected_reason"]))
    lines.append("")
    lines.append("## Tenor signal table")
    lines.append("")
    lines.append(report_df.to_markdown(index=False))
    lines.append("")
    lines.append("## Output files")
    lines.append("")
    lines.append(f"- Snapshot CSV: `{snapshot_csv}`")
    lines.append(f"- Snapshot parquet: `{snapshot_parquet}`")
    lines.append(f"- Summary CSV: `{summary_csv}`")
    lines.append(f"- Summary JSON: `{summary_json}`")
    lines.append("")
    lines.append("## Sizing note")
    lines.append("")
    lines.append("Risk fraction is max-risk at inception. It is not premium, margin, expected loss, or portfolio-level risk. Portfolio approval remains manual.")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return snapshot_csv, snapshot_parquet, summary_json, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build locked 2621 EOD signal snapshot from production feature panel.")
    parser.add_argument("--project-root", required=True, help="VRP project root, e.g. C:\\Users\\patri\\vrp_project")
    parser.add_argument("--signal-date", type=int, default=None, help="YYYYMMDD signal date. If omitted, latest trade_date is used.")
    parser.add_argument("--nav", type=float, default=1_000_000.0, help="Sizing NAV for max-risk dollars. Default 1,000,000.")
    parser.add_argument("--ranking-metrics-file", default=None, help="Optional explicit CSV/parquet with locked ranking metrics.")
    args = parser.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    print("Loading production feature panel and building signal snapshot...")
    snapshot, active_candidates, summary = build_snapshot(project_root, args.signal_date, args.nav, args.ranking_metrics_file)
    print("Writing signal outputs and audit report...")
    snapshot_csv, snapshot_parquet, summary_json, md_path = write_outputs(project_root, snapshot, active_candidates, summary)

    print("\nStep 07 signal snapshot complete.")
    print(f"Signal date:         {summary['signal_date']}")
    print(f"Snapshot rows:       {summary['snapshot_rows']}")
    print(f"Core candidates:     {summary['core_candidates']}")
    print(f"Secondary candidates:{summary['secondary_candidates']}")
    print(f"Selected trade:      {summary['selected_trade_flag']}")
    print(f"Selected layer:      {summary['selected_layer']}")
    print(f"Selected tenor:      {summary['selected_tenor']}")
    print(f"Selected bucket:     {summary['selected_bucket']}")
    print(f"Risk fraction:       {summary['risk_fraction']}")
    print(f"Max risk dollars:    {summary['max_risk_dollars']}")
    print(f"Portfolio status:    {summary['portfolio_approval_status']}")
    print(f"All checks green:    {summary['all_checks_green']}")
    print(f"Snapshot CSV:        {snapshot_csv}")
    print(f"Summary JSON:        {summary_json}")
    print(f"Report:              {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
