
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

LOCKED_FEATURES = [
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


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize_trade_date_series(s: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().mean() > 0.8 and numeric.dropna().between(19000101, 22000101).mean() > 0.8:
        return pd.to_datetime(
            numeric.astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        ).dt.strftime("%Y%m%d").astype("Int64")

    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().mean() < 0.8:
        raise RuntimeError("Could not parse trade_date column.")
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def latest_file(base_dir: Path, pattern: str, trade_date: int | None = None) -> Path:
    if not base_dir.exists():
        raise FileNotFoundError(f"Directory does not exist: {base_dir}")

    paths = list(base_dir.glob(pattern))
    if trade_date is not None:
        token = str(int(trade_date))
        filtered = [p for p in paths if token in p.name]
        if filtered:
            paths = filtered

    if not paths:
        raise FileNotFoundError(f"No files found: {base_dir} / {pattern}")

    return sorted(paths, key=lambda p: (p.stat().st_mtime, str(p)))[-1]


def load_intraday_feature_panel(project_root: Path, trade_date: int | None, explicit_path: str | None) -> tuple[pd.DataFrame, Path]:
    if explicit_path:
        path = Path(explicit_path)
    else:
        path = latest_file(
            project_root / "data" / "audit" / "intraday_feature_vector",
            "intraday_feature_vector_smoke_panel_*.csv",
            trade_date=trade_date,
        )

    df = pd.read_csv(path)

    missing = [c for c in LOCKED_FEATURES if c not in df.columns]
    if missing:
        raise RuntimeError(f"Feature panel missing locked features: {missing}")

    if "tenor" not in df.columns:
        raise RuntimeError(f"Feature panel missing tenor column. Columns: {list(df.columns)}")

    if "trade_date" not in df.columns:
        raise RuntimeError(f"Feature panel missing trade_date column. Columns: {list(df.columns)}")

    df["trade_date"] = normalize_trade_date_series(df["trade_date"]).astype(int)
    df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")

    for c in LOCKED_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df, path


def load_intraday_implied_variance(project_root: Path, trade_date: int | None, explicit_path: str | None) -> tuple[pd.DataFrame, Path]:
    if explicit_path:
        path = Path(explicit_path)
    else:
        path = latest_file(
            project_root / "data" / "audit" / "intraday_implied_variance",
            "intraday_implied_variance_smoke_tenor_panel_*.csv",
            trade_date=trade_date,
        )

    df = pd.read_csv(path)

    if "tenor" not in df.columns:
        if "target_days" in df.columns:
            df["tenor"] = df["target_days"]
        else:
            raise RuntimeError(f"Implied variance panel missing tenor/target_days. Columns: {list(df.columns)}")

    if "trade_date" not in df.columns:
        raise RuntimeError(f"Implied variance panel missing trade_date column. Columns: {list(df.columns)}")

    if "implied_variance_intraday" not in df.columns:
        raise RuntimeError(f"Implied variance panel missing implied_variance_intraday. Columns: {list(df.columns)}")

    df["trade_date"] = normalize_trade_date_series(df["trade_date"]).astype(int)
    df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")
    df["implied_variance_intraday"] = pd.to_numeric(df["implied_variance_intraday"], errors="coerce")

    if "vix_style_vol_intraday" in df.columns:
        df["vix_style_vol_intraday"] = pd.to_numeric(df["vix_style_vol_intraday"], errors="coerce")
    else:
        df["vix_style_vol_intraday"] = np.sqrt(df["implied_variance_intraday"]) * 100.0

    return df, path


def load_final_history(project_root: Path) -> tuple[pd.DataFrame, Path]:
    path = project_root / "data" / "processed" / "vrp_final_signal" / "vrp_final_corsi_signal_base_panel_v1.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing canonical final signal history: {path}")

    df = pd.read_parquet(path)

    required = ["trade_date", "tenor", "model_vrp_log_final"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Final history missing required columns {missing}. Columns: {list(df.columns)}")

    df = df.copy()
    df["trade_date"] = normalize_trade_date_series(df["trade_date"]).astype(int)
    df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")
    df["model_vrp_log_final"] = pd.to_numeric(df["model_vrp_log_final"], errors="coerce")

    df = (
        df.dropna(subset=["trade_date", "tenor", "model_vrp_log_final"])
        .sort_values(["tenor", "trade_date"])
        .drop_duplicates(["trade_date", "tenor"], keep="last")
        .reset_index(drop=True)
    )

    return df, path


def artifact_keys(obj: Any) -> list[str]:
    if isinstance(obj, dict):
        return sorted(map(str, obj.keys()))
    return []


def normalize_tenor_key(k: Any) -> int | None:
    try:
        return int(k)
    except Exception:
        pass

    try:
        text = str(k)
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return int(digits)
    except Exception:
        pass

    return None


def find_models_by_tenor(artifact: Any) -> dict[int, Any]:
    if not isinstance(artifact, dict):
        raise RuntimeError(f"Unsupported artifact type: {type(artifact)}")

    candidate_keys = [
        "models_by_tenor",
        "tenor_models",
        "models",
        "artifacts_by_tenor",
        "pipelines_by_tenor",
        "model_by_tenor",
        "tenor_artifacts",
    ]

    for key in candidate_keys:
        if key in artifact and isinstance(artifact[key], dict):
            raw = artifact[key]
            out = {}
            for k, v in raw.items():
                tenor = normalize_tenor_key(k)
                if tenor is not None:
                    out[tenor] = v
            if out:
                return out

    # Fallback: top-level tenor-keyed dict.
    out = {}
    for k, v in artifact.items():
        tenor = normalize_tenor_key(k)
        if tenor is not None and tenor in EXPECTED_TENORS:
            out[tenor] = v

    if out:
        return out

    raise RuntimeError(
        "Could not find tenor model dictionary in artifact. "
        f"Top-level keys: {artifact_keys(artifact)}"
    )


def find_feature_names(artifact: Any, model_container: Any | None = None) -> list[str]:
    candidate_keys = [
        "locked_features",
        "feature_names",
        "features",
        "model_features",
        "input_features",
        "feature_columns",
    ]

    if isinstance(model_container, dict):
        for k in candidate_keys:
            if k in model_container and isinstance(model_container[k], (list, tuple)):
                return list(model_container[k])

    if isinstance(artifact, dict):
        for k in candidate_keys:
            if k in artifact and isinstance(artifact[k], (list, tuple)):
                return list(artifact[k])

    return LOCKED_FEATURES.copy()


def predict_from_container(model_container: Any, xdf: pd.DataFrame) -> float:
    # Direct sklearn Pipeline / estimator.
    if hasattr(model_container, "predict"):
        pred = model_container.predict(xdf)
        return float(np.asarray(pred).reshape(-1)[0])

    if not isinstance(model_container, dict):
        raise RuntimeError(f"Unsupported model container type: {type(model_container)}")

    for key in ["pipeline", "model", "estimator", "fitted_model", "sklearn_pipeline"]:
        if key in model_container and hasattr(model_container[key], "predict"):
            pred = model_container[key].predict(xdf)
            return float(np.asarray(pred).reshape(-1)[0])

    scaler = None
    ridge = None

    for key in ["scaler", "standard_scaler", "standardizer"]:
        if key in model_container:
            scaler = model_container[key]
            break

    for key in ["ridge", "ridge_model", "regressor", "estimator", "model"]:
        if key in model_container and hasattr(model_container[key], "predict"):
            ridge = model_container[key]
            break

    if scaler is not None and ridge is not None:
        x_scaled = scaler.transform(xdf)
        pred = ridge.predict(x_scaled)
        return float(np.asarray(pred).reshape(-1)[0])

    raise RuntimeError(f"Could not score model container. Keys: {artifact_keys(model_container)}")


def score_live_panel(live: pd.DataFrame, artifact: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    models_by_tenor = find_models_by_tenor(artifact)

    rows = []
    summary_rows = []

    for _, row in live.sort_values("tenor").iterrows():
        tenor = int(row["tenor"])

        if tenor not in models_by_tenor:
            raise RuntimeError(f"Model artifact missing tenor {tenor}. Available: {sorted(models_by_tenor)}")

        container = models_by_tenor[tenor]
        features = find_feature_names(artifact, container)

        missing = [c for c in features if c not in live.columns]
        if missing:
            raise RuntimeError(f"Live panel missing model features for tenor {tenor}: {missing}")

        xdf = pd.DataFrame([{c: float(row[c]) for c in features}], columns=features)

        pred_log = predict_from_container(container, xdf)
        forecast_var = float(math.exp(pred_log))
        forecast_vol = float(math.sqrt(forecast_var) * 100.0)

        out = row.to_dict()
        out["predicted_log_variance_candidate"] = pred_log
        out["forecast_variance_candidate"] = forecast_var
        out["candidate_forecast_vol_pct"] = forecast_vol
        out["model_artifact_feature_count"] = len(features)
        out["model_artifact_features"] = "|".join(features)

        rows.append(out)

        summary_rows.append({
            "tenor": tenor,
            "model_container_type": str(type(container)),
            "model_container_keys": "|".join(artifact_keys(container)),
            "feature_count": len(features),
            "features": "|".join(features),
            "predicted_log_variance_candidate": pred_log,
            "forecast_variance_candidate": forecast_var,
            "candidate_forecast_vol_pct": forecast_vol,
        })

    return pd.DataFrame(rows), pd.DataFrame(summary_rows)


def compute_prior_only_zscores(
    *,
    live_panel: pd.DataFrame,
    history: pd.DataFrame,
    trade_date: int,
    windows: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    audit_rows = []

    hist = history[history["trade_date"] < int(trade_date)].copy()

    for _, row in live_panel.sort_values("tenor").iterrows():
        tenor = int(row["tenor"])
        current = float(row["model_vrp_log_intraday"])

        tenor_hist = (
            hist[hist["tenor"].astype(int).eq(tenor)]
            .dropna(subset=["model_vrp_log_final"])
            .sort_values("trade_date")
            .copy()
        )

        out = row.to_dict()

        for label, window in windows.items():
            tail = tenor_hist.tail(window)
            n = int(len(tail))
            mean = float(tail["model_vrp_log_final"].mean()) if n else np.nan
            std = float(tail["model_vrp_log_final"].std(ddof=1)) if n > 1 else np.nan
            z = float((current - mean) / std) if np.isfinite(std) and std > 0 else np.nan

            out[f"z_{label}_intraday"] = z

            audit_rows.append({
                "tenor": tenor,
                "z_label": label,
                "window": int(window),
                "history_rows_available": int(len(tenor_hist)),
                "history_rows_used": n,
                "first_history_date_used": int(tail["trade_date"].min()) if n else None,
                "last_history_date_used": int(tail["trade_date"].max()) if n else None,
                "prior_mean": mean,
                "prior_std_ddof1": std,
                "current_model_vrp_log_intraday": current,
                "z_value": z,
            })

        rows.append(out)

    return pd.DataFrame(rows), pd.DataFrame(audit_rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--trade-date", default=None, help="YYYYMMDD. Defaults to trade_date in latest audit panels.")
    p.add_argument("--feature-panel", default=None, help="Optional explicit intraday feature smoke CSV.")
    p.add_argument("--implied-panel", default=None, help="Optional explicit intraday implied variance smoke CSV.")
    p.add_argument("--model-artifact", default=None, help="Optional explicit model artifact path.")
    args = p.parse_args()

    project_root = Path(args.project_root)
    run_ts = now_stamp()
    trade_date_arg = int(args.trade_date) if args.trade_date else None

    audit_dir = project_root / "data" / "audit" / "intraday_forecast_vrp"
    audit_dir.mkdir(parents=True, exist_ok=True)

    banner("VRP intraday forecast / VRP smoke v1")
    print(f"Project root:  {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Audit dir:     {audit_dir}")
    print("Mode:          audit-only forecast/VRP smoke; no production signal files modified")

    banner("Load inputs")
    feature_panel, feature_path = load_intraday_feature_panel(project_root, trade_date_arg, args.feature_panel)
    implied_panel, implied_path = load_intraday_implied_variance(project_root, trade_date_arg, args.implied_panel)

    if trade_date_arg is None:
        feature_dates = sorted(feature_panel["trade_date"].dropna().astype(int).unique().tolist())
        implied_dates = sorted(implied_panel["trade_date"].dropna().astype(int).unique().tolist())
        common = sorted(set(feature_dates).intersection(implied_dates))
        if not common:
            raise RuntimeError(f"No common trade_date between feature panel {feature_dates} and implied panel {implied_dates}")
        trade_date = int(common[-1])
    else:
        trade_date = int(trade_date_arg)

    feature_panel = feature_panel[feature_panel["trade_date"].eq(trade_date)].copy()
    implied_panel = implied_panel[implied_panel["trade_date"].eq(trade_date)].copy()

    print(f"Feature panel: {feature_path}")
    print(f"Feature rows:  {len(feature_panel):,}")
    print(f"Implied panel: {implied_path}")
    print(f"Implied rows:  {len(implied_panel):,}")
    print(f"Trade date:    {trade_date}")

    if "quote_time" in feature_panel.columns and "quote_time" in implied_panel.columns:
        feature_q = sorted(feature_panel["quote_time"].dropna().astype(str).unique().tolist())
        implied_q = sorted(implied_panel["quote_time"].dropna().astype(str).unique().tolist())
        if feature_q and implied_q and feature_q != implied_q:
            raise RuntimeError(f"Quote-time mismatch. feature={feature_q}, implied={implied_q}")
        quote_time = feature_q[0] if feature_q else implied_q[0] if implied_q else None
    else:
        quote_time = None

    print(f"Quote time:    {quote_time}")

    artifact_path = (
        Path(args.model_artifact)
        if args.model_artifact
        else project_root / "data" / "processed" / "vrp_model_artifacts" / "unified_fds_no_min_return_model_artifacts_v1.joblib"
    )

    if not artifact_path.exists():
        raise FileNotFoundError(f"Missing model artifact: {artifact_path}")

    artifact = joblib.load(artifact_path)
    print(f"Model artifact: {artifact_path}")
    print(f"Artifact type:  {type(artifact)}")
    print(f"Artifact keys:  {artifact_keys(artifact)}")

    history, history_path = load_final_history(project_root)
    print(f"Final history:  {history_path}")
    print(f"History rows:   {len(history):,}")
    print(f"History latest: {int(history['trade_date'].max())}")

    banner("Merge live features and implied variance")
    feature_cols = [
        "trade_date",
        "quote_time",
        "tenor",
    ] + [c for c in feature_panel.columns if c not in {"trade_date", "quote_time", "tenor"}]

    feature_keep = feature_panel[feature_cols].copy()

    implied_keep_cols = [
        "trade_date",
        "quote_time",
        "tenor",
        "implied_variance_intraday",
        "vix_style_vol_intraday",
    ]
    implied_keep_cols += [
        c for c in [
            "near_root",
            "near_expiration",
            "near_days",
            "near_variance",
            "near_num_options",
            "next_root",
            "next_expiration",
            "next_days",
            "next_variance",
            "next_num_options",
        ]
        if c in implied_panel.columns
    ]

    implied_keep = implied_panel[implied_keep_cols].copy()

    merge_keys = ["trade_date", "tenor"]
    live = feature_keep.merge(
        implied_keep.drop(columns=["quote_time"], errors="ignore"),
        on=merge_keys,
        how="inner",
        validate="one_to_one",
    )

    live = live.sort_values("tenor").reset_index(drop=True)

    print(live[["trade_date", "tenor", "implied_variance_intraday", "vix_style_vol_intraday"]].to_string(index=False))

    banner("Score saved forecast model artifact")
    scored, score_summary = score_live_panel(live, artifact)

    scored["forecast_vol_intraday"] = scored["candidate_forecast_vol_pct"]
    scored["model_vrp_log_intraday"] = np.log(
        pd.to_numeric(scored["implied_variance_intraday"], errors="coerce")
        / pd.to_numeric(scored["forecast_variance_candidate"], errors="coerce")
    )

    banner("Compute prior-only z-scores")
    scored_z, zscore_audit = compute_prior_only_zscores(
        live_panel=scored,
        history=history,
        trade_date=trade_date,
        windows={"3m": 63, "1y": 252},
    )

    scored_z = scored_z.sort_values("tenor").reset_index(drop=True)

    print(scored_z[[
        "trade_date",
        "tenor",
        "implied_variance_intraday",
        "vix_style_vol_intraday",
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
        "model_vrp_log_intraday",
        "z_3m_intraday",
        "z_1y_intraday",
    ]].to_string(index=False))

    banner("Validate smoke output")
    tenors = sorted(scored_z["tenor"].dropna().astype(int).unique().tolist())

    tenors_ok = tenors == EXPECTED_TENORS and len(scored_z) == len(EXPECTED_TENORS)
    implied_ok = bool(
        np.isfinite(scored_z["implied_variance_intraday"]).all()
        and (scored_z["implied_variance_intraday"] > 0).all()
    )
    forecast_ok = bool(
        np.isfinite(scored_z["forecast_variance_candidate"]).all()
        and (scored_z["forecast_variance_candidate"] > 0).all()
        and np.isfinite(scored_z["predicted_log_variance_candidate"]).all()
        and np.isfinite(scored_z["candidate_forecast_vol_pct"]).all()
        and (scored_z["candidate_forecast_vol_pct"] > 0).all()
    )
    vrp_ok = bool(np.isfinite(scored_z["model_vrp_log_intraday"]).all())
    zscore_ok = bool(
        np.isfinite(scored_z["z_3m_intraday"]).all()
        and np.isfinite(scored_z["z_1y_intraday"]).all()
    )

    history_rows_ok = bool(
        (zscore_audit["history_rows_used"] >= zscore_audit["window"]).all()
    )

    smoke_pass = bool(tenors_ok and implied_ok and forecast_ok and vrp_ok and zscore_ok and history_rows_ok)

    print(f"tenors_ok:       {tenors_ok}")
    print(f"implied_ok:      {implied_ok}")
    print(f"forecast_ok:     {forecast_ok}")
    print(f"vrp_ok:          {vrp_ok}")
    print(f"zscore_ok:       {zscore_ok}")
    print(f"history_rows_ok: {history_rows_ok}")

    banner("Save audit outputs")
    panel_path = audit_dir / f"intraday_forecast_vrp_smoke_panel_{trade_date}_{run_ts}.csv"
    score_summary_path = audit_dir / f"intraday_forecast_vrp_smoke_score_summary_{trade_date}_{run_ts}.csv"
    zscore_audit_path = audit_dir / f"intraday_forecast_vrp_smoke_zscore_audit_{trade_date}_{run_ts}.csv"

    scored_z.to_csv(panel_path, index=False)
    score_summary.to_csv(score_summary_path, index=False)
    zscore_audit.to_csv(zscore_audit_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "trade_date": int(trade_date),
        "quote_time": quote_time,
        "inputs": {
            "feature_panel": str(feature_path),
            "implied_panel": str(implied_path),
            "model_artifact": str(artifact_path),
            "final_history": str(history_path),
        },
        "artifact_type": str(type(artifact)),
        "artifact_keys": artifact_keys(artifact),
        "checks": {
            "tenors_ok": tenors_ok,
            "implied_ok": implied_ok,
            "forecast_ok": forecast_ok,
            "vrp_ok": vrp_ok,
            "zscore_ok": zscore_ok,
            "history_rows_ok": history_rows_ok,
        },
        "audit_outputs": {
            "panel": str(panel_path),
            "score_summary": str(score_summary_path),
            "zscore_audit": str(zscore_audit_path),
        },
        "INTRADAY_FORECAST_VRP_SMOKE_PASS": smoke_pass,
        "method_note": "Audit-only intraday forecast/VRP smoke. Scores live feature vector with saved model artifact and computes prior-only z-scores from canonical EOD history. No production files modified.",
    }

    manifest_path = audit_dir / f"intraday_forecast_vrp_smoke_manifest_{trade_date}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    print(f"panel:         {panel_path}")
    print(f"score_summary: {score_summary_path}")
    print(f"zscore_audit:  {zscore_audit_path}")
    print(f"manifest:      {manifest_path}")

    banner("Final result")
    print(f"INTRADAY_FORECAST_VRP_SMOKE_PASS: {smoke_pass}")

    if not smoke_pass:
        raise RuntimeError("INTRADAY_FORECAST_VRP_SMOKE_PASS is False.")

    print("DONE — intraday forecast/VRP smoke complete.")


if __name__ == "__main__":
    main()
