
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
import joblib


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

EXPECTED_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]

DEFAULT_ALPHA_MAP = {
    9: 100.0,
    12: 100.0,
    15: 100.0,
    18: 100.0,
    21: 100.0,
    24: 100.0,
    27: 100.0,
    30: 300.0,
    33: 300.0,
}


def banner(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_date_like(s: pd.Series) -> pd.Series:
    raw = pd.Series(s, index=s.index)

    if pd.api.types.is_datetime64_any_dtype(raw):
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    if len(as_str) and as_str.str.fullmatch(r"\d{8}").mean() > 0.80:
        return pd.to_datetime(as_str, format="%Y%m%d", errors="coerce").dt.normalize()

    return pd.to_datetime(raw, errors="coerce").dt.normalize()


def add_work_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["_work_date"] = parse_date_like(out["date"])
    elif "trade_date" in out.columns:
        out["_work_date"] = parse_date_like(out["trade_date"])
    else:
        raise KeyError("No date/trade_date column found.")

    if "tenor" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["tenor"], errors="coerce").astype("Int64")
    elif "target_days" in out.columns:
        out["_work_tenor"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")
    else:
        raise KeyError("No tenor/target_days column found.")

    out["_work_trade_date"] = out["_work_date"].dt.strftime("%Y%m%d").astype("Int64")
    return out


def latest_file(folder: Path, pattern: str) -> Path | None:
    if not folder.exists():
        return None
    files = list(folder.glob(pattern))
    if not files:
        return None
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def require_path(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def normalize_col_name(c: str) -> str:
    return str(c).lower().replace(" ", "_").replace("-", "_")


def find_col(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    norm = {normalize_col_name(c): c for c in df.columns}

    for cand in candidates:
        cand_norm = normalize_col_name(cand)
        if cand_norm in norm:
            return norm[cand_norm]

    for cand in candidates:
        cand_norm = normalize_col_name(cand)
        for n, original in norm.items():
            if cand_norm in n:
                return original

    if required:
        raise KeyError(f"Could not find any candidate column {candidates}. Available: {list(df.columns)}")

    return None


def get_fit_log_maps(fit_log_path: Path, score_year: int) -> tuple[dict[int, float], dict[int, int | None], pd.DataFrame]:
    fit = pd.read_csv(fit_log_path)

    tenor_col = find_col(fit, ["tenor", "target_days"])
    year_col = find_col(fit, ["test_year", "year", "score_year", "oos_year"], required=False)
    alpha_col = find_col(fit, ["alpha", "selected_alpha", "locked_alpha", "ridge_alpha"])

    train_col = find_col(
        fit,
        [
            "train_rows",
            "expected_fit_log_train_rows",
            "outer_train_rows",
            "n_train",
            "train_n",
            "fit_train_rows",
        ],
        required=False,
    )

    fit["_work_tenor"] = pd.to_numeric(fit[tenor_col], errors="coerce").astype("Int64")

    if year_col is not None:
        fit["_work_year"] = pd.to_numeric(fit[year_col], errors="coerce").astype("Int64")
        fit_use = fit[fit["_work_year"].eq(score_year)].copy()
        if fit_use.empty:
            # Fallback: use latest year available <= score_year.
            available = sorted(fit["_work_year"].dropna().astype(int).unique().tolist())
            le = [y for y in available if y <= score_year]
            if not le:
                raise RuntimeError(f"Fit log has no usable year <= {score_year}. Available years={available}")
            use_year = max(le)
            fit_use = fit[fit["_work_year"].eq(use_year)].copy()
            print(f"WARNING: Fit log has no rows for score_year={score_year}; using latest available year={use_year}.")
    else:
        fit_use = fit.copy()

    alpha_map: dict[int, float] = {}
    expected_train_rows: dict[int, int | None] = {}

    for tenor in EXPECTED_TENORS:
        rows = fit_use[fit_use["_work_tenor"].eq(tenor)].copy()
        if rows.empty:
            alpha_map[tenor] = DEFAULT_ALPHA_MAP[tenor]
            expected_train_rows[tenor] = None
            continue

        # If multiple rows for tenor/year, use the last row after original order.
        r = rows.iloc[-1]

        alpha = float(pd.to_numeric(pd.Series([r[alpha_col]]), errors="coerce").iloc[0])
        if not np.isfinite(alpha):
            alpha = DEFAULT_ALPHA_MAP[tenor]

        alpha_map[tenor] = alpha

        if train_col is not None:
            tr = pd.to_numeric(pd.Series([r[train_col]]), errors="coerce").iloc[0]
            expected_train_rows[tenor] = int(tr) if pd.notna(tr) and np.isfinite(tr) else None
        else:
            expected_train_rows[tenor] = None

    fit_summary = pd.DataFrame(
        {
            "tenor": EXPECTED_TENORS,
            "alpha": [alpha_map[t] for t in EXPECTED_TENORS],
            "expected_train_rows": [expected_train_rows[t] for t in EXPECTED_TENORS],
        }
    )

    return alpha_map, expected_train_rows, fit_summary


def make_training_panel(
    feature_df: pd.DataFrame,
    tenor: int,
    score_year: int,
    expected_rows: int | None,
) -> pd.DataFrame:
    start = pd.Timestamp(f"{score_year}-01-01")

    df_t = feature_df[feature_df["_work_tenor"].eq(tenor)].copy()
    if df_t.empty:
        raise RuntimeError(f"No feature rows for tenor {tenor}")

    eligible = df_t["_work_date"] < start

    if "last_forward_rv_date" in df_t.columns:
        last_fwd = parse_date_like(df_t["last_forward_rv_date"])
        eligible = eligible & (last_fwd < start)

    required = LOCKED_FEATURES + ["target_log_variance"]
    missing = [c for c in required if c not in df_t.columns]
    if missing:
        raise RuntimeError(f"Feature panel missing required columns for training: {missing}")

    train = df_t.loc[eligible, ["_work_date", "_work_tenor"] + required].copy()

    for c in LOCKED_FEATURES + ["target_log_variance"]:
        train[c] = pd.to_numeric(train[c], errors="coerce")

    finite_mask = np.isfinite(train[LOCKED_FEATURES + ["target_log_variance"]]).all(axis=1)
    train = train.loc[finite_mask].sort_values("_work_date").copy()

    raw_rows = len(train)

    if expected_rows is not None:
        if raw_rows < expected_rows:
            raise RuntimeError(
                f"Tenor {tenor}: only {raw_rows} eligible train rows, expected {expected_rows}"
            )
        if raw_rows > expected_rows:
            # This matches the locked forecast updater behavior observed in audit:
            # drop earliest rows to match fit-log train row count.
            train = train.tail(expected_rows).copy()

    return train


def fit_model(train: pd.DataFrame, alpha: float) -> Pipeline:
    X = train[LOCKED_FEATURES].to_numpy(dtype=float)
    y = train["target_log_variance"].to_numpy(dtype=float)

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=alpha)),
        ]
    )

    model.fit(X, y)
    return model


def validate_models(
    *,
    models: dict[int, Pipeline],
    feature_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    score_year: int,
    tolerance_pred_log: float,
    validation_scope: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fc = forecast_df.copy()

    required_fc = [
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
    ]
    missing_fc = [c for c in required_fc if c not in fc.columns]
    if missing_fc:
        raise RuntimeError(f"Forecast panel missing validation columns: {missing_fc}")

    fc_year_all = fc[fc["_work_date"].dt.year.eq(score_year)].copy()
    if fc_year_all.empty:
        raise RuntimeError(f"No forecast panel rows found for score_year={score_year}")

    if validation_scope == "latest_only":
        latest_date = fc_year_all["_work_date"].max()
        fc_year = fc_year_all[fc_year_all["_work_date"].eq(latest_date)].copy()
    elif validation_scope == "full_year":
        fc_year = fc_year_all.copy()
    else:
        raise RuntimeError(f"Unknown validation_scope={validation_scope}")

    # If the forecast panel has duplicate date × tenor rows, keep the last row.
    # This handles reproduced validation dates where the panel may carry both an inherited row
    # and a newly scored/reproduced row. Current/latest production date should still be unique.
    dup_count = int(fc_year.duplicated(["_work_trade_date", "_work_tenor"]).sum())
    if dup_count:
        fc_year = (
            fc_year
            .sort_values(["_work_date", "_work_tenor"])
            .drop_duplicates(["_work_trade_date", "_work_tenor"], keep="last")
            .copy()
        )

    # Forecast panels may already carry feature columns. Rename feature-panel
    # inputs before merge to avoid pandas _x/_y suffix collisions.
    feature_col_map = {c: f"_valfeat_{c}" for c in LOCKED_FEATURES}
    validation_feature_cols = [feature_col_map[c] for c in LOCKED_FEATURES]

    feat_cols = ["_work_trade_date", "_work_tenor"] + LOCKED_FEATURES
    feat = (
        feature_df[feat_cols]
        .copy()
        .rename(columns=feature_col_map)
    )

    merged = fc_year.merge(
        feat,
        on=["_work_trade_date", "_work_tenor"],
        how="left",
        validate="many_to_one",
    )

    missing_features = merged[validation_feature_cols].isna().any(axis=1)
    if missing_features.any():
        bad = merged.loc[missing_features, ["_work_date", "_work_tenor"]].head(20)
        raise RuntimeError("Validation rows missing feature inputs. Examples:\n" + bad.to_string(index=False))

    validation_rows = []

    for tenor in EXPECTED_TENORS:
        mask = merged["_work_tenor"].eq(tenor)
        if not mask.any():
            raise RuntimeError(f"No validation rows for tenor {tenor} in score_year={score_year}")

        model = models[tenor]
        X = merged.loc[mask, validation_feature_cols].to_numpy(dtype=float)

        pred = model.predict(X)
        actual_pred = pd.to_numeric(
            merged.loc[mask, "predicted_log_variance_candidate"],
            errors="coerce",
        ).to_numpy(dtype=float)

        pred_diff = pred - actual_pred

        forecast_var_calc = np.exp(pred)
        actual_forecast_var = pd.to_numeric(
            merged.loc[mask, "forecast_variance_candidate"],
            errors="coerce",
        ).to_numpy(dtype=float)

        vol_calc = np.sqrt(forecast_var_calc) * 100.0
        actual_vol = pd.to_numeric(
            merged.loc[mask, "candidate_forecast_vol_pct"],
            errors="coerce",
        ).to_numpy(dtype=float)

        validation_rows.append({
            "tenor": tenor,
            "score_year": score_year,
            "validation_rows": int(mask.sum()),
            "first_validation_date": str(merged.loc[mask, "_work_date"].min().date()),
            "last_validation_date": str(merged.loc[mask, "_work_date"].max().date()),
            "max_abs_pred_log_diff": float(np.nanmax(np.abs(pred_diff))),
            "mean_abs_pred_log_diff": float(np.nanmean(np.abs(pred_diff))),
            "max_abs_forecast_var_diff": float(np.nanmax(np.abs(forecast_var_calc - actual_forecast_var))),
            "max_abs_forecast_vol_diff": float(np.nanmax(np.abs(vol_calc - actual_vol))),
            "status": "PASS" if float(np.nanmax(np.abs(pred_diff))) <= tolerance_pred_log else "FAIL",
        })

        merged.loc[mask, "_artifact_predicted_log_variance_candidate"] = pred
        merged.loc[mask, "_artifact_forecast_variance_candidate"] = forecast_var_calc
        merged.loc[mask, "_artifact_candidate_forecast_vol_pct"] = vol_calc

    validation = pd.DataFrame(validation_rows)

    return validation, merged


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    parser.add_argument("--feature-panel", default=None)
    parser.add_argument("--forecast-panel", default=None)
    parser.add_argument("--fit-log", default=None)
    parser.add_argument("--score-year", type=int, default=None)
    parser.add_argument("--tolerance-pred-log", type=float, default=1e-8)
    parser.add_argument("--validation-scope", choices=["latest_only", "full_year"], default="latest_only")
    args = parser.parse_args()

    project_root = Path(args.project_root)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    repair_dir = project_root / r"data\processed\vrp_front_middle_corsi_forecast_repair_v1"
    audit_dir = project_root / r"data\audit\vrp_model_artifacts"
    artifact_dir = project_root / r"data\processed\vrp_model_artifacts"

    audit_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    if args.feature_panel:
        feature_panel_path = Path(args.feature_panel)
    else:
        feature_panel_path = latest_file(repair_dir, "04_front_middle_candidate_feature_panel_*.parquet")

    if args.forecast_panel:
        forecast_panel_path = Path(args.forecast_panel)
    else:
        forecast_panel_path = latest_file(repair_dir, "07A_unified_fds_no_min_return_oos_forecast_panel_*schema_repair.parquet")
        if forecast_panel_path is None:
            forecast_panel_path = latest_file(repair_dir, "07A_unified_fds_no_min_return_oos_forecast_panel_*.parquet")

    if args.fit_log:
        fit_log_path = Path(args.fit_log)
    else:
        fit_log_path = latest_file(project_root / r"data\audit\vrp_front_middle_corsi_forecast_repair_v1", "07A_unified_fit_log_*.csv")

    require_path(feature_panel_path, "feature panel")
    require_path(forecast_panel_path, "forecast panel")
    require_path(fit_log_path, "fit log")

    banner("Locked unified FDS model artifact export v1")
    print(f"Project root: {project_root}")
    print(f"Run timestamp: {run_ts}")
    print(f"Feature panel: {feature_panel_path}")
    print(f"Forecast panel: {forecast_panel_path}")
    print(f"Fit log: {fit_log_path}")
    print(f"Artifact dir: {artifact_dir}")
    print(f"Audit dir: {audit_dir}")

    banner("Load inputs")
    feature_df = add_work_keys(pd.read_parquet(feature_panel_path))
    forecast_df = add_work_keys(pd.read_parquet(forecast_panel_path))

    latest_forecast_date = forecast_df["_work_date"].max()
    if pd.isna(latest_forecast_date):
        raise RuntimeError("Forecast panel has no valid latest date.")

    score_year = int(args.score_year) if args.score_year else int(pd.Timestamp(latest_forecast_date).year)

    print(f"Feature rows/cols: {feature_df.shape}")
    print(f"Feature date range: {feature_df['_work_date'].min().date()} to {feature_df['_work_date'].max().date()}")
    print(f"Forecast rows/cols: {forecast_df.shape}")
    print(f"Forecast date range: {forecast_df['_work_date'].min().date()} to {forecast_df['_work_date'].max().date()}")
    print(f"Score year: {score_year}")

    missing_features = [c for c in LOCKED_FEATURES if c not in feature_df.columns]
    if missing_features:
        raise RuntimeError(f"Feature panel missing locked features: {missing_features}")

    if "target_log_variance" not in feature_df.columns:
        raise RuntimeError("Feature panel missing target_log_variance.")

    banner("Fit-log alpha / train-row maps")
    alpha_map, expected_train_rows, fit_summary = get_fit_log_maps(fit_log_path, score_year)
    print(fit_summary.to_string(index=False))

    banner("Fit per-tenor locked models")
    models: dict[int, Pipeline] = {}
    model_metadata: dict[int, dict] = {}
    train_summary_rows = []

    for tenor in EXPECTED_TENORS:
        alpha = float(alpha_map[tenor])
        expected_rows = expected_train_rows.get(tenor)

        train = make_training_panel(
            feature_df=feature_df,
            tenor=tenor,
            score_year=score_year,
            expected_rows=expected_rows,
        )

        model = fit_model(train, alpha)
        models[tenor] = model

        train_summary_rows.append({
            "tenor": tenor,
            "score_year": score_year,
            "alpha": alpha,
            "expected_train_rows": expected_rows,
            "actual_train_rows": int(len(train)),
            "train_first_date": str(train["_work_date"].min().date()),
            "train_last_date": str(train["_work_date"].max().date()),
            "feature_count": len(LOCKED_FEATURES),
        })

        model_metadata[tenor] = {
            "tenor": tenor,
            "score_year": score_year,
            "alpha": alpha,
            "expected_train_rows": expected_rows,
            "actual_train_rows": int(len(train)),
            "train_first_date": str(train["_work_date"].min().date()),
            "train_last_date": str(train["_work_date"].max().date()),
            "features": LOCKED_FEATURES,
        }

    train_summary = pd.DataFrame(train_summary_rows)
    print(train_summary.to_string(index=False))

    banner("Validate against production forecast panel")
    validation, scored_validation = validate_models(
        models=models,
        feature_df=feature_df,
        forecast_df=forecast_df,
        score_year=score_year,
        tolerance_pred_log=float(args.tolerance_pred_log),
        validation_scope=args.validation_scope,
    )

    print(validation.to_string(index=False))

    failed = validation[validation["status"].ne("PASS")].copy()
    hard_pass = failed.empty

    validation_path = audit_dir / f"unified_fds_model_artifact_validation_{score_year}_{run_ts}.csv"
    validation.to_csv(validation_path, index=False)

    train_summary_path = audit_dir / f"unified_fds_model_artifact_train_summary_{score_year}_{run_ts}.csv"
    train_summary.to_csv(train_summary_path, index=False)

    scored_sample_cols = [
        "_work_date",
        "_work_tenor",
        "predicted_log_variance_candidate",
        "_artifact_predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "_artifact_forecast_variance_candidate",
        "candidate_forecast_vol_pct",
        "_artifact_candidate_forecast_vol_pct",
    ]
    scored_sample_path = audit_dir / f"unified_fds_model_artifact_scored_validation_sample_{score_year}_{run_ts}.csv"
    scored_validation[scored_sample_cols].sort_values(["_work_date", "_work_tenor"]).tail(200).to_csv(scored_sample_path, index=False)

    if not hard_pass:
        banner("Validation failed")
        print(f"Failed tenors:\n{failed.to_string(index=False)}")
        print(f"Saved validation: {validation_path}")
        print(f"Saved scored sample: {scored_sample_path}")
        raise RuntimeError("MODEL_ARTIFACT_EXPORT failed validation; artifact not written.")

    banner("Write model artifact")
    artifact_payload = {
        "artifact_version": "unified_fds_no_min_return_model_artifacts_v1",
        "created_at": run_ts,
        "project_root": str(project_root),
        "score_year": score_year,
        "model_spec": "Pipeline(StandardScaler(), Ridge(alpha)) per tenor",
        "denominator": "forecast_variance_candidate = exp(predicted_log_variance_candidate)",
        "features": LOCKED_FEATURES,
        "expected_tenors": EXPECTED_TENORS,
        "alpha_map": {str(k): float(v) for k, v in alpha_map.items()},
        "model_metadata": {str(k): v for k, v in model_metadata.items()},
        "models": {str(k): v for k, v in models.items()},
        "source_feature_panel": str(feature_panel_path),
        "source_forecast_panel": str(forecast_panel_path),
        "source_fit_log": str(fit_log_path),
        "validation_summary": validation.to_dict(orient="records"),
    }

    timestamped_artifact = artifact_dir / f"unified_fds_no_min_return_model_artifacts_v1_{score_year}_{run_ts}.joblib"
    canonical_artifact = artifact_dir / "unified_fds_no_min_return_model_artifacts_v1.joblib"

    joblib.dump(artifact_payload, timestamped_artifact)

    if canonical_artifact.exists():
        backup = artifact_dir / f"{canonical_artifact.stem}_backup_{run_ts}{canonical_artifact.suffix}"
        shutil.copy2(canonical_artifact, backup)
        print(f"Existing canonical artifact backed up: {backup}")

    shutil.copy2(timestamped_artifact, canonical_artifact)

    manifest = {
        "run_ts": run_ts,
        "project_root": str(project_root),
        "score_year": score_year,
        "validation_scope": args.validation_scope,
        "artifact_version": "unified_fds_no_min_return_model_artifacts_v1",
        "timestamped_artifact": str(timestamped_artifact),
        "canonical_artifact": str(canonical_artifact),
        "source_feature_panel": str(feature_panel_path),
        "source_forecast_panel": str(forecast_panel_path),
        "source_fit_log": str(fit_log_path),
        "locked_features": LOCKED_FEATURES,
        "expected_tenors": EXPECTED_TENORS,
        "validation_csv": str(validation_path),
        "train_summary_csv": str(train_summary_path),
        "scored_validation_sample_csv": str(scored_sample_path),
        "max_abs_pred_log_diff": float(validation["max_abs_pred_log_diff"].max()),
        "max_abs_forecast_var_diff": float(validation["max_abs_forecast_var_diff"].max()),
        "max_abs_forecast_vol_diff": float(validation["max_abs_forecast_vol_diff"].max()),
        "hard_pass": bool(hard_pass),
        "method_note": "Saved per-tenor Pipeline(StandardScaler(), Ridge(alpha)) artifacts for intraday forecast scoring. No production signal files modified.",
    }

    manifest_path = audit_dir / f"unified_fds_model_artifact_manifest_{score_year}_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    banner("Saved outputs")
    print(f"timestamped_artifact: {timestamped_artifact}")
    print(f"canonical_artifact:   {canonical_artifact}")
    print(f"manifest:             {manifest_path}")
    print(f"validation:           {validation_path}")
    print(f"train_summary:        {train_summary_path}")
    print(f"scored_sample:         {scored_sample_path}")

    banner("Final result")
    print("MODEL_ARTIFACT_EXPORT_PASS: True")
    print("DONE — locked unified FDS model artifact exported and validated.")


if __name__ == "__main__":
    main()
