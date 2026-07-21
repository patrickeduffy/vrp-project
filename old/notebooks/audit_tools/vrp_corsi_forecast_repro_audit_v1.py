
from pathlib import Path
from datetime import datetime
import traceback

import numpy as np
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge


PROJECT_ROOT = Path(r"C:\Users\patri\vrp_project")
BRANCH = "vrp_front_middle_corsi_forecast_repair_v1"

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / BRANCH
AUDIT_DIR = PROJECT_ROOT / "data" / "audit" / "forecast_update_inventory"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

TARGET_TENORS = [9, 12, 15, 18, 21, 24, 27, 30, 33]
TEST_DATE = pd.Timestamp("2026-07-01")
TEST_YEAR = 2026

UNIFIED_SPEC = "unified_fds_no_min_return"
MODEL_SOURCE = "unified_fds_no_min_return_oos_refit"

TARGET_LOG_COL = "target_log_variance"
TARGET_VAR_COL = "target_realized_variance"

UNIFIED_FEATURES = [
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

EPS = 1e-12


def section(title):
    print("=" * 100)
    print(title)
    print("=" * 100)


def latest_file(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"No file found in {directory} matching {pattern}")
    return matches[0]


def parse_dates(df: pd.DataFrame, cols):
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce").dt.normalize()
    return out


def make_ridge_model(alpha: float) -> Pipeline:
    return Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=alpha)),
        ]
    )


def fit_ridge_predict(train_df, test_df, features, target_col, alpha):
    model = make_ridge_model(alpha)
    model.fit(train_df[features].to_numpy(), train_df[target_col].to_numpy())
    return model.predict(test_df[features].to_numpy())


def main():
    section("VRP Corsi/FDS forecast reproducibility audit v1")
    print("Project root:", PROJECT_ROOT)
    print("Run timestamp:", RUN_TS)
    print("Test date:", TEST_DATE.date())
    print("Test year:", TEST_YEAR)

    feature_panel_path = latest_file(
        PROCESSED_DIR,
        "04_front_middle_candidate_feature_panel_*.parquet",
    )

    forecast_panel_path = latest_file(
        PROCESSED_DIR,
        "07A_unified_fds_no_min_return_oos_forecast_panel_*.parquet",
    )

    fit_log_path = latest_file(
        PROJECT_ROOT / "data" / "audit" / BRANCH,
        "07A_unified_fit_log_*.csv",
    )

    section("Input files")
    print("Feature panel:", feature_panel_path)
    print("Forecast panel:", forecast_panel_path)
    print("Fit log:", fit_log_path)

    features = pd.read_parquet(feature_panel_path)
    forecast = pd.read_parquet(forecast_panel_path)
    fit_log = pd.read_csv(fit_log_path)

    features = parse_dates(features, ["trade_date", "date", "last_forward_rv_date"])
    forecast = parse_dates(forecast, ["trade_date", "date", "last_forward_rv_date"])

    for df_name, df in [("features", features), ("forecast", forecast)]:
        if "tenor" not in df.columns:
            raise RuntimeError(f"{df_name} missing tenor column")
        df["tenor"] = pd.to_numeric(df["tenor"], errors="coerce").astype("Int64")

    section("Loaded shapes")
    print("features:", features.shape)
    print("forecast:", forecast.shape)
    print("fit_log:", fit_log.shape)

    print("\nFeature panel date range:", features["trade_date"].min(), "to", features["trade_date"].max())
    print("Forecast panel date range:", forecast["trade_date"].min(), "to", forecast["trade_date"].max())

    required_feature_cols = ["trade_date", "last_forward_rv_date", "tenor", TARGET_LOG_COL, TARGET_VAR_COL, "implied_variance"] + UNIFIED_FEATURES
    missing = [c for c in required_feature_cols if c not in features.columns]
    if missing:
        raise RuntimeError(f"Feature panel missing required columns: {missing}")

    required_forecast_cols = [
        "trade_date",
        "tenor",
        "model_spec",
        "model_source",
        "fit_status_candidate",
        "selected_alpha_candidate",
        "train_rows_used_candidate",
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
    ]
    missing_forecast = [c for c in required_forecast_cols if c not in forecast.columns]
    if missing_forecast:
        raise RuntimeError(f"Forecast panel missing required columns: {missing_forecast}")

    for c in [TARGET_LOG_COL, TARGET_VAR_COL, "implied_variance"] + UNIFIED_FEATURES:
        features[c] = pd.to_numeric(features[c], errors="coerce")

    universe = features[
        features["tenor"].isin(TARGET_TENORS)
        & features["trade_date"].notna()
        & features["last_forward_rv_date"].notna()
        & np.isfinite(features[TARGET_LOG_COL])
        & np.isfinite(features[TARGET_VAR_COL])
        & (features[TARGET_VAR_COL] > 0)
        & np.isfinite(features["implied_variance"])
        & (features["implied_variance"] > 0)
    ].copy()

    stored = forecast[
        (forecast["trade_date"] == TEST_DATE)
        & (forecast["tenor"].isin(TARGET_TENORS))
        & (forecast["model_spec"] == UNIFIED_SPEC)
        & (forecast["model_source"] == MODEL_SOURCE)
        & (forecast["fit_status_candidate"] == "candidate_fit")
    ].copy()

    section("Stored rows to reproduce")
    print("stored rows:", len(stored))
    print("stored tenors:", sorted(stored["tenor"].dropna().astype(int).unique().tolist()))

    if len(stored) != len(TARGET_TENORS):
        print(stored[["trade_date", "tenor", "model_spec", "model_source", "fit_status_candidate"]].to_string(index=False))
        raise RuntimeError("Stored forecast panel does not have exactly 9 target rows for the test date.")

    rows = []

    for tenor in TARGET_TENORS:
        tenor_df = universe[universe["tenor"] == tenor].copy().sort_values("trade_date").reset_index(drop=True)

        test_start = pd.Timestamp(f"{TEST_YEAR}-01-01")

        train_pool = tenor_df[
            (tenor_df["trade_date"] < test_start)
            & (tenor_df["last_forward_rv_date"] < test_start)
        ].copy()

        train_fit = train_pool.dropna(subset=UNIFIED_FEATURES + [TARGET_LOG_COL]).copy()
        score_fit = tenor_df[
            tenor_df["trade_date"] == TEST_DATE
        ].dropna(subset=UNIFIED_FEATURES + [TARGET_LOG_COL]).copy()

        fit_row = fit_log[
            (fit_log["model_spec"] == UNIFIED_SPEC)
            & (pd.to_numeric(fit_log["tenor"], errors="coerce") == tenor)
            & (pd.to_numeric(fit_log["test_year"], errors="coerce") == TEST_YEAR)
            & (fit_log["fit_status"] == "candidate_fit")
        ].copy()

        if fit_row.empty:
            rows.append({
                "tenor": tenor,
                "status": "failed_missing_fit_log",
            })
            continue

        alpha = float(fit_row.iloc[0]["selected_alpha"])

        stored_row = stored[stored["tenor"].astype(int) == tenor].copy()
        if stored_row.empty:
            rows.append({
                "tenor": tenor,
                "status": "failed_missing_stored_row",
            })
            continue

        if score_fit.empty:
            rows.append({
                "tenor": tenor,
                "status": "failed_missing_score_row",
                "train_rows_rebuilt": len(train_fit),
                "alpha": alpha,
            })
            continue

        pred = fit_ridge_predict(
            train_df=train_fit,
            test_df=score_fit,
            features=UNIFIED_FEATURES,
            target_col=TARGET_LOG_COL,
            alpha=alpha,
        )

        rebuilt_pred = float(pred[0])
        rebuilt_var = float(np.exp(rebuilt_pred))
        rebuilt_vol = float(np.sqrt(max(rebuilt_var, 0.0)) * 100.0)

        stored_pred = float(stored_row.iloc[0]["predicted_log_variance_candidate"])
        stored_var = float(stored_row.iloc[0]["forecast_variance_candidate"])
        stored_vol = float(stored_row.iloc[0]["candidate_forecast_vol_pct"])

        rows.append({
            "tenor": tenor,
            "status": "ok",
            "alpha": alpha,
            "train_rows_fit_log": int(fit_row.iloc[0]["train_rows_used"]),
            "train_rows_rebuilt": int(len(train_fit)),
            "stored_selected_alpha": float(stored_row.iloc[0]["selected_alpha_candidate"]),
            "stored_train_rows": int(stored_row.iloc[0]["train_rows_used_candidate"]),
            "rebuilt_predicted_log_variance": rebuilt_pred,
            "stored_predicted_log_variance": stored_pred,
            "diff_predicted_log_variance": rebuilt_pred - stored_pred,
            "rebuilt_forecast_variance": rebuilt_var,
            "stored_forecast_variance": stored_var,
            "diff_forecast_variance": rebuilt_var - stored_var,
            "rebuilt_forecast_vol_pct": rebuilt_vol,
            "stored_forecast_vol_pct": stored_vol,
            "diff_forecast_vol_pct": rebuilt_vol - stored_vol,
        })

    audit = pd.DataFrame(rows)

    audit_path = AUDIT_DIR / f"corsi_fds_20260701_repro_audit_{RUN_TS}.csv"
    audit.to_csv(audit_path, index=False)

    section("Reproduction audit")
    print(audit.to_string(index=False))
    print("\nSaved:", audit_path)

    ok = audit["status"].eq("ok").all()
    max_abs_pred_diff = float(audit.loc[audit["status"].eq("ok"), "diff_predicted_log_variance"].abs().max()) if ok else np.nan
    max_abs_var_diff = float(audit.loc[audit["status"].eq("ok"), "diff_forecast_variance"].abs().max()) if ok else np.nan
    max_abs_vol_diff = float(audit.loc[audit["status"].eq("ok"), "diff_forecast_vol_pct"].abs().max()) if ok else np.nan

    section("Summary")
    print("all_status_ok:", ok)
    print("max_abs_pred_diff:", max_abs_pred_diff)
    print("max_abs_var_diff:", max_abs_var_diff)
    print("max_abs_vol_diff:", max_abs_vol_diff)

    pass_flag = (
        ok
        and max_abs_pred_diff < 1e-10
        and max_abs_var_diff < 1e-12
        and max_abs_vol_diff < 1e-8
    )

    print("REPRO_AUDIT_PASS:", pass_flag)

    if not pass_flag:
        raise RuntimeError("Reproduction audit failed. Do not build production updater until this is resolved.")

    section("DONE")
    print("Locked model reproduction confirmed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nERROR:", repr(exc))
        traceback.print_exc()
        raise
