
"""
VRP locked unified FDS forecast-panel update v1.

Scope:
  Extend the locked unified_fds_no_min_return forecast panel through requested end date.

Not in scope:
  alpha retuning
  threshold changes
  final signal build
  sizing / selection changes
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


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

LOCKED_ALPHA_BY_TENOR = {
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

MODEL_NAME = "unified_fds_no_min_return"
TRAIN_CUTOFF = pd.Timestamp("2026-01-01")
REPRO_DATE = pd.Timestamp("2026-07-01")
REPRO_TOL = 1.0e-10


def section(title: str) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project-root", default=r"C:\Users\patri\vrp_project")
    p.add_argument("--feature-panel", required=True)
    p.add_argument("--old-forecast-panel", required=True)
    p.add_argument("--fit-log", required=True)
    p.add_argument("--end-date", required=True)
    return p.parse_args()


def parse_dates(s: pd.Series) -> pd.Series:
    raw = pd.Series(s, index=s.index)
    nonnull = raw.dropna()
    if len(nonnull) == 0:
        return pd.to_datetime(raw, errors="coerce").dt.normalize()

    as_str = nonnull.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    if as_str.str.fullmatch(r"\d{8}").mean() > 0.80:
        out = pd.to_datetime(
            raw.astype(str).str.replace(r"\.0$", "", regex=True).str.strip(),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        out = pd.to_datetime(raw, errors="coerce")
    return out.dt.normalize()


def normalize_date_tenor(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "date" in out.columns:
        out["date"] = parse_dates(out["date"])
    elif "trade_date" in out.columns:
        out["date"] = parse_dates(out["trade_date"])
    else:
        raise ValueError("Missing date/trade_date column.")

    out["trade_date"] = out["date"].dt.strftime("%Y%m%d").astype("Int64")

    tenor_col = "tenor" if "tenor" in out.columns else ("target_days" if "target_days" in out.columns else None)
    if tenor_col is None:
        raise ValueError("Missing tenor/target_days column.")

    out["tenor"] = pd.to_numeric(out[tenor_col], errors="coerce").astype("Int64")
    if "target_days" not in out.columns:
        out["target_days"] = out["tenor"]
    else:
        out["target_days"] = pd.to_numeric(out["target_days"], errors="coerce").astype("Int64")

    return out


def find_alpha_column(df: pd.DataFrame) -> str | None:
    candidates = [
        "selected_alpha",
        "alpha",
        "ridge_alpha",
        "model_alpha",
        "best_alpha",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        if "alpha" in c.lower():
            return c
    return None



def parse_fit_log_alpha_check(fit_log: pd.DataFrame) -> pd.DataFrame:
    """
    Parse the locked fit log for selected alpha by tenor.

    The fit log is an audit table and may not contain date/trade_date, so do not
    use the generic production panel normalizer here.
    """
    df = fit_log.copy()

    if "tenor" not in df.columns:
        if "target_days" in df.columns:
            df["tenor"] = df["target_days"]
        else:
            return pd.DataFrame([{
                "status": "WARN",
                "detail": f"Could not parse tenor from fit log. columns={list(fit_log.columns)}",
            }])

    alpha_col = find_alpha_column(df)
    if alpha_col is None:
        return pd.DataFrame([{
            "status": "WARN",
            "detail": f"Could not parse alpha from fit log. columns={list(fit_log.columns)}",
        }])

    rows = df.copy()

    object_cols = rows.select_dtypes(include=["object", "string"]).columns.tolist()
    model_hit = pd.Series(False, index=rows.index)
    for c in object_cols:
        model_hit = model_hit | rows[c].astype(str).str.contains(MODEL_NAME, case=False, na=False)

    if model_hit.any():
        rows = rows[model_hit].copy()

    year_cols = [c for c in rows.columns if c.lower() in ["year", "test_year", "oos_year", "forecast_year"]]
    for c in year_cols:
        year_num = pd.to_numeric(rows[c], errors="coerce")
        if (year_num == 2026).any():
            rows = rows[year_num == 2026].copy()
            break

    rows["tenor"] = pd.to_numeric(rows["tenor"], errors="coerce")
    rows[alpha_col] = pd.to_numeric(rows[alpha_col], errors="coerce")

    out = []
    for tenor, expected_alpha in LOCKED_ALPHA_BY_TENOR.items():
        vals = sorted(rows.loc[rows["tenor"].eq(tenor), alpha_col].dropna().unique().tolist())
        if not vals:
            out.append({
                "tenor": tenor,
                "expected_alpha": expected_alpha,
                "fit_log_alphas": "",
                "status": "WARN",
                "detail": "No parseable fit-log alpha for this tenor; using locked expected alpha map.",
            })
        elif expected_alpha in vals:
            out.append({
                "tenor": tenor,
                "expected_alpha": expected_alpha,
                "fit_log_alphas": "|".join(str(v) for v in vals),
                "status": "PASS",
                "detail": "Expected locked alpha found in fit log.",
            })
        else:
            out.append({
                "tenor": tenor,
                "expected_alpha": expected_alpha,
                "fit_log_alphas": "|".join(str(v) for v in vals),
                "status": "FAIL",
                "detail": "Fit-log alpha does not contain expected locked alpha.",
            })

    return pd.DataFrame(out)

def detect_target_col(df: pd.DataFrame) -> str:
    candidates = [
        "target_log_variance",
        "log_target_variance",
        "log_forward_realized_variance_corsi",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"Could not find target log variance column. columns={list(df.columns)}")


def expected_2026_train_rows_from_fit_log(fit_log: pd.DataFrame) -> dict[int, int]:
    rows = fit_log.copy()

    if "model_spec" in rows.columns:
        rows = rows[rows["model_spec"].astype(str).eq(MODEL_NAME)].copy()

    if "test_year" not in rows.columns or "train_rows_used" not in rows.columns:
        raise ValueError("Fit log must contain test_year and train_rows_used for locked train-row contract.")

    if "tenor" not in rows.columns:
        raise ValueError("Fit log must contain tenor for locked train-row contract.")

    rows["test_year"] = pd.to_numeric(rows["test_year"], errors="coerce")
    rows["tenor"] = pd.to_numeric(rows["tenor"], errors="coerce")
    rows["train_rows_used"] = pd.to_numeric(rows["train_rows_used"], errors="coerce")

    rows = rows[rows["test_year"].eq(2026)].copy()

    out = {}
    for tenor in EXPECTED_TENORS:
        vals = rows.loc[rows["tenor"].eq(tenor), "train_rows_used"].dropna().astype(int).unique().tolist()
        if len(vals) != 1:
            raise RuntimeError(f"Expected exactly one 2026 train_rows_used value for tenor {tenor}; got {vals}")
        out[tenor] = int(vals[0])

    return out


def filter_unified_forecast_rows(old: pd.DataFrame) -> pd.DataFrame:
    rows = old.copy()

    object_cols = rows.select_dtypes(include=["object", "string"]).columns.tolist()
    model_hit = pd.Series(False, index=rows.index)
    for c in object_cols:
        model_hit = model_hit | rows[c].astype(str).str.contains(MODEL_NAME, case=False, na=False)

    if model_hit.any():
        rows = rows[model_hit].copy()

    required_forecast_cols = ["predicted_log_variance_candidate", "forecast_variance_candidate"]
    missing = [c for c in required_forecast_cols if c not in rows.columns]
    if missing:
        raise ValueError(f"Old forecast panel missing required forecast cols: {missing}")

    rows = rows[rows["predicted_log_variance_candidate"].notna() & rows["forecast_variance_candidate"].notna()].copy()
    rows = rows[rows["tenor"].isin(EXPECTED_TENORS)].copy()

    return rows


def select_one_row_per_tenor(rows: pd.DataFrame, alpha_map: dict[int, float]) -> pd.DataFrame:
    out = []
    alpha_col = find_alpha_column(rows)

    for tenor in EXPECTED_TENORS:
        sub = rows[rows["tenor"].eq(tenor)].copy()

        if alpha_col and alpha_col in sub.columns:
            alpha_num = pd.to_numeric(sub[alpha_col], errors="coerce")
            exact = sub[alpha_num.eq(alpha_map[tenor])].copy()
            if len(exact):
                sub = exact

        if len(sub) == 0:
            raise RuntimeError(f"No old forecast template/repro row for tenor {tenor}.")

        if len(sub) > 1:
            sub = sub.sort_values(list(sub.columns)).tail(1)

        out.append(sub.iloc[[0]])

    return pd.concat(out, ignore_index=True)


def fit_models_and_score(feature_panel: pd.DataFrame, score_dates: list[pd.Timestamp], expected_train_rows_by_tenor: dict[int, int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_col = detect_target_col(feature_panel)

    if "last_forward_rv_date" not in feature_panel.columns:
        raise ValueError("Feature panel missing last_forward_rv_date required for leakage guard.")

    work = feature_panel.copy()
    work["last_forward_rv_date"] = parse_dates(work["last_forward_rv_date"])

    missing_features = [c for c in LOCKED_FEATURES if c not in work.columns]
    if missing_features:
        raise ValueError(f"Feature panel missing locked features: {missing_features}")

    for c in LOCKED_FEATURES + [target_col]:
        work[c] = pd.to_numeric(work[c], errors="coerce")

    pred_frames = []
    train_rows_summary = []

    for tenor in EXPECTED_TENORS:
        sub = work[work["tenor"].eq(tenor)].copy().sort_values("date").reset_index(drop=True)

        X_all = sub[LOCKED_FEATURES]
        y_all = sub[target_col]

        train_mask = (
            sub["date"].lt(TRAIN_CUTOFF)
            & sub["last_forward_rv_date"].lt(TRAIN_CUTOFF)
            & y_all.notna()
            & np.isfinite(y_all)
            & X_all.notna().all(axis=1)
            & np.isfinite(X_all).all(axis=1)
        )

        train = sub[train_mask].copy().sort_values("date").reset_index(drop=True)
        raw_train_rows = len(train)

        expected_train_rows = int(expected_train_rows_by_tenor.get(tenor, raw_train_rows))
        if raw_train_rows < expected_train_rows:
            raise RuntimeError(
                f"Eligible train rows below fit-log contract for tenor {tenor}: "
                f"eligible={raw_train_rows}, expected={expected_train_rows}"
            )

        dropped_earliest_rows = raw_train_rows - expected_train_rows
        dropped_first_dates = ""
        if dropped_earliest_rows > 0:
            dropped_first_dates = "|".join(
                train.head(dropped_earliest_rows)["date"].dt.strftime("%Y-%m-%d").tolist()
            )
            train = train.tail(expected_train_rows).copy().reset_index(drop=True)

        if len(train) < 250:
            raise RuntimeError(f"Too few train rows for tenor {tenor}: {len(train)}")

        alpha = LOCKED_ALPHA_BY_TENOR[tenor]
        model = Pipeline([
            ("standard_scaler", StandardScaler()),
            ("ridge", Ridge(alpha=alpha)),
        ])

        model.fit(train[LOCKED_FEATURES], train[target_col])

        score_mask = sub["date"].isin(score_dates)
        score = sub[score_mask].copy()
        score_X = score[LOCKED_FEATURES]

        score_complete = (
            score_X.notna().all(axis=1)
            & np.isfinite(score_X).all(axis=1)
        )

        if not score_complete.all():
            bad = score.loc[~score_complete, ["date", "tenor", *LOCKED_FEATURES]]
            raise RuntimeError(f"Score rows have missing/nonfinite locked features for tenor {tenor}:\n{bad.to_string(index=False)}")

        pred = model.predict(score_X)
        score["predicted_log_variance_candidate"] = pred
        score["forecast_variance_candidate"] = np.exp(pred)
        score["candidate_forecast_vol_pct"] = np.sqrt(score["forecast_variance_candidate"]) * 100.0
        score["selected_alpha"] = alpha
        score["locked_model_name"] = MODEL_NAME

        pred_frames.append(score)

        train_rows_summary.append({
            "tenor": tenor,
            "alpha": alpha,
            "raw_eligible_train_rows": raw_train_rows,
            "expected_fit_log_train_rows": expected_train_rows,
            "dropped_earliest_rows": dropped_earliest_rows,
            "dropped_first_dates": dropped_first_dates,
            "train_rows": len(train),
            "train_first_date": train["date"].min().date().isoformat(),
            "train_last_date": train["date"].max().date().isoformat(),
            "score_rows": len(score),
            "score_dates": "|".join(d.strftime("%Y-%m-%d") for d in sorted(score["date"].unique())),
        })

    return pd.concat(pred_frames, ignore_index=True), pd.DataFrame(train_rows_summary)


def build_append_rows(scored: pd.DataFrame, old_forecast: pd.DataFrame, old_template_by_tenor: pd.DataFrame) -> pd.DataFrame:
    old_cols = list(old_forecast.columns)
    append = pd.DataFrame(index=scored.index)

    template = old_template_by_tenor.copy()
    template_cols = [c for c in template.columns if c not in ["date", "trade_date"]]

    scored_keyed = scored.copy()
    scored_keyed["tenor"] = scored_keyed["tenor"].astype(int)

    for c in old_cols:
        if c in scored_keyed.columns:
            append[c] = scored_keyed[c].values
        else:
            append[c] = np.nan

    # Fill template constants by tenor where the scored feature panel does not have the column.
    template_map = template.set_index("tenor")
    for c in template_cols:
        if c in old_cols and c not in scored_keyed.columns:
            append[c] = scored_keyed["tenor"].map(template_map[c])

    # Force core forecast outputs from the newly scored rows.
    for c in [
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
    ]:
        if c in old_cols:
            append[c] = scored_keyed[c].values

    # Alpha columns.
    for c in old_cols:
        if "alpha" in c.lower():
            append[c] = scored_keyed["tenor"].map(LOCKED_ALPHA_BY_TENOR).values

    # Model-identifying object columns: preserve old template when available; otherwise add model name if column exists.
    for c in old_cols:
        if c.lower() in ["model", "model_name", "forecast_model", "candidate", "candidate_name", "forecast_candidate"]:
            if append[c].isna().all():
                append[c] = MODEL_NAME

    # Ensure date/trade_date/tenor are set cleanly.
    append["date"] = scored_keyed["date"].values
    append["trade_date"] = scored_keyed["trade_date"].values
    append["tenor"] = scored_keyed["tenor"].values
    if "target_days" in append.columns:
        append["target_days"] = scored_keyed["tenor"].values

    return append[old_cols].copy()


def extract_filename_start_token(old_path: Path, old_forecast: pd.DataFrame) -> str:
    m = re.search(r"_oos_forecast_panel_(\d{8})_\d{8}_", old_path.name)
    if m:
        return m.group(1)
    return old_forecast["date"].min().strftime("%Y%m%d")


def compare_reproduction(calc: pd.DataFrame, old_repro: pd.DataFrame) -> pd.DataFrame:
    calc = calc.copy()
    old = old_repro.copy()

    keep = ["date", "trade_date", "tenor", "predicted_log_variance_candidate", "forecast_variance_candidate", "candidate_forecast_vol_pct"]
    calc = calc[keep].copy()
    old = old[keep].copy()

    merged = old.merge(
        calc,
        on=["date", "trade_date", "tenor"],
        how="inner",
        suffixes=("_old", "_calc"),
    )

    rows = []
    for c in ["predicted_log_variance_candidate", "forecast_variance_candidate", "candidate_forecast_vol_pct"]:
        diff = (
            pd.to_numeric(merged[f"{c}_calc"], errors="coerce")
            - pd.to_numeric(merged[f"{c}_old"], errors="coerce")
        ).abs()
        rows.append({
            "column": c,
            "rows": len(merged),
            "max_abs_diff": float(diff.max()) if len(diff) else np.nan,
            "mean_abs_diff": float(diff.mean()) if len(diff) else np.nan,
            "status": "PASS" if len(diff) and float(diff.max()) <= REPRO_TOL else "FAIL",
        })

    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()

    project_root = Path(args.project_root)
    feature_path = Path(args.feature_panel)
    old_forecast_path = Path(args.old_forecast_panel)
    fit_log_path = Path(args.fit_log)
    end_ts = pd.to_datetime(args.end_date, format="%Y%m%d").normalize()
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_dir = project_root / "data" / "processed" / "vrp_front_middle_corsi_forecast_repair_v1"
    audit_dir = project_root / "data" / "audit" / "vrp_front_middle_corsi_forecast_repair_v1"
    output_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    section("Locked unified FDS forecast-panel update v1")
    print("Project root:", project_root)
    print("Feature panel:", feature_path)
    print("Old forecast panel:", old_forecast_path)
    print("Fit log:", fit_log_path)
    print("End date:", end_ts.date())
    print("Locked features:", LOCKED_FEATURES)
    print("Locked alpha map:", LOCKED_ALPHA_BY_TENOR)

    feature = normalize_date_tenor(pd.read_parquet(feature_path))
    old_forecast = normalize_date_tenor(pd.read_parquet(old_forecast_path))
    fit_log = pd.read_csv(fit_log_path)

    feature = feature[feature["date"].le(end_ts)].copy()

    section("Loaded inputs")
    print("Feature rows:", len(feature))
    print("Feature date range:", feature["date"].min().date(), "to", feature["date"].max().date())
    print("Feature latest tenors:", sorted(feature.loc[feature["date"].eq(feature["date"].max()), "tenor"].dropna().astype(int).unique().tolist()))
    print("Old forecast rows:", len(old_forecast))
    print("Old forecast date range:", old_forecast["date"].min().date(), "to", old_forecast["date"].max().date())
    print("Fit log rows/cols:", fit_log.shape)

    alpha_check = parse_fit_log_alpha_check(fit_log)
    alpha_check_path = audit_dir / f"locked_unified_fds_alpha_check_{run_ts}.csv"
    alpha_check.to_csv(alpha_check_path, index=False)

    section("Fit-log alpha check")
    print(alpha_check.to_string(index=False))

    old_unified = filter_unified_forecast_rows(old_forecast)
    old_latest = old_forecast["date"].max()
    old_unified_latest = old_unified[old_unified["date"].eq(old_latest)].copy()
    old_template_by_tenor = select_one_row_per_tenor(old_unified_latest, LOCKED_ALPHA_BY_TENOR)

    score_dates = sorted(feature.loc[
        feature["date"].gt(old_latest) & feature["date"].le(end_ts),
        "date"
    ].drop_duplicates().tolist())

    repro_and_score_dates = sorted(set([REPRO_DATE, *score_dates]))

    section("Score plan")
    print("Old forecast latest date:", old_latest.date())
    print("Reproduction date:", REPRO_DATE.date())
    print("Append score dates:", [d.strftime("%Y-%m-%d") for d in score_dates])
    print("All model scoring dates:", [d.strftime("%Y-%m-%d") for d in repro_and_score_dates])

    expected_train_rows_by_tenor = expected_2026_train_rows_from_fit_log(fit_log)
    print("Locked 2026 fit-log train rows:", expected_train_rows_by_tenor)

    scored_all, train_summary = fit_models_and_score(feature, repro_and_score_dates, expected_train_rows_by_tenor)

    train_summary_path = audit_dir / f"locked_unified_fds_train_summary_{run_ts}.csv"
    train_summary.to_csv(train_summary_path, index=False)

    section("Train summary")
    print(train_summary.to_string(index=False))

    repro_calc = scored_all[scored_all["date"].eq(REPRO_DATE)].copy()
    old_repro_pool = old_unified[old_unified["date"].eq(REPRO_DATE)].copy()
    old_repro = select_one_row_per_tenor(old_repro_pool, LOCKED_ALPHA_BY_TENOR)

    repro_summary = compare_reproduction(repro_calc, old_repro)
    repro_path = audit_dir / f"locked_unified_fds_reproduction_compare_{run_ts}.csv"
    repro_summary.to_csv(repro_path, index=False)

    section("Reproduction validation vs old forecast panel")
    print(repro_summary.to_string(index=False))

    scored_append = scored_all[scored_all["date"].isin(score_dates)].copy()
    append_rows = build_append_rows(scored_append, old_forecast, old_template_by_tenor)

    combined = pd.concat([old_forecast, append_rows], ignore_index=True, sort=False)
    combined = normalize_date_tenor(combined)
    combined = combined.sort_values(["date", "tenor"]).reset_index(drop=True)

    start_token = extract_filename_start_token(old_forecast_path, old_forecast)
    end_token = combined["date"].max().strftime("%Y%m%d")
    output_path = output_dir / f"07A_unified_fds_no_min_return_oos_forecast_panel_{start_token}_{end_token}_{run_ts}.parquet"
    combined.to_parquet(output_path, index=False)

    section("Output written")
    print("Output:", output_path)
    print("Rows:", len(combined))
    print("Cols:", len(combined.columns))
    print("Date range:", combined["date"].min().date(), "to", combined["date"].max().date())

    latest_unified = filter_unified_forecast_rows(combined)
    latest_unified = latest_unified[latest_unified["date"].eq(end_ts)].copy()
    latest_tenors = sorted(latest_unified["tenor"].dropna().astype(int).unique().tolist())

    show_cols = [
        "date",
        "trade_date",
        "tenor",
        "predicted_log_variance_candidate",
        "forecast_variance_candidate",
        "candidate_forecast_vol_pct",
    ]
    show_cols = [c for c in show_cols if c in latest_unified.columns]

    section("Latest unified forecast rows")
    print(latest_unified[show_cols].sort_values("tenor").to_string(index=False))

    checks = []

    def check(name: str, ok: bool, detail: str):
        checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})

    alpha_fails = alpha_check[alpha_check.get("status", pd.Series(dtype=str)).eq("FAIL")]
    check("fit_log_has_no_alpha_mismatch", len(alpha_fails) == 0, f"failed_rows={len(alpha_fails)}")
    check("reproduction_rows_all_tenors", len(repro_calc) == 9, f"rows={len(repro_calc)}")
    check(
        "reproduction_matches_old_forecast",
        bool(repro_summary["status"].eq("PASS").all()),
        f"max_diff={repro_summary['max_abs_diff'].max()}",
    )
    check("output_reaches_target_date", combined["date"].max() == end_ts, f"target={end_ts.date()}; max={combined['date'].max().date()}")
    check("latest_has_expected_tenors", latest_tenors == EXPECTED_TENORS, f"latest_tenors={latest_tenors}")
    check("latest_forecast_variance_positive", (latest_unified["forecast_variance_candidate"].astype(float) > 0).all(), "all latest forecast variances > 0")
    check(
        "latest_forecast_variance_reconstructs_vol",
        np.allclose(
            np.sqrt(latest_unified["forecast_variance_candidate"].astype(float)) * 100.0,
            latest_unified["candidate_forecast_vol_pct"].astype(float),
            rtol=0,
            atol=1e-12,
        ),
        "candidate_forecast_vol_pct == sqrt(forecast_variance_candidate)*100",
    )

    validation = pd.DataFrame(checks)
    validation_path = audit_dir / f"locked_unified_fds_forecast_update_validation_{run_ts}.csv"
    validation.to_csv(validation_path, index=False)

    manifest = {
        "run_ts": run_ts,
        "feature_panel": str(feature_path),
        "old_forecast_panel": str(old_forecast_path),
        "fit_log": str(fit_log_path),
        "output_panel": str(output_path),
        "alpha_check": str(alpha_check_path),
        "train_summary": str(train_summary_path),
        "reproduction_compare": str(repro_path),
        "validation": str(validation_path),
        "model_name": MODEL_NAME,
        "locked_features": LOCKED_FEATURES,
        "locked_alpha_by_tenor": LOCKED_ALPHA_BY_TENOR,
        "train_cutoff": TRAIN_CUTOFF.date().isoformat(),
        "score_dates": [d.strftime("%Y-%m-%d") for d in score_dates],
    }
    manifest_path = audit_dir / f"locked_unified_fds_forecast_update_manifest_{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    section("Validation")
    print(validation.to_string(index=False))
    print("Saved validation:", validation_path)
    print("Saved manifest:", manifest_path)

    fails = validation[validation["status"].eq("FAIL")]

    section("Final result")
    print("Hard checks failed:", len(fails))
    if len(fails):
        print(fails.to_string(index=False))
        raise RuntimeError("LOCKED_UNIFIED_FDS_FORECAST_UPDATE failed validation.")

    print("LOCKED_UNIFIED_FDS_FORECAST_UPDATE_PASS: True")
    print("DONE — locked unified FDS forecast panel extended.")


if __name__ == "__main__":
    main()
