# Hybrid v2 completed-EOD production operations

**Model lock:** `vrp_corsi_intraday_hybrid_v2`  
**Operating pipeline:** `vrp_hybrid_v2_eod`  
**Current operating mode:** completed EOD only

## 1. Production boundary

The production stack refreshes data through the latest completed XNYS session, rebuilds affected history, reconstructs the locked forecast, calculates signals, applies locked qualification/sizing/selection rules, validates the result, and publishes canonical dashboard outputs atomically.

It does not:

- Create a live intraday trading signal
- Select or round live option contracts automatically
- Place orders
- Approve portfolio overlap, stress, or hedge exposure
- Re-estimate thresholds, sizes, or selector weights

Only the completed-EOD historical intraday realized-variance predictors remain part of the locked forecast.

## 2. Required local services and files

Before a normal run:

1. ThetaData Terminal is running at `127.0.0.1:25503`.
2. The active production scripts and runtime configuration are on `main`.
3. The canonical model lock and production configuration match `vrp_corsi_intraday_hybrid_v2`.
4. The accepted SPY EOD, Wilder RSI, SOFR, implied-variance, forecast, and signal histories are present.
5. No temporary audit or repair path is configured as an active canonical source.

The exact locked historical artifacts needed for benchmark reconstruction remain historical dependencies. They are not substitutes for current canonical production outputs.

## 3. Authoritative data contracts

### SPY market data, RV21D, and RSI

- Canonical SPY EOD closes come from ThetaData.
- RV21D is calculated from canonical SPY EOD closes.
- Wilder RSI14 is extended from the accepted long-warmup state using SPY close-to-close changes.
- Do not use FRED SPX as a fallback.
- Do not infer or repair SPY/RV21D/RSI values inside the dashboard.

### SOFR

- SOFR is refreshed before implied-variance construction.
- Date-level upserts must rank valid refreshed rows correctly even when legacy rows have null run timestamps.
- A run must reject unresolved duplicate dates or a stale value that outranks a corrected row.

### Implied variance

- The production panel must contain one valid row for each target date and each tenor: `9, 12, 15, 18, 21, 24, 27, 30, 33`.
- Quote times follow the completed-session and exchange-calendar contract.
- Current and interior gaps are detected before publication.

### Forecast and signal history

- The forecast uses the exact annual expanding Ridge fit contract and the locked nine features.
- Training rows obey both observation-date and `last_forward_rv_date` leakage controls.
- Z-scores use prior history only; the current value does not enter its own rolling moments.
- The publisher must enforce the intended model decision universe and must not create pre-universe selections merely because forecast history exists.

## 4. Standard run paths

### Streamlit

Launch:

```text
launch_vrp_hybrid_v2_streamlit.bat
```

Then use the dashboard refresh control to backfill missing data and recalculate through the latest completed EOD.

### Command line

```bat
py -u "C:\Users\patri\vrp_project\notebooks\vrp_hybrid_v2_eod_pipeline.py" ^
  --project-root "C:\Users\patri\vrp_project" ^
  --approved-nav 1000000
```

Useful diagnostic modes remain:

- `--target-date YYYYMMDD`
- `--force-recalculate`
- `--skip-upstream`
- `--no-publish`

Use diagnostic flags for controlled investigation, not as the normal operating path.

## 5. Pipeline sequence

1. Resolve the latest completed XNYS session using the exchange close plus the configured close buffer.
2. Validate the lock, runtime configuration, required files, and ThetaData connectivity.
3. Scan source and derived components for latest-date and interior gaps.
4. Refresh SOFR.
5. Update the VIX-style implied-variance term structure and validate the handoff.
6. Update canonical SPY EOD history, returns, and RV21D.
7. Extend the accepted Wilder RSI14 history.
8. Update the completed-EOD Corsi source features, including the locked 5D/21D/63D intraday RV predictors.
9. Rebuild affected locked feature history.
10. Reconstruct the locked annual expanding Ridge forecast.
11. Recalculate log VRP and prior-only 63/252-session z-scores.
12. Apply the locked Hybrid v2 Core and Secondary thresholds, per-trade sizes, and selector.
13. Stage dashboard products.
14. Run the final semantic health gate.
15. Publish atomically only after every hard check passes; otherwise restore the pre-run canonical state.

## 6. Minimum post-run controls

A normal successful run must show:

- Target date equals the latest completed XNYS session
- One row per date and tenor where applicable
- Exact nine-tenor source and forecast coverage
- Finite positive implied and forecast variance
- SPY EOD, RV21D, and RSI dates aligned to canonical history
- Prior-only z-score construction
- Exact threshold and sizing configuration loaded from the versioned config
- At most one selected trade per decision date
- No selections outside the approved decision universe
- Canonical outputs published only after the final health gate
- A run manifest and audit directory retained for the production run

## 7. Current audit status

The July audit identified and repaired two material operational defects:

- A SOFR upsert/deduplication issue in which older rows with null run timestamps could outrank corrected rows.
- Publisher date-universe drift that allowed selections before the intended locked decision universe.

The repair is complete. The next ordinary EOD run from `main` is the final operational confirmation. After a clean run:

1. Archive the Phase 4B rollback ZIP.
2. Remove temporary repair packages and extracted audit folders.
3. Stop using the repair branch.
4. Continue normal production from `main`.

## 8. Portfolio and execution boundary

The model output is not the final trade approval. Before execution, a separate portfolio-control layer must consider:

- Existing overlapping trades
- Total open max loss
- Bucket and tenor concentration
- Moderate and extreme stress losses
- Hedge capacity
- Whole-contract rounding
- Current executable option quotes and slippage

Actual execution must be recorded separately from model assumptions. Do not rewrite the model decision to match the fill.

## 9. Change control

A production code repair that restores the locked contract does not create a new model version. A change to the forecast target, features, estimator, fit contract, VRP/z-score construction, thresholds, sizing, selector, spread methodology, or calendar contract requires a new versioned model-lock process.
