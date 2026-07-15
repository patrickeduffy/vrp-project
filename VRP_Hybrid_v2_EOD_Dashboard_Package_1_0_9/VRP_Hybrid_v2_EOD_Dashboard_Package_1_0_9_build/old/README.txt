VRP Hybrid v2 EOD Production Dashboard
======================================

Release
-------
Model lock: vrp_corsi_intraday_hybrid_v2
Dashboard pipeline: vrp_hybrid_v2_eod
Dashboard package release: 1.0.5
Purpose: gap-aware completed-EOD refresh, locked signal generation, and Streamlit display.

This is a new application. It does not patch or import the old Streamlit dashboard.
The Streamlit file contains no model mathematics. It launches the standalone pipeline and reads canonical outputs.

Install
-------
1. Extract this package.
2. Run install_vrp_hybrid_v2_eod_dashboard.bat.
3. Install dependencies:

   py -m pip install -r "C:\Users\patri\vrp_project\requirements_vrp_hybrid_v2_eod.txt"

4. Confirm ThetaData Terminal is running at 127.0.0.1:25503.
5. Double-click:

   C:\Users\patri\vrp_project\launch_vrp_hybrid_v2_streamlit.bat

6. In Streamlit, click:

   Backfill Missing Data and Recalculate Through Latest EOD

Primary files installed
-----------------------
notebooks\vrp_hybrid_v2_common.py
notebooks\vrp_hybrid_v2_health_check.py
notebooks\vrp_hybrid_v2_wilder_rsi_update.py
notebooks\vrp_hybrid_v2_signal_publish.py
notebooks\vrp_hybrid_v2_eod_pipeline.py
notebooks\streamlit_vrp_hybrid_v2_eod.py
config\vrp_hybrid_v2_eod_runtime_config.json
config\vrp_corsi_intraday_hybrid_v2_production_config.json
config\vrp_corsi_intraday_hybrid_v2_lock.json

Existing project dependencies required
--------------------------------------
notebooks\vrp_implied_variance_eod_update_v1.py
notebooks\vrp_market_data_build_v1.py
notebooks\vrp_corsi_source_update_v1.py
notebooks\vrp_locked_cell4_feature_panel_update_v1.py

The exact locked forecast fit log, historical benchmark forecast, locked intraday forecast reference,
and the v2 research artifacts used to materialize the static selection tie-break snapshot must remain at
the paths specified in config\vrp_hybrid_v2_eod_runtime_config.json.

Button workflow
---------------
1. Resolve the latest completed XNYS session using the actual exchange close plus a 15-minute buffer.
2. Validate the Hybrid v2 lock and ThetaData connectivity.
3. Scan all components for recent and interior missing dates.
4. Refresh the VIX-style implied-variance surface, including SOFR and option-chain cache dependencies.
5. Rebuild canonical SPY EOD, realized volatility, and RV21D through the target date.
6. Extend the accepted long-warmup Wilder RSI14 history from its stored recursive state.
7. Update Corsi intraday realized-variance sources.
8. Rebuild the locked feature panel from before the earliest affected date when necessary.
9. Reconstruct the annual expanding intraday Ridge forecast using:
   - benchmark nine-feature row contract;
   - last_forward_rv_date leakage control;
   - exact fit-log train_rows_used and selected_alpha when present;
   - locked train-only yearly walk-forward alpha fallback when selected_alpha is blank;
   - the locked six parsimonious plus three intraday model features.
10. Compare the full historical overlap with the locked intraday forecast reference at 1e-8 tolerance.
11. Recalculate log VRP and prior-only 63/252-session z-scores from complete history.
12. Apply exact Hybrid v2 Core and Secondary thresholds, sizes, and the size-led daily selector.
13. Stage all dashboard products.
14. Publish atomically only after the final hard health gate passes.
15. Restore every pre-run canonical file and remove newly created canonical outputs if the final gate fails.

Accepted RSI contract
---------------------
Formula version: wilder_rsi14_spy_close_v2_long_warmup
Input: SPY close-to-close changes, not log returns.
Seed: simple average of the first 14 gains and losses.
Update: recursive Wilder smoothing.
Both average gain and average loss equal zero: RSI = 50.
Average loss zero and average gain positive: RSI = 100.
Canonical output begins 2018-01-02. The accepted file stores the locked long-warmup recursive state
(close, average gain, and average loss). Daily EOD updates preserve that accepted state and recursively
extend it using canonical SPY EOD closes. The updater does not request historical SPY stock data from
ThetaData and therefore does not require a PROFESSIONAL stock-data subscription.




Release 1.0.5 missing-alpha fit-contract repair
----------------------------------------------
Some canonical fit-log tenor/year rows retain the exact train_rows_used contract but have a blank selected_alpha.
The historical benchmark reconstruction does not pass that blank value into Ridge; it reselects alpha using the
trimmed pre-test-year benchmark-contract sample and the locked yearly walk-forward procedure over
1, 10, 100, 300, and 1000, with 100 as the no-fold fallback and original grid order as the tie-break. Release
1.0.5 restores that exact branch in the EOD publisher. The final intraday Ridge is still fit on the locked six
parsimonious plus three intraday features, the current test year never enters alpha selection, and the full
historical overlap must still reconcile to the locked intraday forecast reference at 1e-8. The forecast fit audit
now records the reference alpha, resolved alpha, alpha source, fold count, validation rows, and selected RMSE.
The health gate accepts a blank selected_alpha only when train_rows_used is present and positive.

Release 1.0.4 intraday-feature join repair
------------------------------------------
The locked baseline feature panel contains the nine-feature benchmark row contract and target, while the
three accepted intraday predictors are derived from the separately maintained Corsi component source. Earlier
dashboard publishers incorrectly required those three rolling features to be physically stored in the baseline
feature panel. Release 1.0.4 resolves the latest canonical Corsi component source, validates a unique non-forward
raw intraday variance value by date, constructs log(252 * rolling_mean(raw intraday variance, 5/21/63)), and
merges those date-level features into all nine tenors before the locked annual Ridge forecast is fit. The full
historical overlap is still reconciled to the locked intraday forecast reference at 1e-8, so this repair cannot
silently change the forecast denominator. A versioned intraday-feature audit CSV is written with the source
column, formula, windows, availability, and target-date values.

Release 1.0.3 date-normalization repair
---------------------------------------
Some canonical project tables store dates as integer or float YYYYMMDD values. Generic pandas parsing can
interpret values such as 20260710 as nanoseconds after 1970-01-01, causing every normalized row to collapse
to 1970-01-01. Release 1.0.3 fixes the shared date utility used by the runner, health gate, forecast publisher,
and RSI updater. YYYYMMDD integers/floats and strings are now parsed explicitly; common Unix epoch units,
Excel serial dates, and normal date strings remain supported.

Release 1.0.2 duplicate-state repair
------------------------------------
Some accepted RSI parquet histories contain duplicate trade_date rows. The v1.0.2 updater audits every
duplicate group and collapses it only when the locked recursive state is semantically identical, or when one
valid accepted row is accompanied only by incomplete/invalid copies. A disagreement in close, close change,
Wilder average gain, Wilder average loss, RSI, or formula version remains a hard failure. Every collapsed date
is recorded in a versioned duplicate-date audit CSV.

Release 1.0.1 RSI source repair
-------------------------------
The initial dashboard package attempted to repull SPY history from 1993 through ThetaData. A STANDARD
subscription returns HTTP 403 for that stock-history endpoint. Release 1.0.1 removes that dependency.
The accepted `spy_wilder_rsi14_history_v1.parquet` is the authoritative long-warmup seed and already
contains `wilder_avg_gain_14` and `wilder_avg_loss_14`; the updater extends those states through new
canonical SPY EOD dates. Interior gaps are recalculated forward from the last valid accepted state.

Canonical dashboard outputs
---------------------------
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_forecast_history.parquet
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_signal_history.parquet
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_latest_snapshot.parquet
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_selected_decisions.parquet
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_static_tiebreaks.csv
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_latest_execution_handoff.csv
data\processed\vrp_hybrid_v2_eod\vrp_hybrid_v2_data_status.json

Per-run audit directory
-----------------------
data\audit\vrp_hybrid_v2_eod\YYYYMMDD_HHMMSS\

Each run stores pre/post gap reports, logs, staged data, backups, status, step results, and manifests.

Dashboard sections
------------------
- Latest TRADE / NO TRADE / DATA NOT READY status
- Locked size and target max-risk dollars for the entered NAV
- Selected signal inputs and exact thresholds
- Nine-tenor decision table with Core and Secondary pass/fail reasons
- Data-health component table
- Latest/prior/five-session VIX-style term structures
- Forecast volatility versus RV21D
- VRP term structure
- Filterable one-year-plus historical signal table and CSV download
- Live refresh progress and run console

Important boundaries
--------------------
- A displayed TRADE is a completed-EOD model decision, not a live executable quote.
- Whole-contract rounding, current option quotes, portfolio overlap approval, stress limits, and hedging remain separate controls.
- The dashboard does not place orders.
- The dashboard does not alter Hybrid v2 parameters.
- A new calendar year cannot be scored until the locked fit log contains an explicit tenor/year row and train_rows_used contract.
- A blank selected_alpha is allowed only through the locked yearly walk-forward fallback documented in release 1.0.5.

Command-line refresh
--------------------
Double-click run_vrp_hybrid_v2_eod_once.bat, or run:

py -u "C:\Users\patri\vrp_project\notebooks\vrp_hybrid_v2_eod_pipeline.py" ^
  --project-root "C:\Users\patri\vrp_project" ^
  --approved-nav 1000000

Diagnostic modes
----------------
--target-date YYYYMMDD       Manual completed-session target.
--force-recalculate          Rebuild from the earliest detected gap and force upstream refreshes.
--skip-upstream              Use existing upstream files only; publisher validations still apply.
--no-publish                 Build and validate staging products without replacing canonical dashboard outputs.

Testing status
--------------
The delivered source files compile successfully. Synthetic contract tests passed for:
- benchmark-row annual Ridge fitting;
- locked forecast reference reconciliation;
- prior-only z-score history;
- all 13 threshold/sizing sleeves;
- the locked daily selector;
- accepted RSI semantic loading;
- flat and rising Wilder RSI edge cases;
- static tie-break snapshot loading.

A live ThetaData/FRED/Streamlit end-to-end run cannot be executed outside the user's local project environment.
The first local button run is therefore the final integration test and will not retain canonical dashboard outputs
unless every hard health check passes.

Documentation installed
-----------------------
- docs\VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx
- docs\VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx
- docs\EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt
- docs\VRP_Hybrid_v2_EOD_Dashboard_README.txt

Patch level 1.0.8
-----------------
The publisher validates RV21D on the exact locked forecast/signal date set, audits unused pre-signal warm-up/anomaly rows, and writes a dedicated RV21D audit artifact.

Patch 1.0.9
-----------
The final health gate now mirrors the publisher's historical benchmark-key contract and accepts the validated
SPY EOD close aliases. This prevents false rollback after an otherwise successful NO_TRADE or TRADE publish.
