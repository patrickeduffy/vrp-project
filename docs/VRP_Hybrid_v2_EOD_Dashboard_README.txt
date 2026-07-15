VRP Hybrid v2 EOD Production Dashboard
======================================

Release
-------
Model lock: vrp_corsi_intraday_hybrid_v2
Dashboard pipeline: vrp_hybrid_v2_eod
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
6. Update the accepted long-warmup Wilder RSI14 history.
7. Update Corsi intraday realized-variance sources.
8. Rebuild the locked feature panel from before the earliest affected date when necessary.
9. Reconstruct the annual expanding intraday Ridge forecast using:
   - benchmark nine-feature row contract;
   - last_forward_rv_date leakage control;
   - exact fit-log selected_alpha and train_rows_used;
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
Canonical output begins 2018-01-02, but the calculation uses a longer ThetaData history.

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
- A new calendar year cannot be scored until the locked fit log contains an explicit contract for that year.

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
