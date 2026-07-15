VRP Hybrid v2 EOD patch 1.0.8
================================

Purpose
-------
Repair the RV21D publication contract.

Observed failure
----------------
The raw SPY realized-volatility history contains rolling warm-up NaNs and at least one pre-signal source-boundary anomaly. The publisher rejected any invalid RV21D value anywhere in raw history, even though the locked forecast/signal history begins later and does not consume those rows.

Repair
------
- Validate RV21D against the exact set of dates consumed by the locked forecast history.
- Require every consumed date, including the target date, to have one finite positive RV21D value.
- Audit unused invalid pre-signal dates rather than silently using them or blocking production.
- Prefer a finite positive row when duplicate dates contain both valid and invalid copies.
- Save vrp_hybrid_v2_rv21d_audit.csv and include the audit in the publish manifest.

No model change
---------------
The RV21D formula, forecast, signal thresholds, sizing, selection, RSI, spread, and VIX-style calculations are unchanged.

Installation
------------
This patch assumes dashboard package/patch 1.0.7 is installed. Close Streamlit, run install_vrp_hybrid_v2_eod_patch_1_0_8.bat, relaunch Streamlit, and use the normal refresh with Force recalculation OFF.
