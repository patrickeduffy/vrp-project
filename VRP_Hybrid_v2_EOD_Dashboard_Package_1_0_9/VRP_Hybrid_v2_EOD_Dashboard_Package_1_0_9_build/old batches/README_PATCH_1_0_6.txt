VRP Hybrid v2 EOD patch 1.0.6
================================

Install:
1. Close the Streamlit terminal.
2. Run install_vrp_hybrid_v2_eod_patch_1_0_6.bat.
3. Relaunch C:\Users\patri\vrp_project\launch_vrp_hybrid_v2_streamlit.bat.
4. Run the normal refresh with Force recalculation OFF.

Repair:
The canonical fit log includes pre-model bookkeeping rows (including 2018) with train_rows_used = 0.
Those rows are not Ridge fit contracts. Patch 1.0.6 accepts them only when they occur strictly before the
first active positive-row fit year for the same tenor, excludes them from fitting, and records the ignored
placeholder count in the health report.

Safety rules retained:
- every active tenor must have at least one positive train_rows_used contract;
- negative or missing train-row values fail closed;
- a zero-row contract inside or after active fit history fails closed;
- duplicate tenor/year rows fail closed;
- finite selected_alpha values are used exactly;
- blank selected_alpha values use the locked train-only yearly walk-forward fallback from patch 1.0.5.

No forecast features, signal thresholds, sizing, selector, RSI, or spread methodology changed.
