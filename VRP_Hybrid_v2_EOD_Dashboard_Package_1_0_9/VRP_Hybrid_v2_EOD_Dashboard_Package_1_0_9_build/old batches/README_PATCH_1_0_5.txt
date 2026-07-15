VRP Hybrid v2 EOD patch 1.0.5
================================

Install:
1. Close the Streamlit terminal.
2. Run install_vrp_hybrid_v2_eod_patch_1_0_5.bat.
3. Relaunch C:\Users\patri\vrp_project\launch_vrp_hybrid_v2_streamlit.bat.
4. Run the normal refresh with Force recalculation OFF.

Repair:
The canonical fit log can contain a valid tenor/year row with an exact train_rows_used contract but a blank
selected_alpha. The historical benchmark reconstruction resolves that case by reselecting alpha on the trimmed
pre-test-year benchmark-contract training sample using train-only yearly walk-forward validation.

Patch 1.0.5 restores that exact branch in the EOD publisher:
  alpha grid: 1, 10, 100, 300, 1000
  metric: pooled inner-validation RMSE on target_log_variance
  minimum inner training rows: 250
  minimum inner validation rows: 30
  no-fold fallback alpha: 100
  tie-break: original alpha-grid order

The final intraday Ridge is still fit on the locked six parsimonious plus three intraday features. The alpha
fallback is selected using the benchmark nine-feature contract because that is the contract that generated the
canonical fit log. Current-year outcomes never enter the selection.

The health gate now treats a blank selected_alpha as valid only when train_rows_used is present and positive;
the publisher records alpha_source and the selected inner-CV diagnostics in the forecast fit audit.

No signal threshold, sizing, selector, spread, RSI, or forecast-feature specification changed.
