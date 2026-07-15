VRP Hybrid v2 EOD patch 1.0.7
================================

Purpose
-------
Repair the implied-variance handoff between the validated EOD updater and the Hybrid v2 publisher.

Observed failure
----------------
The updater's stable processed surface contained a complete nine-tenor grid for 2026-07-10, while the
legacy canonical alias had been restored to 2026-07-09 after an earlier downstream rollback. On later
runs, the updater saw no missing dates in its processed surface and therefore the publisher could still read
the stale alias.

Repair
------
- Add an explicit runtime path for the updater's stable processed implied-variance surface.
- Validate the target-date grid immediately before publication.
- Prefer the stable processed surface and synchronize it to the legacy canonical alias.
- Pass the exact validated implied-variance path explicitly to the signal publisher.
- Save implied_variance_handoff_audit.csv and implied_variance_handoff.json in each run directory.
- Include the stable processed surface in pipeline backup targets.

No model change
---------------
No forecast, signal threshold, sizing, selection, RSI, spread, SOFR, or VIX-style calculation changed.

Installation
------------
This patch assumes dashboard package/patch 1.0.6 is already installed. Close Streamlit, run
install_vrp_hybrid_v2_eod_patch_1_0_7.bat, relaunch Streamlit, and use the normal refresh with Force
recalculation OFF.
