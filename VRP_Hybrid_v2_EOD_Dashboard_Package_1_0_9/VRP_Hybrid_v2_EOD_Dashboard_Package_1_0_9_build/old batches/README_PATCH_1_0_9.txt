VRP Hybrid v2 EOD Patch 1.0.9
================================

Purpose
-------
Repair two false failures in the final production health gate after a successful Hybrid v2 publish stage.

Changes
-------
1. Locked forecast benchmark
   The canonical benchmark contains multiple model/spec rows for many date x tenor keys. The publisher uses
   the distinct historical date-grid anchor. The health check now mirrors that contract and reports raw
   multi-spec duplicates as informational rather than rejecting them.

2. SPY EOD close schema
   The canonical SPY EOD file can use eod_close rather than close. The health check now accepts these aliases:
   close, eod_close, spy_close, adj_close, adjusted_close.

Hard controls retained
----------------------
- All nine forecast tenors must remain present in the benchmark anchor.
- SPY EOD date coverage must reach the target date without gaps or duplicate dates.
- The target-date SPY close must be finite and strictly positive.
- All other health-gate components remain unchanged.

Installation
------------
1. Close Streamlit.
2. Run install_vrp_hybrid_v2_eod_patch_1_0_9.bat.
3. Relaunch C:\Users\patri\vrp_project\launch_vrp_hybrid_v2_streamlit.bat.
4. Run the normal refresh with Force recalculation OFF.
