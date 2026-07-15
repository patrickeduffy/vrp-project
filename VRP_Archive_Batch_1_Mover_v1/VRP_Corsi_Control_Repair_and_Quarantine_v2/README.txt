VRP Corsi Control Repair + Failed Panel Quarantine v2
======================================================

What changed from v1
--------------------
The earlier quarantine dry run correctly stopped because the staging control
panel was still missing.

The reviewed archive manifest shows that the missing staging files were exact
duplicates of keeper files that remain inside the active project:

  data\processed\production_feature_panel_v0_1.parquet
  data\processed\production_feature_panel_v0_1.csv

This package uses those verified keepers instead of depending on an archive
restore.

Actions
-------
1. Verifies keeper file size and SHA-256.
2. Verifies the keeper Parquet contains:
     spx_close
     spx_log_return
3. Verifies the valid pre-failure Corsi panel still exists and contains those
   columns.
4. Recreates the missing staging Parquet and CSV using temporary copies and
   hash verification.
5. Finds only Corsi model feature panels created on 2026-07-12.
6. Quarantines a panel only when BOTH protected SPX columns are absent.
7. Copies and SHA-256 verifies before removing the active poisoned copy.

Run order
---------
1. Close Streamlit.
2. Double-click:

     01_DRY_RUN_repair_corsi_control_and_quarantine.bat

3. Confirm:

     DRY RUN PASSED

4. Double-click:

     02_EXECUTE_repair_corsi_control_and_quarantine.bat

5. Confirm:

     REPAIR COMPLETE

6. Relaunch Streamlit.
7. Run the normal refresh with Force recalculation OFF.

Quarantine location
-------------------
C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712\
  _quarantine\failed_corsi_panels\

No model logic, thresholds, sizing, signal history, or canonical production
outputs are changed by this repair.
