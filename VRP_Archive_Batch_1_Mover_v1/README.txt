VRP Archive Batch 1 Mover v1
============================

Approved scope
--------------
Source:
  C:\Users\patri\vrp_project

Archive:
  C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712

Approved manifest:
  2,099 files
  Approximately 13.13 GB

The archive is on the same C: drive. This cleans and simplifies the active
project but does not reduce C: drive usage.

Run order
---------
1. Close Streamlit and confirm no VRP refresh or Python process is writing data.
2. Double-click:

     01_DRY_RUN_archive_batch_1.bat

3. The dry run must end with:

     PREFLIGHT PASSED

4. Then double-click:

     02_EXECUTE_archive_batch_1.bat

5. Type exactly:

     ARCHIVE BATCH 1

Safety design
-------------
- Uses the exact approved file-by-file manifest.
- Rejects paths outside the source and archive roots.
- Rejects known Hybrid v2 production/canonical paths.
- Rejects missing or size-changed source files.
- Checks reviewed SHA-256 values when available.
- Copies each source to a temporary archive file.
- Computes the source SHA-256.
- Re-reads and hashes the copied archive file.
- Finalizes the archive copy only when hashes match.
- Re-hashes the source immediately before removal.
- Removes the source only after successful verification.
- Records durable progress events before and after source removal.
- Is resumable after interruption.
- Prunes only directories that became completely empty.
- Never deletes a non-empty directory.

Archive metadata
----------------
Written under:

  C:\Users\patri\VRP_Archive\batch_1_historical_research_20260712\
      _archive_metadata\

Important files:
- approved_manifest.csv
- archive_progress.jsonl
- completed_manifest.csv
- archive_summary.json
- archive_errors.csv, only if an error occurs

Restore
-------
To validate a restore without moving files:

  03_DRY_RUN_restore_batch_1.bat

To restore the entire completed batch:

  04_EXECUTE_restore_batch_1.bat

The restore tool verifies the archive hash, copies each file back to a temporary
source path, verifies the restored hash, finalizes it, and only then removes the
archive copy.

Interruption
------------
An interruption does not require starting over. Run the execute BAT again.
Previously completed files are skipped. A file with a verified archive copy but
an unremoved source is re-verified before the source is removed.

Do not manually edit:
- VRP_Archive_Batch_1_Approved.csv
- _archive_metadata\archive_progress.jsonl
- _archive_metadata\completed_manifest.csv
