VRP Project Storage Inventory v1
================================

Purpose
-------
Creates a read-only storage inventory for:

  C:\Users\patri\vrp_project

Proposed future archive destination:

  C:\Users\patri\VRP_Archive

This tool DOES NOT move, copy, rename, or delete any files.

How to run
----------
1. Extract this package.
2. Double-click:

   run_vrp_project_storage_inventory_v1.bat

Or run directly:

   py vrp_project_storage_inventory_v1.py

Outputs
-------
A timestamped folder is created under:

  C:\Users\patri\vrp_project\data\audit\vrp_storage_inventory\<timestamp>\

Files produced:

- all_files_inventory.csv
- largest_files.csv
- largest_folders.csv
- duplicate_files.csv
- archive_candidates.csv
- protected_files.csv
- scan_errors.csv
- summary.json
- summary.txt

Safety behavior
---------------
- Hashes only same-sized files of at least 1 MB by default.
- Protects known Hybrid v2 production scripts and canonical data.
- Reads Hybrid v2 lock/config JSON files and protects referenced project paths.
- Protects the latest successful EOD production audit.
- Classifies candidates but makes no filesystem changes.
- Exact duplicates are recommended for SAFE_DELETE_AFTER_ARCHIVE only when a keeper is identified.

Recommended next step
---------------------
Upload or paste summary.txt, largest_folders.csv, and archive_candidates.csv.
Then create a separate reviewed archive-move manifest before moving anything.

Note
----
Moving files from the project to C:\Users\patri\VRP_Archive organizes the
project but does not free capacity on the C: drive.
