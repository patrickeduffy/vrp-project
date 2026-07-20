# VRP repository documentation cleanup

## Scope

This package cleans the uploaded `docs/` directory and updates the root `README.md`. It does not modify Python code, model parameters, configuration, data, or Git history.

## Kept unchanged

- `docs/VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx`
- `docs/VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt`

## Added

- `docs/DOCUMENTATION_INDEX.md`
- `docs/PRODUCTION_OPERATIONS.md`
- `docs/CURRENT_STATUS_AND_ROADMAP.md`
- `docs/MODEL_LOCK_AND_OPERATIONAL_SUPERSESSION.md`

## Updated

- root `README.md`

## Removed from active documentation

- `docs/EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt`
- `docs/pip_freeze_current.txt`
- `docs/README.md`
- `docs/requirements.txt`
- `docs/thetadata_vix_style_variance_process_v0_1.docx`
- `docs/VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx`
- `docs/VRP_Corsi_Intraday_Hybrid_v2_README.txt`
- `docs/VRP_Corsi_Intraday_Hybrid_v2_Reproduction_Package.zip`
- `docs/VRP_DOCS_REVIEW_REPORT.md`
- `docs/VRP_Hybrid_v2_EOD_Dashboard_README.txt`
- `docs/vrp_corsi_intraday_hybrid_v2_lock.json`
- `docs/vrp_corsi_intraday_hybrid_v2_production_config.json`

The duplicate JSON files are removed from `docs/`; their authoritative copies remain in `config/`.

## Why

The removed files were duplicate artifacts, superseded operating instructions, historical release packages, or environment snapshots. Keeping all of them active created contradictory guidance and made it unclear which document controlled current production.

## Safety

`APPLY_DOCS_CLEANUP.ps1`:

- requires a Git repository and a clean working tree;
- verifies the immutable model-lock and release-note hashes;
- verifies required active code/config files exist;
- creates a timestamped backup ZIP outside the repository;
- removes only the explicit obsolete-file list;
- copies the new root README and current Markdown documentation;
- leaves all changes uncommitted for inspection.
