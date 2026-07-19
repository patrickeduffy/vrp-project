# Documentation index and cleanup decisions

This index separates the current operating documentation from immutable model-lock history and obsolete material.

## Active documentation

| File | Status | Purpose |
|---|---|---|
| `README.md` | Active | Repository overview, current status, architecture, and boundaries |
| `docs/PRODUCTION_OPERATIONS.md` | Active | Current completed-EOD operating procedure and controls |
| `docs/CURRENT_STATUS_AND_ROADMAP.md` | Active | Current status and work order |
| `docs/model_lock/OPERATIONAL_SUPERSESSION_NOTE.md` | Active | Clarifies post-lock operational changes without changing methodology |
| `docs/model_lock/VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx` | Immutable | Authoritative methodology, parameters, research evidence, and change control |
| `docs/model_lock/VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt` | Immutable | Historical July 11 release summary |
| `config/vrp_corsi_intraday_hybrid_v2_lock.json` | Immutable | Full machine-readable model lock |
| `config/vrp_corsi_intraday_hybrid_v2_production_config.json` | Active locked config | Compact thresholds, sizes, selector, and spread contract |
| `requirements.txt` | Active | Direct production dependencies |

## Important interpretation rule

The July 11 model-lock DOCX remains authoritative for methodology and locked parameters. It is intentionally not edited because doing so would invalidate its reproduction-package checksums.

Its operational statements were written before the consolidated EOD runner, dashboard, and July audit repair were complete. Current operational statements in `PRODUCTION_OPERATIONS.md` and `OPERATIONAL_SUPERSESSION_NOTE.md` supersede only those historical operating-status passages. They do not change the model lock.

## Removed from the active documentation set

| Original file | Decision | Reason |
|---|---|---|
| `VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx` | Remove from active docs | Predates the consolidated orchestrator and duplicates current operations; replaced by `PRODUCTION_OPERATIONS.md` |
| `VRP_Corsi_Intraday_Hybrid_v2_README.txt` | Remove | Duplicates the lock, release notes, and repository README; also says no consolidated runner exists |
| `VRP_Hybrid_v2_EOD_Dashboard_README.txt` | Remove | Installation/release document from the original dashboard delivery; contains obsolete first-run language |
| `EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt` | Remove | Its content is now incorporated into current operations documentation |
| `VRP_Corsi_Intraday_Hybrid_v2_Reproduction_Package.zip` | Remove from active repo docs | Large immutable research/evidence package; retain only in external archive when needed for formal reproduction |
| `thetadata_vix_style_variance_process_v0_1.docx` | Remove from active docs | Historical v0.1 process document with outdated dates and superseded FRED SPX/RV/RSI dependencies |
| `pip_freeze_current.txt` | Remove | Full local environment snapshot is noisy, stale, UTF-16 encoded, and not a maintainable dependency contract |

## Repository placement

- Keep model parameters under `config/`, not duplicated in `docs/`.
- Keep direct dependencies in root `requirements.txt`.
- Keep generated audit folders, repair packages, rollback ZIPs, and large reproduction packages outside the active Git repository.
- Preserve the historical reproduction package in a separate archive only when formal model reconstruction evidence is required.
