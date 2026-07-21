# Archive index

This directory retains superseded research, repair artifacts, abandoned experiments, and previous production implementations. Archived files remain in Git for provenance, but they are not supported production entry points unless explicitly listed below.

## Production-critical exceptions

Two historical-looking sources are still loaded by the current production pipeline and must not be moved or deleted:

- `old/notebooks forecast model corsi v1/01_corsi_har_rv_forecast_model_research_cleaned.ipynb` is executed by `notebooks/vrp_corsi_source_update_v1.py`.
- `notebooks v0/old/01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb` is required by `notebooks/vrp_implied_variance_eod_update_v1.py`.

The implied-variance updater also uses generated function-source dumps under `data/audit/production_inventory/`. The ignored `data/audit/` tree is not disposable as a whole because it contains both runtime inputs and audit evidence.

## July 2026 cleanup

The cleanup moved these reviewed non-production families out of the active repository surface:

- `old/notebooks/legacy_production_v1/` — superseded pre-Hybrid-v2 pipeline and dashboard scripts;
- `old/notebooks/abandoned_intraday_v1/` — unfinished live-intraday experiments explicitly deferred for a future rebuild;
- `old/notebooks/repair_artifacts/` — obsolete SOFR and Wilder-RSI patch copies;
- `old/notebooks/development_notebooks/` — notebook versions superseded by production Python modules;
- `old/notebooks/audit_tools/` — completed one-off source reconstruction and audit utilities;
- `old/repository_maintenance/` — completed one-off storage inventory and archive-mover utilities.

Ignored local ZIP backups were moved to `old/local_archives/`. They are not tracked by Git.

Moving files into this directory cleans the active tree but does not reduce existing Git history or repository clone size.
