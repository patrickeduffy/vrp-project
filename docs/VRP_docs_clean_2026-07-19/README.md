# VRP Project

Private repository for the VRP options research and production system.

The active production system is the completed-EOD **Hybrid v2** stack. The model lock is `vrp_corsi_intraday_hybrid_v2`. The word *intraday* in the lock name refers to historical intraday realized-variance predictors used by the forecast; it does **not** mean the system produces a live intraday trading signal.

## Current status

- The July 2026 production audit and repair are complete.
- The next ordinary completed-EOD run from `main` is the final operational confirmation.
- After that run passes, archive the Phase 4B rollback ZIP and remove temporary repair packages and extracted audit folders.
- The unfinished live intraday process has been abandoned and is outside the production boundary.

## Active production architecture

```text
Streamlit dashboard
    -> Hybrid v2 EOD pipeline
        -> 00 SOFR refresh
        -> 01 implied-variance update
        -> 02 SPY market-data update
        -> 03 Wilder RSI update
        -> 04 Corsi source update
        -> 05 locked feature-panel update
        -> 05b implied-variance validation and handoff
    -> Hybrid v2 signal publisher
    -> production health check
```

### Active production files

- `notebooks/streamlit_vrp_hybrid_v2_eod.py` — Streamlit interface.
- `notebooks/vrp_hybrid_v2_eod_pipeline.py` — completed-EOD orchestrator.
- `notebooks/vrp_hybrid_v2_signal_publish.py` — forecast, VRP, qualification, sizing, selection, and canonical outputs.
- `notebooks/vrp_hybrid_v2_health_check.py` — source, calculation, and publication validations.
- `notebooks/vrp_hybrid_v2_common.py` — shared runtime configuration, paths, JSON, and file handling.
- `notebooks/vrp_hybrid_v2_wilder_rsi_update.py` — accepted long-warmup Wilder RSI extension.

## Authoritative production rules

- SPY ThetaData completed-EOD closes are the source of truth for SPY returns, RV21D, and the RSI update path. Do not use a FRED SPX fallback or infer those values inside Streamlit.
- The dashboard is a display and control surface. It reads canonical published outputs and does not recreate model mathematics.
- The locked forecast uses the completed-EOD historical intraday realized-variance predictors `challenge_log_intraday_rv_5d`, `challenge_log_intraday_rv_21d`, and `challenge_log_intraday_rv_63d`.
- A displayed `TRADE` is a completed-EOD model decision. It is not an executable quote or an order.
- Portfolio overlap caps, stress limits, hedging, and final execution approval remain outside the model lock.

## Documentation

- [`docs/PRODUCTION_OPERATIONS.md`](docs/PRODUCTION_OPERATIONS.md) — current operating procedure and controls.
- [`docs/CURRENT_STATUS_AND_ROADMAP.md`](docs/CURRENT_STATUS_AND_ROADMAP.md) — current status and recommended work order.
- [`docs/DOCUMENTATION_INDEX.md`](docs/DOCUMENTATION_INDEX.md) — authoritative documents and cleanup decisions.
- [`docs/model_lock/OPERATIONAL_SUPERSESSION_NOTE.md`](docs/model_lock/OPERATIONAL_SUPERSESSION_NOTE.md) — operational statements that supersede outdated portions of the immutable July 11 lock package.
- `docs/model_lock/VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx` — immutable methodology and parameter lock.
- `config/vrp_corsi_intraday_hybrid_v2_production_config.json` — compact runtime parameters.
- `config/vrp_corsi_intraday_hybrid_v2_lock.json` — full machine-readable lock and evidence metadata.

## Data policy

The following remain outside Git:

- Raw ThetaData option chains
- External market-data histories
- Large Parquet and serialized datasets
- Generated production outputs
- Per-run audit folders and logs
- Credentials and local environment files
- Local Excel research workbooks
- Temporary repair and rollback packages

## Repository status

The private GitHub migration was completed in July 2026. The preserved pre-cleanup baseline tag is:

```text
v0.1-pre-github-cleanup-baseline
```
