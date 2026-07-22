# Production source inventory

## Accepted operating surface

The following files form the current completed-EOD production path. Structural migration must preserve their behavior until the golden contract and focused regression tests prove an extracted replacement equivalent.

| Stage | Current source | Primary responsibility |
| --- | --- | --- |
| Stable entry point | `scripts/run_eod.py` | Delegates to the accepted orchestrator without changing model logic |
| Orchestration | `notebooks/vrp_hybrid_v2_eod_pipeline.py` | Resolves gaps, runs stages, validates handoffs, and publishes a recoverable canonical file set |
| Shared utilities | `notebooks/vrp_hybrid_v2_common.py` | Paths, calendars, atomic file operations, normalization, and process helpers |
| SOFR | `notebooks/vrp_sofr_eod_update_v1.py` | Updates and validates the prior-observation SOFR cache |
| Implied variance | `notebooks/vrp_implied_variance_eod_update_v1.py` | Builds the nine-tenor SPX/SPXW VIX-style surface |
| SPY market data | `notebooks/vrp_market_data_build_v1.py` | Maintains canonical SPY closes and realized-volatility inputs |
| Corsi source | `notebooks/vrp_corsi_source_update_v1.py` | Updates the locked forecast source panel |
| Locked features | `notebooks/vrp_locked_cell4_feature_panel_update_v1.py` | Builds the locked model-ready feature panel |
| Forecast | `notebooks/vrp_locked_unified_fds_forecast_update_v1.py` | Extends the accepted forecast history |
| Wilder RSI | `notebooks/vrp_hybrid_v2_wilder_rsi_update.py` | Maintains the canonical clean-session RSI history |
| Signal and selection | `notebooks/vrp_hybrid_v2_signal_publish.py` | Applies locked thresholds, ranking, sizing, and publication rules |
| Health check | `notebooks/vrp_hybrid_v2_health_check.py` | Validates source freshness, schemas, model contracts, and final outputs |
| Dashboard | `notebooks/streamlit_vrp_hybrid_v2_eod.py` | Displays canonical outputs without independent model mathematics |

Operational paths and component selection are controlled by `config/vrp_hybrid_v2_eod_runtime_config.json`. Locked parameters remain in the production-config and lock JSON files.

## Canonical local data

Large datasets and generated results remain excluded from Git.

| Family | Current location | Intended long-term home |
| --- | --- | --- |
| Raw ThetaData responses and chains | `data/raw/` | Immutable partitioned Parquet in object storage |
| Standardized and research panels | `data/processed/` | Partitioned Parquet queried with DuckDB for research |
| Operational EOD outputs | `data/processed/vrp_hybrid_v2_eod/` | PostgreSQL plus retained Parquet exports |
| Run evidence and repair lineage | `data/audit/` | Object storage with PostgreSQL manifests and checksums |
| SOFR cache | `data/external/fred_sofr_history.csv` | Versioned source asset plus operational manifest |

## Known production coupling

These dependencies are valid at the baseline but must be removed before cloud deployment:

1. `vrp_corsi_source_update_v1.py` executes `old/notebooks forecast model corsi v1/01_corsi_har_rv_forecast_model_research_cleaned.ipynb`.
2. `vrp_implied_variance_eod_update_v1.py` depends on `notebooks v0/old/01_clean_vix_replication_v0_7_exchange_calendar_fred_sofr.ipynb`.
3. The implied-variance path uses generated function-source dumps under ignored `data/audit/production_inventory/`.
4. Several operational defaults and repair-lineage records contain absolute Windows paths under `C:\Users\patri\vrp_project`.
5. The dashboard launcher pins a machine-specific Python executable.
6. Operational state exists only as local files; there is not yet a run ledger or transactional latest-signal publication in PostgreSQL.
7. Canonical publication replaces six files sequentially and runs final health afterward. Backup and rollback reduce recovery risk, but concurrent readers can observe a mixed set and the operation is not transactionally atomic.

The `old/` and `notebooks v0/` trees therefore cannot be deleted merely because they appear archived.

## Migration order

Use this order to minimize calculation risk:

1. Keep the accepted orchestrator callable through `scripts/run_eod.py`.
2. Protect canonical historical outputs with the golden fixture.
3. Introduce the PostgreSQL schema and storage contracts without changing calculation sources.
4. Record file-based EOD runs and outputs in PostgreSQL in shadow-write mode.
5. Reconcile file and database representations before the database serves the latest signal.
6. Extract shared path, calendar, normalization, and atomic-publication utilities into `src/vrp/`.
7. Replace the two runtime notebook dependencies with reviewed Python modules, one at a time.
8. Move each calculation stage behind a stable interface and rerun unit plus golden tests after every extraction.
9. Switch the EOD orchestrator to package modules only after full output reconciliation.
10. Build intraday snapshots on the same package and storage contracts.

Do not begin by moving every file or loading the full historical option chain into PostgreSQL. The first database writes should be run metadata, derived term structures, signal evaluations, selected decisions, and QA results.
