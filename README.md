# VRP Project

Public research and production repository for the SPY/SPX volatility-risk-premium system. Market data, generated outputs, credentials, and local workbooks remain excluded from Git.

The current production release is `vrp_corsi_intraday_hybrid_v2`. Despite the historical release name, the active trading decision is a **completed-EOD** process. It does not place orders or produce a live intraday execution signal.

## Production flow

```text
ThetaData + SOFR
    -> implied-variance update (SPX/SPXW)
    -> SPY market-data update
    -> Wilder RSI update
    -> Corsi source and locked feature-panel update
    -> Hybrid v2 signal publisher
    -> canonical outputs
    -> production health check
    -> exact completed-run handoff
    -> PostgreSQL reference sync and reconciled EOD shadow
    -> finalization status in the EOD audit directory
    -> Streamlit display
```

The canonical file result remains authoritative. The PostgreSQL step is an
automatic, non-authoritative shadow comparison and does not insert a row into
`vrp.signal_publications`.

The July 2026 EOD audit repair is part of the production contract:

- Forecast return features use canonical SPY adjusted closes and SPY log returns.
- SPX or generic-close fallback is prohibited.
- Implied variance remains SPX/SPXW based.
- SOFR uses the latest published observation strictly before the trade date.
- SPXW expiration clocks use the actual XNYS session close, including early closes.
- Wilder RSI uses `wilder_rsi14_spy_close_v3_clean_session_rebuild`.

The accepted repair baseline is through 2026-07-16. Normal production runs advance the canonical histories after that date.

## Active production entry points

- `scripts/run_eod.py` — stable production-facing EOD entry point
- `scripts/finalize_eod_postgres.py` — exact-run PostgreSQL finalization retry
- `scripts/golden_eod.py` — golden-output capture and reconciliation
- `scripts/load_reference_history.py` — validated, revision-safe SOFR/SPY historical backfill
- `scripts/load_eod_snapshot.py` — low-level EOD shadow validation and load
- `notebooks/vrp_hybrid_v2_eod_pipeline.py` — EOD orchestrator
- `notebooks/vrp_hybrid_v2_signal_publish.py` — locked signal, sizing, and selection logic
- `notebooks/vrp_hybrid_v2_health_check.py` — production data and contract validation
- `notebooks/streamlit_vrp_hybrid_v2_eod.py` — dashboard
- `config/vrp_hybrid_v2_eod_runtime_config.json` — operational source and path contract
- `config/vrp_corsi_intraday_hybrid_v2_production_config.json` — locked production parameters
- `config/vrp_corsi_intraday_hybrid_v2_lock.json` — model-lock manifest

## Run the EOD pipeline

From the repository root:

```powershell
python scripts\run_eod.py `
  --project-root C:\Users\patri\vrp_project `
  --approved-nav 1000000
```

Set `VRP_DATABASE_URL` in the invoking process before a normal published run.
The stable entry point delegates the locked calculations to the accepted
notebook-era orchestrator, then automatically synchronizes compact reference
history and reconciles that exact completed run into PostgreSQL. Add
`--dry-run` to inspect the delegated command and post-pass policy without
executing either step.

The wrapper holds a project-wide operation lock across the file run and the
database post-pass. It also stops before a new production run when an older
published run marked as requiring PostgreSQL finalization remains unresolved,
or when recomputing that prior run's PostgreSQL read-back fingerprint finds
missing or changed data. The obligation queue remains fixed at
`data/audit/vrp_hybrid_v2_eod` so configuration drift cannot hide older debt.
Resolve such runs oldest-first with `scripts/finalize_eod_postgres.py`.

Useful diagnostic options:

- `--target-date YYYYMMDD`
- `--force-recalculate`
- `--no-publish`
- `--skip-upstream --no-publish`
- `--no-postgres-shadow`

`--skip-upstream` is diagnostic-only and is rejected unless `--no-publish` is
also supplied. `--no-postgres-shadow` deliberately publishes only the file
result (unless paired with `--no-publish`); it does not require
`VRP_DATABASE_URL`, does not waive older unresolved finalizations, and does not
mark the new run for later automatic finalization. It is a break-glass bypass,
not the way to retry a failed post-pass. A published bypass is explicitly
recorded as `postgres_postpass_bypass_reason: explicit-no-postgres-shadow`;
direct legacy publication without either the normal obligation or that audited
bypass is rejected before the model pipeline starts.

## Run the health check

```powershell
python notebooks\vrp_hybrid_v2_health_check.py `
  --project-root C:\Users\patri\vrp_project `
  --runtime-config C:\Users\patri\vrp_project\config\vrp_hybrid_v2_eod_runtime_config.json `
  --no-thetadata-probe
```

Omit `--no-thetadata-probe` when ThetaData connectivity should be tested.

## Run regression and golden checks

```powershell
python -m unittest discover -s tests -v
python scripts\golden_eod.py verify `
  --source-root C:\Users\patri\vrp_project
```

The ordinary regression suite runs in code-only checkouts. Its production-data reconciliation test runs automatically when canonical data is present or when `VRP_GOLDEN_SOURCE_ROOT` points to the production checkout.

## Validate compact reference history

This command checks the complete SOFR, SPY close/return, Wilder RSI14, and signal
RV21D histories without writing files or requiring PostgreSQL:

```powershell
python scripts\load_reference_history.py all `
  --project-root C:\Users\patri\vrp_project `
  --validate-only
```

Normal published EOD runs perform the database synchronization automatically.
Standalone mutating loaders are administrative tools: they use the same
project and database coordination gates and cannot advance past unresolved EOD
finalization debt. See
[`docs/REFERENCE_DATA_STORAGE.md`](docs/REFERENCE_DATA_STORAGE.md) and
[`docs/EOD_POSTGRES_SHADOW.md`](docs/EOD_POSTGRES_SHADOW.md).

## Launch the dashboard

On the production computer, run `START VRP HYBRID V2.bat`. The equivalent direct command is:

```powershell
python -m streamlit run notebooks\streamlit_vrp_hybrid_v2_eod.py
```

## Repository layout

- `config/` — active model and runtime configuration
- `src/vrp/` — stable production package interfaces
- `scripts/` — production and administrative command-line entry points
- `migrations/` — versioned PostgreSQL operational schema
- `notebooks/` — active production Python entry points and current research
- `tests/` — regression and golden tests for production contracts
- `docs/` — active documentation and immutable model-lock records
- `data/` — local-only market data, generated outputs, and audit records; excluded from Git
- `old/` — retained historical code, superseded repairs, and abandoned experiments; see `old/ARCHIVE_INDEX.md`

## Data policy

The repository intentionally excludes:

- raw ThetaData chains;
- external market-data caches;
- Parquet and serialized datasets;
- generated production outputs and audit logs;
- credentials and local environment files;
- local Excel research workbooks;
- compressed output and reproduction packages.

These remain on the production computer and are covered by `.gitignore`.

The storage boundary is Parquet for immutable raw and standardized market data,
PostgreSQL for compact reference history and reconciled operational shadow
state, and DuckDB for research queries over Parquet. Database signal
publication remains disabled until a separately approved cutover. See
[`docs/DATABASE_ARCHITECTURE.md`](docs/DATABASE_ARCHITECTURE.md).

## Documentation

Start with [`docs/DOCUMENTATION_INDEX.md`](docs/DOCUMENTATION_INDEX.md).

The model-lock DOCX is immutable. Current operations are documented separately so that operational repairs do not alter the historical model-lock artifact.

## Current development priority

The active sequence is deterministic EOD production, 15-minute intraday shadow signals, and remote deployment. The short-call sleeve, combined portfolio research, and larger dashboard expansion are deferred until those three stages are complete. See [`docs/CURRENT_STATUS_AND_ROADMAP.md`](docs/CURRENT_STATUS_AND_ROADMAP.md).
