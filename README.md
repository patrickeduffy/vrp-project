# VRP Project

Private research and production repository for the SPY/SPX volatility-risk-premium system.

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
    -> Streamlit display
```

The July 2026 EOD audit repair is part of the production contract:

- Forecast return features use canonical SPY adjusted closes and SPY log returns.
- SPX or generic-close fallback is prohibited.
- Implied variance remains SPX/SPXW based.
- SOFR uses the latest published observation strictly before the trade date.
- SPXW expiration clocks use the actual XNYS session close, including early closes.
- Wilder RSI uses `wilder_rsi14_spy_close_v3_clean_session_rebuild`.

The accepted repair baseline is through 2026-07-16. Normal production runs advance the canonical histories after that date.

## Active production entry points

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
python -u notebooks\vrp_hybrid_v2_eod_pipeline.py `
  --project-root C:\Users\patri\vrp_project `
  --approved-nav 1000000
```

Useful diagnostic options:

- `--target-date YYYYMMDD`
- `--force-recalculate`
- `--skip-upstream`
- `--no-publish`

## Run the health check

```powershell
python notebooks\vrp_hybrid_v2_health_check.py `
  --project-root C:\Users\patri\vrp_project `
  --runtime-config C:\Users\patri\vrp_project\config\vrp_hybrid_v2_eod_runtime_config.json `
  --no-thetadata-probe
```

Omit `--no-thetadata-probe` when ThetaData connectivity should be tested.

## Launch the dashboard

```powershell
python -m streamlit run notebooks\streamlit_vrp_hybrid_v2_eod.py
```

## Repository layout

- `config/` — active model and runtime configuration
- `notebooks/` — research notebooks and production Python entry points
- `tests/` — regression tests for production contracts
- `docs/` — active documentation and immutable model-lock records
- `data/` — local-only market data, generated outputs, and audit records; excluded from Git

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

## Documentation

Start with [`docs/DOCUMENTATION_INDEX.md`](docs/DOCUMENTATION_INDEX.md).

The model-lock DOCX is immutable. Current operations are documented separately so that operational repairs do not alter the historical model-lock artifact.

## Current research priority

The next primary workstream is portfolio-level overlap, concentration, and stress sizing for the put sleeve. Signal re-optimization is not the priority. See [`docs/CURRENT_STATUS_AND_ROADMAP.md`](docs/CURRENT_STATUS_AND_ROADMAP.md).
