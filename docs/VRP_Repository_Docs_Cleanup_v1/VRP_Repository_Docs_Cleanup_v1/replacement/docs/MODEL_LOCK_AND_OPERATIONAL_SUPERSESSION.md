# Model lock and operational supersession

## Purpose

The file `VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx` is an immutable historical model-lock artifact. It should not be edited to incorporate later production integration or data-source repairs.

This document explains which later operational records supersede older instructions without changing the locked signal methodology.

## What remains locked

The following remain governed by the Hybrid v2 model lock and production-config JSON:

- forecast specification;
- signal thresholds;
- strict comparison operators;
- per-trade sizing schedule;
- one-trade-per-day selection rule and static tie-breaks;
- SPY put-spread construction and held-to-expiration outcome framework.

Any intentional change to those items requires a new model-lock version.

## What was operationally repaired

The July 2026 EOD audit repair corrected source and calendar implementation contracts:

- canonical SPY adjusted closes and SPY log returns for forecast features;
- no SPX or generic-close fallback;
- SPX/SPXW implied variance retained as the implied leg;
- latest SOFR observation strictly before each trade date;
- actual XNYS expiration close, including early-close sessions;
- clean-session Wilder RSI rebuild;
- deterministic history upserts;
- removal of unsupported forecasts and decisions.

These are implementation and data-integrity corrections, not parameter re-optimization.

## Active operating documents

Use the following for current operations:

- `PRODUCTION_OPERATIONS.md`
- `config/vrp_hybrid_v2_eod_runtime_config.json`
- production Python scripts and regression tests on `main`

The older production runbook and dashboard delivery documents were removed because they predate the consolidated runner or the completed audit repair.

## Naming clarification

`vrp_corsi_intraday_hybrid_v2` is the historical release ID. The active production decision is completed EOD. The locked feature set may include variables originally developed as intraday-enhanced predictors, but the repository does not currently operate a live intraday trading signal.

## Precedence

For operational behavior, current code, tests, and runtime configuration take precedence over obsolete run instructions. For locked model parameters, the production-config and lock JSON files remain authoritative.
