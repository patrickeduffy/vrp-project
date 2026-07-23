# Compact historical data in PostgreSQL

## Purpose

Migration `0002_reference_data.sql` adds the small daily histories needed by the
EOD and future intraday signal engines. Full option chains and large research
panels remain in Parquet.

The first compact datasets are:

| Dataset | PostgreSQL values | Canonical file remains |
|---|---|---|
| SOFR | observation date, source percentage rate, derived decimal rate | `data/external/fred_sofr_history.csv` |
| SPY daily features | close, log return, Wilder RSI state, signal RV21D | the accepted SPY, RSI, and realized-volatility Parquet files |

PostgreSQL does not replace those canonical files during the dual-write phase.
It receives validated copies with file digests, formula versions, units, and
pipeline lineage so the two paths can be reconciled before database publication
becomes authoritative.

## Source contracts

### SOFR

The FRED source column is in annual percentage points. A value of `3.57` means
3.57 percent. PostgreSQL retains that exact source value as `rate_percent` and
generates `rate_decimal = 0.0357` for calculations. EOD selection remains the
latest available observation strictly before the valuation date.

The normal FRED download is latest-revised history, not a true point-in-time
vintage. Initial backfills must therefore use `LATEST_REVISED`. A source
correction inserts a successor row; it never overwrites the earlier accepted
observation.

### SPY close and log return

The canonical price file comes from ThetaData's SPY EOD `close` field. Existing
documentation has sometimes called it adjusted, but the artifact does not carry
an explicit adjustment flag. The first database definition must therefore use
`price_adjustment = 'UNKNOWN'` and record the source field. It must not claim an
adjustment convention until that convention is independently confirmed.

The return is the decimal log return:

```text
ln(spy_close[t] / spy_close[t-1])
```

The first return in a bounded history can legitimately be null.

### Wilder RSI14

The accepted formula version is
`wilder_rsi14_spy_close_v3_clean_session_rebuild`. The database retains RSI,
Wilder average gain, and Wilder average loss so the accepted recursive state is
not lost. The definition metadata must also retain the seed provenance and seed
state digest. A migration must copy the accepted state; it must not invent a new
seed from the currently retained price window.

### Signal RV21D

The signal input `rv21d_volatility_pct` is:

```text
rolling_std(spy_log_return, 21, ddof=1) * sqrt(252) * 100
```

It is annualized volatility in percentage points. It is distinct from
`spy_vol_21d_pct`, which is derived from a rolling mean of squared returns.
The database feature definition pins `window=21`, `ddof=1`, and
`annualization_sessions=252` so the two series cannot be silently confused.
Warm-up nulls remain null.

## Revision and idempotency rules

The four new tables are append-only:

- `reference_data_releases` records each accepted normalized dataset version;
- `reference_rate_observations` stores new or revised SOFR rows;
- `daily_market_feature_definitions` pins formulas, units, and source semantics;
- `daily_market_features` stores new or revised daily SPY feature rows.

An unchanged normalized input has the same SHA-256 digest and reuses its release
without inserting rows. A changed value inserts a row pointing to the value it
supersedes. The `current_*` views select the unsuperseded leaf. Database triggers
reject updates and deletes, preserving prior accepted values and published-run
reproducibility.

The loader runs under a dataset-scoped transaction lock. File registration,
the release, its new rows, lineage, QA, and stage completion belong in one outer
transaction; the storage adapter never commits independently. PostgreSQL stores
the transaction ID on the release and its rows, which prevents additional rows
from being attached after that release has committed.

## Historical loader

`scripts/load_reference_history.py` is the production-facing backfill command.
It freezes the exact source bytes before reading them, validates the locked
formulas, writes content-addressed evidence, and then publishes the run, assets,
release, new or corrected rows, and QA result in one PostgreSQL transaction.
It never publishes a trade signal.

Validate the current files without writing anything or connecting to a database:

```powershell
python scripts\load_reference_history.py all `
  --project-root C:\Users\patri\vrp_project `
  --validate-only
```

After a PostgreSQL target has migrations `0001` and `0002` and
`VRP_DATABASE_URL` is configured, load both histories with:

```powershell
python scripts\load_reference_history.py all `
  --project-root C:\Users\patri\vrp_project `
  --environment local
```

Safety properties:

- the accepted SOFR history must begin on 2018-04-03;
- the accepted SPY history must begin on 2018-01-02 and exactly match XNYS
  sessions through its end date;
- SPY, RSI, and RV files must have identical dates and closes;
- bad numeric text cannot be converted into a warm-up null;
- the RSI formula version, recursive state, returns, and signal RV21D are
  recalculated and reconciled before any database write;
- a repeated invocation is a no-op, while a correction adds a successor row;
- an older completed release remains reproducible after a newer correction;
- a failed publication rolls back every data row and records a failed run;
- credentials come only from `VRP_DATABASE_URL`, never a command argument.

For the integrated post-publication EOD path, the same loader runs first under
the narrower `vrp_reference_loader` capability through
`VRP_REFERENCE_DATABASE_URL`. The subsequent EOD snapshot uses a different
`VRP_EOD_DATABASE_URL` identity. The wrapper exposes only one database target
to each child process.

The pinned July 21 source acceptance record lives at
`tests/fixtures/reference_history_20260721_baseline.json`. It preserves source
digests, row counts, date coverage, and latest accepted values for review.

## Operational links

During dual writing, the new foreign keys remain nullable. New database-backed
runs can pin:

- the exact SOFR observation used by a market snapshot; and
- the exact official daily SPY feature row supplying prior-close context and
  fixed RV21D.

For EOD, QA also requires the copied RSI14 to match that daily row. An intraday
preview may use a separately identified `INTRADAY_ESTIMATE` RSI14 and a live SPY
price while continuing to pin RV21D to the prior official daily row.

The duplicated values on immutable run outputs remain intentional. A later
source correction changes the current reference view but cannot change what an
already published run used.

## Development setup

No local PostgreSQL installation is required to validate the files or review
this stage. GitHub Actions starts a disposable PostgreSQL 17 service, applies
migrations `0001` and `0002`, exercises initial load, identical rerun,
correction, rollback, and retry behavior, and confirms the append-only trigger.

A durable local PostgreSQL target has passed initial reference-history and EOD
shadow acceptance. Manual loader commands use `VRP_DATABASE_URL`; the opt-in
integrated path uses the two narrower environment values described above.
Production or durable credentials must never be committed or placed on command
lines. The password in CI belongs only to its disposable test container.
