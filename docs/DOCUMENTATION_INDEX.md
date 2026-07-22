# Documentation index

## Read first

1. [`PRODUCTION_OPERATIONS.md`](PRODUCTION_OPERATIONS.md) — how the current completed-EOD system runs.
2. [`CURRENT_STATUS_AND_ROADMAP.md`](CURRENT_STATUS_AND_ROADMAP.md) — what is finished and what comes next.
3. [`MODEL_LOCK_AND_OPERATIONAL_SUPERSESSION.md`](MODEL_LOCK_AND_OPERATIONAL_SUPERSESSION.md) — precedence between the immutable model lock and later operational repairs.
4. [`GOLDEN_EOD_CONTRACT.md`](GOLDEN_EOD_CONTRACT.md) — immutable examples that protect the accepted calculation contract.
5. [`PRODUCTION_SOURCE_INVENTORY.md`](PRODUCTION_SOURCE_INVENTORY.md) — active components, local data, known coupling, and migration order.
6. [`DATABASE_ARCHITECTURE.md`](DATABASE_ARCHITECTURE.md) — PostgreSQL operational schema and Parquet boundaries.

## Immutable model-lock records

- [`VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx`](VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx)
- [`VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt`](VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt)

Do not edit the model-lock DOCX. Its purpose is to preserve the approved methodology at the time of the lock.

The machine-readable model files live in `config/`, not `docs/`:

- `config/vrp_corsi_intraday_hybrid_v2_lock.json`
- `config/vrp_corsi_intraday_hybrid_v2_production_config.json`
- `config/vrp_hybrid_v2_eod_runtime_config.json`

## Source-of-truth order

When documents appear to conflict, use this order:

1. Current code, regression tests, and the golden EOD contract on `main`.
2. `config/vrp_hybrid_v2_eod_runtime_config.json` for operational data-source and path contracts.
3. The production-config and lock JSON files for locked signal, sizing, and selection parameters.
4. `PRODUCTION_OPERATIONS.md` for operator instructions.
5. The immutable model-lock DOCX for historical methodology context.

## Historical material removed from active documentation

The following were removed because they were duplicated, superseded, or better retained outside the active repository:

- pre-orchestrator production runbook DOCX;
- dashboard delivery README and integration addendum;
- duplicate Hybrid v2 README;
- duplicate lock/config JSON files under `docs/`;
- reproduction-package ZIP;
- historical ThetaData/VIX v0.1 process DOCX;
- full-environment `pip freeze` dump;
- duplicate `README.md` and `requirements.txt` under `docs/`;
- temporary documentation-review report.

They remain recoverable from Git history or, where retained for provenance, under [`old/`](../old/ARCHIVE_INDEX.md).
