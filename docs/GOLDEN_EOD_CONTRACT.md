# Golden EOD contract

## Purpose

The golden EOD fixture freezes representative outputs from the accepted Hybrid v2 production baseline before orchestration, storage, and package boundaries are changed.

The fixture does not recalculate the model. It records canonical signal and selected-decision rows, then compares completed output artifacts within explicit numeric tolerances. Implementation equivalence is established only when a candidate calculation path produces staged artifacts and those staged artifacts pass this comparison.

Baseline:

- Clean checkout `HEAD` at accepted recapture:
  `c8efe2ed22d53e57ab5e93890dd962e75e8a1448`
- Original model-baseline tag: `eod-v2-production-baseline-2026-07-21`
- Fixture: `tests/golden/eod_v2_production_baseline.json`
- Absolute tolerance: `1e-12`
- Relative tolerance: `1e-10`

The fixture was recaptured after the successful July 22, 2026 EOD run accepted
ThetaData's revised official July 21 SPY record (`open` 746.25 to 746.27,
`low` 744.19 to 744.18, `close` 748.32 to 748.28, and `volume` 19,940,992
to 34,173,496). Only the `latest_no_trade` case changed; the other five
golden cases remained byte-for-byte equivalent at the case-payload level, and
the July 21 decision remained `NO_TRADE`. This was an accepted historical
source revision, not a model, threshold, sizing, or selector change.

## Covered cases

| Case | Date | Contract behavior |
| --- | --- | --- |
| `latest_no_trade` | 2026-07-21 | Latest accepted no-trade EOD decision |
| `dense_core_back_tiebreak` | 2026-04-21 | Core Back selection with many qualifying candidates |
| `core_middle` | 2026-03-16 | Core Middle selection |
| `secondary_back` | 2026-05-15 | Secondary Back selection |
| `secondary_middle` | 2026-05-12 | Secondary Middle selection |
| `secondary_front` | 2026-05-14 | Secondary Front selection under the locked Hybrid v2 implementation |

Each case contains the full nine-tenor signal grid plus its single selected-decision row. The fixture therefore protects forecast values, implied variance, VRP, rolling z-scores, RSI, realized volatility, thresholds, pass/fail states, ranking, sizing, and the final decision.

## Verify the current canonical history

From the repository root:

```powershell
python scripts\golden_eod.py verify `
  --source-root C:\Users\patri\vrp_project
```

Success prints `GOLDEN_STATUS: PASS`. Any differing field is reported with its case, tenor or decision location, expected value, and actual value.

## Verify a staged candidate calculation

A refactored runner must write isolated outputs and verify them before canonical publication:

```powershell
python scripts\golden_eod.py verify `
  --source-root C:\Users\patri\vrp_project `
  --signal-history C:\path\to\staging\candidate_signal_history.parquet `
  --selected-decisions C:\path\to\staging\candidate_selected_decisions.parquet `
  --manifest C:\path\to\staging\golden_verification_manifest.json
```

Passing against the existing canonical files only proves that the stored historical rows have not changed. It does not prove that new calculation code can reproduce them. Candidate-path verification must be part of the migration gate for each extracted calculation stage.

The staged manifest binds the result to the resolved artifact paths, their SHA-256 digests, the exact fixture bytes used for comparison, the accepted baseline commit, the covered cases, and a deterministic content verification ID. That ID excludes the timestamp and machine-specific absolute paths, so identical fixture and artifact content produces the same identity across verification hosts.

Before publication, call the publisher-side manifest validator with the accepted baseline SHA, accepted fixture path and digest, and the two allowed staging paths. It requires `PASS` and `STAGED`, validates the manifest schema and verification ID, enforces the pinned fixture and path policy, and rehashes all three inputs. The publisher must then consume those exact staged artifacts without an intervening mutable step. A bare `GOLDEN_STATUS: PASS` console line is not sufficient authorization for automated publication.

The manifest is unkeyed audit and content-binding evidence, not a cryptographic signature. Its trust model depends on restricting write access to the accepted fixture, staging area, runner, and publication service. A party that can rewrite all of those inputs can forge matching evidence.

The regression suite automatically performs the same reconciliation when canonical data exists under the repository root. In a code-only checkout, set `VRP_GOLDEN_SOURCE_ROOT` to the production-data checkout. Otherwise, only the data-dependent canonical test is skipped; fixture-structure, staged-path, and mismatch-detection tests still run. CI therefore protects the fixture and comparison machinery, while a data-bearing production or staging environment performs calculation reconciliation.

## Recapture policy

Do not recapture this fixture during an ordinary refactor or after a routine daily data append. Historical rows are expected to remain unchanged as canonical histories advance.

Recapture requires one of the following:

1. An explicitly approved new model-lock version.
2. An explicitly accepted historical source repair with its own lineage and reconciliation record.
3. Evidence that the fixture itself was captured incorrectly.

Any recapture must update the baseline commit, explain the reason in current operations documentation, and receive a focused review of every changed field.

The capture command refuses to replace an existing fixture unless `--overwrite` is supplied. The supplied baseline commit must be a full Git SHA and must match the source checkout's current `HEAD`.

The source-file hashes in the fixture record capture provenance. They are not used for routine verification because canonical Parquet files legitimately change when new dates are appended.

The baseline SHA proves which clean checkout was present when the fixture was captured; the current file pipeline does not embed code and configuration identity inside every Parquet artifact, so it cannot cryptographically prove that those bytes were generated by that commit. The baseline is accepted because the July 2026 audit and publication records reconciled it. Future recaptures should use a freshly staged run whose manifest records code, model, and configuration versions; the PostgreSQL run ledger will make that lineage enforceable.
