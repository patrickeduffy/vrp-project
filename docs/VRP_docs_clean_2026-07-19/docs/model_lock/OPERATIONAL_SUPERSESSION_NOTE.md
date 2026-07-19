# Operational supersession note for Hybrid v2

**Effective date:** July 19, 2026  
**Model lock:** `vrp_corsi_intraday_hybrid_v2`

This note updates operating-status statements made in the immutable July 11, 2026 model-lock package. It does not modify the forecast, features, thresholds, sizing, selector, spread construction, or any other locked model parameter.

## Statements superseded

The following July 11 operating statements are now historical:

1. A consolidated daily runner was described as future work.
2. The dashboard integration was described as a separate delivery awaiting its first local integration test.
3. The July 2 implied-variance gap was described as unrepaired.
4. The unfinished live intraday process could be mistaken for part of the production roadmap.

## Current operating state

- The consolidated completed-EOD runner, locked publisher, health gate, accepted Wilder RSI updater, and Streamlit dashboard are the active production stack.
- The July production audit and repair are complete.
- The next ordinary EOD run from `main` is the final operational confirmation.
- The repaired production contract includes correct SOFR upsert precedence and the intended model decision-date universe.
- The unfinished live intraday process is abandoned and outside the audit and production scope.
- The only intraday information retained in the model is the completed-EOD historical 5D, 21D, and 63D intraday realized-variance predictors.

## Authority hierarchy

1. The model-lock DOCX and lock JSON govern methodology and exact locked parameters.
2. The production configuration JSON governs compact runtime parameters.
3. `PRODUCTION_OPERATIONS.md` governs current operating procedure.
4. This note governs interpretation where the July 11 lock package describes outdated operational status.

No new model-lock version is required because these changes restore and operate the existing locked contract rather than alter it.
