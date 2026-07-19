# VRP documentation review

Reviewed: July 19, 2026  
Source: uploaded `docs.zip`

## Overall assessment

The archive contains a sound immutable Hybrid v2 model lock, but the surrounding documentation has accumulated duplicated and contradictory operational material from several release stages. The correct cleanup is to preserve the model lock unchanged while replacing the old operating documents with a single current operations guide and a separate roadmap.

## Main findings

### 1. The model lock should not be edited

The following files are exact duplicates of copies already embedded inside the reproduction package:

- `VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx`
- `VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx`
- `VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt`
- `vrp_corsi_intraday_hybrid_v2_lock.json`
- `vrp_corsi_intraday_hybrid_v2_production_config.json`

The model-lock DOCX and JSON are checksum-controlled historical lock artifacts. Editing the DOCX to update later operations would break the reproduction contract. It should remain immutable, with later operational changes documented separately.

### 2. Operational documents conflict with one another

The July 11 model lock and README say that a consolidated runner is not included and may be future work. The dashboard addendum says the runner and dashboard now exist. The original dashboard README says the first local button run is still the final integration test. The July audit/repair is later still.

These are release-history layers, not documents that should all remain active at once.

### 3. The current operating boundary needs to be explicit

The active system is completed EOD. The locked “intraday” predictors are completed-EOD historical realized-variance features. The unfinished live intraday process is abandoned and should not appear as an active production path.

### 4. The old ThetaData/VIX document is no longer an active production specification

`thetadata_vix_style_variance_process_v0_1.docx` is a useful historical record, but it contains superseded statements, including:

- Coverage ending June 25, 2026
- FRED SPX closes as the downstream RV/RSI source
- SPX RSI terminology
- “naked ATM put” process language
- Old output paths and notebook-centric workflow

Current production uses canonical ThetaData SPY EOD closes for SPY returns, RV21D, and RSI. The v0.1 DOCX should not remain in active documentation.

### 5. The reproduction package is not an active repository document

The embedded reproduction ZIP contains research scripts, evidence logs, duplicated docs/configs, and checksums. It is appropriate as an external formal archive, but unnecessary in the active Git documentation set. It increases clutter and duplicates files already stored elsewhere.

### 6. `pip_freeze_current.txt` should be removed

It is a UTF-16 full environment dump containing notebook, server, and unrelated transitive dependencies. It is not a maintainable production dependency contract. Keep the concise root `requirements.txt` instead.

## Cleanup implemented

A cleaned active package was created with:

- Updated root `README.md`
- Current `PRODUCTION_OPERATIONS.md`
- Current `CURRENT_STATUS_AND_ROADMAP.md`
- `DOCUMENTATION_INDEX.md`
- An operational supersession note for the immutable model lock
- The unchanged model-lock DOCX and release notes
- The unchanged lock and production configuration JSON files
- Root `requirements.txt`
- A cleanup manifest

## Files excluded from the active package

- Pre-orchestrator production runbook DOCX
- Duplicate Hybrid v2 README text file
- Original dashboard delivery README
- Dashboard integration addendum
- Heavy reproduction package ZIP
- Historical ThetaData/VIX v0.1 DOCX
- Full `pip freeze` snapshot

The original uploaded archive remains unchanged, so excluded historical files are still recoverable.

## One item not changed

The direct dependency list in `requirements.txt` was retained as supplied. A separate code-import audit against the current repository should be used before changing dependency names or version floors.
