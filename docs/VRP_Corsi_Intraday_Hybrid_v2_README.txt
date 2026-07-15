VRP Corsi Intraday-Enhanced Hybrid v2 Production Lock
======================================================

Lock ID: vrp_corsi_intraday_hybrid_v2
Locked: July 11, 2026
Supersedes: vrp_corsi_intraday_full00444_current_sizing_v1

Primary documents
-----------------
- docs\VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx
- docs\VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx

Machine-readable files
----------------------
- config\vrp_corsi_intraday_hybrid_v2_lock.json
- config\vrp_corsi_intraday_hybrid_v2_production_config.json

Production-locked stack
-----------------------
- Forecast: intraday_ridge_locked
- Core qualification: smooth DTE-specific common-z and RSI lines; bucketed VRP/RV
- Secondary qualification: unchanged FULL_00444 thresholds
- Sizing: continuous-score 50% shrinkage schedule
- Daily selection: highest locked size, then deterministic static tie-breaks
- Trade: SPY ATM / 2SD put credit spread, held to holiday-adjusted expiration

Boundary
--------
Portfolio overlap caps, stress controls, and hedging are external to this model lock. The validated script chain is authoritative; a single consolidated daily runner is not included in this package.

The package includes the inherited forecast/reproduction scripts, all v2 threshold/sizing/selection scripts, evidence logs, and SHA-256 checksums.
