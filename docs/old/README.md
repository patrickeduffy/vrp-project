# VRP Project

Private repository for the VRP options research and production system.

This repository contains research code, production pipelines, signal-generation logic, validation tools, and dashboard code. Large market-data files, generated outputs, caches, and credentials are intentionally excluded from Git.

## Current Production Architecture

Streamlit dashboard
    -> Hybrid v2 EOD pipeline
        -> 00 SOFR refresh
        -> 01 Implied-variance update
        -> 02 SPY market-data update
        -> 03 Wilder RSI update
        -> 04 Corsi source update
        -> 05 Locked feature-panel update
        -> 05b Implied-variance validation and handoff
    -> Hybrid v2 signal publisher
    -> Production health check

## Active Production Files

- notebooks/streamlit_vrp_hybrid_v2_eod.py
  - Streamlit user interface.

- notebooks/vrp_hybrid_v2_eod_pipeline.py
  - Main EOD production orchestrator.

- notebooks/vrp_hybrid_v2_signal_publish.py
  - Builds forecast, VRP, qualification, sizing, selection, and canonical signal outputs.

- notebooks/vrp_hybrid_v2_health_check.py
  - Validates source coverage, calculations, and published outputs.

- notebooks/vrp_hybrid_v2_common.py
  - Shared runtime configuration, path, JSON, and file-handling utilities.

## Data Policy

The following are intentionally excluded from GitHub:

- Raw ThetaData option chains
- External market-data histories
- Large Parquet and serialized datasets
- Generated production outputs
- Audit runs and logs
- Credentials and local environment files
- Local Excel research workbooks

These files remain on the production computer and are protected by .gitignore.

## Repository Status

Initial private GitHub migration completed in July 2026.

Preserved baseline tag:

v0.1-pre-github-cleanup-baseline
