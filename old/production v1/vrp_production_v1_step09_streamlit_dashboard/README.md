# VRP Production v1 - Step 09 Streamlit Dashboard

This is a local, user-friendly control center for the VRP production pipeline.

It does not replace the tested production scripts. It wraps them in a dashboard so you can:

- Check data health without using PowerShell commands.
- See whether term structure, realized variance, VRP, and feature panels are current.
- Review the latest locked 2621 signal snapshot.
- Run the full daily EOD pipeline with one button.
- Run the latest signal only with one button.
- See recent audit reports.

## Install / run

Unzip this folder into:

```text
C:\Users\patri\vrp_project\production v1
```

Then double-click:

```text
Start VRP Dashboard.bat
```

The first launch may take a minute because it installs Streamlit if needed.

## Normal daily workflow

1. Open the dashboard with `Start VRP Dashboard.bat`.
2. Look at the **Health** tab.
3. Go to the **Run** tab.
4. Click **Run daily EOD pipeline**.
5. Go to the **Signal** tab and review the result.

## Important safety note

The signal output remains an input to your manual trading process. The dashboard does not approve portfolio-level risk, margin, stress, or hedging. Trade signals that pass model thresholds are marked `MANUAL_REVIEW_REQUIRED`.

## Current limitation

This is a local desktop dashboard. It is the bridge toward the eventual nicer web/mobile dashboard. The later version can add charts, richer diagnostics, and scheduled refresh.
