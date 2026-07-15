# VRP Production v1 - Step 09 Streamlit Dashboard v2

This is the local VRP Production Control Center with a new **Data Explorer** tab.

## New in v2

The Data Explorer tab gives you a one-year default audit table with:

- SPX close and log return
- SOFR/rate percentage from the feature panel
- Tenor and tenor bucket
- VIX-style vol and implied variance
- Forecast vol and forecast variance
- Trailing realized vol and realized variance
- VRP log
- 3m and 1y VRP z-scores
- RV21D
- RSI14
- Core/Secondary pass flags
- Selected trade flag
- Selected layer and risk fraction
- Repair and quote-time audit fields when present

It also includes a daily signal summary table and quick term-structure charts.

## Start

Double-click:

`Start VRP Dashboard.bat`

Or run manually:

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step09_streamlit_dashboard_v2"
py -m pip install -r requirements.txt
py -m streamlit run app.py -- --project-root "C:\Users\patri\vrp_project"
```

## Notes

- This dashboard reads your local production parquet/CSV files.
- It does not change data unless you click a run-control button.
- The Data Explorer calculates locked signal flags for the displayed history using the same threshold/ranking structure used by Step 07.
