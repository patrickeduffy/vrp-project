# VRP Production v1 — Step 09 Streamlit Dashboard v3

This version adds a dedicated **Charts** tab to the local VRP dashboard.

## New charts

1. VIX-style vol across the 9 fixed tenors, with current date, prior trading day, and 5 trading days ago.
2. Implied / VIX-style vol versus forecast vol and trailing realized vol across tenors.
3. VRP across tenors, with toggle between log ratio and percent premium.
4. 3m and 1y VRP z-scores across tenors.

The existing Health, Signal, Data Explorer, Run, and Reports tabs remain in place.

## Start

Double-click:

```text
Start VRP Dashboard v3.bat
```

This uses Streamlit port 8503 so it does not conflict with older dashboard instances on 8501/8502.
