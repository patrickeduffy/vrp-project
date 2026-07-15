# VRP Production v1 — Streamlit Dashboard v4

This version improves the chart page after user feedback:

- Forecast and trailing realized vol are no longer shown as two separate overlapping lines when they are identical.
- The chart explicitly labels the current v1 forecast as a baseline forecast equal to trailing realized vol.
- Adds an implied-minus-forecast vol spread chart, which is easier to read than a choppy three-line comparison.
- Adds an optional visual-only smoothing checkbox for the forecast baseline chart.

Run `Start VRP Dashboard v4.bat` or:

```powershell
py -m streamlit run app.py --server.port 8504
```
