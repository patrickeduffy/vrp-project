# Current status and roadmap

## Current status

The production EOD audit repair is complete.

- Repaired canonical history was published and accepted.
- Forecast return features now use canonical SPY data with no SPX/generic fallback.
- SOFR, early-close expiration clocks, Wilder RSI, and deterministic history updates were repaired.
- Unsupported 2019 forecasts and premature 2020 decisions were removed.
- Regression tests and production health checks pass.
- The repair code was merged into `main`.
- The July 20 implied-variance calendar bottleneck and incremental Wilder-RSI extension defect were repaired and validated in production.

The locked put-sleeve signal methodology should not be re-optimized merely because the repair changed historical results. Portfolio-level put sizing is intentionally deferred. The next workstream is exact replication of the existing 30D Excel short-call sleeve in Python.

## 1. Finish the short-call sleeve

Use this sequence:

1. Replicate the existing 30D Excel call sleeve exactly in Python.
2. Reconcile Python-versus-Excel signal frequency.
3. Validate SPY trade construction, holiday-aware expiration, 1-SD short / 3-SD long strikes, and held-to-expiration outcomes.
4. Match the Excel-style `LN(VIX^2 / RV21D)` signal before changing the denominator.
5. Replace RV21D with the locked Corsi forecast only after replication is trustworthy.
6. Expand to 9-33 DTE.
7. Keep initial 3-month and 1-year z-score thresholds equal during sweeps.
8. Backtest the selection rule across multiple qualifying tenors.
9. Determine call-sleeve sizing and caps.

## 2. Deferred put-sleeve portfolio sizing

This remains useful future work, but it is not the current priority. Until it resumes, the existing per-trade sizing remains unchanged and aggregate exposure approval remains a manual risk decision.

Build and test portfolio-level controls for:

- overlapping open trades;
- aggregate max-loss exposure;
- concentration by layer and tenor bucket;
- rolling downside stress;
- moderate and extreme SPY shock scenarios;
- drawdown-sensitive scaling;
- interaction between per-trade sizing and portfolio caps;
- whether explicit hedges support more efficient gross exposure.

The intended production rule is simple: allow the locked per-trade size unless portfolio overlap, concentration, or stress requires a haircut.

## 3. Combine put and call sleeves

After both sleeves are independently validated:

- permit one put and one call trade on the same date;
- track combined downside and upside stress;
- allocate risk by sleeve, layer, and tenor;
- measure net beta, convexity, vega, and crash exposure;
- test whether call premium materially offsets put stress;
- define unified portfolio caps and hedge rules.

## 4. Extend the dashboard

The completed-EOD dashboard already exists. Add the call sleeve only after its replication and research are validated. Portfolio-risk displays can wait until portfolio sizing resumes.

Priority additions:

- open positions and aggregate risk;
- overlap and concentration caps;
- downside and upside stress results;
- hedge status;
- put/call sleeve attribution;
- concise production alerts and last-successful-run status.

Move to 15-minute intraday refresh only after the EOD system and portfolio layer are stable.

## 5. Production automation

Then add:

- scheduled source refreshes;
- orchestration and failure alerts;
- data-quality alerts;
- retained decision snapshots and run manifests;
- remote, phone-accessible deployment.

Normal operation should rely on compact automated controls rather than large one-off audit packages.

## 6. Later extensions

Only after the SPY/SPX system is fully operational:

- test Corsi portability to QQQ and IWM;
- build ticker-specific implied-variance histories;
- compare absolute and relative VRP across ETFs;
- research SPY-versus-QQQ relative-volatility trades;
- evaluate additional hedge overlays.

## Recommended order

```text
Excel call-sleeve replication
    -> Corsi call research
    -> deferred put portfolio sizing and stress controls
    -> unified put/call portfolio
    -> dashboard risk expansion
    -> automation
    -> multi-ticker research
```
