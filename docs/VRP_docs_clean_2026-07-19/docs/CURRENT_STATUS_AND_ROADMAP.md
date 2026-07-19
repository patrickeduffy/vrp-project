# Current status and roadmap

Updated: July 19, 2026

## Current status

- Hybrid v2 model methodology is locked.
- The completed-EOD pipeline and Streamlit dashboard exist and are the active operating stack.
- The July production audit and repair are complete.
- The next ordinary EOD run from `main` is the final operational confirmation.
- The unfinished live intraday process is abandoned. Only completed-EOD historical intraday realized-variance predictors remain in the forecast.
- Portfolio overlap, stress caps, hedging, and execution approval remain outside the model lock.

## 1. Confirm normal production operation

Run the next ordinary completed-EOD cycle from `main` without repair overrides.

After a clean run:

- Archive the Phase 4B rollback ZIP.
- Remove temporary repair packages and extracted audit folders.
- Retire the repair branch.

## 2. Finish put-sleeve portfolio sizing

The next major research task is portfolio-level sizing, not further signal optimization.

Test:

- Multiple overlapping trades
- Total open max-loss exposure
- Exposure by Core/Secondary and tenor bucket
- Rolling downside stress
- Moderate and extreme SPY shocks
- Drawdown-based scaling
- Whether hedges permit more gross size than simple exposure caps

Target output:

> Permit the locked per-trade size unless overlap, stress loss, or concentration requires a haircut.

## 3. Complete the short-call sleeve

Use this sequence:

1. Replicate the current Excel 30D call sleeve exactly in Python.
2. Reconcile the Python-versus-Excel signal-frequency discrepancy.
3. Validate SPY trade construction, holiday-aware expiration selection, 1-SD/3-SD strikes, held-to-expiration outcomes, and the Excel-style `LN(VIX^2 / RV21D)` signal.
4. Only after replication matches, replace RV21D with the Corsi forecast denominator.
5. Expand to 9–33 DTE.
6. Sweep equal 3-month and 1-year z-score thresholds.
7. Backtest selection among multiple qualifying call tenors.
8. Determine call-sleeve sizing and portfolio caps.

## 4. Build the unified portfolio layer

After put and call sleeves are independently validated:

- Permit one put and one call trade on the same date when both qualify.
- Track combined downside and upside stress.
- Allocate risk by sleeve, tier, and tenor.
- Evaluate beta, convexity, vega, crash exposure, and premium offsets.
- Define total portfolio exposure caps and hedge rules.

## 5. Stabilize and extend the existing dashboard

The EOD dashboard already exists. The near-term goal is stable, auditable operation—not rebuilding it.

After the portfolio layer is defined, add:

- Open positions
- Portfolio overlap and concentration
- Stress results
- Hedge status
- Combined put/call exposure

Keep the dashboard as a consumer of canonical outputs. Add 15-minute intraday refresh only after completed-EOD operation and portfolio controls are stable.

## 6. Production automation enhancements

After the core portfolio layer is controlled:

- Scheduled source updates
- Failure and data-quality alerts
- Compact run manifests and decision snapshots
- Reliable phone-accessible deployment
- Automated cleanup/retention policies for audit artifacts

## 7. Later extensions

Only after the SPY/SPX system is fully operational:

- Test Corsi portability to QQQ and IWM
- Build ticker-specific implied-variance histories
- Compare absolute and relative VRP across ETFs
- Research SPY-versus-QQQ relative-volatility trades
- Consider additional hedging overlays

## Recommended work order

**Final normal EOD confirmation -> put portfolio overlap/stress/sizing -> call Excel replication -> Corsi call research -> unified portfolio -> dashboard portfolio extensions -> automation enhancements.**
