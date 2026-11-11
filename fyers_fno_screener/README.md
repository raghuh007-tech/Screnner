# Oldpal Dashboard

A private, mobile-first dashboard for NSE stock F&O underlyings. It combines multiple scanner conditions into one duplicate-free list. It only reads market data and does not place orders.

## Screening rules

1. **PDH breakout:** LTP above the previous completed trading day's high, with volume at least 3× the average cumulative volume at the same time over the previous 20 sessions.
2. **Daily momentum:** Daily change of at least +2.00% from the previous close, with the same 3× volume condition.
3. **Near 5-year high:** LTP within 3.00% below the highest daily high in the available five-year data. This is not guaranteed to be a lifetime all-time high. The dashboard links to Strota's separate full-history ATH screen as a reference.
4. **High + momentum:** LTP within 5.00% below the available five-year highest daily high AND daily change at least +2.00% AND volume at least 3× average cumulative same-time volume. This is a five-year-high proxy, not verified lifetime ATH. Strota's separate record is based on split/bonus-adjusted highest daily close and live LTP; it is a cross-check, not an integrated signal.

A stock appears once. All matching signals are grouped in its row/card.

## Full-history ATH reference\n\nThe dashboard links to [Strota's full-history ATH screen](https://strota.in/screens/all-time-high-stocks). It reports split/bonus-adjusted record closing highs, using live LTP during market hours, and a 5% near-record band. It is an external cross-check, not an integrated feed. Strota's data hub lists machine-readable symbol and live-snapshot endpoints but does not currently document a machine-readable ATH-screen endpoint.\n\n## Data method

- The universe is the unique stock underlyings in NSE F&O. Index underlyings are excluded.
- Provider-specific instrument masters are translated into a common internal NSE cash-symbol format.
- Current volume is compared with average cumulative volume at the same time over the previous 20 trading sessions, based on 5-minute candles.
- Previous-day high and the available five-year high are calculated from daily candles.
- Historical baselines load in the background and populate progressively.
- Market quotes are refreshed as snapshots every 20 seconds. This is not a tick-by-tick WebSocket feed.
- Historical data is kept in process memory. A server restart clears it and triggers a fresh warm-up.

## Broker selection

Set `BROKER_PROVIDER` to exactly one of:

- `zerodha` — first provider for the November 2026 rollout.
- `fyers` — planned provider for the later switch.

The dashboard, symbols, scan calculations and display remain the same. Switching provider requires adding the second provider's credentials in the host environment, changing `BROKER_PROVIDER`, and redeploying. Do not put API secrets or access tokens in GitHub.

### Zerodha Kite

Set these Render environment variables:

- `BROKER_PROVIDER=zerodha`
- `KITE_API_KEY`
- `KITE_API_SECRET`
- `KITE_REDIRECT_URI=https://fyers-fno-live-screener.onrender.com/callback`

Create a Kite Connect app and register that exact HTTPS redirect URL. Then open the private dashboard and choose **Connect Zerodha Kite**. The server completes the request-token exchange; the API secret is never sent to the browser.

**Important:** Zerodha's free Personal API tier does not include live or historical market data. The Kite Connect paid plan is required for this screener's quote and candle data. Kite access tokens expire daily, so reconnect after expiry.

### FYERS (later provider)

Set these Render environment variables when switching:

- `BROKER_PROVIDER=fyers`
- `FYERS_APP_ID`
- `FYERS_APP_SECRET`
- `FYERS_REDIRECT_URI=https://fyers-fno-live-screener.onrender.com/callback`

Register the same redirect URL in the FYERS API app. The dashboard then uses the same common scanner calculations with FYERS quotes and candles.

## Common environment variables

- `SCREENER_PASSWORD`: private dashboard password.
- `FLASK_SECRET_KEY`: long random session-signing secret.
- `COOKIE_SECURE=true`: enable secure cookies on HTTPS.
- `KITE_ACCESS_TOKEN` / `FYERS_ACCESS_TOKEN`: optional initial token values; normally leave blank and authenticate from the dashboard.

Use one Gunicorn worker because the current quote and history caches are in process memory:

```bash
gunicorn --workers 1 --threads 8 --bind 0.0.0.0:$PORT app:app
```

## iPhone

Open the deployed HTTPS URL in Safari, unlock the dashboard, connect the selected broker, then use Share → Add to Home Screen.

## Hosting and security notes

- The app has no order-placement endpoints.
- Keep the repository private and do not commit `.env`.
- Never share API keys, API secrets, access tokens, or the dashboard password in chat.
- Render Free services may sleep after inactivity. That can interrupt market monitoring and clear in-memory history.


## Historical cache and API-call control

Historical candles are stored in SQLite by symbol and timeframe.

- First run: fetches the five-year daily history once per stock (in one-year request chunks) and the last 45 calendar days of 5-minute candles once.
- Later trading days: fetches only the most recent 5 calendar days of daily candles and 5-minute candles, then merges them into the stored records. The 45-day 5-minute window is not downloaded again. It does not repeat the five-year backfill.
- Within the same day: the scanner reuses cached history; the 20-second quote refresh does not trigger historical downloads.
- Failed history requests are put on a six-hour retry cooldown.
- The dashboard status endpoint reports how many symbols have both daily and intraday history cached.

**Render storage limitation:** the current Free web service has an ephemeral filesystem. Render deletes local SQLite files when the service spins down, restarts, or redeploys. To preserve the cache across those events, use a paid Render web-service instance and attach a persistent disk mounted at `/var/data`, then set `HISTORY_DB_PATH=/var/data/history_cache.sqlite3`. Do not attach a disk to the current Free instance; Render does not support persistent disks on Free web services. A Free Render Postgres database is not a long-term substitute because it expires after 30 days.


## Oldpal Dashboard market context and sentiment

- The stock universe remains the NSE stock F&O universe only. Indices are excluded from stock breadth.
- Every F&O stock is classified from its daily percentage change:
  - Bullish: +2.00% or higher
  - Mild Bullish: above +0.25% and below +2.00%
  - Neutral: -0.25% to +0.25%
  - Mild Bearish: below -0.25% and above -2.00%
  - Bearish: -2.00% or lower
- Overall F&O sentiment is a weighted breadth score: Bullish stocks count +2, Mild Bullish +1, Mild Bearish -1, Bearish -2, and Neutral 0. It is a breadth summary, not a predictive signal.
- Nifty 50 live snapshot and available NSE sector-index snapshots are requested with the F&O stock quotes through Kite's quote endpoint.
- Top sector is the strongest available mapped NSE sector index by daily percentage change.
- Stock labels use the published Nifty 500 constituent Industry classification. F&O stocks not present in that classification are marked Other F&O. Classification is cached and refreshed monthly; failures are put on a 12-hour cooldown.
- Additional opportunity: **5Y HIGH +2% + 3X** means LTP within 5% below the available five-year highest daily high, daily change at least +2%, and volume at least 3× the average cumulative volume at the same time over the previous 20 sessions. This is not a verified lifetime ATH condition.
