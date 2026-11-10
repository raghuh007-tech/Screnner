# FYERS F&O Live Screener — Mobile V1.1

A private, mobile-first web dashboard for NSE stock F&O underlyings. It combines three scanners into one duplicate-free list and does not place orders.

## Confirmed screening rules

1. **PDH breakout:** LTP > previous completed trading day's high AND current cumulative volume >= 3x the average cumulative volume at the same time over the previous 20 trading sessions.
2. **Daily momentum:** FYERS quote percentage change >= +2.00% from previous close AND the same >= 3x volume condition.
3. **Near ATH:** LTP is within 3.00% below the highest daily high available in FYERS history. On Standard, FYERS documents five years of daily history, so this is a **5-year available-history high**, not guaranteed lifetime ATH. Prime provides longer daily history, but still should not be described as lifetime ATH without checking listing history.

A stock appears once. All qualifying signal labels are grouped in its row/card.

## Data method and limits

- F&O universe: unique stock underlyings parsed from FYERS `NSE_FO.csv`; index underlyings are excluded.
- Quotes: FYERS `/data/quotes` in batches of up to 50 symbols, refreshed every 20 seconds.
- Same-time volume: 5-minute historical candles are grouped by trading date. Each past session's cumulative volume is summed through the same 5-minute time bucket. The average of the last 20 available sessions is compared with the current day's cumulative quote volume.
- Previous day high and available-history high: daily candles. Standard daily history is five years; FYERS allows up to 366 days per history request, so the high history is assembled in chunks.
- Historical baselines load in the background and populate progressively. A full initial history warm-up can take a while due to FYERS Standard's published 50 Data API requests/minute and 5,000/day limits. The app throttles requests below that ceiling.
- Historical data is held in process memory. Restarting the server causes a new warm-up.
- This V1 uses REST quote snapshots every 20 seconds, not tick-by-tick WebSocket data. It is a live snapshot scanner, not a tick-level feed.

## Private deployment

Deploy this Flask app to a host that supports a persistent Python web process and HTTPS (for example, Render or another managed host). Set the environment variables from `.env.example` in the hosting dashboard. Do not upload `.env` to source control.

Required environment variables:
- `SCREENER_PASSWORD`: private dashboard password.
- `FLASK_SECRET_KEY`: long random secret for signed sessions.
- `COOKIE_SECURE=true` when HTTPS is enabled.
- `FYERS_APP_ID`, `FYERS_APP_SECRET`, `FYERS_REDIRECT_URI`.

Set the FYERS API app redirect URI to exactly the deployed `https://your-domain/callback` URL. Enable Market Data permission for Quotes and Historical Data. The user first unlocks the private dashboard, then selects **Connect FYERS** and completes FYERS OAuth. The app stores the resulting token in process memory; FYERS tokens expire and may need reconnection.

Start command:

```bash
gunicorn --workers 1 --threads 8 --bind 0.0.0.0:$PORT app:app
```

Use one worker because quote and historical caches are in process memory. Multiple workers would have separate caches and duplicate API calls. Add persistent storage/database and a shared job queue before scaling horizontally.

## iPhone use

Open the deployed HTTPS URL in Safari, unlock the dashboard, connect FYERS, then use Share → Add to Home Screen. The page is responsive and has a standalone web-app manifest.

## Security

- Never share FYERS App Secret, access token, or dashboard password in chat.
- Do not commit `.env`.
- Keep HTTPS enabled and use a strong `FLASK_SECRET_KEY` and `SCREENER_PASSWORD`.
- The app has no order-placement endpoints.
