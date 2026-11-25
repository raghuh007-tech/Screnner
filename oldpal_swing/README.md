# Oldpal Swing V1

Standalone local backtest. This project is isolated from the live intraday screener. No Render deployment or scheduled service is used.

## Locked strategy
- Universe: **historical** Nifty 500 constituents, using dated membership intervals.
- Supertrend: ATR 10, multiplier 3, independently on daily and weekly OHLC.
- Entry: weekly Supertrend changes bearish -> bullish; buy at the first available session open of the following week.
- Exit: daily Supertrend changes bullish -> bearish; sell at the next available session open.
- Long only. No extra filters.

## Data requirements and limitations
Kite historical candles are available only for instrument tokens/data that Kite makes available to the account. The current instrument master is not a complete historical symbol/token archive. Kite does **not** provide historical Nifty 500 membership history.

Provide `data/universe_membership.csv` with columns:
`tradingsymbol,instrument_token,valid_from,valid_to`
- Dates must be YYYY-MM-DD.
- Use one row per membership interval.
- `valid_to` may be blank for a current constituent.
- Include the correct historical token for each interval. Delisted securities or token changes may need a verified external mapping and/or data source.

**Do not replace this file with today's Nifty 500 list** and call the result survivorship-bias-free. The program stops if the file is missing or malformed.

## Setup (on your computer)
1. Python 3.10+.
2. Create and activate a virtual environment: `python -m venv .venv`.
3. Install: `pip install -r requirements.txt`.
4. Copy `.env.example` to `.env`. Add your Kite API key, access token, and optionally API secret for your own token-refresh workflow. Keep this file local; never commit or send credentials in chat.
5. Add the verified historical membership/token CSV described above.
6. Run: `python run_backtest.py --from 2016-01-01 --to 2026-11-24`.

The Kite access token is session-based and may need renewal. The script reads it locally from environment variables and never prints it.

## Data handling
- Daily candles are cached under `data/cache/`.
- API calls are throttled.
- Missing token history is reported; it is not silently replaced with another instrument.
- Weekly candles are aggregated from daily candles, with Friday week-ending labels.
- Incomplete final weekly candles are excluded from generating entry signals.

## Outputs
- `output/oldpal_swing_trades.xlsx`: summary, trade ledger, yearly performance, and data coverage.
- `output/oldpal_swing_trades.csv`: flat trade ledger.
- `data/cache/`: downloaded daily candles.

## Backtest interpretation
Trade returns are calculated stock by stock. The workbook also includes a transparent equal-weight concurrent-position portfolio curve: on each day, the portfolio's gross daily return is the arithmetic mean of the returns of positions open during that day; uninvested cash earns zero. This is a research convention, not a user-approved live position-sizing rule. CAGR/drawdown from this curve must be interpreted with that convention in mind.

Costs are configurable in `.env` as percentage estimates. Defaults are zero until the user supplies a brokerage/slippage assumption; the report labels gross/net accordingly.

Corporate-action adjustments, historical membership intervals, and historical instrument-token continuity must be verified before treating results as investment-grade. The engine does not fabricate missing data.
