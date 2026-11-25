# Oldpal Swing V1

Standalone local backtest. This project is isolated from the live intraday screener. No Render deployment or scheduled service is used.

## Locked strategy
- Universe: **historical** Nifty 500 constituents, using dated membership intervals.
- Supertrend: ATR 10, multiplier 3, independently on daily and weekly OHLC.
- Entry: weekly Supertrend changes bearish -> bullish; buy at the first available session open of the following week.
- Exit: daily Supertrend changes bullish -> bearish; sell at the next available session open.
- Long only. No extra filters.

## Data requirements and limitations
Kite historical candles are available only for instrument tokens/data that Kite makes available to the account. The current instrument master is not a complete historical symbol/token archive. Kite does **not** provide historical Nifty 500 membership history. The helper uses a community reconstruction, not an official NSE PIT feed: https://github.com/aditya-jha/nse-historical-membership. Its published coverage notes show stronger confidence from 2018 onward and pre-2018 membership/cardinality drift. The source data is current through 2026-10-13. Historical symbols with no current Kite instrument token are recorded in the Excel Data Coverage sheet and are not silently fabricated.

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
4. Copy `.env.example` to `.env`. Add your Kite API key and current access token. Keep this file local; never commit or send credentials in chat.
5. Run `python prepare_universe.py --from 2016-01-01 --to 2026-11-24`. This downloads a community-reconstructed point-in-time Nifty 500 membership table and maps symbols to the current Kite NSE instrument master. It writes `data/universe_membership.csv` and `data/unresolved_symbols.csv`.
6. Review unresolved symbols and membership source types. The source project reports weaker/inexact membership reconstruction before April 2018; current Kite instruments cannot map every delisted or renamed security. Therefore the result is **not guaranteed fully survivorship-bias-free** unless those gaps are independently resolved.
7. Run: `python run_backtest.py --from 2016-01-01 --to 2026-11-24`. The workbook's **Data Coverage** tab includes both mapped symbols and unresolved historical symbols.

The Kite access token is session-based and may need renewal. The script reads it locally from environment variables and never prints it.

## Data handling
- Daily candles are cached under `data/cache/`.
- API calls are throttled.
- Missing token history and unresolved/delisted/renamed symbols are reported in **Data Coverage**; they do not stop the run. They are not silently replaced with another instrument.
- Weekly candles are aggregated from daily candles, with Friday week-ending labels.
- Incomplete final weekly candles are excluded from generating entry signals.

## Outputs
- `output/oldpal_swing_trades.xlsx`: summary, trade ledger, yearly performance, and data coverage.
- `output/oldpal_swing_trades.csv`: flat trade ledger.
- `output/oldpal_swing_coverage.csv`: per-symbol data coverage and unresolved history flags.
- `data/cache/`: downloaded daily candles.

## Backtest interpretation
Trade returns are calculated stock by stock. The workbook also includes an equal-weight active-position portfolio curve: capital is split evenly across positions open on each session, with daily equal-weight returns. This assumes daily rebalancing across active positions; maximum simultaneous positions and live sizing still need to be specified. CAGR/drawdown must be interpreted with this convention in mind.

Costs are configurable in `.env` as percentage estimates. Defaults are zero until the user supplies a brokerage/slippage assumption; the report labels gross/net accordingly.

Corporate-action adjustments, historical membership intervals, and historical instrument-token continuity must be verified before treating results as investment-grade. The engine does not fabricate missing data.
