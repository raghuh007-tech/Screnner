import os
import csv
import io
import time
import json
import math
import queue
import re
import hashlib
import secrets
import threading
import sqlite3
from collections import deque
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, redirect, request, session, url_for

load_dotenv()

app = Flask(__name__)
flask_secret_key = os.getenv('FLASK_SECRET_KEY', '').strip()
if not flask_secret_key:
    # Never use a predictable fallback for a private dashboard. A random
    # process-local key keeps sessions unforgeable until the owner configures
    # a persistent key in the hosting environment.
    flask_secret_key = secrets.token_urlsafe(48)
    app.logger.warning('FLASK_SECRET_KEY is not set; using an ephemeral key until it is configured.')
app.secret_key = flask_secret_key
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=os.getenv('COOKIE_SECURE', 'false').lower() == 'true',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

BROKER_PROVIDER = os.getenv('BROKER_PROVIDER', 'zerodha').strip().lower()
if BROKER_PROVIDER not in {'zerodha', 'fyers'}:
    raise RuntimeError("BROKER_PROVIDER must be 'zerodha' or 'fyers'.")

KITE_API_KEY = os.getenv('KITE_API_KEY', '').strip()
KITE_API_SECRET = os.getenv('KITE_API_SECRET', '').strip()
KITE_REDIRECT_URI = os.getenv('KITE_REDIRECT_URI', '').strip()
FYERS_APP_ID = os.getenv('FYERS_APP_ID', '').strip()
FYERS_APP_SECRET = os.getenv('FYERS_APP_SECRET', '').strip()
FYERS_REDIRECT_URI = os.getenv('FYERS_REDIRECT_URI', '').strip()

APP_ID = KITE_API_KEY if BROKER_PROVIDER == 'zerodha' else FYERS_APP_ID
APP_SECRET = KITE_API_SECRET if BROKER_PROVIDER == 'zerodha' else FYERS_APP_SECRET
REDIRECT_URI = KITE_REDIRECT_URI if BROKER_PROVIDER == 'zerodha' else FYERS_REDIRECT_URI
TOKEN = (os.getenv('KITE_ACCESS_TOKEN', '').strip() if BROKER_PROVIDER == 'zerodha'
         else os.getenv('FYERS_ACCESS_TOKEN', '').strip())
SCREENER_PASSWORD = os.getenv('SCREENER_PASSWORD', '').strip()
KITE_ROOT = 'https://api.kite.trade'
FYERS_ROOT = 'https://api-t1.fyers.in'
FYERS_DATA_ROOT = FYERS_ROOT + '/data'
IST = ZoneInfo('Asia/Kolkata')
INDEX_NAMES = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50', 'SENSEX', 'BANKEX'}

# Tradingsymbol -> display label for NSE sector indices present in Kite's daily instrument master.
SECTOR_INDEX_LABELS = {
    'NIFTY AUTO': 'Auto', 'NIFTY BANK': 'Bank', 'NIFTY REALTY': 'Realty',
    'NIFTY INFRA': 'Infrastructure', 'NIFTY ENERGY': 'Energy', 'NIFTY FMCG': 'FMCG',
    'NIFTY PHARMA': 'Pharma', 'NIFTY PSU BANK': 'PSU Bank', 'NIFTY PVT BANK': 'Private Bank',
    'NIFTY PRIVATE BANK': 'Private Bank', 'NIFTY MEDIA': 'Media', 'NIFTY METAL': 'Metals',
    'NIFTY FIN SERVICE': 'Financial Services', 'NIFTY FINANCIAL SERVICES': 'Financial Services',
    'NIFTY IT': 'IT', 'NIFTY COMMODITIES': 'Commodities', 'NIFTY CONSUMPTION': 'Consumption',
    'NIFTY HEALTHCARE': 'Healthcare', 'NIFTY CHEMICALS': 'Chemicals', 'NIFTY CEMENT': 'Cement',
    'NIFTY CAPITAL GOODS': 'Capital Goods', 'NIFTY CONSUMER DURABLES': 'Consumer Durables',
    'NIFTY CONSUMER SERVICES': 'Consumer Services', 'NIFTY OIL & GAS': 'Oil & Gas',
    'NIFTY INSURANCE': 'Insurance', 'NIFTY NBFC': 'NBFC',
    'NIFTY COMMERCIAL & TRANSPORT SERVICES': 'Commercial & Transport',
    'NIFTY RETAIL': 'Retail', 'NIFTY TELECOMMUNICATIONS': 'Telecommunications',
}
NIFTY500_SECTOR_CSV = 'https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv'
NIFTY500_SECTOR_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36',
    'Referer': 'https://www.nseindia.com/',
    'Accept': 'text/csv,text/plain,*/*',
}

cache = {
    'symbols': [], 'quotes': {}, 'intraday': {}, 'daily': {}, 'instrument_tokens': {},
    'last_scan': 0, 'last_error': None, 'history_done': 0,
    'history_total': 0, 'history_running': False, 'history_message': 'Waiting for broker connection',
    'connected': bool(TOKEN), 'last_quote_count': 0,
    'market_quotes': {}, 'market_index_symbols': [], 'sector_index_labels': {},
    'stock_sectors': {}, 'sector_updated_at': None, 'sector_retry_after': None, 'sector_refresh_running': False,
    'sector_error': None,
}
lock = threading.RLock()
# Per-process one-minute price snapshots used for live 5-minute momentum detection.
momentum_snapshots = {}
telegram_alert_state = {'fresh': set(), 'strong': set()}
telegram_alert_lock = threading.Lock()

# Historical candle cache. Use /var/data on a Render service with a
# persistent disk; the default local path is only durable for the life of
# an ephemeral Render instance.
HISTORY_DB_PATH = os.getenv(
    'HISTORY_DB_PATH',
    os.path.join(os.path.dirname(__file__), 'instance', 'history_cache.sqlite3')
)
history_db_lock = threading.RLock()
history_meta = {}

# Per-symbol throttle for live chart candle refreshes. The chart is user-driven;
# do not refresh all 218 symbols every minute.
chart_history_refresh_at = {}
chart_history_refresh_lock = threading.Lock()
CHART_HISTORY_REFRESH_SECONDS = 45


def refresh_chart_history(symbol):
    """Fetch today's latest 5-minute candles for a chart currently being viewed."""
    now = datetime.now(IST)
    with chart_history_refresh_lock:
        last_refresh = chart_history_refresh_at.get(symbol, 0)
        if time.monotonic() - last_refresh < CHART_HISTORY_REFRESH_SECONDS:
            return
        chart_history_refresh_at[symbol] = time.monotonic()

    try:
        today = now.date()
        recent = fetch_history(symbol, '5', today.isoformat(), today.isoformat())
        if not recent:
            return
        with lock:
            existing = cache['intraday'].get(symbol, [])
            merged = merge_candles(existing, recent)
            cutoff = today - timedelta(days=45)
            merged = [
                candle for candle in merged
                if len(candle) >= 6
                and datetime.fromtimestamp(int(candle[0]), IST).date() >= cutoff
            ]
            cache['intraday'][symbol] = merged
        save_history(symbol, '5m', merged, today.isoformat())
    except Exception as exc:
        app.logger.warning('Live chart candle refresh failed for %s: %s', symbol, exc)
        # Permit an earlier retry after a transient provider/network failure.
        with chart_history_refresh_lock:
            chart_history_refresh_at[symbol] = 0


def _history_db_connect():
    folder = os.path.dirname(os.path.abspath(HISTORY_DB_PATH))
    os.makedirs(folder, exist_ok=True)
    connection = sqlite3.connect(HISTORY_DB_PATH, timeout=30)
    connection.execute('PRAGMA busy_timeout=30000')
    return connection


def summarize_daily_candles(candles, today=None):
    """Keep only previous-session high and rolling 52-week high in RAM."""
    today = today or datetime.now(IST).date()
    cutoff = today - timedelta(days=366)
    latest_date = None
    latest_high = None
    week52_high = None
    for candle in candles or []:
        try:
            if len(candle) < 6:
                continue
            candle_date = datetime.fromtimestamp(int(candle[0]), IST).date()
            if candle_date >= today:
                continue
            high = float(candle[2])
            if candle_date >= cutoff:
                week52_high = high if week52_high is None else max(week52_high, high)
            if latest_date is None or candle_date > latest_date:
                latest_date, latest_high = candle_date, high
            elif candle_date == latest_date:
                latest_high = max(latest_high, high)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
    return {
        'last_date': latest_date.isoformat() if latest_date else None,
        'prev_day_high': latest_high,
        'week52_high': week52_high,
    }


def init_history_store():
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS candle_cache (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    candles_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, timeframe)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS history_meta (
                    symbol TEXT PRIMARY KEY,
                    intraday_fetch_date TEXT,
                    daily_fetch_date TEXT,
                    retry_after TEXT,
                    last_error TEXT
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS stock_sector_cache (
                    symbol TEXT PRIMARY KEY,
                    sector TEXT NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS app_meta (
                    meta_key TEXT PRIMARY KEY,
                    meta_value TEXT NOT NULL
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS momentum_alert_active (
                    kind TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY (kind, symbol)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS top_gainer_daily_state (
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    is_new INTEGER NOT NULL DEFAULT 0,
                    alert_sent INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (trade_date, symbol)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS top_gainer_snapshot (
                    trade_date TEXT PRIMARY KEY,
                    symbols_json TEXT NOT NULL
                )
            """)
            # Retain compatibility with the legacy active-signal table.
            alert_rows = db.execute(
                "SELECT kind, symbol FROM momentum_alert_active"
            ).fetchall()
            rows = db.execute(
                "SELECT symbol, timeframe, candles_json FROM candle_cache"
            ).fetchall()
            meta_rows = db.execute(
                "SELECT symbol, intraday_fetch_date, daily_fetch_date, retry_after, last_error FROM history_meta"
            ).fetchall()
            sector_rows = db.execute(
                "SELECT symbol, sector FROM stock_sector_cache"
            ).fetchall()
            app_meta_rows = db.execute(
                "SELECT meta_key, meta_value FROM app_meta"
            ).fetchall()

        for symbol, timeframe, candles_json in rows:
            try:
                candles = json.loads(candles_json)
                if timeframe == '5m':
                    cutoff = datetime.now(IST).date() - timedelta(days=45)
                    trimmed = []
                    for candle in candles:
                        try:
                            if len(candle) >= 6 and datetime.fromtimestamp(int(candle[0]), IST).date() >= cutoff:
                                trimmed.append(candle)
                        except (TypeError, ValueError, OverflowError, OSError):
                            continue
                    cache['intraday'][symbol] = trimmed
                elif timeframe == '1d':
                    cache['daily'][symbol] = candles if isinstance(candles, dict) else summarize_daily_candles(candles)
            except (TypeError, ValueError, json.JSONDecodeError):
                app.logger.warning('Ignoring invalid cached history for %s (%s)', symbol, timeframe)

        for row in meta_rows:
            history_meta[row[0]] = {
                'intraday_fetch_date': row[1],
                'daily_fetch_date': row[2],
                'retry_after': row[3],
                'last_error': row[4],
            }
        cache['stock_sectors'].update({symbol: sector for symbol, sector in sector_rows})
        for kind, symbol in alert_rows:
            if kind in telegram_alert_state:
                telegram_alert_state[kind].add(symbol)

        for key, value in app_meta_rows:
            if key == 'sector_updated_at':
                cache['sector_updated_at'] = value
            elif key == 'sector_retry_after':
                cache['sector_retry_after'] = value
            elif key == 'broker_access_token' and not TOKEN:
                # The Render persistent disk keeps the broker session across daily suspends.
                globals()['TOKEN'] = value

        app.logger.info(
            'Historical cache loaded from %s: %d symbols with daily candles, %d with intraday candles',
            HISTORY_DB_PATH, len(cache['daily']), len(cache['intraday'])
        )


def save_history(symbol, timeframe, candles, fetched_date=None):
    fetched_date = fetched_date or datetime.now(IST).date().isoformat()
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute("""
                INSERT INTO candle_cache(symbol, timeframe, candles_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe) DO UPDATE SET
                    candles_json=excluded.candles_json,
                    updated_at=excluded.updated_at
            """, (symbol, timeframe, json.dumps(candles, separators=(',', ':')), datetime.now(IST).isoformat()))
            if timeframe in {'5m', '1d'}:
                field = 'intraday_fetch_date' if timeframe == '5m' else 'daily_fetch_date'
                db.execute(f"""
                    INSERT INTO history_meta(symbol, {field})
                    VALUES (?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET {field}=excluded.{field}
                """, (symbol, fetched_date))
    history_meta.setdefault(symbol, {})[
        'intraday_fetch_date' if timeframe == '5m' else 'daily_fetch_date'
    ] = fetched_date


def update_history_error(symbol, message, cooldown_hours=6):
    retry_at = (datetime.now(IST) + timedelta(hours=cooldown_hours)).isoformat()
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute("""
                INSERT INTO history_meta(symbol, retry_after, last_error)
                VALUES (?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    retry_after=excluded.retry_after,
                    last_error=excluded.last_error
            """, (symbol, retry_at, str(message)[:500]))
    history_meta.setdefault(symbol, {}).update({
        'retry_after': retry_at,
        'last_error': str(message)[:500],
    })


def clear_history_retry(symbol):
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute("UPDATE history_meta SET retry_after=NULL, last_error=NULL WHERE symbol=?", (symbol,))
    history_meta.setdefault(symbol, {}).update({'retry_after': None, 'last_error': None})


def merge_candles(old_candles, new_candles):
    """Merge sorted candle lists without allocating a large timestamp dictionary."""
    old = [c for c in (old_candles or []) if isinstance(c, (list, tuple)) and len(c) >= 6]
    new = [c for c in (new_candles or []) if isinstance(c, (list, tuple)) and len(c) >= 6]
    merged = []
    i = j = 0
    while i < len(old) and j < len(new):
        try:
            old_ts = int(old[i][0])
            new_ts = int(new[j][0])
        except (TypeError, ValueError, OverflowError):
            if i < len(old):
                i += 1
            if j < len(new):
                j += 1
            continue
        if old_ts < new_ts:
            merged.append(old[i])
            i += 1
        elif new_ts < old_ts:
            merged.append(new[j])
            j += 1
        else:
            merged.append(new[j])
            i += 1
            j += 1
    merged.extend(old[i:])
    merged.extend(new[j:])
    return merged


def history_due(symbol, today=None):
    today = today or datetime.now(IST).date()
    meta = history_meta.get(symbol, {})
    retry_after = meta.get('retry_after')
    last_error = str(meta.get('last_error') or '')
    # Allow recovery from the previous undefined-history-variable bug immediately
    # after deployment instead of waiting out its generic six-hour cooldown.
    fixed_history_name_error = (
        "not defined" in last_error
        and any(name in last_error for name in (
            'daily_exists', 'daily_existing', 'intraday_exists', 'intraday_existing'
        ))
    )
    if retry_after and not fixed_history_name_error:
        try:
            if datetime.fromisoformat(retry_after) > datetime.now(IST):
                return False
        except ValueError:
            pass
    needs_daily = symbol not in cache['daily'] or meta.get('daily_fetch_date') != today.isoformat()
    needs_intraday = symbol not in cache['intraday'] or meta.get('intraday_fetch_date') != today.isoformat()
    return needs_daily or needs_intraday


try:
    init_history_store()
except Exception as history_store_error:
    app.logger.exception('Historical cache could not be initialised: %s', history_store_error)

# FYERS Standard Data API limits: keep a safety margin below its published quota.
_api_times = deque()
_history_times = deque()
_api_lock = threading.Lock()

# Kite Connect documented limits: quote 1 request/second; historical 3 requests/second.
_kite_rate_lock = threading.Lock()
_kite_last_quote = 0.0
_kite_history_times = deque()


def api_slot(priority=False):
    """Throttle provider calls within the active broker's documented limits."""
    global _kite_last_quote
    if BROKER_PROVIDER == 'zerodha':
        while True:
            with _kite_rate_lock:
                now = time.monotonic()
                while _kite_history_times and now - _kite_history_times[0] >= 1:
                    _kite_history_times.popleft()
                if priority:
                    wait = max(0.0, 1.05 - (now - _kite_last_quote))
                    if wait <= 0:
                        _kite_last_quote = now
                        return
                else:
                    wait = max(0.0, 0.36 - (now - _kite_history_times[-1])) if _kite_history_times else 0.0
                    if len(_kite_history_times) < 3 and wait <= 0:
                        _kite_history_times.append(now)
                        return
                    if len(_kite_history_times) >= 3:
                        wait = max(wait, 1.0 - (now - _kite_history_times[0]) + 0.02)
            time.sleep(max(wait, 0.02))
    else:
        while True:
            with _api_lock:
                now = time.monotonic()
                while _api_times and now - _api_times[0] >= 60:
                    _api_times.popleft()
                while _history_times and now - _history_times[0] >= 60:
                    _history_times.popleft()
                total_ok = len(_api_times) < 44
                history_ok = priority or len(_history_times) < 25
                spacing_ok = not _api_times or now - _api_times[-1] >= 0.22
                if total_ok and history_ok and spacing_ok:
                    _api_times.append(now)
                    if not priority:
                        _history_times.append(now)
                    return
                waits = [0.23]
                if not total_ok and _api_times:
                    waits.append(60 - (now - _api_times[0]) + 0.05)
                if not history_ok and _history_times:
                    waits.append(60 - (now - _history_times[0]) + 0.05)
                wait = max(waits)
            time.sleep(wait)


def kite_headers():
    if not KITE_API_KEY or not TOKEN:
        raise RuntimeError('Zerodha Kite is not connected. Select Connect Broker and sign in.')
    return {
        'Authorization': f'token {KITE_API_KEY}:{TOKEN}',
        'X-Kite-Version': '3',
        'Content-Type': 'application/x-www-form-urlencoded',
    }


def fyers_headers():
    if not FYERS_APP_ID or not TOKEN:
        raise RuntimeError('FYERS is not connected. Select Connect Broker and sign in.')
    return {'Authorization': f'{FYERS_APP_ID}:{TOKEN}', 'Content-Type': 'application/json'}


def broker_get(path, params=None, priority=False):
    if BROKER_PROVIDER == 'zerodha':
        api_slot(priority=priority)
        response = requests.get(KITE_ROOT + path, headers=kite_headers(), params=params, timeout=30)
        if response.status_code == 429:
            raise RuntimeError('Kite Connect rate limit reached. The scanner will retry on its next refresh.')
        response.raise_for_status()
        payload = response.json()
        if payload.get('status') == 'error':
            raise RuntimeError(payload.get('message') or str(payload))
        return payload
    api_slot(priority=priority)
    response = requests.get(FYERS_DATA_ROOT + path, headers=fyers_headers(), params=params, timeout=25)
    if response.status_code == 429:
        raise RuntimeError('FYERS data API rate limit reached. The scanner will retry on its next refresh.')
    response.raise_for_status()
    payload = response.json()
    if payload.get('s') == 'error':
        raise RuntimeError(payload.get('message') or str(payload))
    return payload




def _sector_symbol_key(value):
    return re.sub(r'[^A-Z0-9]', '', (value or '').strip().upper())


def refresh_stock_sectors(symbols):
    """Refresh broad sector labels from Nifty 500's published constituent classification."""
    if BROKER_PROVIDER != 'zerodha':
        return
    updated_at = cache.get('sector_updated_at')
    retry_after = cache.get('sector_retry_after')
    if retry_after:
        try:
            if datetime.fromisoformat(retry_after) > datetime.now(IST):
                return
        except ValueError:
            pass
    missing = [symbol for symbol in symbols if symbol not in cache['stock_sectors']]
    refresh_due = not updated_at
    if updated_at:
        try:
            refresh_due = datetime.fromisoformat(updated_at) <= datetime.now(IST) - timedelta(days=30)
        except ValueError:
            refresh_due = True
    if not refresh_due and not missing:
        return

    with lock:
        if cache['sector_refresh_running']:
            return
        cache['sector_refresh_running'] = True
    try:
        response = requests.get(NIFTY500_SECTOR_CSV, headers=NIFTY500_SECTOR_HEADERS, timeout=30)
        response.raise_for_status()
        rows = csv.DictReader(io.StringIO(response.text))
        mapping = {}
        for row in rows:
            key = _sector_symbol_key(row.get('Symbol'))
            sector = (row.get('Industry') or '').strip()
            if key and sector:
                mapping[key] = sector
        if not mapping:
            raise RuntimeError('Nifty 500 sector file returned no symbol classifications.')

        timestamp = datetime.now(IST).isoformat(timespec='seconds')
        updates = {}
        for symbol in symbols:
            ticker = symbol.replace('NSE:', '').replace('-EQ', '')
            # F&O stocks outside Nifty 500 are kept visible and clearly marked.
            updates[symbol] = mapping.get(_sector_symbol_key(ticker), 'Other F&O')

        with history_db_lock:
            with _history_db_connect() as db:
                db.executemany("""
                    INSERT INTO stock_sector_cache(symbol, sector, source, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        sector=excluded.sector,
                        source=excluded.source,
                        updated_at=excluded.updated_at
                """, [(symbol, sector, 'Nifty 500 Industry', timestamp) for symbol, sector in updates.items()])
                db.execute("""
                    INSERT INTO app_meta(meta_key, meta_value) VALUES ('sector_updated_at', ?)
                    ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value
                """, (timestamp,))
        cache['stock_sectors'].update(updates)
        cache['sector_updated_at'] = timestamp
        cache['sector_retry_after'] = None
        cache['sector_error'] = None
        app.logger.info('Loaded sector classifications for %d F&O stocks from Nifty 500 constituent file', len(updates))
    except Exception as exc:
        cache['sector_error'] = str(exc)
        retry_at = (datetime.now(IST) + timedelta(hours=12)).isoformat(timespec='seconds')
        cache['sector_retry_after'] = retry_at
        try:
            with history_db_lock:
                with _history_db_connect() as db:
                    db.execute("""
                        INSERT INTO app_meta(meta_key, meta_value) VALUES ('sector_retry_after', ?)
                        ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value
                    """, (retry_at,))
        except Exception as store_error:
            app.logger.warning('Could not save sector retry cooldown: %s', store_error)
        app.logger.warning('Sector classification refresh failed; retry after %s: %s', retry_at, exc)
        for symbol in symbols:
            cache['stock_sectors'].setdefault(symbol, 'Sector unavailable')
    finally:
        with lock:
            cache['sector_refresh_running'] = False


def _normalise_symbol(name):
    name = (name or '').strip().upper()
    return f'NSE:{name}-EQ' if name else ''


def get_symbol_universe():
    """Return the unique stock F&O underlyings in a broker-neutral NSE:SYMBOL-EQ format."""
    if BROKER_PROVIDER == 'zerodha':
        response = requests.get(KITE_ROOT + '/instruments', headers=kite_headers(), timeout=60)
        response.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(response.text)))
        cash_rows = [
            row for row in rows
            if row.get('exchange') == 'NSE'
            and row.get('segment') == 'NSE'
            and row.get('instrument_type') == 'EQ'
        ]
        cash_by_name = {}
        cash_symbols = set()
        for row in cash_rows:
            symbol = row.get('tradingsymbol', '').strip().upper()
            name = row.get('name', '').strip().upper()
            if symbol:
                cash_symbols.add(symbol)
                if name:
                    cash_by_name[name] = symbol
        # Some instrument-master rows have a blank company name. Fall back to
        # matching the longest NSE cash ticker at the start of the futures symbol.
        sorted_cash = sorted(cash_symbols, key=len, reverse=True)
        underlyings = set()
        for row in rows:
            if row.get('exchange') != 'NFO' or row.get('segment') != 'NFO-FUT' or row.get('instrument_type') != 'FUT':
                continue
            name = row.get('name', '').strip().upper()
            symbol = cash_by_name.get(name)
            if not symbol:
                future_symbol = row.get('tradingsymbol', '').strip().upper()
                symbol = next((candidate for candidate in sorted_cash if future_symbol.startswith(candidate)), None)
            if symbol and symbol not in INDEX_NAMES:
                underlyings.add(symbol)

        token_map = {
            row['tradingsymbol'].strip().upper(): int(row['instrument_token'])
            for row in cash_rows
            if row.get('tradingsymbol') and row.get('instrument_token')
        }
        index_names = {
            row.get('tradingsymbol', '').strip().upper()
            for row in rows
            if row.get('exchange') == 'NSE' and row.get('segment') == 'INDICES'
            and row.get('tradingsymbol')
        }
        sector_indices = {
            f'NSE:{name}': SECTOR_INDEX_LABELS[name]
            for name in index_names if name in SECTOR_INDEX_LABELS
        }
        if 'NIFTY 50' in index_names:
            market_index_symbols = ['NSE:NIFTY 50'] + sorted(sector_indices)
        else:
            market_index_symbols = sorted(sector_indices)
        with lock:
            cache['market_index_symbols'] = market_index_symbols
            cache['sector_index_labels'] = sector_indices
            cache['instrument_tokens'] = {
                _normalise_symbol(symbol): token_map[symbol]
                for symbol in underlyings if symbol in token_map
            }
        return sorted(_normalise_symbol(symbol) for symbol in underlyings if symbol in token_map)

    # FYERS instrument masters are public CSVs. Keep the same canonical
    # broker-neutral symbol shape used by the rest of the scanner.
    fo_response = requests.get('https://public.fyers.in/sym_details/NSE_FO.csv', timeout=30)
    fo_response.raise_for_status()
    cm_response = requests.get('https://public.fyers.in/sym_details/NSE_CM.csv', timeout=30)
    cm_response.raise_for_status()
    fo_rows = list(csv.reader(io.StringIO(fo_response.text)))
    cm_rows = list(csv.reader(io.StringIO(cm_response.text)))
    if not fo_rows or not cm_rows:
        return []
    fo_header = [cell.strip().lower() for cell in fo_rows[0]]
    cm_header = [cell.strip().lower() for cell in cm_rows[0]]
    fo_has_header = any('underlying symbol' in cell for cell in fo_header)
    cm_has_header = any('symbol ticker' in cell for cell in cm_header)
    fo_data = fo_rows[1:] if fo_has_header else fo_rows
    cm_data = cm_rows[1:] if cm_has_header else cm_rows
    underlying_idx = next((i for i, h in enumerate(fo_header) if 'underlying symbol' in h), 13) if fo_has_header else 13
    ticker_idx = next((i for i, h in enumerate(cm_header) if 'symbol ticker' in h), 9) if cm_has_header else 9
    cash_symbols = {
        row[ticker_idx].strip().upper()
        for row in cm_data
        if len(row) > ticker_idx and row[ticker_idx].strip().upper().startswith('NSE:')
        and row[ticker_idx].strip().upper().endswith('-EQ')
    }
    result = set()
    for row in fo_data:
        if len(row) <= underlying_idx:
            continue
        name = row[underlying_idx].strip().upper()
        candidate = _normalise_symbol(name)
        if name and name not in INDEX_NAMES and candidate in cash_symbols:
            result.add(candidate)
    return sorted(result)


def _normalise_kite_quote(symbol, item):
    ohlc = item.get('ohlc') or {}
    close = ohlc.get('close')
    last_price = item.get('last_price')
    net_change = item.get('net_change')
    change_pct = None
    if close and last_price is not None:
        # Derive percentage from the actual LTP and prior close. This also
        # avoids displaying a false 0% when the API's net_change is empty/zero.
        change_pct = (float(last_price) / float(close) - 1.0) * 100.0
    elif close and net_change is not None:
        change_pct = float(net_change) / float(close) * 100.0
    return {
        'lp': item.get('last_price'),
        'prev_close_price': close,
        'chp': change_pct,
        'volume': item.get('volume'),
        'high_price': ohlc.get('high'),
    }


def refresh_quotes(symbols):
    """Fetch stock and index snapshots together, then keep their caches separate."""
    result = {}
    market_result = {}
    if BROKER_PROVIDER == 'zerodha':
        kite_symbols = [symbol.replace('-EQ', '') for symbol in symbols]
        with lock:
            market_symbols = list(cache.get('market_index_symbols', []))
        requested = kite_symbols + market_symbols
        for start in range(0, len(requested), 500):
            batch = requested[start:start + 500]
            params = [('i', symbol) for symbol in batch]
            payload = broker_get('/quote', params=params, priority=True)
            for key, item in (payload.get('data') or {}).items():
                normalized = _normalise_kite_quote(key, item)
                if key in market_symbols:
                    market_result[key] = normalized
                else:
                    result[key + '-EQ'] = normalized
    else:
        for start in range(0, len(symbols), 50):
            batch = symbols[start:start + 50]
            payload = broker_get('/quotes', {'symbols': ','.join(batch)}, priority=True)
            for item in payload.get('d', []):
                if item.get('s') == 'ok':
                    result[item.get('n', '')] = item.get('v', {})
    with lock:
        cache['quotes'].update(result)
        cache['market_quotes'] = market_result
        cache['last_scan'] = time.time()
        cache['last_quote_count'] = len(result)
        cache['last_error'] = None
    return result


def market_context():
    """Summarise Nifty 50 and the best-performing available NSE sector index."""
    with lock:
        quotes = dict(cache.get('market_quotes', {}))
        labels = dict(cache.get('sector_index_labels', {}))
    nifty = quotes.get('NSE:NIFTY 50')
    nifty_data = {
        'price': nifty.get('lp') if nifty else None,
        'change_pct': nifty.get('chp') if nifty else None,
    }
    sectors = []
    for symbol, label in labels.items():
        quote = quotes.get(symbol)
        if not quote or quote.get('chp') is None:
            continue
        try:
            sectors.append({
                'name': label,
                'change_pct': float(quote['chp']),
                'symbol': symbol.replace('NSE:', ''),
            })
        except (TypeError, ValueError):
            continue
    sectors.sort(key=lambda item: item['change_pct'], reverse=True)
    return {
        'nifty50': nifty_data,
        'top_sector': sectors[0] if sectors else None,
        'sector_count': len(sectors),
        'sectors': sectors,
    }


def _kite_timestamp_epoch(value):
    try:
        stamp = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S%z')
    except ValueError:
        try:
            stamp = datetime.fromisoformat(value)
        except ValueError:
            stamp = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=IST)
    return int(stamp.timestamp())


def fetch_history(symbol, resolution, start_date, end_date):
    if BROKER_PROVIDER == 'zerodha':
        with lock:
            instrument_token = cache['instrument_tokens'].get(symbol)
        if not instrument_token:
            raise RuntimeError(f'Kite instrument token unavailable for {symbol}. Refresh the F&O instrument master.')
        interval = {'5': '5minute', 'D': 'day'}.get(str(resolution), str(resolution))
        params = {
            'from': f'{start_date} 00:00:00',
            'to': f'{end_date} 23:59:59',
            'oi': 0,
        }
        payload = broker_get(f'/instruments/historical/{instrument_token}/{interval}', params=params)
        candles = ((payload.get('data') or {}).get('candles') or [])
        normalised = []
        for candle in candles:
            if len(candle) >= 6:
                normalised.append([_kite_timestamp_epoch(str(candle[0]))] + list(candle[1:6]))
        return normalised

    payload = broker_get('/history', {
        'symbol': symbol,
        'resolution': resolution,
        'date_format': '1',
        'range_from': start_date,
        'range_to': end_date,
    })
    return payload.get('candles', [])


def date_chunks(start: date, end: date, days=366):
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=days - 1))
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def warm_history_for_symbol(symbol):
    """Backfill once, then refresh only recent candles once per calendar day."""
    today = datetime.now(IST).date()
    meta = history_meta.get(symbol, {})
    # Snapshot existing in-memory history before deciding which provider calls are needed.
    with lock:
        daily_exists = symbol in cache['daily']
        daily_existing = cache['daily'].get(symbol)
        intraday_exists = symbol in cache['intraday']
        intraday_existing = cache['intraday'].get(symbol)

    # One year of daily candles is enough for rolling 52-week high.
    # Refresh the window once per day so old highs roll off correctly.
    if not daily_exists or not isinstance(daily_existing, dict) or meta.get('daily_fetch_date') != today.isoformat():
        daily_start = today - timedelta(days=366)
        app.logger.info('52-week daily history refresh for %s: %s to %s', symbol, daily_start, today - timedelta(days=1))
        daily_candles = fetch_history(symbol, 'D', daily_start.isoformat(), today.isoformat())
        daily_summary = summarize_daily_candles(daily_candles, today)
        # Persist only the compact summary, not full daily candle history.
        save_history(symbol, '1d', daily_summary, today.isoformat())
        del daily_candles
    else:
        daily_summary = daily_existing

    if daily_summary is not daily_existing:
        with lock:
            cache['daily'][symbol] = daily_summary

    # Initial volume baseline: 45 days once. Later: fetch only the latest
    # 5 calendar days and merge. This avoids downloading the same 45-day set
    # every day for every stock.
    if not intraday_exists:
        intraday_start = today - timedelta(days=45)
        app.logger.info('Initial 5m history backfill for %s: %s to %s', symbol, intraday_start, today)
        intraday = fetch_history(symbol, '5', intraday_start.isoformat(), today.isoformat())
    elif meta.get('intraday_fetch_date') != today.isoformat():
        intraday_start = today - timedelta(days=5)
        app.logger.info('Incremental 5m history update for %s: %s to %s', symbol, intraday_start, today)
        recent_intraday = fetch_history(symbol, '5', intraday_start.isoformat(), today.isoformat())
        intraday = merge_candles(intraday_existing, recent_intraday)
    else:
        intraday = intraday_existing

    # Retain only 45 calendar days of 5m candles in memory and persistent cache.
    cutoff = today - timedelta(days=45)
    trimmed_intraday = []
    for candle in intraday:
        try:
            if len(candle) >= 6 and datetime.fromtimestamp(int(candle[0]), IST).date() >= cutoff:
                trimmed_intraday.append(candle)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
    intraday = trimmed_intraday

    if intraday is not intraday_existing:
        with lock:
            cache['intraday'][symbol] = intraday
        save_history(symbol, '5m', intraday, today.isoformat())

    clear_history_retry(symbol)

def history_worker(symbols):
    with lock:
        cache['history_running'] = True
        cache['history_total'] = len(symbols)
        cache['history_done'] = 0
        cache['history_message'] = 'Loading saved history; fetching only missing or due candles.'
    try:
        for symbol in symbols:
            try:
                warm_history_for_symbol(symbol)
            except Exception as exc:
                update_history_error(symbol, exc)
                with lock:
                    cache['last_error'] = f'{symbol}: {exc}'
            with lock:
                cache['history_done'] += 1
                cache['history_message'] = (
                    f"History checked for {cache['history_done']} of {cache['history_total']} stocks"
                )
    finally:
        with lock:
            cache['history_running'] = False
            cache['history_message'] = 'Historical cache up to date' if cache['history_done'] == cache['history_total'] else cache['history_message']


def start_history_worker(symbols):
    with lock:
        if cache['history_running']:
            return
        today = datetime.now(IST).date()
        due = [symbol for symbol in symbols if history_due(symbol, today)]
        if not due:
            return
        # Reserve the worker slot before starting the thread. This prevents
        # concurrent /api/scan requests from launching duplicate history jobs.
        cache['history_running'] = True
        thread = threading.Thread(target=history_worker, args=(due,), daemon=True)
        try:
            thread.start()
        except Exception:
            cache['history_running'] = False
            raise


def volume_ratio_for_time(candles, now=None):
    """Compare current cumulative volume to mean cumulative volume at same 5m time over 20 prior sessions."""
    if not candles:
        return None, None, 0
    now = now or datetime.now(IST)
    today = now.date()
    # Compare only through the last completed 5-minute bucket.
    current_bucket_minute = (now.hour * 60 + now.minute) // 5 * 5
    # The live quote includes a partial current 5-minute bucket. Compare only
    # with completed historical buckets to avoid overstating the baseline.
    sessions = {}
    for candle in candles:
        if len(candle) < 6:
            continue
        stamp = datetime.fromtimestamp(int(candle[0]), IST)
        if stamp.date() >= today:
            continue
        minute = stamp.hour * 60 + stamp.minute
        sessions.setdefault(stamp.date(), []).append((minute, float(candle[5] or 0)))
    selected_dates = sorted(sessions.keys())[-20:]
    if len(selected_dates) < 20:
        return None, None, len(selected_dates)
    cumulative = []
    for session_date in selected_dates:
        total = sum(vol for minute, vol in sessions[session_date] if minute < current_bucket_minute)
        cumulative.append(total)
    average = sum(cumulative) / len(cumulative) if cumulative else 0
    return average, cumulative[-1] if cumulative else None, len(cumulative)


def classify_sentiment(change_pct):
    """Three daily-return bands for every NSE F&O stock."""
    if change_pct is None:
        return 'Neutral'
    if change_pct > 0.25:
        return 'Bullish'
    if change_pct < -0.25:
        return 'Bearish'
    return 'Neutral'


def summarize_sentiment(quotes):
    counts = {'Bullish': 0, 'Neutral': 0, 'Bearish': 0}
    valid = 0
    for quote in quotes.values():
        change = quote.get('chp')
        if change is None:
            continue
        try:
            sentiment = classify_sentiment(float(change))
        except (TypeError, ValueError):
            continue
        counts[sentiment] += 1
        valid += 1

    if valid == 0:
        overall = 'Unavailable'
        score = None
    else:
        # Net breadth: bullish share minus bearish share of all classified F&O stocks.
        score = (counts['Bullish'] - counts['Bearish']) / valid
        if score >= 0.10:
            overall = 'Bullish'
        elif score <= -0.10:
            overall = 'Bearish'
        else:
            overall = 'Neutral'
    return {'overall': overall, 'score': score, 'counts': counts, 'valid_stocks': valid}



def build_top_gainers(quotes, symbols, now):
    gainers = []
    for symbol in symbols:
        quote = quotes.get(symbol)
        if not quote: continue
        try:
            ltp = float(quote.get('lp')); change_pct = float(quote.get('chp')); day_volume = float(quote.get('volume') or 0)
        except (TypeError, ValueError): continue
        if change_pct <= 0: continue
        with lock:
            intraday = cache['intraday'].get(symbol); daily = cache['daily'].get(symbol)
        avg_same_time, _, _ = volume_ratio_for_time(intraday or [], now)
        volume_ratio = day_volume / avg_same_time if avg_same_time and avg_same_time > 0 else None
        prev_day_high = daily.get('prev_day_high') if isinstance(daily, dict) else None
        week52_high = daily.get('week52_high') if isinstance(daily, dict) else None
        week52_distance = ((week52_high - ltp) / week52_high * 100) if week52_high and week52_high > 0 else None
        confirmations = []
        if change_pct >= 2.0: confirmations.append('Daily momentum')
        if volume_ratio is not None and volume_ratio >= 3.0: confirmations.append('3× volume')
        if prev_day_high is not None and ltp > prev_day_high: confirmations.append('PDH breakout')
        if week52_distance is not None and week52_distance <= 3.0: confirmations.append('Near 52W high')
        count = len(confirmations)
        level = 'strong' if count >= 3 else 'moderate' if count == 2 else 'limited' if count == 1 else 'none'
        gainers.append({
            'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
            'ltp': ltp, 'change_pct': change_pct, 'volume_ratio': volume_ratio,
            'confirmations': confirmations, 'confirmation_count': count, 'confirmation_level': level,
            'volume_confirmed': volume_ratio is not None and volume_ratio >= 3.0,
            'pdh_breakout': prev_day_high is not None and ltp > float(prev_day_high),
            'near_52w_high': week52_distance is not None and week52_distance <= 3.0,
        })
    gainers.sort(key=lambda row: row['change_pct'], reverse=True)
    return gainers[:10]


def mark_new_top_gainer_entries(gainers, now):
    """Persist first-time Top 10 entries and retain NEW status for the full IST day."""
    trade_date = now.astimezone(IST).date().isoformat()
    current_symbols = [str(row['symbol']).upper() for row in gainers]
    with history_db_lock:
        with _history_db_connect() as db:
            previous = db.execute(
                "SELECT symbols_json FROM top_gainer_snapshot WHERE trade_date=?", (trade_date,)
            ).fetchone()
            if previous is None:
                # First scan is baseline; only later entrants receive NEW.
                for row in gainers:
                    db.execute(
                        "INSERT OR IGNORE INTO top_gainer_daily_state(trade_date,symbol,first_seen_at,is_new,alert_sent) VALUES (?,?,?,0,1)",
                        (trade_date, str(row['symbol']).upper(), now.isoformat(timespec='seconds'))
                    )
            else:
                previous_symbols = set(json.loads(previous[0] or '[]'))
                for row in gainers:
                    symbol = str(row['symbol']).upper()
                    if symbol not in previous_symbols:
                        db.execute(
                            "INSERT OR IGNORE INTO top_gainer_daily_state(trade_date,symbol,first_seen_at,is_new,alert_sent) VALUES (?,?,?,1,0)",
                            (trade_date, symbol, now.isoformat(timespec='seconds'))
                        )
            db.execute(
                "INSERT INTO top_gainer_snapshot(trade_date,symbols_json) VALUES (?,?) "
                "ON CONFLICT(trade_date) DO UPDATE SET symbols_json=excluded.symbols_json",
                (trade_date, json.dumps(current_symbols))
            )
            states = {
                symbol: (first_seen_at, bool(is_new))
                for symbol, first_seen_at, is_new in db.execute(
                    "SELECT symbol,first_seen_at,is_new FROM top_gainer_daily_state WHERE trade_date=?",
                    (trade_date,)
                ).fetchall()
            }
    for row in gainers:
        first_seen_at, is_new = states.get(str(row['symbol']).upper(), (None, False))
        row['is_new_entry'] = is_new
        row['new_entry_time'] = datetime.fromisoformat(first_seen_at).astimezone(IST).strftime('%H:%M') if first_seen_at else None
        checks = [
            ('Volume', bool(row.get('volume_confirmed'))),
            ('PDH breakout', bool(row.get('pdh_breakout'))),
            ('Near 52W high', bool(row.get('near_52w_high'))),
        ]
        row['new_entry_confirmations'] = [{'label': label, 'passed': passed} for label, passed in checks]
        row['new_entry_confirmation_count'] = sum(1 for _, passed in checks if passed)
    return gainers


def send_new_top_gainer_alerts(gainers, now):
    """Alert once for every new Top 10 entrant, but only through 10:30 AM IST."""
    local_now = now.astimezone(IST)
    if (local_now.hour, local_now.minute) > (10, 30):
        return
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    if not bot_token or not chat_id:
        return
    trade_date = now.astimezone(IST).date().isoformat()
    with telegram_alert_lock:
        for row in gainers:
            if not row.get('is_new_entry'):
                continue
            symbol = str(row['symbol']).upper()
            with _history_db_connect() as db:
                state = db.execute(
                    "SELECT alert_sent, first_seen_at FROM top_gainer_daily_state WHERE trade_date=? AND symbol=?",
                    (trade_date, symbol)
                ).fetchone()
            if not state or state[0]:
                continue
            volume_text = 'PASS' if row.get('volume_confirmed') else 'NO'
            pdh_text = 'PASS' if row.get('pdh_breakout') else 'NO'
            high_text = 'NEAR 52W HIGH' if row.get('near_52w_high') else 'NOT NEAR / DATA N/A'
            message = (
                f"🆕 NEW TOP 10 GAINER\\n{symbol} | {row['change_pct']:+.2f}%\\n"
                f"Entered: {now.strftime('%H:%M IST')}\\n"
                f"Volume ≥3x: {volume_text} | PDH breakout: {pdh_text}\\n"
                f"52W: {high_text} (info only)\\n"
                f"Score: {row.get('new_entry_confirmation_count', 0)}/3\\n"
                "Open screener to view chart: https://fyers-fno-live-screener.onrender.com"
            )
            try:
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={'chat_id': chat_id, 'text': message, 'disable_web_page_preview': True},
                    timeout=8,
                )
                payload = response.json() if response.content else {}
                if response.status_code == 200 and payload.get('ok') is True:
                    with _history_db_connect() as db:
                        db.execute(
                            "UPDATE top_gainer_daily_state SET alert_sent=1 WHERE trade_date=? AND symbol=?",
                            (trade_date, symbol)
                        )
                    app.logger.info('Telegram NEW Top 10 Gainer alert sent for %s', symbol)
                else:
                    app.logger.warning('Telegram NEW Top 10 Gainer alert failed for %s: HTTP %s', symbol, response.status_code)
            except (requests.RequestException, ValueError) as exc:
                app.logger.warning('Telegram NEW Top 10 Gainer alert request failed for %s: %s', symbol, exc)


def build_scan_row(symbol, quote, now=None):
    now = now or datetime.now(IST)
    ltp = quote.get('lp')
    prev_close = quote.get('prev_close_price')
    change_pct = quote.get('chp')
    day_volume = quote.get('volume')
    if ltp is None:
        return None
    try:
        ltp = float(ltp)
        change_pct = float(change_pct) if change_pct is not None else ((ltp / float(prev_close) - 1) * 100 if prev_close else None)
        day_volume = float(day_volume or 0)
    except (TypeError, ValueError, ZeroDivisionError):
        return None

    with lock:
        intraday = cache['intraday'].get(symbol)
        daily = cache['daily'].get(symbol)
        sector = cache['stock_sectors'].get(symbol, 'Sector unavailable')

    avg_same_time, _, sessions = volume_ratio_for_time(intraday or [], now)
    volume_ratio = day_volume / avg_same_time if avg_same_time and avg_same_time > 0 else None

    prev_day_high = daily.get('prev_day_high') if isinstance(daily, dict) else None
    week52_high = daily.get('week52_high') if isinstance(daily, dict) else None
    week52_distance = ((week52_high - ltp) / week52_high * 100) if week52_high and week52_high > 0 else None

    # Mandatory scanner gate: PDH breakout AND at least +2% daily change
    # AND 3x relative volume. 52-week proximity is an additional confirmation,
    # never a substitute for the three required conditions.
    volume_ok = volume_ratio is not None and volume_ratio >= 3.0
    pdh_ok = prev_day_high is not None and ltp > prev_day_high
    gain_ok = change_pct is not None and change_pct >= 2.0
    week52_ok = week52_distance is not None and week52_distance <= 3.0
    week52_momentum_ok = week52_distance is not None and week52_distance <= 5.0 and gain_ok and volume_ok
    sentiment = classify_sentiment(change_pct)

    signals = []
    if pdh_ok:
        signals.append('PDH BREAKOUT')
    if gain_ok:
        signals.append('+2% MOMENTUM')
    if volume_ok:
        signals.append('3X VOLUME')
    if week52_ok:
        signals.append('NEAR 52W HIGH')
    if week52_momentum_ok:
        signals.append('52W HIGH +2% + 3X')
    if not (pdh_ok or gain_ok or volume_ok or week52_ok):
        return None

    return {
        'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
        'sector': sector,
        'sentiment': sentiment,
        'ltp': ltp,
        'change_pct': change_pct,
        'volume': day_volume,
        'volume_ratio': volume_ratio,
        'avg_same_time_volume': avg_same_time,
        'volume_sessions': sessions,
        'prev_day_high': prev_day_high,
        'week52_high': week52_high,
        'week52_distance_pct': week52_distance,
        'ath_distance_pct': week52_distance,
        'signals': signals,
        'core_signal_count': sum((pdh_ok, gain_ok, volume_ok)),
        'signal_count': sum((pdh_ok, gain_ok, volume_ok)),
        'history_ready': bool(intraday is not None and daily is not None),
    }


@app.before_request
def require_private_login():
    allowed = {'login', 'logout', 'callback', 'static', 'health'}
    if request.endpoint in allowed or request.path.startswith('/static/'):
        return None
    if not session.get('private_ok'):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Private screener login required.'}), 401
        return redirect(url_for('login'))


@app.route('/health')
def health():
    return jsonify({'ok': True})


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if not SCREENER_PASSWORD:
            return render_template('login.html', error='Set SCREENER_PASSWORD in the server environment first.'), 503
        supplied = request.form.get('password', '')
        if secrets.compare_digest(supplied, SCREENER_PASSWORD):
            session.clear()
            session['private_ok'] = True
            session.permanent = True
            return redirect(url_for('home'))
        return render_template('login.html', error='Incorrect password.'), 401
    return render_template('login.html', error=None)


@app.route('/logout', methods=['POST'])
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
def home():
    return render_template('index.html', broker_name=('Zerodha Kite' if BROKER_PROVIDER == 'zerodha' else 'FYERS'))


@app.route('/connect-broker')
@app.route('/connect-fyers')
def connect_broker():
    """Start OAuth for the selected broker; the browser never receives the app secret."""
    if not APP_ID or not APP_SECRET or not REDIRECT_URI:
        return f'Set the {BROKER_PROVIDER.upper()} API key, API secret and redirect URL in the server environment.', 400
    state = secrets.token_urlsafe(24)
    session['broker_oauth_state'] = state
    if BROKER_PROVIDER == 'zerodha':
        params = {
            'v': '3',
            'api_key': KITE_API_KEY,
            'redirect_params': urlencode({'state': state}),
        }
        return redirect('https://kite.zerodha.com/connect/login?' + urlencode(params))
    params = {'client_id': FYERS_APP_ID, 'redirect_uri': FYERS_REDIRECT_URI, 'response_type': 'code', 'state': state}
    return redirect('https://api-t1.fyers.in/api/v3/generate-authcode?' + urlencode(params))


@app.route('/callback')
def callback():
    global TOKEN
    expected = session.pop('broker_oauth_state', None)
    state = request.args.get('state')
    if not expected or not secrets.compare_digest(state or '', expected):
        return 'Broker login failed: missing or invalid OAuth state. Return to the private dashboard and reconnect.', 400

    if BROKER_PROVIDER == 'zerodha':
        request_token = request.args.get('request_token')
        if not request_token:
            return 'Zerodha did not return a request_token. Restart the broker login.', 400
        checksum = hashlib.sha256(f'{KITE_API_KEY}{request_token}{KITE_API_SECRET}'.encode()).hexdigest()
        response = requests.post(
            KITE_ROOT + '/session/token',
            data={'api_key': KITE_API_KEY, 'request_token': request_token, 'checksum': checksum},
            headers={'X-Kite-Version': '3'},
            timeout=25,
        )
        data = response.json()
        if response.status_code >= 400 or data.get('status') != 'success':
            return f"Zerodha token exchange failed: {data.get('message', 'Unknown error')}", 400
        TOKEN = (data.get('data') or {}).get('access_token', '')
        if not TOKEN:
            return 'Zerodha did not return an access token.', 400
    else:
        auth_code = request.args.get('auth_code')
        if not auth_code:
            return 'FYERS did not return an auth_code. Restart broker login.', 400
        app_hash = hashlib.sha256(f'{FYERS_APP_ID}:{FYERS_APP_SECRET}'.encode()).hexdigest()
        response = requests.post(FYERS_ROOT + '/api/v3/validate-authcode', json={
            'grant_type': 'authorization_code', 'appIdHash': app_hash, 'code': auth_code
        }, timeout=25)
        data = response.json()
        if response.status_code >= 400 or data.get('s') != 'ok' or not data.get('access_token'):
            return f"FYERS token exchange failed: {data.get('message', 'Unknown error')}", 400
        TOKEN = data['access_token']

    # Persist the broker token on the mounted Render disk so daily suspend/resume
    # does not force a fresh broker login. The database is private to this service.
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute(
                "INSERT INTO app_meta (meta_key, meta_value) VALUES (?, ?) "
                "ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value",
                ('broker_access_token', TOKEN),
            )

    cache['connected'] = True
    cache['symbols'] = []
    cache['quotes'] = {}
    cache['intraday'] = {}
    cache['daily'] = {}
    cache['instrument_tokens'] = {}
    return redirect(url_for('home'))



@app.route('/api/status')
def status():
    with lock:
        return jsonify({
            'connected': bool(APP_ID and TOKEN),
            'broker': BROKER_PROVIDER,
            'symbols_loaded': len(cache['symbols']),
            'last_update': cache['last_scan'],
            'error': cache['last_error'],
            'history_done': cache['history_done'],
            'history_total': cache['history_total'],
            'history_running': cache['history_running'],
            'history_message': cache['history_message'],
            'history_cache_path': HISTORY_DB_PATH,
            'history_cache_persistent_mount': (HISTORY_DB_PATH.startswith('/var/data/') and os.path.ismount('/var/data')),
            'history_cache_symbols': len(set(cache['daily']) & set(cache['intraday'])),
            'history_due_today': sum(1 for symbol in cache['symbols'] if history_due(symbol)),
            'sector_error': cache['sector_error'],
            'sector_updated_at': cache['sector_updated_at'],
            'quote_count': cache['last_quote_count'],
            'telegram_configured': bool(os.getenv('TELEGRAM_BOT_TOKEN', '').strip() and os.getenv('TELEGRAM_CHAT_ID', '').strip()),
        })



@app.route('/api/test-telegram')
def test_telegram_alert():
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
    if not bot_token or not chat_id:
        return jsonify({'ok': False, 'error': 'Telegram credentials are not configured in Render.'}), 503
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={'chat_id': chat_id, 'text': '✅ F&O Scanner Telegram test successful. Fresh and Strong Momentum alerts are configured.', 'disable_web_page_preview': True},
            timeout=8,
        )
        payload = response.json()
        if response.status_code != 200 or not payload.get('ok'):
            app.logger.warning('Telegram test failed: HTTP %s', response.status_code)
            return jsonify({'ok': False, 'error': 'Telegram rejected the test message. Check bot token and chat ID.'}), 502
        return jsonify({'ok': True, 'message': 'Telegram test message sent.'})
    except requests.RequestException:
        return jsonify({'ok': False, 'error': 'Could not reach Telegram API. Try again.'}), 502


@app.route('/api/universe')
def universe():
    try:
        if not cache['symbols']:
            cache['symbols'] = get_symbol_universe()
        return jsonify({'count': len(cache['symbols'])})
    except Exception as exc:
        cache['last_error'] = str(exc)
        return jsonify({'error': str(exc)}), 500


@app.route('/api/scan')
def scan():
    try:
        if not TOKEN:
            return jsonify({'error': f'Connect {BROKER_PROVIDER.upper()} to start the live scan.', 'rows': []}), 400
        if not cache['symbols']:
            cache['symbols'] = get_symbol_universe()
        symbols = cache['symbols']
        refresh_stock_sectors(symbols)
        quotes = refresh_quotes(symbols)
        start_history_worker(symbols)
        now = datetime.now(IST)
        rows = []
        for symbol in symbols:
            quote = quotes.get(symbol)
            if not quote:
                continue
            row = build_scan_row(symbol, quote, now)
            if row:
                rows.append(row)
        rows.sort(key=lambda row: (row['signal_count'], row.get('volume_ratio') or 0, row.get('change_pct') or 0), reverse=True)
        with lock:
            ready = sum(1 for symbol in symbols if symbol in cache['intraday'] and symbol in cache['daily'])
            history_running = cache['history_running']
        sentiments = summarize_sentiment(quotes)
        top_gainers = build_top_gainers(quotes, symbols, now)
        top_gainers = mark_new_top_gainer_entries(top_gainers, now)
        send_new_top_gainer_alerts(top_gainers, now)
        top_gainer_symbols = {row['symbol'].upper() for row in top_gainers}
        context = market_context()
        return jsonify({
            'rows': rows,
            'top_gainers': top_gainers,
            'coverage': len(quotes),
            'quote_volume_nonzero': sum(1 for quote in quotes.values() if quote.get('volume') not in (None, 0)),
            'universe': len(symbols),
            'matched': len(rows),
            'pd_breakouts': sum('PDH BREAKOUT' in row['signals'] for row in rows),
            'momentum_count': sum('+2% MOMENTUM' in row['signals'] for row in rows),
            'near_52w': sum('NEAR 52W HIGH' in row['signals'] for row in rows),
            'week52_momentum': sum('52W HIGH +2% + 3X' in row['signals'] for row in rows),
            'sentiment': sentiments,
            'market_context': context,
            'sector_coverage': sum(1 for symbol in symbols if cache['stock_sectors'].get(symbol) not in (None, 'Sector unavailable')),
            'history_ready': ready,
            'history_running': history_running,
            'updated_at': now.isoformat(timespec='seconds'),
            'market_time': now.strftime('%H:%M:%S IST'),
        })
    except Exception as exc:
        cache['last_error'] = str(exc)
        return jsonify({'error': str(exc), 'rows': []}), 400



@app.route('/api/chart/<symbol>')
def chart_data(symbol):
    # The frontend sends display symbols (e.g. RELIANCE), while the
    # scanner/history cache uses canonical broker symbols (NSE:RELIANCE-EQ).
    requested_symbol = (symbol or '').upper().strip()
    canonical_symbol = requested_symbol
    if canonical_symbol not in cache['symbols']:
        ticker = requested_symbol.removeprefix('NSE:').removesuffix('-EQ').strip()
        canonical_symbol = _normalise_symbol(ticker)
    if canonical_symbol not in cache['symbols']:
        return jsonify({'error': 'Unknown F&O symbol'}), 404

    interval = request.args.get('interval', '5')
    if interval not in {'1', '5', '15', '60'}:
        return jsonify({'error': 'Unsupported interval'}), 400

    # The persistent daily history worker is intentionally once-per-day.
    # Refresh only the symbol whose chart is open, with a 45-second throttle.
    refresh_chart_history(canonical_symbol)
    candles = cache['intraday'].get(canonical_symbol, [])
    if not candles:
        return jsonify({'candles': [], 'message': 'Intraday history is still loading. Please try again shortly.'})
    if interval in {'15', '60'}:
        step = int(interval); grouped = {}
        for c in candles:
            bucket = int(c[0]) - int(c[0]) % (step * 60)
            if bucket not in grouped: grouped[bucket] = [bucket,c[1],c[2],c[3],c[4],c[5]]
            else:
                g=grouped[bucket];g[2]=max(g[2],c[2]);g[3]=min(g[3],c[3]);g[4]=c[4];g[5]+=c[5]
        candles=[grouped[k] for k in sorted(grouped)]
    return jsonify({'candles': candles, 'interval': interval})

@app.route('/manifest.webmanifest')
def manifest():
    return jsonify({
        'name': 'Oldpal Dashboard', 'short_name': 'Oldpal', 'start_url': '/',
        'display': 'standalone', 'background_color': '#0b1018', 'theme_color': '#0b1018',
        'icons': [{'src': '/static/icon.svg', 'sizes': 'any', 'type': 'image/svg+xml', 'purpose': 'any'}]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=False)
