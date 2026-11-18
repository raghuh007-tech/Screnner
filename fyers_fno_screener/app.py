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
import tempfile
import gzip
import shutil
from collections import deque
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, redirect, request, session, url_for, send_file

from ivf_v2_validator import validate_ivf_v2

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

# IVF refreshes only PDH-breakout candidates, not the full F&O universe.
ivf_refresh_lock = threading.Lock()
ivf_refresh_running = False
ivf_refresh_at = {}
IVF_REFRESH_SECONDS = 60


def refresh_ivf_candidate_history(symbol):
    now = datetime.now(IST)
    with ivf_refresh_lock:
        last = ivf_refresh_at.get(symbol, 0)
        if time.monotonic() - last < IVF_REFRESH_SECONDS:
            return
        ivf_refresh_at[symbol] = time.monotonic()
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
        app.logger.warning('IVF 5m refresh failed for %s: %s', symbol, exc)
        with ivf_refresh_lock:
            ivf_refresh_at[symbol] = 0


def start_ivf_refresh(symbols):
    global ivf_refresh_running
    candidates = list(dict.fromkeys(symbols))
    if not candidates:
        return
    with ivf_refresh_lock:
        if ivf_refresh_running:
            return
        ivf_refresh_running = True

    def worker():
        global ivf_refresh_running
        try:
            for symbol in candidates:
                refresh_ivf_candidate_history(symbol)
        finally:
            with ivf_refresh_lock:
                ivf_refresh_running = False

    threading.Thread(target=worker, daemon=True, name='ivf-candle-refresh').start()


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


def _persist_backtest_result(strategy_key, state):
    """Store completed backtest output as JSON inside the persistent history database."""
    payload = {
        'status': state.get('status'),
        'message': state.get('message'),
        'trade_count': state.get('trade_count', 0),
        'trades': state.get('trades', []),
        'summary': state.get('summary', {}),
        'updated_at': state.get('updated_at'),
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    saved_at = datetime.now(IST).isoformat()
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO backtest_results(strategy_key,payload_json,updated_at) VALUES(?,?,?)",
                (strategy_key, encoded, saved_at)
            )


def _restore_backtest_result(strategy_key, target_state, target_lock):
    """Restore the last successful result after a Render process restart."""
    try:
        with history_db_lock:
            with _history_db_connect() as db:
                row = db.execute(
                    "SELECT payload_json FROM backtest_results WHERE strategy_key=?",
                    (strategy_key,)
                ).fetchone()
        if not row:
            return
        payload = json.loads(row[0])
        if payload.get('status') != 'complete':
            return
        with target_lock:
            target_state.update(payload)
            target_state['message'] = 'Loaded saved results from persistent storage.'
        app.logger.info('Restored %s backtest result (%s trades).',
                        strategy_key, payload.get('trade_count', 0))
    except Exception as exc:
        app.logger.warning('Could not restore %s backtest result: %s', strategy_key, exc)


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
                CREATE TABLE IF NOT EXISTS backtest_results (
                    strategy_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
            elif BROKER_PROVIDER == 'zerodha' and key == 'broker_access_token' and not TOKEN:
                # Keep the existing Zerodha token isolated for rollback/fallback.
                globals()['TOKEN'] = value
            elif BROKER_PROVIDER == 'fyers' and key == 'fyers_access_token' and not TOKEN:
                globals()['TOKEN'] = value
            elif BROKER_PROVIDER == 'fyers' and key == 'fyers_test_access_token' and not TOKEN:
                # Promote the separately validated FYERS market-data token on first switch.
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
        _fyers_api_slot(priority=priority)


def _fyers_api_slot(priority=False):
    """Serialize all FYERS data calls below 5 requests/second with rolling safety caps.

    The FYERS test connection is separate from the active broker, so it must
    use this same limiter even when Zerodha is the production provider.
    """
    while True:
        with _api_lock:
            now = time.monotonic()
            while _api_times and now - _api_times[0] >= 60:
                _api_times.popleft()
            while _history_times and now - _history_times[0] >= 60:
                _history_times.popleft()
            total_ok = len(_api_times) < 44
            history_ok = priority or len(_history_times) < 25
            # 0.30s spacing gives a 3.3 req/s ceiling, safely below 5 req/s.
            spacing_ok = not _api_times or now - _api_times[-1] >= 0.30
            if total_ok and history_ok and spacing_ok:
                _api_times.append(now)
                if not priority:
                    _history_times.append(now)
                return
            waits = [0.31]
            if not total_ok and _api_times:
                waits.append(60 - (now - _api_times[0]) + 0.05)
            if not history_ok and _history_times:
                waits.append(60 - (now - _history_times[0]) + 0.05)
            wait = max(waits)
        time.sleep(wait)


def fyers_data_get(path, headers, params=None, timeout=25, priority=False):
    """Make a throttled FYERS data request; retry one explicit rate-limit response."""
    for attempt in range(2):
        _fyers_api_slot(priority=priority)
        response = requests.get(
            FYERS_DATA_ROOT + path, headers=headers, params=params, timeout=timeout
        )
        rate_limited = response.status_code == 429
        if not rate_limited and response.ok:
            try:
                payload = response.json()
                message = str(payload.get('message') or payload.get('errmsg') or '').lower()
                rate_limited = payload.get('s') == 'error' and (
                    'rate' in message or 'request limit' in message or 'too many' in message
                )
            except (ValueError, AttributeError):
                pass
        if rate_limited and attempt == 0:
            app.logger.warning('FYERS rate limit response for %s; retrying once after backoff.', path)
            time.sleep(1.5)
            continue
        return response


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
    response = fyers_data_get(
        path, headers=fyers_headers(), params=params, timeout=25, priority=priority
    )
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
    # FYERS index symbols are compact (for example NSE:NIFTY50-INDEX),
    # unlike the spaced Kite index names.
    fyers_index_symbols = {
        row[ticker_idx].strip().upper()
        for row in cm_data
        if len(row) > ticker_idx and row[ticker_idx].strip().upper().startswith('NSE:')
        and row[ticker_idx].strip().upper().endswith('-INDEX')
    }
    def compact_index_name(symbol):
        return symbol.removeprefix('NSE:').removesuffix('-INDEX').replace(' ', '').replace('&', '').replace('-', '')
    wanted_indices = {'NIFTY50': ('NIFTY 50', 'Nifty 50')}
    wanted_indices.update({
        key.replace(' ', '').replace('&', '').replace('-', ''): (key, label)
        for key, label in SECTOR_INDEX_LABELS.items()
    })
    market_symbols, sector_labels = [], {}
    for symbol in sorted(fyers_index_symbols):
        match = wanted_indices.get(compact_index_name(symbol))
        if not match:
            continue
        label_key, label = match
        market_symbols.append(symbol)
        if label_key != 'NIFTY 50':
            sector_labels[symbol] = label
    with lock:
        cache['market_index_symbols'] = market_symbols
        cache['sector_index_labels'] = sector_labels
    app.logger.info('FYERS index symbols discovered: %d; sector indices: %d',
                    len(market_symbols), len(sector_labels))
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
        with lock:
            market_symbols = list(cache.get('market_index_symbols', []))
        requested = list(symbols) + market_symbols
        for start in range(0, len(requested), 50):
            batch = requested[start:start + 50]
            payload = broker_get('/quotes', {'symbols': ','.join(batch)}, priority=True)
            for item in payload.get('d', []):
                if item.get('s') == 'ok':
                    symbol = item.get('n', '')
                    if symbol in market_symbols:
                        market_result[symbol] = item.get('v', {})
                    else:
                        result[symbol] = item.get('v', {})
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
    nifty = next((quote for symbol, quote in quotes.items()
                  if symbol.startswith('NSE:')
                  and symbol.removeprefix('NSE:').removesuffix('-INDEX').replace(' ', '').upper() == 'NIFTY50'), None)
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


def intraday_volume_filter(candles, now=None):
    """Compare the latest completed 5-minute candle with the same slot over 20 prior sessions."""
    now = now or datetime.now(IST)
    if not candles:
        return None, None, 0, None
    sessions = {}
    for candle in candles:
        if len(candle) < 6:
            continue
        try:
            stamp = datetime.fromtimestamp(int(candle[0]), IST)
            volume = float(candle[5] or 0)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        sessions.setdefault(stamp.date(), {})[stamp.hour * 60 + stamp.minute] = volume
    current_day = sessions.get(now.date(), {})
    completed = [
        (minute, volume) for minute, volume in current_day.items()
        if minute + 5 <= now.hour * 60 + now.minute
    ]
    if not completed:
        return None, None, 0, None
    slot_minute, current_volume = max(completed, key=lambda item: item[0])
    prior_dates = sorted(day for day in sessions if day < now.date())[-20:]
    if len(prior_dates) < 20:
        return None, current_volume, len(prior_dates), slot_minute
    samples = [sessions[day][slot_minute] for day in prior_dates if slot_minute in sessions[day]]
    if len(samples) < 15:
        return None, current_volume, len(samples), slot_minute
    average = sum(samples) / len(samples)
    ratio = current_volume / average if average > 0 else None
    return ratio, current_volume, len(samples), slot_minute


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
    ivf_ratio, ivf_candle_volume, ivf_sessions, ivf_slot = intraday_volume_filter(intraday or [], now)

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
    ivf_ok = pdh_ok and ivf_ratio is not None and ivf_ratio >= 3.0
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
    if ivf_ok:
        signals.append('IVF')
    if not (pdh_ok or gain_ok or volume_ok or week52_ok or ivf_ok):
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
        'ivf_volume_ratio': ivf_ratio,
        'ivf_candle_volume': ivf_candle_volume,
        'ivf_volume_sessions': ivf_sessions,
        'ivf_candle_time': (f"{ivf_slot // 60:02d}:{ivf_slot % 60:02d}" if ivf_slot is not None else None),
        'ivf_ok': ivf_ok,
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


@app.route('/connect-fyers')
def connect_fyers_test():
    """Authenticate FYERS separately without changing the active Zerodha feed."""
    if not FYERS_APP_ID or not FYERS_APP_SECRET or not FYERS_REDIRECT_URI:
        return 'Set FYERS_APP_ID, FYERS_APP_SECRET and FYERS_REDIRECT_URI in the server environment.', 400
    state = secrets.token_urlsafe(24)
    session['fyers_test_oauth_state'] = state
    params = {
        'client_id': FYERS_APP_ID,
        'redirect_uri': FYERS_REDIRECT_URI,
        'response_type': 'code',
        'state': state,
    }
    return redirect('https://api-t1.fyers.in/api/v3/generate-authcode?' + urlencode(params))


@app.route('/callback')
def callback():
    global TOKEN
    state = request.args.get('state')
    fyers_test_state = session.pop('fyers_test_oauth_state', None)
    if fyers_test_state and secrets.compare_digest(state or '', fyers_test_state):
        auth_code = request.args.get('auth_code')
        if not auth_code:
            return 'FYERS did not return an auth_code. Restart FYERS connection.', 400
        app_hash = hashlib.sha256(f'{FYERS_APP_ID}:{FYERS_APP_SECRET}'.encode()).hexdigest()
        response = requests.post(FYERS_ROOT + '/api/v3/validate-authcode', json={
            'grant_type': 'authorization_code', 'appIdHash': app_hash, 'code': auth_code
        }, timeout=25)
        data = response.json()
        if response.status_code >= 400 or data.get('s') != 'ok' or not data.get('access_token'):
            return f"FYERS token exchange failed: {data.get('message', 'Unknown error')}", 400
        with history_db_lock:
            with _history_db_connect() as db:
                db.execute(
                    "INSERT INTO app_meta (meta_key, meta_value) VALUES (?, ?) "
                    "ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value",
                    ('fyers_test_access_token', data['access_token']),
                )
        return redirect(url_for('home') + '?fyers_connected=1')

    expected = session.pop('broker_oauth_state', None)
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
                ('broker_access_token' if BROKER_PROVIDER == 'zerodha' else 'fyers_access_token', TOKEN),
            )

    cache['connected'] = True
    cache['symbols'] = []
    cache['quotes'] = {}
    cache['intraday'] = {}
    cache['daily'] = {}
    cache['instrument_tokens'] = {}
    return redirect(url_for('home'))




@app.route('/api/fyers-test-status')
def fyers_test_status():
    """Report whether the separate FYERS test token was saved; never expose the token."""
    try:
        with history_db_lock:
            with _history_db_connect() as db:
                row = db.execute(
                    "SELECT 1 FROM app_meta WHERE meta_key = 'fyers_test_access_token' AND length(meta_value) > 0"
                ).fetchone()
        return jsonify({'connected': bool(row)})
    except Exception as exc:
        app.logger.warning('Could not read separate FYERS test status: %s', exc)
        return jsonify({'connected': False, 'error': 'Could not verify FYERS token storage.'}), 500


@app.route('/api/fyers-test-data', methods=['POST'])
def fyers_test_data():
    """Run a small, sequential FYERS quote and 5-minute history smoke test."""
    try:
        with history_db_lock:
            with _history_db_connect() as db:
                row = db.execute(
                    "SELECT meta_value FROM app_meta WHERE meta_key = 'fyers_test_access_token'"
                ).fetchone()
        if not row or not row[0]:
            return jsonify({'ok': False, 'message': 'FYERS is not authenticated yet.'}), 400

        headers = {
            'Authorization': f'{FYERS_APP_ID}:{row[0]}',
            'Content-Type': 'application/json',
        }
        symbols = [
            'NSE:RELIANCE-EQ',
            'NSE:HDFCBANK-EQ',
            'NSE:ICICIBANK-EQ',
            'NSE:INFY-EQ',
            'NSE:SBIN-EQ',
        ]
        result = {'quotes': [], 'history': []}
        quote_response = fyers_data_get(
            '/quotes', headers=headers,
            params={'symbols': ','.join(symbols)},
            timeout=20, priority=True,
        )
        quote_data = quote_response.json()
        quote_items = {
            item.get('n'): item.get('v') or {}
            for item in quote_data.get('d', [])
            if item.get('s') == 'ok'
        }
        for symbol in symbols:
            values = quote_items.get(symbol) or {}
            result['quotes'].append({
                'symbol': symbol,
                'ok': bool(quote_response.ok and values.get('lp') is not None),
                'ltp': values.get('lp'),
                'change_pct': values.get('chp'),
            })

        end_date = datetime.now(IST).date() - timedelta(days=1)
        start_date = end_date - timedelta(days=5)
        for symbol in symbols:
            try:
                history_response = fyers_data_get(
                    '/history',
                    headers=headers,
                    params={
                        'symbol': symbol,
                        'resolution': '5',
                        'date_format': '1',
                        'range_from': start_date.isoformat(),
                        'range_to': end_date.isoformat(),
                        'cont_flag': '0',
                    },
                    timeout=20, priority=False,
                )
                history_data = history_response.json()
                candles = history_data.get('candles') or []
                history_ok = bool(
                    history_response.ok and history_data.get('s') == 'ok' and candles
                )
                result['history'].append({
                    'symbol': symbol,
                    'ok': history_ok,
                    'candles': len(candles) if history_ok else 0,
                    'http_status': history_response.status_code,
                    'api_status': history_data.get('s'),
                    'api_code': history_data.get('code'),
                    'message': '' if history_ok else str(
                        history_data.get('message') or history_data.get('errmsg') or
                        ('No candles returned' if not candles else 'FYERS history request failed')
                    )[:180],
                })
            except Exception as exc:
                result['history'].append({
                    'symbol': symbol, 'ok': False, 'candles': 0,
                    'message': str(exc)[:180],
                })

        result['quote_passed'] = sum(1 for item in result['quotes'] if item['ok'])
        result['history_passed'] = sum(1 for item in result['history'] if item['ok'])
        result['ok'] = (
            result['quote_passed'] == len(symbols)
            and result['history_passed'] == len(symbols)
        )
        result['message'] = (
            'All five FYERS quote and history checks passed.'
            if result['ok'] else 'Some FYERS symbol checks failed; see per-symbol results.'
        )
        return jsonify(result)
    except Exception as exc:
        app.logger.warning('FYERS test-data check failed: %s', exc)
        return jsonify({'ok': False, 'message': 'FYERS test failed. Check server logs for details.'}), 502


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


@app.route('/api/ivf/first-candle-scan')
def ivf_first_candle_scan():
    """Read-only scan: today's 09:15 5m volume vs prior 20 sessions' same slot."""
    target_date = request.args.get('date', datetime.now(IST).date().isoformat())
    try:
        target = datetime.strptime(target_date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

    results = []
    insufficient = 0
    missing_today = 0
    with history_db_lock:
        with _history_db_connect() as db:
            rows = db.execute(
                "SELECT symbol, candles_json FROM candle_cache WHERE timeframe='5m'"
            ).fetchall()

    for symbol, raw in rows:
        try:
            candles = json.loads(raw)
            by_date = {}
            for candle in candles:
                if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                    continue
                stamp = datetime.fromtimestamp(int(candle[0]), IST)
                if stamp.time().hour == 9 and stamp.time().minute == 15:
                    by_date[stamp.date().isoformat()] = float(candle[5])
            today_volume = by_date.get(target_date)
            if today_volume is None:
                missing_today += 1
                continue
            prior_dates = sorted(d for d in by_date if d < target_date)[-20:]
            if len(prior_dates) < 20:
                insufficient += 1
                continue
            average_volume = sum(by_date[d] for d in prior_dates) / 20.0
            if average_volume <= 0:
                continue
            ratio = today_volume / average_volume
            if ratio >= 3.0:
                results.append({
                    'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
                    'today_volume': today_volume,
                    'average_first_5m_volume_20d': round(average_volume, 2),
                    'ivf': round(ratio, 2),
                })
        except (TypeError, ValueError, json.JSONDecodeError, OverflowError, OSError):
            continue

    results.sort(key=lambda item: item['ivf'], reverse=True)
    return jsonify({
        'date': target_date,
        'rule': '09:15-09:20 volume >= 3x average same 5-minute slot over previous 20 sessions',
        'matched': len(results),
        'stocks': results,
        'symbols_missing_today_candle': missing_today,
        'symbols_with_insufficient_20_session_history': insufficient,
        'source': 'persisted scanner 5-minute candle cache',
    })


@app.route('/api/ivf/volume-spike-scan')
def ivf_volume_spike_scan():
    """Read-only daily scan of volume spikes among +2% and PDH-breakout F&O stocks."""
    target_date = request.args.get('date', datetime.now(IST).date().isoformat())
    try:
        target = datetime.strptime(target_date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

    # candle_cache contains the F&O universe's persisted 5-minute history.
    # The daily cache supplies the previous-session high (PDH).
    with history_db_lock:
        with _history_db_connect() as db:
            five_rows = db.execute(
                "SELECT symbol, candles_json FROM candle_cache WHERE timeframe='5m'"
            ).fetchall()
            daily_rows = db.execute(
                "SELECT symbol, candles_json FROM candle_cache WHERE timeframe='1d'"
            ).fetchall()

    daily_by_symbol = {}
    for symbol, raw in daily_rows:
        try:
            daily_by_symbol[symbol] = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

    stocks = []
    missing_today = 0
    missing_pdh_or_previous_close = 0
    insufficient_baseline = 0
    target_timestamp_for_scan = int(datetime.combine(target, datetime.min.time(), tzinfo=IST).timestamp()) + (9 * 3600) + (15 * 60)
    for symbol, raw in five_rows:
        try:
            candles = json.loads(raw)
            parsed = []
            for candle in candles:
                if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                    continue
                stamp = int(candle[0])
                parsed.append((stamp, float(candle[1]), float(candle[2]),
                               float(candle[3]), float(candle[4]), float(candle[5])))
            parsed.sort(key=lambda item: item[0])
            today_bars = [
                bar for bar in parsed
                if datetime.fromtimestamp(bar[0], IST).date() == target
            ]
            prior_bars = [
                bar for bar in parsed
                if datetime.fromtimestamp(bar[0], IST).date() < target
            ]
            if not today_bars:
                missing_today += 1
                continue
            if not prior_bars:
                missing_pdh_or_previous_close += 1
                continue

            previous_close = prior_bars[-1][4]
            today_close = today_bars[-1][4]
            if previous_close <= 0:
                continue
            change_pct = (today_close / previous_close - 1.0) * 100.0

            daily_info = daily_by_symbol.get(symbol)
            if isinstance(daily_info, dict):
                pdh = daily_info.get('prev_day_high')
            else:
                pdh = None
            if pdh is None:
                missing_pdh_or_previous_close += 1
                continue
            pdh = float(pdh)

            # Candidate gate is evaluated on the latest completed candle of the day.
            if change_pct < 2.0 or today_close <= pdh:
                continue

            # For every candle today, compare volume with the preceding 100
            # completed 5-minute candles, including prior-day candles.
            all_prior_volumes = [bar[5] for bar in parsed
                                 if datetime.fromtimestamp(bar[0], IST).date() < target]
            spikes = []
            for bar in parsed:
                stamp, opn, high, low, close, volume = bar
                if datetime.fromtimestamp(stamp, IST).date() != target:
                    continue
                if len(all_prior_volumes) >= 100:
                    baseline = sum(all_prior_volumes[-100:]) / 100.0
                    if baseline > 0:
                        multiple = volume / baseline
                        if multiple >= 3.0:
                            candle_time = datetime.fromtimestamp(stamp, IST)
                            spikes.append({
                                'time': candle_time.strftime('%H:%M'),
                                'volume': volume,
                                'average_previous_100_5m_volume': round(baseline, 2),
                                'volume_multiple': round(multiple, 2),
                                'open': opn, 'high': high, 'low': low, 'close': close,
                                'signal': ('exceptional' if multiple >= 8.0
                                           else 'strong' if multiple >= 5.0 else 'spike'),
                            })
                all_prior_volumes.append(volume)

            if len(parsed) and len([bar for bar in parsed if bar[0] < target_timestamp_for_scan]) < 100:
                insufficient_baseline += 1

            if spikes:
                stocks.append({
                    'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
                    'change_pct': round(change_pct, 2),
                    'last_price': round(today_close, 2),
                    'pdh': round(pdh, 2),
                    'pdh_breakout': True,
                    'spike_count': len(spikes),
                    'spikes': spikes,
                })
        except (TypeError, ValueError, json.JSONDecodeError, OverflowError, OSError):
            continue

    stocks.sort(key=lambda item: (item['spike_count'],
                                  max((x['volume_multiple'] for x in item['spikes']), default=0)),
                reverse=True)
    return jsonify({
        'date': target_date,
        'universe': 'F&O stocks represented in persisted scanner history',
        'candidate_filter': 'latest completed 5-minute close is at least 2% above previous session close and above PDH',
        'volume_rule': 'each 5-minute candle today compared with average volume of the previous 100 completed 5-minute candles; current candle excluded',
        'spike_threshold': '>=3x; strong >=5x; exceptional >=8x',
        'matched_stocks': len(stocks),
        'stocks': stocks,
        'symbols_missing_today_candles': missing_today,
        'symbols_missing_pdh_or_previous_close': missing_pdh_or_previous_close,
        'symbols_with_insufficient_100_candle_baseline': insufficient_baseline,
        'source': 'persisted scanner 5-minute and daily candle cache',
        'note': 'Read-only scan. No V2 backtest, saved trades, or broker orders are changed.',
    })


@app.route('/api/ivf/volume-method-comparison')
def ivf_volume_method_comparison():
    """Read-only historical comparison of cumulative RVOL and same-slot RVOL."""
    target_date = request.args.get('date', datetime.now(IST).date().isoformat())
    try:
        target = datetime.strptime(target_date, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({'error': 'date must be YYYY-MM-DD'}), 400

    with history_db_lock:
        with _history_db_connect() as db:
            five_rows = db.execute(
                "SELECT symbol, candles_json FROM candle_cache WHERE timeframe='5m'"
            ).fetchall()
            daily_rows = db.execute(
                "SELECT symbol, candles_json FROM candle_cache WHERE timeframe='1d'"
            ).fetchall()

    daily_by_symbol = {}
    for symbol, raw in daily_rows:
        try:
            daily_by_symbol[symbol] = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

    results = []
    missing_history = 0
    skipped_baseline = 0
    for symbol, raw in five_rows:
        try:
            source = json.loads(raw)
            bars = []
            for c in source:
                if not isinstance(c, (list, tuple)) or len(c) < 6:
                    continue
                stamp = int(c[0])
                dt = datetime.fromtimestamp(stamp, IST)
                bars.append({
                    'stamp': stamp, 'date': dt.date(), 'minute': dt.hour * 60 + dt.minute,
                    'open': float(c[1]), 'high': float(c[2]), 'low': float(c[3]),
                    'close': float(c[4]), 'volume': float(c[5] or 0),
                })
            bars.sort(key=lambda x: x['stamp'])
            today = [b for b in bars if b['date'] == target and 9*60+15 <= b['minute'] < 15*60+30]
            prior = [b for b in bars if b['date'] < target]
            if not today or not prior:
                missing_history += 1
                continue

            daily_info = daily_by_symbol.get(symbol)
            pdh = daily_info.get('prev_day_high') if isinstance(daily_info, dict) else None
            if pdh is None:
                continue
            pdh = float(pdh)
            prev_close = prior[-1]['close']
            if prev_close <= 0:
                continue

            final_close = today[-1]['close']
            final_change = (final_close / prev_close - 1.0) * 100.0
            # Analyse only stocks that ended the session above +2% and above PDH.
            if final_change < 2.0 or final_close <= pdh:
                continue

            sessions = {}
            for b in prior:
                if b['minute'] < 15*60+30 and b['minute'] >= 9*60+15:
                    sessions.setdefault(b['date'], {})[b['minute']] = b['volume']
            prior_dates = sorted(sessions.keys())[-20:]
            if len(prior_dates) < 20:
                skipped_baseline += 1
                continue

            # Calculate first time both price filters were true on a completed candle.
            first_gate = None
            for b in today:
                change = (b['close'] / prev_close - 1.0) * 100.0
                if change >= 2.0 and b['close'] > pdh:
                    first_gate = b['minute']
                    break

            cumulative_today = 0.0
            first_cumulative = None
            first_slot = None
            max_cum_ratio = 0.0
            max_slot_ratio = 0.0
            for b in today:
                cumulative_today += b['volume']
                minute = b['minute']
                cumulative_samples = []
                slot_samples = []
                for day in prior_dates:
                    day_data = sessions[day]
                    if minute not in day_data:
                        continue
                    slot_samples.append(day_data[minute])
                    cumulative_samples.append(sum(v for m, v in day_data.items() if m <= minute))
                if len(cumulative_samples) < 20 or len(slot_samples) < 20:
                    continue
                avg_cum = sum(cumulative_samples) / 20.0
                avg_slot = sum(slot_samples) / 20.0
                cum_ratio = cumulative_today / avg_cum if avg_cum > 0 else 0.0
                slot_ratio = b['volume'] / avg_slot if avg_slot > 0 else 0.0
                max_cum_ratio = max(max_cum_ratio, cum_ratio)
                max_slot_ratio = max(max_slot_ratio, slot_ratio)
                if first_cumulative is None and cum_ratio >= 3.0:
                    first_cumulative = minute
                if first_slot is None and slot_ratio >= 3.0:
                    first_slot = minute

            def fmt(minute):
                return f"{minute//60:02d}:{minute%60:02d}" if minute is not None else None

            def lead(volume_minute):
                if volume_minute is None or first_gate is None:
                    return None
                return first_gate - volume_minute

            results.append({
                'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
                'change_pct': round(final_change, 2),
                'last_price': round(final_close, 2),
                'pdh': round(pdh, 2),
                'first_both_2pct_pdh_time': fmt(first_gate),
                'first_cumulative_rvol_3x_time': fmt(first_cumulative),
                'first_same_slot_rvol_3x_time': fmt(first_slot),
                'cumulative_lead_minutes_before_gate': lead(first_cumulative),
                'same_slot_lead_minutes_before_gate': lead(first_slot),
                'max_cumulative_rvol': round(max_cum_ratio, 2),
                'max_same_slot_rvol': round(max_slot_ratio, 2),
                'baseline_sessions': 20,
            })
        except (TypeError, ValueError, json.JSONDecodeError, OverflowError, OSError):
            continue

    results.sort(key=lambda x: x['change_pct'], reverse=True)
    return jsonify({
        'date': target_date,
        'scope': 'F&O symbols in persisted candle cache that closed above +2% and PDH',
        'method_1': 'Cumulative volume from 09:15 through each completed candle divided by average cumulative volume at the same time over the previous 20 trading sessions',
        'method_2': 'Each completed 5-minute candle volume divided by average volume of the same 5-minute slot over the previous 20 trading sessions',
        'trigger': '3.0x',
        'matched_stocks': len(results),
        'stocks': results,
        'symbols_missing_today_or_previous_close': missing_history,
        'symbols_with_insufficient_20_session_baseline': skipped_baseline,
        'note': 'Read-only analysis. No dashboard logic, alerts, V2 backtest, saved trades, or broker orders are changed.',
    })


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
        # Refresh today's 5m candles only for stocks currently above PDH.
        # Updated candles feed the IVF calculation on the next 60-second scan.
        start_ivf_refresh([
            row['symbol'] if row['symbol'].startswith('NSE:') else _normalise_symbol(row['symbol'])
            for row in rows if row.get('prev_day_high') is not None
            and row.get('ltp') is not None and row['ltp'] > row['prev_day_high']
        ])
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
            'momentum_count': sum('+2% MOMENTUM' in row['signals'] and '3X VOLUME' in row['signals'] for row in rows),
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


# --------------------------- Backtesting V1 data layer ---------------------------
# Kept separate from the live scanner's 45-day chart/volume cache.
BACKTEST_DAYS = 183
BACKTEST_CHUNK_DAYS = 30
BACKTEST_DAILY_CHUNK_DAYS = 180
backtest_lock = threading.RLock()
backtest_worker_running = False
backtest_state = {
    'status': 'not_started', 'message': 'Historical backfill has not been started.',
    'completed_calls': 0, 'total_calls': 0, 'symbols_total': 0,
    'symbols_done': 0, 'errors': 0, 'started_at': None, 'updated_at': None,
    'trade_start': None, 'trade_end': None,
}


def init_backtest_store():
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS backtest_candles (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    session_date TEXT NOT NULL,
                    candles_json TEXT NOT NULL,
                    PRIMARY KEY(symbol, timeframe, session_date)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS backtest_chunks (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    range_from TEXT NOT NULL,
                    range_to TEXT NOT NULL,
                    candle_count INTEGER NOT NULL DEFAULT 0,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY(symbol, timeframe, range_from, range_to)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS backtest_errors (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    range_from TEXT NOT NULL,
                    range_to TEXT NOT NULL,
                    error TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(symbol, timeframe, range_from, range_to)
                )
            """)


def _backtest_get_meta(db, key, default=None):
    row = db.execute("SELECT meta_value FROM app_meta WHERE meta_key=?", (key,)).fetchone()
    return row[0] if row else default


def _backtest_set_meta(db, key, value):
    db.execute(
        "INSERT INTO app_meta(meta_key,meta_value) VALUES(?,?) "
        "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value",
        (key, str(value))
    )


def _backtest_chunk_done(db, symbol, timeframe, start_text, end_text):
    return db.execute(
        "SELECT 1 FROM backtest_chunks WHERE symbol=? AND timeframe=? AND range_from=? AND range_to=?",
        (symbol, timeframe, start_text, end_text)
    ).fetchone() is not None


def _backtest_store_chunk(symbol, timeframe, start_text, end_text, candles):
    # A real empty list is a valid zero-bar response (e.g. market holidays).
    # None, non-list payloads, or non-empty payloads with no valid bars are errors.
    if not isinstance(candles, list):
        raise ValueError('Historical provider returned a non-list candle payload.')
    sessions = {}
    invalid_rows = 0
    for candle in candles:
        try:
            if len(candle) < 6:
                invalid_rows += 1
                continue
            epoch = int(candle[0])
            values = [float(candle[index]) for index in range(1, 6)]
            if not all(math.isfinite(value) for value in values):
                invalid_rows += 1
                continue
            session_date = datetime.fromtimestamp(epoch, IST).date().isoformat()
            sessions.setdefault(session_date, []).append(candle)
        except (TypeError, ValueError, OverflowError, OSError):
            invalid_rows += 1
    if candles and not sessions:
        raise ValueError('Historical provider returned candles, but none were valid.')
    if invalid_rows:
        app.logger.warning('Skipped %d invalid historical candles for %s (%s)', invalid_rows, symbol, timeframe)
    with history_db_lock:
        with _history_db_connect() as db:
            for session_date, rows in sessions.items():
                rows.sort(key=lambda item: int(item[0]))
                db.execute(
                    "INSERT INTO backtest_candles(symbol,timeframe,session_date,candles_json) VALUES(?,?,?,?) "
                    "ON CONFLICT(symbol,timeframe,session_date) DO UPDATE SET candles_json=excluded.candles_json",
                    (symbol, timeframe, session_date, json.dumps(rows, separators=(',', ':')))
                )
            db.execute(
                "INSERT INTO backtest_chunks(symbol,timeframe,range_from,range_to,candle_count,completed_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(symbol,timeframe,range_from,range_to) DO UPDATE SET "
                "candle_count=excluded.candle_count,completed_at=excluded.completed_at",
                (symbol, timeframe, start_text, end_text, sum(len(v) for v in sessions.values()), datetime.now(IST).isoformat())
            )
            db.execute(
                "DELETE FROM backtest_errors WHERE symbol=? AND timeframe=? AND range_from=? AND range_to=?",
                (symbol, timeframe, start_text, end_text)
            )


def _backtest_record_error(symbol, timeframe, start_text, end_text, error):
    with history_db_lock:
        with _history_db_connect() as db:
            db.execute(
                "INSERT INTO backtest_errors(symbol,timeframe,range_from,range_to,error,updated_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(symbol,timeframe,range_from,range_to) DO UPDATE SET "
                "error=excluded.error,updated_at=excluded.updated_at",
                (symbol, timeframe, start_text, end_text, str(error)[:500], datetime.now(IST).isoformat())
            )


def _backtest_update_state(**values):
    with backtest_lock:
        backtest_state.update(values)
        backtest_state['updated_at'] = datetime.now(IST).isoformat(timespec='seconds')
        snapshot = dict(backtest_state)
    with history_db_lock:
        with _history_db_connect() as db:
            for key, value in snapshot.items():
                _backtest_set_meta(db, 'backtest_' + key, json.dumps(value) if isinstance(value, (dict, list)) else value)


def _backtest_worker(symbols, trade_start, trade_end):
    global backtest_worker_running
    daily_start = trade_start - timedelta(days=366)
    # Keep provider requests comfortably below historical API date-range limits.
    daily_ranges = list(date_chunks(daily_start, trade_end, BACKTEST_DAILY_CHUNK_DAYS))
    intraday_ranges = list(date_chunks(trade_start, trade_end, BACKTEST_CHUNK_DAYS))
    total_calls = len(symbols) * (len(daily_ranges) + len(intraday_ranges))
    completed_calls = successful_calls = failed_calls = 0
    symbols_done = 0
    _backtest_update_state(
        status='running',
        message='Collecting 18 months of daily candles and six months of 5-minute candles in resumable batches.',
        completed_calls=0, successful_calls=0, failed_calls=0,
        total_calls=total_calls, symbols_total=len(symbols), symbols_done=0,
        errors=0, active_symbol=None, active_timeframe=None,
        trade_start=trade_start.isoformat(), trade_end=trade_end.isoformat(),
        started_at=datetime.now(IST).isoformat(timespec='seconds')
    )
    try:
        for symbol in symbols:
            for timeframe, ranges in (('D', daily_ranges), ('5', intraday_ranges)):
                for chunk_start, chunk_end in ranges:
                    start_text, end_text = chunk_start.isoformat(), chunk_end.isoformat()
                    try:
                        with history_db_lock:
                            with _history_db_connect() as db:
                                done = _backtest_chunk_done(db, symbol, timeframe, start_text, end_text)
                        if not done:
                            candles = fetch_history(symbol, timeframe, start_text, end_text)
                            # A successful empty response is stored as a completed zero-bar
                            # chunk; provider/network exceptions remain retryable errors.
                            _backtest_store_chunk(symbol, timeframe, start_text, end_text, candles)
                        else:
                            # A completed chunk is authoritative. Clear any stale error
                            # left behind by an earlier attempt so retries can reconcile
                            # the error ledger even when no provider request is needed.
                            with history_db_lock:
                                with _history_db_connect() as db:
                                    db.execute(
                                        "DELETE FROM backtest_errors WHERE symbol=? AND timeframe=? AND range_from=? AND range_to=?",
                                        (symbol, timeframe, start_text, end_text)
                                    )
                        successful_calls += 1
                    except Exception as exc:
                        failed_calls += 1
                        _backtest_record_error(symbol, timeframe, start_text, end_text, exc)
                        app.logger.warning(
                            'Backtest history chunk failed for %s %s %s to %s: %s',
                            symbol, timeframe, start_text, end_text, exc
                        )
                    completed_calls += 1
                    with history_db_lock:
                        with _history_db_connect() as db:
                            remaining_errors = db.execute(
                                "SELECT COUNT(*) FROM backtest_errors"
                            ).fetchone()[0]
                    _backtest_update_state(
                        completed_calls=completed_calls,
                        successful_calls=successful_calls,
                        failed_calls=failed_calls,
                        symbols_done=symbols_done,
                        errors=remaining_errors,
                        active_symbol=symbol,
                        active_timeframe=timeframe,
                        message=f'Checked {symbol} · {timeframe}-minute history · {completed_calls}/{total_calls} batches.'
                    )
            symbols_done += 1
            _backtest_update_state(
                symbols_done=symbols_done,
                message=f'Historical data checked for {symbols_done}/{len(symbols)} stocks.'
            )
        with history_db_lock:
            with _history_db_connect() as db:
                # Reconcile the entire error ledger, not only the current date
                # plan. Older runs may have left error rows for chunks that are
                # already present in backtest_chunks but no longer fall inside
                # the current collection window.
                db.execute("""
                    DELETE FROM backtest_errors
                    WHERE EXISTS (
                        SELECT 1 FROM backtest_chunks
                        WHERE backtest_chunks.symbol = backtest_errors.symbol
                          AND backtest_chunks.timeframe = backtest_errors.timeframe
                          AND backtest_chunks.range_from = backtest_errors.range_from
                          AND backtest_chunks.range_to = backtest_errors.range_to
                    )
                """)
                remaining = db.execute("SELECT COUNT(*) FROM backtest_errors").fetchone()[0]
        final_status = 'partial' if remaining else 'data_ready'
        _backtest_update_state(
            status=final_status,
            message=('Collection finished with errors. Select Start / Resume to retry failed batches.'
                     if remaining else
                     'Historical data collection complete. Strategy validation can now proceed.'),
            completed_calls=completed_calls, successful_calls=successful_calls,
            failed_calls=failed_calls, total_calls=total_calls,
            symbols_done=symbols_done, errors=remaining,
            active_symbol=None, active_timeframe=None
        )
    except Exception as exc:
        app.logger.exception('Backtest history worker stopped unexpectedly: %s', exc)
        _backtest_update_state(
            status='partial', message='Collection stopped unexpectedly. Completed batches are saved; resume to continue.',
            completed_calls=completed_calls, successful_calls=successful_calls,
            failed_calls=failed_calls, total_calls=total_calls,
            symbols_done=symbols_done, active_symbol=None, active_timeframe=None
        )
        raise
    finally:
        with backtest_lock:
            backtest_worker_running = False


def _backtest_restore_state():
    """Restore progress metadata after a Render restart; candle chunks remain authoritative."""
    with history_db_lock:
        with _history_db_connect() as db:
            rows = db.execute(
                "SELECT meta_key, meta_value FROM app_meta WHERE meta_key LIKE 'backtest_%'"
            ).fetchall()
    restored = {}
    for key, value in rows:
        name = key[len('backtest_'):]
        try:
            restored[name] = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            restored[name] = value
    with backtest_lock:
        backtest_state.update(restored)
        if backtest_state.get('status') == 'running':
            backtest_state.update({
                'status': 'partial',
                'message': 'Previous collection was interrupted by a restart. Saved batches are retained; resume to continue.'
            })



def _backtest_status_payload():
    with history_db_lock:
        with _history_db_connect() as db:
            symbol_count = db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='5'").fetchone()[0]
            daily_symbols = db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='D'").fetchone()[0]
            intraday_days = db.execute("SELECT COUNT(*) FROM backtest_candles WHERE timeframe='5'").fetchone()[0]
            intraday_bars = db.execute(
                "SELECT COALESCE(SUM(json_array_length(candles_json)),0) FROM backtest_candles WHERE timeframe='5'"
            ).fetchone()[0]
            error_count = db.execute("SELECT COUNT(*) FROM backtest_errors").fetchone()[0]
    with backtest_lock:
        state = dict(backtest_state)
        state['running'] = backtest_worker_running
    state.update({
        'symbols_with_5m': symbol_count, 'symbols_with_daily': daily_symbols,
        'intraday_session_rows': intraday_days, 'intraday_bars': intraday_bars,
        'error_chunks': error_count, 'storage_path': HISTORY_DB_PATH,
    })
    return state


init_backtest_store()
_backtest_restore_state()


@app.route('/api/backtest/test-history')
def backtest_test_history():
    """Read-only single-stock check of broker daily and 5-minute candle access."""
    if not TOKEN:
        return jsonify({'ok': False, 'error': f'Connect {BROKER_PROVIDER.upper()} first.'}), 400
    if BROKER_PROVIDER != 'zerodha':
        return jsonify({'ok': False, 'error': 'This diagnostic is intended for the active Zerodha connection.'}), 400
    try:
        symbols = list(cache.get('symbols') or [])
        if not symbols:
            symbols = get_symbol_universe()
            with lock:
                cache['symbols'] = symbols
        symbol = next((s for s in symbols if s.split(':')[-1].replace('-EQ', '') == 'SBIN'), None)
        if not symbol:
            return jsonify({'ok': False, 'error': 'SBIN is not present in the loaded F&O universe.'}), 503
        today = datetime.now(IST).date()
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)
        daily = fetch_history(symbol, 'D', start.isoformat(), end.isoformat())
        intraday = fetch_history(symbol, '5', start.isoformat(), end.isoformat())
        def candle_summary(candles):
            if not candles:
                return {'count': 0, 'first': None, 'last': None}
            return {
                'count': len(candles),
                'first': datetime.fromtimestamp(int(candles[0][0]), IST).isoformat(),
                'last': datetime.fromtimestamp(int(candles[-1][0]), IST).isoformat(),
            }
        daily_summary = candle_summary(daily)
        intraday_summary = candle_summary(intraday)
        ok = daily_summary['count'] > 0 and intraday_summary['count'] > 0
        app.logger.info(
            'Backtest single-stock history diagnostic: symbol=%s daily=%d intraday=%d ok=%s',
            symbol, daily_summary['count'], intraday_summary['count'], ok
        )
        return jsonify({
            'ok': ok, 'provider': BROKER_PROVIDER, 'symbol': symbol,
            'range_from': start.isoformat(), 'range_to': end.isoformat(),
            'daily': daily_summary, 'five_minute': intraday_summary,
            'message': 'Both candle intervals returned data.' if ok else 'One or both intervals returned no candles.',
        }), (200 if ok else 502)
    except Exception as exc:
        app.logger.warning('Backtest single-stock history diagnostic failed: %s', exc)
        return jsonify({'ok': False, 'error': str(exc)}), 502


@app.route('/api/backtest/bullbear/export')
def backtest_bullbear_export():
    """Export historical candles as a compressed SQLite snapshot for isolated research."""
    sqlite_path = None
    gzip_path = None
    try:
        with history_db_lock:
            with _history_db_connect() as source:
                row = source.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_candles'"
                ).fetchone()
                if not row:
                    return jsonify({'error': 'Historical backtest_candles table is not available.'}), 404
                handle = tempfile.NamedTemporaryFile(prefix='bullbear-candles-', suffix='.sqlite3', delete=False)
                sqlite_path = handle.name
                handle.close()
                with sqlite3.connect(sqlite_path) as target:
                    source.backup(target, pages=256, name='main')
                    # Keep only historical candles; exclude app tokens, login metadata,
                    # backtest results, errors, and other private tables.
                    for table in (
                        'backtest_chunks', 'backtest_errors', 'backtest_results',
                        'candle_cache', 'history_meta', 'stock_sector_cache', 'app_meta',
                        'momentum_alert_active', 'top_gainer_daily_state', 'top_gainer_snapshot'
                    ):
                        target.execute('DROP TABLE IF EXISTS "' + table + '"')
                    target.commit()

        # Compress in bounded-memory chunks, then stream the compressed file.
        handle = tempfile.NamedTemporaryFile(prefix='bullbear-candles-', suffix='.sqlite3.gz', delete=False)
        gzip_path = handle.name
        handle.close()
        with open(sqlite_path, 'rb') as source_file, gzip.open(gzip_path, 'wb', compresslevel=6) as compressed:
            shutil.copyfileobj(source_file, compressed, length=1024 * 1024)
        os.unlink(sqlite_path)
        sqlite_path = None

        response = send_file(
            gzip_path,
            mimetype='application/gzip',
            as_attachment=True,
            download_name='bullbear_backtest_candles.sqlite3.gz',
            conditional=False,
        )
        def cleanup_export_files():
            for path in (sqlite_path, gzip_path):
                if path and os.path.exists(path):
                    try:
                        os.unlink(path)
                    except OSError:
                        app.logger.warning('Could not remove temporary bull/bear export file: %s', path)
        response.call_on_close(cleanup_export_files)
        return response
    except Exception as exc:
        for path in (sqlite_path, gzip_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        app.logger.exception('Bull/bear historical candle export failed: %s', exc)
        return jsonify({'error': 'Historical candle export failed.'}), 500


@app.route('/api/backtest/status')
def backtest_status():
    return jsonify(_backtest_status_payload())


@app.route('/api/backtest/diagnostics')
def backtest_diagnostics():
    """Read-only inspection of failed history chunks; protected by private login."""
    today = datetime.now(IST).date()
    trade_end = today - timedelta(days=1)
    trade_start = trade_end - timedelta(days=BACKTEST_DAYS - 1)
    daily_start = trade_start - timedelta(days=366)
    intraday_start_text = trade_start.isoformat()
    intraday_end_text = trade_end.isoformat()
    daily_start_text = daily_start.isoformat()
    daily_end_text = trade_end.isoformat()
    with history_db_lock:
        with _history_db_connect() as db:
            total = db.execute("SELECT COUNT(*) FROM backtest_errors").fetchone()[0]
            matched = db.execute("""
                SELECT COUNT(*) FROM backtest_errors e
                WHERE EXISTS (
                    SELECT 1 FROM backtest_chunks c
                    WHERE c.symbol=e.symbol AND c.timeframe=e.timeframe
                      AND c.range_from=e.range_from AND c.range_to=e.range_to
                )
            """).fetchone()[0]
            outside = db.execute("""
                SELECT COUNT(*) FROM backtest_errors
                WHERE NOT (
                    (timeframe='5' AND range_from >= ? AND range_to <= ?)
                    OR (timeframe='D' AND range_from >= ? AND range_to <= ?)
                )
            """, (intraday_start_text, intraday_end_text, daily_start_text, daily_end_text)).fetchone()[0]
            groups = db.execute("""
                SELECT timeframe, error, COUNT(*) AS count
                FROM backtest_errors
                GROUP BY timeframe, error
                ORDER BY count DESC, timeframe, error
                LIMIT 100
            """).fetchall()
            rows = db.execute("""
                SELECT e.symbol, e.timeframe, e.range_from, e.range_to, e.error,
                       e.updated_at,
                       CASE WHEN c.symbol IS NULL THEN 0 ELSE 1 END AS chunk_saved,
                       COALESCE(c.candle_count, 0) AS candle_count
                FROM backtest_errors e
                LEFT JOIN backtest_chunks c
                  ON c.symbol=e.symbol AND c.timeframe=e.timeframe
                 AND c.range_from=e.range_from AND c.range_to=e.range_to
                ORDER BY e.timeframe, e.range_from, e.symbol
                LIMIT 300
            """).fetchall()
    return jsonify({
        'read_only': True,
        'error_count': total,
        'errors_with_matching_completed_chunk': matched,
        'errors_outside_current_collection_window': outside,
        'current_window': {
            'intraday_5m': {'from': intraday_start_text, 'to': intraday_end_text},
            'daily': {'from': daily_start_text, 'to': daily_end_text},
        },
        'error_groups': [
            {'timeframe': r[0], 'error': r[1], 'count': r[2]} for r in groups
        ],
        'failed_chunks': [
            {'symbol': r[0], 'timeframe': r[1], 'from': r[2], 'to': r[3],
             'error': r[4], 'updated_at': r[5], 'matching_chunk_saved': bool(r[6]),
             'stored_candle_count': r[7]} for r in rows
        ],
    })


@app.route('/api/backtest/start', methods=['POST'])
def backtest_start():
    global backtest_worker_running
    if not TOKEN:
        return jsonify({'error': f'Connect {BROKER_PROVIDER.upper()} before starting historical collection.'}), 400
    try:
        if not cache['symbols']:
            cache['symbols'] = get_symbol_universe()
        symbols = list(cache['symbols'])
        if not symbols:
            return jsonify({'error': 'The F&O stock universe is empty.'}), 503
        with backtest_lock:
            if backtest_worker_running:
                return jsonify({'ok': True, 'message': 'Historical collection is already running.'})
            backtest_worker_running = True
        today = datetime.now(IST).date()
        trade_end = today - timedelta(days=1)
        trade_start = trade_end - timedelta(days=BACKTEST_DAYS - 1)

        # Remove obsolete failures from older collection windows (for example,
        # FYERS 401 errors) before resuming the current provider's collection.
        # This only prunes error metadata; saved candle and completed-chunk data
        # are never deleted.
        daily_start = trade_start - timedelta(days=366)
        with history_db_lock:
            with _history_db_connect() as db:
                cursor = db.execute("""
                    DELETE FROM backtest_errors
                    WHERE (timeframe='5' AND NOT (range_from >= ? AND range_to <= ?))
                       OR (timeframe='D' AND NOT (range_from >= ? AND range_to <= ?))
                       OR timeframe NOT IN ('5', 'D')
                """, (
                    trade_start.isoformat(), trade_end.isoformat(),
                    daily_start.isoformat(), trade_end.isoformat()
                ))
                stale_errors_removed = cursor.rowcount
        if stale_errors_removed:
            app.logger.info(
                'Removed %d obsolete backtest errors outside the current collection window.',
                stale_errors_removed
            )

        worker = threading.Thread(
            target=_backtest_worker, args=(symbols, trade_start, trade_end),
            name='backtest-history-worker', daemon=True
        )
        try:
            worker.start()
        except Exception:
            with backtest_lock:
                backtest_worker_running = False
            raise
        return jsonify({
            'ok': True, 'message': 'Historical collection started. It runs in the background and can resume completed chunks after interruption.',
            'symbols': len(symbols), 'trade_start': trade_start.isoformat(),
            'trade_end': trade_end.isoformat()
        })
    except Exception as exc:
        app.logger.exception('Could not start backtest history collection: %s', exc)
        return jsonify({'error': 'Could not start historical collection. Check the server log.'}), 500



# --------------------------- Strategy validation V1 ---------------------------
BACKTEST_CAPITAL = 250000.0
BACKTEST_BROKERAGE = 137.0  # User-specified flat brokerage/cost per completed trade.
strategy_run_guard = threading.Lock()
strategy_lock = threading.RLock()
strategy_worker_running = False
strategy_state = {
    'status': 'not_run',
    'message': 'Strategy validation has not been run.',
    'trade_count': 0,
    'trades': [],
    'summary': {},
}


def _bt_candle_time(candle):
    return datetime.fromtimestamp(int(candle[0]), IST)


def _strategy_compute():
    """Run the agreed PDH + rolling 52-week + 3x same-time volume setup on stored 5m data."""
    global strategy_worker_running
    _backtest_update_state(strategy_status='calculating')
    try:
        with history_db_lock:
            with _history_db_connect() as db:
                symbols = [r[0] for r in db.execute(
                    "SELECT DISTINCT symbol FROM backtest_candles WHERE timeframe='5' ORDER BY symbol"
                ).fetchall()]

        trades = []
        excluded = {'missing_reference': 0, 'missing_daily_history': 0, 'insufficient_volume_history': 0}
        # Load and process one symbol at a time to keep peak RAM bounded.
        for symbol in symbols:
            with history_db_lock:
                with _history_db_connect() as db:
                    intraday_rows = db.execute(
                        "SELECT session_date,candles_json FROM backtest_candles WHERE symbol=? AND timeframe='5' ORDER BY session_date",
                        (symbol,)
                    ).fetchall()
                    daily_rows = db.execute(
                        "SELECT session_date,candles_json FROM backtest_candles WHERE symbol=? AND timeframe='D' ORDER BY session_date",
                        (symbol,)
                    ).fetchall()
            daily = {}
            for session_date, raw in daily_rows:
                try:
                    parsed = json.loads(raw)
                    if parsed:
                        daily[session_date] = parsed[-1]
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
            daily_dates = sorted(daily)
            prior_day_high = {}
            rolling_high = {}
            for i, d in enumerate(daily_dates):
                if i > 0:
                    prior_day_high[d] = float(daily[daily_dates[i-1]][2])
                window = daily_dates[max(0, i-252):i]
                rolling_high[d] = max((float(daily[x][2]) for x in window), default=None)

            sessions = {}
            for session_date, raw in intraday_rows:
                try:
                    candles = json.loads(raw)
                    candles = sorted((c for c in candles if isinstance(c, list) and len(c) >= 6), key=lambda c: int(c[0]))
                    if candles:
                        sessions[session_date] = candles
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue

            # Cache cumulative volume at each 5-minute slot for the prior 20 sessions.
            session_dates = sorted(sessions)
            for day_index, trade_date in enumerate(session_dates):
                if trade_date not in prior_day_high or rolling_high.get(trade_date) is None:
                    excluded['missing_daily_history'] += 1
                    continue
                candles = sessions[trade_date]
                ref_index = None
                for i, c in enumerate(candles):
                    t = _bt_candle_time(c)
                    if t.hour == 9 and t.minute == 15:
                        ref_index = i
                        break
                if ref_index is None:
                    excluded['missing_reference'] += 1
                    continue
                ref = candles[ref_index]
                ref_high, ref_low = float(ref[2]), float(ref[3])
                pdh = prior_day_high[trade_date]
                week_high = rolling_high[trade_date]
                if not (week_high > 0 and ref_high >= 0):
                    continue

                previous_sessions = [sessions[d] for d in session_dates[max(0, day_index-20):day_index]]
                if len(previous_sessions) < 20:
                    excluded['insufficient_volume_history'] += 1
                    continue

                entry_index = None
                entry_price = None
                for i in range(ref_index + 1, len(candles)):
                    c = candles[i]
                    t = _bt_candle_time(c)
                    if (t.hour, t.minute) >= (15, 0):
                        break
                    high = float(c[2])
                    if high > ref_high:
                        entry_index = i
                        entry_price = max(ref_high, float(c[1]))
                        break
                if entry_index is None or entry_price <= 0:
                    continue

                # Confirm PDH and 52-week-high proximity at the time of entry.
                if entry_price <= pdh:
                    continue
                if (week_high - entry_price) / week_high > 0.05:
                    continue

                entry_candle = candles[entry_index]
                entry_time = _bt_candle_time(entry_candle)
                slot = entry_index - ref_index
                prior_cum = []
                for old in previous_sessions:
                    cum = 0.0
                    seen = 0
                    for old_c in old:
                        old_t = _bt_candle_time(old_c)
                        if (old_t.hour, old_t.minute) < (9, 15):
                            continue
                        # Include the historical candle at the entry timestamp.
                        # Current cumulative volume also includes the entry candle,
                        # so both sides use the same inclusive time boundary.
                        if (old_t.hour, old_t.minute) > (entry_time.hour, entry_time.minute):
                            break
                        cum += float(old_c[5])
                        seen += 1
                    if seen >= slot:
                        prior_cum.append(cum)
                if len(prior_cum) < 20:
                    excluded['insufficient_volume_history'] += 1
                    continue
                current_cum = sum(float(c[5]) for c in candles[ref_index:entry_index+1])
                avg_cum = sum(prior_cum) / len(prior_cum)
                if avg_cum <= 0 or current_cum < 3.0 * avg_cum:
                    continue

                qty = int(BACKTEST_CAPITAL // entry_price)
                if qty <= 0:
                    continue
                stop = ref_low
                exit_price = None
                exit_time = None
                exit_reason = '3:00 PM'
                # Conservative treatment: if the entry candle also reaches the stop,
                # assume the stop was hit after entry.
                if float(entry_candle[3]) <= stop:
                    exit_price = stop
                    exit_time = entry_time
                    exit_reason = 'Stop loss (same-candle conservative)'
                else:
                    for c in candles[entry_index+1:]:
                        t = _bt_candle_time(c)
                        if (t.hour, t.minute) >= (15, 0):
                            break
                        if float(c[3]) <= stop:
                            exit_price = stop
                            exit_time = t
                            exit_reason = 'Stop loss'
                            break
                if exit_price is None:
                    eligible = [c for c in candles if ( _bt_candle_time(c).hour, _bt_candle_time(c).minute) <= (15, 0)]
                    if not eligible:
                        continue
                    last = eligible[-1]
                    exit_price = float(last[4])
                    exit_time = _bt_candle_time(last)
                gross = (exit_price - entry_price) * qty
                net = gross - BACKTEST_BROKERAGE
                trades.append({
                    'date': trade_date, 'symbol': symbol.split(':')[-1].replace('-EQ',''),
                    'entry_time': entry_time.strftime('%H:%M'), 'entry': round(entry_price,2),
                    'stop': round(stop,2), 'exit_time': exit_time.strftime('%H:%M'),
                    'exit': round(exit_price,2), 'qty': qty, 'exit_reason': exit_reason,
                    'gross_pnl': round(gross,2), 'brokerage': BACKTEST_BROKERAGE,
                    'net_pnl': round(net,2), 'volume_multiple': round(current_cum/avg_cum,2),
                    'week52_high': round(week_high,2)
                })

        trades.sort(key=lambda x: (x['date'], x['entry_time'], x['symbol']))
        wins = [t for t in trades if t['net_pnl'] > 0]
        losses = [t for t in trades if t['net_pnl'] <= 0]
        gross_total = sum(t['gross_pnl'] for t in trades)
        net_total = sum(t['net_pnl'] for t in trades)
        gross_profit = sum(t['gross_pnl'] for t in trades if t['gross_pnl'] > 0)
        gross_loss = abs(sum(t['gross_pnl'] for t in trades if t['gross_pnl'] < 0))
        equity = peak = max_dd = 0.0
        for t in trades:
            equity += t['net_pnl']
            peak = max(peak, equity)
            max_dd = max(max_dd, peak-equity)
        summary = {
            'total_trades': len(trades), 'winning_trades': len(wins), 'losing_trades': len(losses),
            'win_rate_pct': round(len(wins)/len(trades)*100,2) if trades else 0,
            'gross_pnl': round(gross_total,2), 'brokerage_total': round(len(trades)*BACKTEST_BROKERAGE,2),
            'net_pnl': round(net_total,2), 'profit_factor': round(gross_profit/gross_loss,2) if gross_loss else None,
            'max_drawdown': round(max_dd,2),
            'avg_net_per_trade': round(net_total/len(trades),2) if trades else 0,
            'capital_per_trade': BACKTEST_CAPITAL, 'brokerage_per_trade': BACKTEST_BROKERAGE,
            'excluded_days': excluded,
            'data_note': '5-minute OHLCV only. Same-candle stop is treated conservatively. Flat ₹137 per completed trade is deducted; other statutory charges and slippage are not included.'
        }
        with strategy_lock:
            strategy_state.update({'status':'complete','message':'Strategy validation complete.','trade_count':len(trades),
                                   'trades':trades,'summary':summary,'updated_at':datetime.now(IST).isoformat()})
            saved_state = dict(strategy_state)
        _persist_backtest_result('v1', saved_state)
    except Exception as exc:
        app.logger.exception('Strategy validation failed: %s', exc)
        with strategy_lock:
            strategy_state.update({'status':'error','message':'Strategy validation failed. Check server logs.','error':str(exc)})
    finally:
        with strategy_lock:
            strategy_worker_running = False


@app.route('/api/backtest/strategy/status')
def backtest_strategy_status():
    with strategy_lock:
        return jsonify({k:v for k,v in strategy_state.items() if k != 'trades'})


@app.route('/api/backtest/strategy/results')
def backtest_strategy_results():
    with strategy_lock:
        return jsonify({'status':strategy_state.get('status'), 'summary':strategy_state.get('summary',{}),
                        'trades':strategy_state.get('trades',[])})


@app.route('/api/backtest/strategy/download')
def backtest_strategy_download():
    with strategy_lock:
        payload = {'status': strategy_state.get('status'), 'summary': strategy_state.get('summary', {}),
                   'trades': strategy_state.get('trades', []), 'updated_at': strategy_state.get('updated_at')}
    if payload['status'] != 'complete':
        return jsonify({'error': 'No completed V1 result is currently available.'}), 409
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype='application/json',
                    headers={'Content-Disposition': 'attachment; filename="strategy_v1_results.json"'})


@app.route('/api/backtest/strategy/run', methods=['POST'])
def backtest_strategy_run():
    global strategy_worker_running
    with history_db_lock:
        with _history_db_connect() as db:
            errors = db.execute("SELECT COUNT(*) FROM backtest_errors").fetchone()[0]
            five = db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='5'").fetchone()[0]
            daily = db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='D'").fetchone()[0]
    if errors:
        return jsonify({'error':f'{errors} historical data errors remain. Resolve them before strategy validation.'}), 409
    if five == 0 or daily == 0:
        return jsonify({'error':'Historical 5-minute and daily data are required.'}), 409
    with strategy_run_guard:
        with strategy_v2_lock:
            if strategy_v2_worker_running:
                return jsonify({'error':'IVF V2 is running. Wait for it to finish before starting V1.'}), 409
        with strategy_lock:
            if strategy_worker_running:
                return jsonify({'ok':True,'message':'Strategy validation is already running.'})
            strategy_worker_running = True
            strategy_state.update({'status':'running','message':'Calculating trades across stored historical candles.'})
    threading.Thread(target=_strategy_compute, name='backtest-strategy-worker', daemon=True).start()
    return jsonify({'ok':True,'message':'Strategy validation started.'})


# --------------------------- IVF Strategy validation V2 ---------------------------
strategy_v2_lock = threading.RLock()
strategy_v2_worker_running = False
strategy_v2_state = {'status':'not_run','message':'IVF Version 2 has not been run.','trade_count':0,'trades':[],'summary':{}}


# Restore the latest successful runs from the persistent /var/data history database.
_restore_backtest_result('v1', strategy_state, strategy_lock)
_restore_backtest_result('v2', strategy_v2_state, strategy_v2_lock)


def _strategy_v2_compute():
    global strategy_v2_worker_running
    with strategy_v2_lock:
        strategy_v2_state.update({'status':'calculating','message':'Calculating IVF V2 on stored 5-minute candles.'})
    try:
        with history_db_lock:
            with _history_db_connect() as db:
                symbols=[r[0] for r in db.execute("SELECT DISTINCT symbol FROM backtest_candles WHERE timeframe='5' ORDER BY symbol").fetchall()]
        trades=[]
        excluded={'missing_0920_or_0925':0,'insufficient_20_slot_history':0,'pdh_not_broken':0,'no_0925_breakout':0,'invalid_or_over_2pct_stop':0}
        # Load and process one symbol at a time to keep peak RAM bounded.
        for symbol in symbols:
            with history_db_lock:
                with _history_db_connect() as db:
                    intraday_rows=db.execute("SELECT session_date,candles_json FROM backtest_candles WHERE symbol=? AND timeframe='5' ORDER BY session_date",(symbol,)).fetchall()
                    daily_rows=db.execute("SELECT session_date,candles_json FROM backtest_candles WHERE symbol=? AND timeframe='D' ORDER BY session_date",(symbol,)).fetchall()
            daily={}
            for d,raw in daily_rows:
                try:
                    x=json.loads(raw)
                    if x: daily[d]=x[-1]
                except (TypeError,ValueError,json.JSONDecodeError): pass
            dates=sorted(daily)
            pdh={dates[i]:float(daily[dates[i-1]][2]) for i in range(1,len(dates))}
            sessions={}
            for d,raw in intraday_rows:
                try:
                    cs=sorted([c for c in json.loads(raw) if isinstance(c,list) and len(c)>=6],key=lambda c:int(c[0]))
                    if cs: sessions[d]=cs
                except (TypeError,ValueError,json.JSONDecodeError): pass
            session_dates=sorted(sessions)
            for di,d in enumerate(session_dates):
                if d not in pdh: continue
                by_time={}
                for c in sessions[d]:
                    t=_bt_candle_time(c)
                    by_time[(t.hour,t.minute)]=c
                c920=by_time.get((9,20)); c925=by_time.get((9,25))
                if c920 is None or c925 is None:
                    excluded['missing_0920_or_0925']+=1; continue
                trigger=float(c920[2])
                if float(c925[2])<=trigger:
                    excluded['no_0925_breakout']+=1; continue
                # Compare the 09:25 candle volume with the same slot in each of 20 prior sessions.
                prior=[]
                for old_date in session_dates[max(0,di-20):di]:
                    old=next((c for c in sessions[old_date] if (_bt_candle_time(c).hour,_bt_candle_time(c).minute)==(9,25)),None)
                    if old is not None: prior.append(float(old[5]))
                if len(prior)<20:
                    excluded['insufficient_20_slot_history']+=1; continue
                avg=sum(prior)/20.0
                if avg<=0 or float(c925[5]) < 3.0*avg: continue
                # Avoid assuming a fill below an opening gap.
                entry=max(trigger,float(c925[1]))
                if entry<=pdh[d]:
                    excluded['pdh_not_broken']+=1; continue
                stop=float(c925[3])
                risk=(entry-stop)/entry if entry>0 else 99
                if risk<=0 or risk>0.02:
                    excluded['invalid_or_over_2pct_stop']+=1; continue
                qty=int(250000.0//entry)
                if qty<=0: continue
                exit_price=None; exit_time=None; reason='3:00 PM'
                # The 09:25 low defines the stop. Start monitoring from the next candle:
                # 5-minute OHLCV cannot establish whether that low preceded the breakout.
                for c in sessions[d]:
                    t=_bt_candle_time(c)
                    if t<=_bt_candle_time(c925): continue
                    if (t.hour,t.minute)>=(15,0): break
                    if float(c[3])<=stop:
                        exit_price=stop; exit_time=t; reason='Stop loss'; break
                if exit_price is None:
                    eligible=[c for c in sessions[d] if (_bt_candle_time(c).hour,_bt_candle_time(c).minute)<=(15,0)]
                    if not eligible: continue
                    last=eligible[-1]; exit_price=float(last[4]); exit_time=_bt_candle_time(last)
                gross=(exit_price-entry)*qty
                trades.append({'date':d,'symbol':symbol.split(':')[-1].replace('-EQ',''),
                    'entry_time':'09:25','entry':round(entry,2),'stop':round(stop,2),
                    'stop_pct':round(risk*100,2),'exit_time':exit_time.strftime('%H:%M'),
                    'exit':round(exit_price,2),'qty':qty,'exit_reason':reason,
                    'gross_pnl':round(gross,2),'brokerage':137.0,'net_pnl':round(gross-137.0,2),
                    'ivf_multiple':round(float(c925[5])/avg,2),'pdh':round(pdh[d],2)})
        trades.sort(key=lambda x:(x['date'],x['entry_time'],x['symbol']))
        wins=[t for t in trades if t['net_pnl']>0]; losses=[t for t in trades if t['net_pnl']<=0]
        gross=sum(t['gross_pnl'] for t in trades); net=sum(t['net_pnl'] for t in trades)
        gp=sum(t['gross_pnl'] for t in trades if t['gross_pnl']>0)
        gl=abs(sum(t['gross_pnl'] for t in trades if t['gross_pnl']<0))
        equity=peak=dd=0.0
        for t in trades:
            equity+=t['net_pnl']; peak=max(peak,equity); dd=max(dd,peak-equity)
        summary={'total_trades':len(trades),'winning_trades':len(wins),'losing_trades':len(losses),
            'win_rate_pct':round(len(wins)/len(trades)*100,2) if trades else 0,
            'gross_pnl':round(gross,2),'brokerage_total':round(len(trades)*137.0,2),
            'net_pnl':round(net,2),'profit_factor':round(gp/gl,2) if gl else None,
            'max_drawdown':round(dd,2),'avg_net_per_trade':round(net/len(trades),2) if trades else 0,
            'capital_per_trade':250000,'brokerage_per_trade':137,'excluded_days':excluded,
            'data_note':'IVF = 09:25 candle volume / average 09:25 candle volume over prior 20 sessions; ≥3×. PDH mandatory. Entry trigger is 09:20 high when 09:25 candle breaks it; fill is max(trigger, 09:25 open). Stop is 09:25 low; stop checks begin on the next 5-minute candle because intrabar order is unknown. Trades with stop distance >2% or invalid stop are skipped. Exit at stop or last close at/before 15:00. One trade per stock/day. ₹2,50,000 notional; flat ₹137 brokerage. Other charges and slippage excluded.'}
        with strategy_v2_lock:
            strategy_v2_state.update({'status':'complete','message':'IVF Version 2 backtest complete.','trade_count':len(trades),'trades':trades,'summary':summary,'updated_at':datetime.now(IST).isoformat()})
            saved_state = dict(strategy_v2_state)
        _persist_backtest_result('v2', saved_state)
    except Exception as exc:
        app.logger.exception('IVF V2 failed: %s',exc)
        with strategy_v2_lock: strategy_v2_state.update({'status':'error','message':'IVF V2 failed. Check server logs.','error':str(exc)})
    finally:
        with strategy_v2_lock: strategy_v2_worker_running=False


# --------------------------- Independent IVF V2 audit ---------------------------
ivf_v2_audit_lock = threading.RLock()
ivf_v2_audit_running = False
ivf_v2_audit_state = {
    'status': 'not_run',
    'message': 'Independent IVF V2 audit has not been run.',
    'report': None,
}


def _run_ivf_v2_audit():
    global ivf_v2_audit_running
    with ivf_v2_audit_lock:
        ivf_v2_audit_state.update({
            'status': 'running',
            'message': 'Recalculating IVF V2 independently from stored candles. Read-only audit.',
            'report': None,
        })
    try:
        # Separate read-only SQLite connection. No table writes or backtest reruns.
        db_uri = 'file:' + os.path.abspath(HISTORY_DB_PATH) + '?mode=ro'
        db = sqlite3.connect(db_uri, uri=True, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            row = db.execute(
                "SELECT payload_json FROM backtest_results WHERE strategy_key='v2'"
            ).fetchone()
            if row is None:
                raise RuntimeError('No saved IVF V2 result exists in the history database.')
            saved = json.loads(row['payload_json'])
            if saved.get('status') != 'complete':
                raise RuntimeError('Saved IVF V2 result is not complete.')
            from ivf_v2_validator import validate_ivf_v2
            report = validate_ivf_v2(db, saved.get('trades', []), saved.get('summary', {}))
            report['saved_result_updated_at'] = saved.get('updated_at')
        finally:
            db.close()
        with ivf_v2_audit_lock:
            ivf_v2_audit_state.update({
                'status': 'complete',
                'message': 'Independent IVF V2 audit complete. Original results and candles were not modified.',
                'report': report,
                'updated_at': datetime.now(IST).isoformat(),
            })
    except Exception as exc:
        app.logger.exception('Independent IVF V2 audit failed: %s', exc)
        with ivf_v2_audit_lock:
            ivf_v2_audit_state.update({
                'status': 'error',
                'message': 'Independent audit failed. Check server logs.',
                'error': str(exc),
            })
    finally:
        with ivf_v2_audit_lock:
            ivf_v2_audit_running = False


@app.route('/api/backtest/strategy/v2/audit/status')
def backtest_strategy_v2_audit_status():
    with ivf_v2_audit_lock:
        return jsonify({k: v for k, v in ivf_v2_audit_state.items() if k != 'report'})


@app.route('/api/backtest/strategy/v2/audit/results')
def backtest_strategy_v2_audit_results():
    with ivf_v2_audit_lock:
        return jsonify(ivf_v2_audit_state)


@app.route('/api/backtest/strategy/v2/audit/run', methods=['POST'])
def backtest_strategy_v2_audit_run():
    global ivf_v2_audit_running
    with ivf_v2_audit_lock:
        if ivf_v2_audit_running:
            return jsonify({'ok': True, 'message': 'Independent audit is already running.'})
        ivf_v2_audit_running = True
    threading.Thread(target=_run_ivf_v2_audit, name='ivf-v2-independent-audit', daemon=True).start()
    return jsonify({'ok': True, 'message': 'Independent read-only audit started.'})


@app.route('/api/backtest/strategy/v2/status')
def backtest_strategy_v2_status():
    with strategy_v2_lock: return jsonify({k:v for k,v in strategy_v2_state.items() if k!='trades'})


@app.route('/api/backtest/strategy/v2/results')
def backtest_strategy_v2_results():
    with strategy_v2_lock: return jsonify({'status':strategy_v2_state.get('status'),'summary':strategy_v2_state.get('summary',{}),'trades':strategy_v2_state.get('trades',[])})


@app.route('/api/backtest/strategy/v2/download')
def backtest_strategy_v2_download():
    with strategy_v2_lock:
        payload = {'status': strategy_v2_state.get('status'), 'summary': strategy_v2_state.get('summary', {}),
                   'trades': strategy_v2_state.get('trades', []), 'updated_at': strategy_v2_state.get('updated_at')}
    if payload['status'] != 'complete':
        return jsonify({'error': 'No completed IVF V2 result is currently available.'}), 409
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype='application/json',
                    headers={'Content-Disposition': 'attachment; filename="ivf_v2_results.json"'})


@app.route('/api/backtest/strategy/v2/run',methods=['POST'])
def backtest_strategy_v2_run():
    global strategy_v2_worker_running
    with history_db_lock:
        with _history_db_connect() as db:
            five=db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='5'").fetchone()[0]
            daily=db.execute("SELECT COUNT(DISTINCT symbol) FROM backtest_candles WHERE timeframe='D'").fetchone()[0]
            errors=db.execute("SELECT COUNT(*) FROM backtest_errors").fetchone()[0]
    if not five or not daily: return jsonify({'error':'Historical 5-minute and daily data are required.'}),409
    if errors: return jsonify({'error':f'{errors} historical data errors remain. Resolve them before running IVF V2.'}),409
    with strategy_run_guard:
        with strategy_lock:
            if strategy_worker_running:
                return jsonify({'error':'V1 backtest is running. Wait for it to finish before starting IVF V2.'}), 409
        with strategy_v2_lock:
            if strategy_v2_worker_running: return jsonify({'ok':True,'message':'IVF V2 is already running.'})
            strategy_v2_worker_running=True
            strategy_v2_state.update({'status':'running','message':'IVF V2 calculation started.'})
    threading.Thread(target=_strategy_v2_compute,name='backtest-strategy-v2-worker',daemon=True).start()
    return jsonify({'ok':True,'message':'IVF V2 calculation started.'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=False)
