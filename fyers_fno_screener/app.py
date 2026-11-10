import os
import csv
import io
import time
import json
import math
import queue
import hashlib
import secrets
import threading
from collections import deque
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, redirect, request, session, url_for

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'CHANGE_THIS_LONG_RANDOM_SECRET')
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=os.getenv('COOKIE_SECURE', 'false').lower() == 'true',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

APP_ID = os.getenv('FYERS_APP_ID', '').strip()
APP_SECRET = os.getenv('FYERS_APP_SECRET', '').strip()
REDIRECT_URI = os.getenv('FYERS_REDIRECT_URI', '').strip()
TOKEN = os.getenv('FYERS_ACCESS_TOKEN', '').strip()
SCREENER_PASSWORD = os.getenv('SCREENER_PASSWORD', '').strip()
API_ROOT = 'https://api-t1.fyers.in'
DATA_ROOT = 'https://api-t1.fyers.in/data'
IST = ZoneInfo('Asia/Kolkata')
INDEX_NAMES = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50', 'SENSEX', 'BANKEX'}

cache = {
    'symbols': [], 'quotes': {}, 'intraday': {}, 'daily': {},
    'last_scan': 0, 'last_error': None, 'history_done': 0,
    'history_total': 0, 'history_running': False, 'history_message': 'Waiting for FYERS connection',
    'connected': bool(TOKEN), 'last_quote_count': 0,
}
lock = threading.RLock()

# FYERS Standard Data API limits (as documented from 16 Oct 2026): 50/min.
# Keep a safety margin and share the limiter across quote + history requests.
_api_times = deque()
_history_times = deque()
_api_lock = threading.Lock()


def api_slot(priority=False):
    """Reserve capacity for quote refreshes; throttle background history separately."""
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


def auth_headers():
    if not APP_ID or not TOKEN:
        raise RuntimeError('FYERS is not connected. Sign in and select Connect FYERS.')
    return {'Authorization': f'{APP_ID}:{TOKEN}', 'Content-Type': 'application/json'}


def fyers_get(path, params=None, priority=False):
    api_slot(priority=priority)
    response = requests.get(DATA_ROOT + path, headers=auth_headers(), params=params, timeout=25)
    if response.status_code == 429:
        raise RuntimeError('FYERS data API rate limit reached. The scanner will retry on its next refresh.')
    response.raise_for_status()
    payload = response.json()
    if payload.get('s') == 'error':
        raise RuntimeError(payload.get('message') or str(payload))
    return payload


def get_symbol_universe():
    """Unique cash-equity symbols whose underlyings exist in the NSE F&O master."""
    url = 'https://public.fyers.in/sym_details/NSE_FO.csv'
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    rows = list(csv.reader(io.StringIO(response.text)))
    if not rows:
        return []
    first = [cell.strip().lower() for cell in rows[0]]
    has_header = any('underlying symbol' in cell for cell in first)
    if has_header:
        headers, data = first, rows[1:]
        def find_col(term):
            return next((i for i, h in enumerate(headers) if term in h), None)
        underlying_idx = find_col('underlying symbol')
    else:
        data, underlying_idx = rows, 13
    underlyings = set()
    for row in data:
        if underlying_idx is None or len(row) <= underlying_idx:
            continue
        name = row[underlying_idx].strip().upper()
        if not name or name in INDEX_NAMES:
            continue
        # Avoid malformed/index values; preserve valid NSE symbols such as M&M.
        if all(ch.isalnum() or ch in '&-_' for ch in name):
            underlyings.add(name)
    return sorted(f'NSE:{name}-EQ' for name in underlyings)


def refresh_quotes(symbols):
    """Fetch the entire F&O stock universe in FYERS Standard 50-symbol batches."""
    result = {}
    for start in range(0, len(symbols), 50):
        batch = symbols[start:start + 50]
        payload = fyers_get('/data/quotes', {'symbols': ','.join(batch)}, priority=True)
        for item in payload.get('d', []):
            if item.get('s') == 'ok':
                result[item.get('n', '')] = item.get('v', {})
    with lock:
        cache['quotes'].update(result)
        cache['last_scan'] = time.time()
        cache['last_quote_count'] = len(result)
        cache['last_error'] = None
    return result


def fetch_history(symbol, resolution, start_date, end_date):
    payload = fyers_get('/data/history', {
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
    """Cache 20-session same-time volume baseline and available 5Y daily ATH/PDH."""
    today = datetime.now(IST).date()
    # Intraday API allows up to 100 calendar days per request. 45 days covers 20 sessions.
    intraday_start = today - timedelta(days=45)
    intraday = fetch_history(symbol, '5', intraday_start.isoformat(), today.isoformat())
    daily_start = today - timedelta(days=5 * 366)
    daily_candles = []
    for start, end in date_chunks(daily_start, today - timedelta(days=1), 366):
        daily_candles.extend(fetch_history(symbol, 'D', start.isoformat(), end.isoformat()))
    with lock:
        cache['intraday'][symbol] = intraday
        cache['daily'][symbol] = daily_candles


def history_worker(symbols):
    with lock:
        cache['history_running'] = True
        cache['history_total'] = len(symbols)
        cache['history_done'] = 0
        cache['history_message'] = 'Building historical baselines. Results will populate progressively.'
    try:
        for symbol in symbols:
            with lock:
                have_intraday = symbol in cache['intraday']
                have_daily = symbol in cache['daily']
            if not have_intraday or not have_daily:
                try:
                    warm_history_for_symbol(symbol)
                except Exception as exc:
                    with lock:
                        cache['last_error'] = f'{symbol}: {exc}'
            with lock:
                cache['history_done'] += 1
                cache['history_message'] = f"Historical data ready for {cache['history_done']} of {cache['history_total']} stocks"
    finally:
        with lock:
            cache['history_running'] = False
            cache['history_message'] = 'Historical baseline ready' if cache['history_done'] == cache['history_total'] else cache['history_message']


def start_history_worker(symbols):
    with lock:
        if cache['history_running']:
            return
        # Refresh history once per process/day. Keep the already loaded data on manual quote refreshes.
        missing = [s for s in symbols if s not in cache['intraday'] or s not in cache['daily']]
        if not missing:
            return
        thread = threading.Thread(target=history_worker, args=(missing,), daemon=True)
        thread.start()


def volume_ratio_for_time(candles, now=None):
    """Compare current cumulative volume to mean cumulative volume at same 5m time over 20 prior sessions."""
    if not candles:
        return None, None, 0
    now = now or datetime.now(IST)
    today = now.date()
    # Include bars whose start time is at or before the current 5-minute bucket.
    current_bucket_minute = (now.hour * 60 + now.minute) // 5 * 5
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
        total = sum(vol for minute, vol in sessions[session_date] if minute <= current_bucket_minute)
        cumulative.append(total)
    average = sum(cumulative) / len(cumulative) if cumulative else 0
    return average, cumulative[-1] if cumulative else None, len(cumulative)


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

    avg_same_time, _, sessions = volume_ratio_for_time(intraday or [], now)
    volume_ratio = day_volume / avg_same_time if avg_same_time and avg_same_time > 0 else None

    prior_daily = []
    if daily:
        for candle in daily:
            if len(candle) >= 6:
                candle_date = datetime.fromtimestamp(int(candle[0]), IST).date()
                if candle_date < now.date():
                    prior_daily.append(candle)
    prev_day_high = float(prior_daily[-1][2]) if prior_daily else None
    historical_ath = max((float(c[2]) for c in prior_daily), default=None)
    day_high = quote.get('high_price')
    ath = max([x for x in (historical_ath, float(day_high) if day_high is not None else None) if x is not None], default=None)
    ath_distance = ((ath - ltp) / ath * 100) if ath and ath > 0 else None

    volume_ok = volume_ratio is not None and volume_ratio >= 3.0
    pdh_ok = prev_day_high is not None and ltp > prev_day_high and volume_ok
    gain_ok = change_pct is not None and change_pct >= 2.0 and volume_ok
    ath_ok = ath_distance is not None and ath_distance <= 3.0

    signals = []
    if pdh_ok:
        signals.append('PDH BREAKOUT')
    if gain_ok:
        signals.append('+2% MOMENTUM')
    if ath_ok:
        signals.append('NEAR 5Y ATH')
    if not signals:
        return None

    return {
        'symbol': symbol.replace('NSE:', '').replace('-EQ', ''),
        'ltp': ltp,
        'change_pct': change_pct,
        'volume': day_volume,
        'volume_ratio': volume_ratio,
        'avg_same_time_volume': avg_same_time,
        'volume_sessions': sessions,
        'prev_day_high': prev_day_high,
        'ath': ath,
        'ath_distance_pct': ath_distance,
        'signals': signals,
        'signal_count': len(signals),
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
    return render_template('index.html')


@app.route('/connect-fyers')
def connect_fyers():
    if not APP_ID or not APP_SECRET or not REDIRECT_URI:
        return 'Set FYERS_APP_ID, FYERS_APP_SECRET and FYERS_REDIRECT_URI in server environment.', 400
    state = secrets.token_urlsafe(24)
    session['fyers_oauth_state'] = state
    params = {'client_id': APP_ID, 'redirect_uri': REDIRECT_URI, 'response_type': 'code', 'state': state}
    return redirect('https://api-t1.fyers.in/api/v3/generate-authcode?' + urlencode(params))


@app.route('/callback')
def callback():
    global TOKEN
    code = request.args.get('auth_code')
    state = request.args.get('state')
    expected = session.pop('fyers_oauth_state', None)
    if not code or not expected or not secrets.compare_digest(state or '', expected):
        return 'FYERS login failed: missing auth_code or invalid state. Return to the private dashboard and reconnect.', 400
    app_hash = hashlib.sha256(f'{APP_ID}:{APP_SECRET}'.encode()).hexdigest()
    response = requests.post(API_ROOT + '/api/v3/validate-authcode', json={
        'grant_type': 'authorization_code', 'appIdHash': app_hash, 'code': code
    }, timeout=25)
    data = response.json()
    if data.get('s') != 'ok' or not data.get('access_token'):
        return f"Token exchange failed: {data.get('message', 'Unknown error')}", 400
    TOKEN = data['access_token']
    cache['connected'] = True
    return redirect(url_for('home'))


@app.route('/api/status')
def status():
    with lock:
        return jsonify({
            'connected': bool(APP_ID and TOKEN),
            'symbols_loaded': len(cache['symbols']),
            'last_update': cache['last_scan'],
            'error': cache['last_error'],
            'history_done': cache['history_done'],
            'history_total': cache['history_total'],
            'history_running': cache['history_running'],
            'history_message': cache['history_message'],
            'quote_count': cache['last_quote_count'],
        })


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
        if not cache['symbols']:
            cache['symbols'] = get_symbol_universe()
        symbols = cache['symbols']
        if not TOKEN:
            return jsonify({'error': 'Connect FYERS to start the live scan.', 'rows': [], 'universe': len(symbols)}), 400
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
        return jsonify({
            'rows': rows,
            'coverage': len(quotes),
            'universe': len(symbols),
            'matched': len(rows),
            'pd_breakouts': sum('PDH BREAKOUT' in row['signals'] for row in rows),
            'momentum': sum('+2% MOMENTUM' in row['signals'] for row in rows),
            'near_ath': sum('NEAR 5Y ATH' in row['signals'] for row in rows),
            'history_ready': ready,
            'history_running': history_running,
            'updated_at': now.isoformat(timespec='seconds'),
            'market_time': now.strftime('%H:%M:%S IST'),
        })
    except Exception as exc:
        cache['last_error'] = str(exc)
        return jsonify({'error': str(exc), 'rows': []}), 400


@app.route('/manifest.webmanifest')
def manifest():
    return jsonify({
        'name': 'F&O Live Screener', 'short_name': 'F&O Screener', 'start_url': '/',
        'display': 'standalone', 'background_color': '#0b1018', 'theme_color': '#0b1018',
        'icons': [{'src': '/static/icon.svg', 'sizes': 'any', 'type': 'image/svg+xml', 'purpose': 'any'}]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '8000')), debug=False)
