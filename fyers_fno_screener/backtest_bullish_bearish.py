"""Independent Bullish & Bearish Backtesting Strategy.

Broker-independent and read-only: consumes existing backtest_candles SQLite rows.
Does not import or modify the live scanner. All timestamps are Asia/Kolkata.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
CAPITAL_PER_TRADE = 250_000.0
BROKERAGE_PER_TRADE = 137.0
EXIT_CUTOFF = time(14, 55)
FIRST_CANDLE = time(9, 15)
CANDLE_920 = time(9, 20)
CANDLE_925 = time(9, 25)


def local_time(candle):
    return datetime.fromtimestamp(int(candle[0]), IST)


def valid_candles(raw):
    try:
        rows = json.loads(raw) if isinstance(raw, str) else raw
        result = []
        for c in rows or []:
            if not isinstance(c, (list, tuple)) or len(c) < 6:
                continue
            try:
                values = [float(c[i]) for i in range(1, 6)]
                if all(v == v and abs(v) != float("inf") for v in values):
                    result.append(list(c))
            except (TypeError, ValueError, OverflowError):
                continue
        return sorted(result, key=lambda c: int(c[0]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []


def _slot(rows, wanted):
    return next((c for c in rows if local_time(c).time() == wanted), None)


def _trend_for_day(data, day):
    bullish = bearish = valid = 0
    for symbol, item in data.items():
        first = _slot(item['5'].get(day, []), FIRST_CANDLE)
        prior_days = [d for d in item['D'] if d < day]
        if first is None or not prior_days:
            continue
        previous = item['D'][max(prior_days)]
        previous_close = float(previous[-1][4])
        if previous_close <= 0:
            continue
        pct = (float(first[4]) / previous_close - 1.0) * 100.0
        valid += 1
        if pct > 0.25: bullish += 1
        elif pct < -0.25: bearish += 1
    if not valid: return 'Unavailable'
    score = (bullish - bearish) / valid
    return 'Bullish' if score >= 0.10 else 'Bearish' if score <= -0.10 else 'Neutral'


def _daily_levels(daily_by_day, day):
    dates = sorted(d for d in daily_by_day if d < day)
    if not dates:
        return None
    previous = daily_by_day[dates[-1]]
    current_date = datetime.fromisoformat(day).date()
    trailing = [daily_by_day[d] for d in dates
                if (current_date - datetime.fromisoformat(d).date()).days <= 366]
    if not trailing:
        return None
    return {
        "pdh": float(previous[2]),
        "pdl": float(previous[3]),
        "week52_high": max(float(c[2]) for c in trailing),
        "week52_low": min(float(c[3]) for c in trailing),
        "ath": max(float(daily_by_day[d][2]) for d in dates),
        "atl": min(float(daily_by_day[d][3]) for d in dates),
    }


def _prior_slot_average(sessions, ordered_dates, day_index, slot_time):
    prior = []
    for old_day in ordered_dates[max(0, day_index - 20):day_index]:
        candle = _slot(sessions[old_day], slot_time)
        if candle is not None:
            prior.append(float(candle[5]))
    if len(prior) < 20:
        return None
    average = sum(prior) / len(prior)
    return average if average > 0 else None


def _make_trade(symbol, day, direction, entry, stop, trigger, entry_time,
                exit_price, exit_time, reason, qty, volume_multiple, levels):
    sign = 1 if direction == "BULLISH" else -1
    gross = (exit_price - entry) * qty * sign
    return {
        "date": day, "symbol": symbol, "direction": direction,
        "entry_time": entry_time, "entry": round(entry, 2),
        "stop": round(stop, 2), "trigger": round(trigger, 2),
        "exit_time": exit_time, "exit": round(exit_price, 2),
        "qty": qty, "exit_reason": reason,
        "gross_pnl": round(gross, 2),
        "brokerage": BROKERAGE_PER_TRADE,
        "net_pnl": round(gross - BROKERAGE_PER_TRADE, 2),
        "first_5m_volume_multiple": round(volume_multiple, 2),
        "pdh": round(levels["pdh"], 2), "pdl": round(levels["pdl"], 2),
        "week52_high": round(levels["week52_high"], 2),
        "week52_low": round(levels["week52_low"], 2),
        "ath": round(levels["ath"], 2), "atl": round(levels["atl"], 2),
    }


def _simulate_one(symbol, day, candles, direction, levels, volume_multiple):
    by_time = {local_time(c).time(): c for c in candles}
    c920, c925 = by_time.get(CANDLE_920), by_time.get(CANDLE_925)
    if c920 is None or c925 is None:
        return None, "missing_0920_or_0925"

    first = by_time.get(FIRST_CANDLE)
    if first is None:
        return None, "missing_first_5m"

    # First candle range must be strictly below 6% of its open.
    first_open = float(first[1])
    if first_open <= 0 or (float(first[2]) - float(first[3])) / first_open >= 0.06:
        return None, "first_candle_range_6pct_or_more"

    if direction == "BULLISH":
        if not (float(first[2]) >= levels["ath"] or
                float(first[2]) >= levels["week52_high"]):
            return None, "not_at_ath_or_52week_high"
        c920_high, c925_high = float(c920[2]), float(c925[2])
        if c925_high > c920_high:
            trigger, stop, entry_candle = c925_high, float(c925[3]), c925
        else:
            trigger, stop, entry_candle = c920_high, float(c925[3]), c925
        crossed = False
        for c in candles:
            t = local_time(c).time()
            if t <= CANDLE_925 or t >= EXIT_CUTOFF:
                continue
            if float(c[2]) >= trigger:
                crossed = True
                entry = max(trigger, float(c[1]))
                entry_time = local_time(c).strftime("%H:%M")
                break
        if not crossed:
            return None, "no_entry_trigger"
        risk_pct = (entry - stop) / entry
    else:
        if not (float(first[3]) <= levels["atl"] or
                float(first[3]) <= levels["week52_low"]):
            return None, "not_at_atl_or_52week_low"
        c920_low, c925_low = float(c920[3]), float(c925[3])
        if c925_low < c920_low:
            trigger, stop = c925_low, float(c925[2])
        else:
            trigger, stop = c920_low, float(c925[2])
        crossed = False
        for c in candles:
            t = local_time(c).time()
            if t <= CANDLE_925 or t >= EXIT_CUTOFF:
                continue
            if float(c[3]) <= trigger:
                crossed = True
                entry = min(trigger, float(c[1]))
                entry_time = local_time(c).strftime("%H:%M")
                break
        if not crossed:
            return None, "no_entry_trigger"
        risk_pct = (stop - entry) / entry

    if risk_pct <= 0 or risk_pct > 0.02:
        return None, "invalid_or_over_2pct_stop"
    qty = int(CAPITAL_PER_TRADE // entry)
    if qty < 1:
        return None, "quantity_zero"

    exit_price = exit_time = reason = None
    entered = False
    for c in candles:
        t = local_time(c).time()
        if t <= CANDLE_925:
            continue
        if t >= EXIT_CUTOFF:
            exit_price, exit_time, reason = float(c[4]), local_time(c).strftime("%H:%M"), "3:00 PM"
            break
        # Intrabar stop sequencing is unknowable; conservatively assume a stop
        # touch after entry only on candles later than the trigger candle.
        if local_time(c).strftime("%H:%M") <= entry_time:
            continue
        if direction == "BULLISH" and float(c[3]) <= stop:
            exit_price, exit_time, reason = min(stop, float(c[1])), local_time(c).strftime("%H:%M"), "Stop loss"
            break
        if direction == "BEARISH" and float(c[2]) >= stop:
            exit_price, exit_time, reason = max(stop, float(c[1])), local_time(c).strftime("%H:%M"), "Stop loss"
            break
    if exit_price is None:
        eligible = [c for c in candles if local_time(c).time() <= EXIT_CUTOFF]
        if not eligible:
            return None, "missing_3pm_exit_candle"
        last = eligible[-1]
        exit_price, exit_time, reason = float(last[4]), local_time(last).strftime("%H:%M"), "Last close at/before 3:00 PM"

    return _make_trade(symbol, day, direction, entry, stop, trigger, entry_time,
                       exit_price, exit_time, reason, qty, volume_multiple, levels), None


def run_backtest(connection: sqlite3.Connection, nifty_symbol: str = "NIFTY 50"):
    """Read-only calculation over existing backtest_candles using reconstructed Oldpal breadth."""
    connection.row_factory = sqlite3.Row
    symbols = [r[0] for r in connection.execute(
        "SELECT DISTINCT symbol FROM backtest_candles WHERE timeframe IN ('5','5m') ORDER BY symbol"
    )]
    data = {}
    for symbol in symbols:
        rows = connection.execute(
            "SELECT session_date,timeframe,candles_json FROM backtest_candles WHERE symbol=? ORDER BY session_date",
            (symbol,)
        ).fetchall()
        data[symbol] = {"5": {}, "D": {}}
        for row in rows:
            tf = "5" if row["timeframe"] in ("5", "5m") else "D"
            parsed = valid_candles(row["candles_json"])
            if parsed:
                data[symbol][tf][row["session_date"]] = parsed



    trades, excluded = [], Counter()
    for symbol, item in data.items():
        if symbol == index_key:
            continue
        sessions, daily = item["5"], item["D"]
        dates = sorted(sessions)
        for i, day in enumerate(dates):
            trend = _trend_for_day(data, day)
            if trend not in ("Bullish", "Bearish"):
                excluded["oldpal_sentiment_not_directional_or_missing"] += 1
                continue
            levels = _daily_levels({d: cs[-1] for d, cs in daily.items()}, day)
            if not levels:
                excluded["missing_daily_levels"] += 1
                continue
            candles = sessions[day]
            first = _slot(candles, FIRST_CANDLE)
            if first is None:
                excluded["missing_first_5m"] += 1
                continue
            avg = _prior_slot_average(sessions, dates, i, FIRST_CANDLE)
            if avg is None:
                excluded["insufficient_20_first_slot_history"] += 1
                continue
            multiple = float(first[5]) / avg
            if multiple < 3.0:
                excluded["first_5m_volume_below_3x"] += 1
                continue
            direction = "BULLISH" if trend == "Bullish" else "BEARISH"
            trade, reason = _simulate_one(symbol.split(":")[-1].replace("-EQ", ""),
                                          day, candles, direction, levels, multiple)
            if reason:
                excluded[reason] += 1
            elif trade:
                trades.append(trade)

    trades.sort(key=lambda t: (t["date"], t["entry_time"], t["symbol"]))
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    gross = sum(t["gross_pnl"] for t in trades)
    net = sum(t["net_pnl"] for t in trades)
    gross_profit = sum(t["gross_pnl"] for t in trades if t["gross_pnl"] > 0)
    gross_loss = abs(sum(t["gross_pnl"] for t in trades if t["gross_pnl"] < 0))
    equity = peak = max_dd = 0.0
    for t in trades:
        equity += t["net_pnl"]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    summary = {
        "strategy": "Bullish & Bearish Backtesting Strategy",
        "total_trades": len(trades), "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate_pct": round(len(wins) / len(trades) * 100, 2) if trades else 0,
        "gross_pnl": round(gross, 2),
        "brokerage_total": round(len(trades) * BROKERAGE_PER_TRADE, 2),
        "net_pnl": round(net, 2),
        "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss else None,
        "max_drawdown": round(max_dd, 2),
        "avg_net_per_trade": round(net / len(trades), 2) if trades else 0,
        "capital_per_trade": CAPITAL_PER_TRADE,
        "brokerage_per_trade": BROKERAGE_PER_TRADE,
        "exit_rule": "Exit at stop or 3:00 PM (14:55 candle close)",
        "excluded_candidates": dict(excluded),
        "data_note": "Historical Oldpal breadth reconstructed from available F&O symbols using live thresholds (>+0.25% bullish, <-0.25% bearish; breadth score +/-0.10). ATH/ATL and 52-week levels use prior daily candles only. IVF uses first 09:15 candle volume versus the same slot average over 20 prior sessions. Entry is approximated from 5-minute OHLC; intrabar order is unknown. 14:55 candle close represents 3:00 PM. ₹137 flat brokerage estimate; slippage and statutory charges excluded. Historical F&O universe coverage depends on symbols stored."
    }
    return {"status": "complete", "summary": summary, "trades": trades}
