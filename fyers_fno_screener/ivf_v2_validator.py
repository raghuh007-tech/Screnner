"""Independent, read-only validator for the saved IVF V2 backtest.

This module deliberately does not import the Flask application or call its
strategy calculation. It reads the existing SQLite candle/result tables only.
"""
from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
CAPITAL_PER_TRADE = 250000.0
BROKERAGE_PER_TRADE = 137.0
FIELDS = ("entry", "stop", "exit", "qty", "entry_time", "exit_time",
          "exit_reason", "gross_pnl", "brokerage", "net_pnl", "ivf_multiple", "pdh")


def _local_time(candle):
    return datetime.fromtimestamp(int(candle[0]), IST)


def _read_candles(raw, issues, symbol, timeframe, day):
    try:
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise ValueError("candle payload is not a list")
        valid = []
        for item in payload:
            if not isinstance(item, list) or len(item) < 6:
                issues.append({"symbol": symbol, "date": day, "timeframe": timeframe,
                               "issue": "malformed candle row"})
                continue
            try:
                int(item[0])
                values = [float(item[i]) for i in range(1, 6)]
                if not all(math.isfinite(v) for v in values):
                    raise ValueError("non-finite OHLCV")
                if values[4] < 0:
                    raise ValueError("negative volume")
                valid.append(item)
            except (TypeError, ValueError, OverflowError) as exc:
                issues.append({"symbol": symbol, "date": day, "timeframe": timeframe,
                               "issue": str(exc)})
        return sorted(valid, key=lambda row: int(row[0]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        issues.append({"symbol": symbol, "date": day, "timeframe": timeframe,
                       "issue": "unreadable JSON: " + str(exc)})
        return []


def _summary(trades):
    wins = [t for t in trades if t["net_pnl"] > 0]
    gross = sum(t["gross_pnl"] for t in trades)
    net = sum(t["net_pnl"] for t in trades)
    positive = sum(t["gross_pnl"] for t in trades if t["gross_pnl"] > 0)
    negative = abs(sum(t["gross_pnl"] for t in trades if t["gross_pnl"] < 0))
    equity = peak = drawdown = 0.0
    for trade in sorted(trades, key=lambda t: (t["date"], t["entry_time"], t["symbol"])):
        equity += trade["net_pnl"]
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    count = len(trades)
    return {
        "total_trades": count,
        "winning_trades": len(wins),
        "losing_trades": count - len(wins),
        "win_rate_pct": round(len(wins) / count * 100, 2) if count else 0,
        "gross_pnl": round(gross, 2),
        "brokerage_total": round(count * BROKERAGE_PER_TRADE, 2),
        "net_pnl": round(net, 2),
        "profit_factor": round(positive / negative, 2) if negative else None,
        "max_drawdown": round(drawdown, 2),
        "avg_net_per_trade": round(net / count, 2) if count else 0,
    }


def validate_ivf_v2(connection: sqlite3.Connection, saved_trades, saved_summary):
    """Rebuild candidates independently and compare them with the stored trades."""
    issues = []
    excluded = Counter()
    rebuilt = []
    symbols = [row[0] for row in connection.execute(
        "SELECT DISTINCT symbol FROM backtest_candles WHERE timeframe='5' ORDER BY symbol"
    )]

    for raw_symbol in symbols:
        five_rows = connection.execute(
            "SELECT session_date, candles_json FROM backtest_candles "
            "WHERE symbol=? AND timeframe='5' ORDER BY session_date", (raw_symbol,)
        ).fetchall()
        daily_rows = connection.execute(
            "SELECT session_date, candles_json FROM backtest_candles "
            "WHERE symbol=? AND timeframe='D' ORDER BY session_date", (raw_symbol,)
        ).fetchall()

        daily_close_rows = {}
        for day, raw in daily_rows:
            parsed = _read_candles(raw, issues, raw_symbol, "D", day)
            if parsed:
                # Daily OHLC candle order is timestamp, open, high, low, close, volume.
                daily_close_rows[day] = parsed[-1]

        ordered_daily = sorted(daily_close_rows)
        previous_high = {
            ordered_daily[index]: float(daily_close_rows[ordered_daily[index - 1]][2])
            for index in range(1, len(ordered_daily))
        }

        sessions = {}
        for day, raw in five_rows:
            parsed = _read_candles(raw, issues, raw_symbol, "5", day)
            if parsed:
                sessions[day] = parsed

        ordered_sessions = sorted(sessions)
        for day_index, day in enumerate(ordered_sessions):
            if day not in previous_high:
                excluded["no_previous_daily_high"] += 1
                continue

            candles = sessions[day]
            slots = {}
            for candle in candles:
                stamp = _local_time(candle)
                slots[(stamp.hour, stamp.minute)] = candle
            candle_920 = slots.get((9, 20))
            candle_925 = slots.get((9, 25))
            if candle_920 is None or candle_925 is None:
                excluded["missing_0920_or_0925"] += 1
                continue

            # Strategy condition: the 09:25 high must exceed the 09:20 high.
            if float(candle_925[2]) <= float(candle_920[2]):
                excluded["no_0925_breakout"] += 1
                continue

            # Same clock-time volume slot across the preceding 20 available sessions.
            prior_slot_volumes = []
            for prior_day in ordered_sessions[max(0, day_index - 20):day_index]:
                prior_candle = next(
                    (row for row in sessions[prior_day]
                     if (_local_time(row).hour, _local_time(row).minute) == (9, 25)),
                    None
                )
                if prior_candle is not None:
                    prior_slot_volumes.append(float(prior_candle[5]))
            if len(prior_slot_volumes) < 20:
                excluded["insufficient_20_slot_history"] += 1
                continue
            average_volume = sum(prior_slot_volumes) / 20.0
            if average_volume <= 0 or float(candle_925[5]) < 3.0 * average_volume:
                excluded["ivf_below_3x"] += 1
                continue

            entry_price = max(float(candle_920[2]), float(candle_925[1]))
            pdh = previous_high[day]
            if entry_price <= pdh:
                excluded["pdh_not_broken_at_entry"] += 1
                continue

            stop_price = float(candle_925[3])
            stop_fraction = (entry_price - stop_price) / entry_price if entry_price > 0 else math.inf
            if stop_fraction <= 0 or stop_fraction > 0.02:
                excluded["invalid_or_over_2pct_stop"] += 1
                continue

            quantity = int(CAPITAL_PER_TRADE // entry_price)
            if quantity < 1:
                excluded["quantity_zero"] += 1
                continue

            exit_price = None
            exit_stamp = None
            exit_reason = "3:00 PM"
            candle_925_time = _local_time(candle_925)
            for candle in candles:
                stamp = _local_time(candle)
                if stamp <= candle_925_time:
                    continue
                if (stamp.hour, stamp.minute) >= (15, 0):
                    break
                if float(candle[3]) <= stop_price:
                    exit_price, exit_stamp, exit_reason = stop_price, stamp, "Stop loss"
                    break

            if exit_price is None:
                eod_candles = [
                    row for row in candles
                    if (_local_time(row).hour, _local_time(row).minute) <= (15, 0)
                ]
                if not eod_candles:
                    excluded["no_exit_candle"] += 1
                    continue
                final_candle = eod_candles[-1]
                exit_price = float(final_candle[4])
                exit_stamp = _local_time(final_candle)

            gross = (exit_price - entry_price) * quantity
            rebuilt.append({
                "date": day,
                "symbol": raw_symbol.split(":")[-1].replace("-EQ", ""),
                "entry_time": "09:25",
                "entry": round(entry_price, 2),
                "stop": round(stop_price, 2),
                "stop_pct": round(stop_fraction * 100, 2),
                "exit_time": exit_stamp.strftime("%H:%M"),
                "exit": round(exit_price, 2),
                "qty": quantity,
                "exit_reason": exit_reason,
                "gross_pnl": round(gross, 2),
                "brokerage": BROKERAGE_PER_TRADE,
                "net_pnl": round(gross - BROKERAGE_PER_TRADE, 2),
                "ivf_multiple": round(float(candle_925[5]) / average_volume, 2),
                "pdh": round(pdh, 2),
            })

    rebuilt.sort(key=lambda item: (item["date"], item["entry_time"], item["symbol"]))
    saved = sorted(saved_trades or [], key=lambda item: (
        str(item.get("date", "")), str(item.get("entry_time", "")), str(item.get("symbol", ""))
    ))
    rebuilt_by_key = {(t["date"], t["symbol"]): t for t in rebuilt}
    saved_by_key = {(str(t.get("date", "")), str(t.get("symbol", ""))): t for t in saved}

    duplicate_saved = [key for key, count in Counter(
        (str(t.get("date", "")), str(t.get("symbol", ""))) for t in saved
    ).items() if count > 1]
    duplicate_rebuilt = [key for key, count in Counter(
        (t["date"], t["symbol"]) for t in rebuilt
    ).items() if count > 1]

    missing = sorted(set(rebuilt_by_key) - set(saved_by_key))
    unexpected = sorted(set(saved_by_key) - set(rebuilt_by_key))
    mismatches = []
    for key in sorted(set(rebuilt_by_key) & set(saved_by_key)):
        differences = {}
        for field in FIELDS:
            expected = rebuilt_by_key[key].get(field)
            actual = saved_by_key[key].get(field)
            if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
                if not math.isclose(float(expected), float(actual), rel_tol=0, abs_tol=0.011):
                    differences[field] = {"rebuilt": expected, "saved": actual}
            elif expected != actual:
                differences[field] = {"rebuilt": expected, "saved": actual}
        if differences:
            mismatches.append({"date": key[0], "symbol": key[1], "differences": differences})

    rebuilt_summary = _summary(rebuilt)
    summary_differences = {
        field: {"rebuilt": rebuilt_summary.get(field), "saved": (saved_summary or {}).get(field)}
        for field in rebuilt_summary
        if rebuilt_summary.get(field) != (saved_summary or {}).get(field)
    }
    return {
        "status": "complete",
        "mode": "read_only_independent_recalculation",
        "saved_trade_count": len(saved),
        "rebuilt_trade_count": len(rebuilt),
        "matched_trade_count": len(set(rebuilt_by_key) & set(saved_by_key)) - len(mismatches),
        "mismatch_count": len(mismatches),
        "missing_saved_trades": [{"date": d, "symbol": s} for d, s in missing],
        "unexpected_saved_trades": [{"date": d, "symbol": s} for d, s in unexpected],
        "mismatches": mismatches,
        "duplicate_saved_keys": [{"date": d, "symbol": s} for d, s in duplicate_saved],
        "duplicate_rebuilt_keys": [{"date": d, "symbol": s} for d, s in duplicate_rebuilt],
        "rebuilt_summary": rebuilt_summary,
        "saved_summary": saved_summary or {},
        "summary_differences": summary_differences,
        "excluded_candidates": dict(excluded),
        "data_quality_issue_count": len(issues),
        "data_quality_issues": issues[:200],
        "rule_notes": [
            "PDH is tested against the calculated entry price, matching the published IVF V2 implementation.",
            "The 09:25 candle high must exceed the 09:20 candle high.",
            "IVF is 09:25 volume divided by the mean 09:25 volume over the prior 20 available intraday sessions; missing slots are skipped, but at least 20 observations are required.",
            "Stop checks begin after the 09:25 candle because 5-minute OHLCV cannot reveal intrabar event order.",
            "A trade exits at the stop or at the close of the last candle timestamped no later than 15:00.",
            "Quantity uses ₹2,50,000 notional and brokerage is ₹137 per trade; slippage and statutory charges are excluded."
        ],
    }
