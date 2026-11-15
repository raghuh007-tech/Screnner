"""Deterministic, broker-independent backtest primitives for Oldpal Strategy V1.

This module intentionally has no broker calls and does not alter the live scanner.
All timestamps are Unix seconds interpreted in Asia/Kolkata.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import Iterable

IST = ZoneInfo("Asia/Kolkata")
ENTRY_CANDLE_TIME = time(9, 15)  # first 5-minute candle, 09:15–09:20
FORCE_EXIT_TIME = time(15, 30)  # use the final 15:25 candle close for an end-of-day exit
CAPITAL_PER_TRADE = 250_000.0


@dataclass
class TradeResult:
    symbol: str
    trade_date: str
    status: str
    entry_time: str | None = None
    entry_price: float | None = None
    exit_time: str | None = None
    exit_price: float | None = None
    stop_price: float | None = None
    trigger_price: float | None = None
    quantity: int = 0
    gross_pnl: float = 0.0
    risk_per_share: float | None = None
    risk_reward_realized: float | None = None
    reason: str = ""
    ambiguous: bool = False

    def to_dict(self):
        return asdict(self)


def _local_dt(candle):
    return datetime.fromtimestamp(int(candle[0]), IST)


def _valid_candle(c):
    try:
        return len(c) >= 6 and all(float(c[i]) == float(c[i]) for i in range(1, 6))
    except (TypeError, ValueError, IndexError):
        return False


def simulate_intraday_trade(symbol: str, trade_date: str, candles: Iterable,
                            capital: float = CAPITAL_PER_TRADE) -> TradeResult:
    """Enter on the first break of the 09:15–09:20 candle high; stop at its low.

    Candles are [epoch, open, high, low, close, volume], sorted or unsorted.
    Entry is only considered from 09:20 onward, after the first candle closes.
    For a 5-minute candle crossing both entry and stop, sequence is unknowable;
    return AMBIGUOUS rather than assume a favourable path.
    A stop gap is filled at candle open; a breakout gap is filled at candle open.
    Open positions exit at the close of the final available 15:25 candle.
    """
    day = []
    for c in candles or []:
        if not _valid_candle(c):
            continue
        dt = _local_dt(c)
        if dt.date().isoformat() == trade_date:
            day.append((dt, c))
    day.sort(key=lambda x: x[0])

    first = next(((dt, c) for dt, c in day if dt.time().hour == 9 and dt.time().minute == 15), None)
    if not first:
        return TradeResult(symbol, trade_date, "NO_DATA", reason="Missing 09:15 five-minute candle")

    first_dt, first_candle = first
    trigger = float(first_candle[2])
    stop = float(first_candle[3])
    if trigger <= 0 or stop <= 0 or stop >= trigger:
        return TradeResult(symbol, trade_date, "INVALID_SETUP", trigger_price=trigger,
                           stop_price=stop, reason="Invalid first-candle high/low")

    entry = None
    entry_dt = None
    entry_qty = 0
    for dt, c in day:
        if dt <= first_dt or dt.time() < time(9, 20) or dt.time() >= FORCE_EXIT_TIME:
            continue
        o, h, l = float(c[1]), float(c[2]), float(c[3])
        if h >= trigger:
            # If candle's low also reaches the SL before we can establish order,
            # conservative policy is to mark sequence ambiguous.
            if l <= stop:
                return TradeResult(symbol, trade_date, "AMBIGUOUS", trigger_price=trigger,
                                   stop_price=stop, reason="Entry and stop both touched in same 5m candle",
                                   ambiguous=True)
            entry = max(trigger, o)  # adverse gap-through fill
            entry_dt = dt
            entry_qty = int(capital // entry)
            if entry_qty < 1:
                return TradeResult(symbol, trade_date, "NO_TRADE", trigger_price=trigger,
                                   stop_price=stop, reason="Capital insufficient for one share")
            break

    if entry is None:
        return TradeResult(symbol, trade_date, "NO_ENTRY", trigger_price=trigger,
                           stop_price=stop, reason="First-candle high never broken")

    exit_price = None
    exit_dt = None
    reason = ""
    for dt, c in day:
        if dt < entry_dt:
            continue
        if dt.time() >= FORCE_EXIT_TIME:
            exit_price = float(c[1]) if dt.time() == FORCE_EXIT_TIME else float(c[4])
            exit_dt = dt
            reason = "TIME_EXIT_15:00" if dt.time() == FORCE_EXIT_TIME else "LAST_AVAILABLE_AFTER_15:00"
            break
        o, h, l = float(c[1]), float(c[2]), float(c[3])
        # Entry candle itself is checked after the entry event; high/low order is
        # unknown, so any SL touch on that same candle is conservatively ambiguous.
        if dt == entry_dt and l <= stop:
            return TradeResult(symbol, trade_date, "AMBIGUOUS", entry_time=entry_dt.isoformat(),
                               entry_price=entry, stop_price=stop, trigger_price=trigger,
                               quantity=entry_qty, reason="Stop touched in entry candle; sequence unknown",
                               ambiguous=True)
        if l <= stop:
            exit_price = min(stop, o) if o < stop else stop
            exit_dt = dt
            reason = "STOP_LOSS"
            break

    if exit_price is None:
        later = [(dt, c) for dt, c in day if dt >= entry_dt]
        if not later:
            return TradeResult(symbol, trade_date, "OPEN_NO_EXIT_DATA", entry_time=entry_dt.isoformat(),
                               entry_price=entry, stop_price=stop, trigger_price=trigger,
                               quantity=entry_qty, reason="No candles after entry")
        dt, c = later[-1]
        exit_price, exit_dt, reason = float(c[4]), dt, "LAST_AVAILABLE_CLOSE"

    pnl = (exit_price - entry) * entry_qty
    risk = entry - stop
    return TradeResult(
        symbol, trade_date, "CLOSED", entry_dt.isoformat(), entry,
        exit_dt.isoformat(), exit_price, stop, trigger, entry_qty, pnl,
        risk, ((exit_price - entry) / risk if risk > 0 else None), reason
    )


def qualifies_for_strategy(price: float, week52_high: float | None, ltp: float,
                           pdh: float, volume_ratio: float,
                           daily_change_pct: float | None = None) -> tuple[bool, dict]:
    """Core setup is PDH +2% daily momentum + 3x volume; 52W proximity is informational."""
    try:
        gap_pct = (
            max(0.0, (float(week52_high) - float(price)) / float(week52_high) * 100)
            if week52_high is not None and float(week52_high) > 0 else None
        )
        checks = {
            "pdh_breakout": float(ltp) > float(pdh),
            "daily_change_2pct": daily_change_pct is not None and float(daily_change_pct) >= 2.0,
            "volume_3x": float(volume_ratio) >= 3.0,
            "within_5pct_52w_high": gap_pct is not None and gap_pct <= 5.0,
        }
        core_qualifies = checks["pdh_breakout"] and checks["daily_change_2pct"] and checks["volume_3x"]
        return core_qualifies, {**checks, "week52_gap_pct": gap_pct}
    except (TypeError, ValueError, ZeroDivisionError):
        return False, {"pdh_breakout": False, "daily_change_2pct": False,
                       "volume_3x": False, "within_5pct_52w_high": False,
                       "week52_gap_pct": None}



def summarize_trades(trades: Iterable[TradeResult], cost_per_trade: float = 0.0) -> dict:
    """Summarize closed trades. Cost is a configurable round-trip INR estimate.

    Use zero until the app supplies a verified, date-appropriate Indian cash
    brokerage/tax schedule. This avoids presenting guessed charges as fact.
    """
    rows = [t for t in trades if t.status == "CLOSED"]
    if not rows:
        return {
            "closed_trades": 0, "gross_pnl": 0.0, "estimated_costs": 0.0,
            "net_pnl": 0.0, "wins": 0, "losses": 0, "win_rate_pct": None,
            "profit_factor": None, "average_r": None, "max_drawdown": 0.0,
            "monthly": {}, "status_counts": _status_counts(trades),
        }
    wins = [t for t in rows if t.gross_pnl > 0]
    losses = [t for t in rows if t.gross_pnl < 0]
    gross = sum(t.gross_pnl for t in rows)
    costs = len(rows) * max(0.0, float(cost_per_trade))
    net_values = [t.gross_pnl - max(0.0, float(cost_per_trade)) for t in rows]
    equity = peak = drawdown = 0.0
    for value in net_values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    gross_profit = sum(t.gross_pnl for t in wins)
    gross_loss = abs(sum(t.gross_pnl for t in losses))
    monthly = {}
    for t in rows:
        key = t.trade_date[:7]
        monthly[key] = monthly.get(key, 0.0) + t.gross_pnl - max(0.0, float(cost_per_trade))
    r_values = [t.risk_reward_realized for t in rows if t.risk_reward_realized is not None]
    return {
        "closed_trades": len(rows), "gross_pnl": round(gross, 2),
        "estimated_costs": round(costs, 2), "net_pnl": round(gross - costs, 2),
        "wins": len(wins), "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(rows) * 100, 2),
        "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss else None,
        "average_r": round(sum(r_values) / len(r_values), 3) if r_values else None,
        "max_drawdown": round(drawdown, 2),
        "monthly": {k: round(v, 2) for k, v in sorted(monthly.items())},
        "status_counts": _status_counts(trades),
    }


def _status_counts(trades):
    counts = {}
    for trade in trades:
        counts[trade.status] = counts.get(trade.status, 0) + 1
    return counts
