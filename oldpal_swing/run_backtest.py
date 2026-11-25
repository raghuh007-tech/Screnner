#!/usr/bin/env python3
"""Oldpal Swing V1: weekly Supertrend entry, daily Supertrend exit."""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from kiteconnect import KiteConnect

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CACHE = DATA / "cache"
OUTPUT = ROOT / "output"
ATR_PERIOD = 10
MULTIPLIER = 3.0


def supertrend(df: pd.DataFrame, period: int = ATR_PERIOD, multiplier: float = MULTIPLIER) -> pd.DataFrame:
    """TradingView-style Supertrend using Wilder ATR and recursive final bands."""
    x = df.copy()
    prev_close = x["close"].shift(1)
    tr = pd.concat([
        x["high"] - x["low"],
        (x["high"] - prev_close).abs(),
        (x["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    # Wilder's moving average; min_periods prevents premature signals.
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    hl2 = (x["high"] + x["low"]) / 2
    basic_upper = hl2 + multiplier * atr
    basic_lower = hl2 - multiplier * atr
    final_upper = pd.Series(index=x.index, dtype=float)
    final_lower = pd.Series(index=x.index, dtype=float)
    trend = pd.Series(index=x.index, dtype=bool)

    for i in range(len(x)):
        if pd.isna(atr.iloc[i]):
            continue
        if i == 0 or pd.isna(atr.iloc[i - 1]):
            final_upper.iloc[i] = basic_upper.iloc[i]
            final_lower.iloc[i] = basic_lower.iloc[i]
            trend.iloc[i] = True
            continue
        pu, pl, pc = final_upper.iloc[i - 1], final_lower.iloc[i - 1], x["close"].iloc[i - 1]
        bu, bl = basic_upper.iloc[i], basic_lower.iloc[i]
        final_upper.iloc[i] = bu if (bu < pu or pc > pu) else pu
        final_lower.iloc[i] = bl if (bl > pl or pc < pl) else pl
        if trend.iloc[i - 1]:
            trend.iloc[i] = not (x["close"].iloc[i] < final_lower.iloc[i])
        else:
            trend.iloc[i] = x["close"].iloc[i] > final_upper.iloc[i]

    x["atr"] = atr
    x["st_upper"] = final_upper
    x["st_lower"] = final_lower
    x["bullish"] = trend.astype("boolean")
    x["supertrend"] = np.where(x["bullish"].fillna(False), final_lower, final_upper)
    x["flip_bull"] = x["bullish"].fillna(False) & ~x["bullish"].shift(1).fillna(False)
    x["flip_bear"] = ~x["bullish"].fillna(True) & x["bullish"].shift(1).fillna(False)
    return x


def read_membership(path: Path, start: date, end: date) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Supply historical Nifty 500 membership intervals; "
            "today's constituents are not a valid substitute."
        )
    m = pd.read_csv(path, dtype={"tradingsymbol": str, "instrument_token": str})
    required = {"tradingsymbol", "instrument_token", "valid_from", "valid_to"}
    if not required.issubset(m.columns):
        raise ValueError(f"Membership CSV needs columns: {sorted(required)}")
    m["valid_from"] = pd.to_datetime(m["valid_from"], errors="coerce")
    m["valid_to"] = pd.to_datetime(m["valid_to"], errors="coerce")
    if m[["tradingsymbol", "instrument_token", "valid_from"]].isna().any().any():
        raise ValueError("Membership file contains blank symbol/token/valid_from or invalid dates.")
    m["valid_to"] = m["valid_to"].fillna(pd.Timestamp(end))
    m = m[(m["valid_from"] <= pd.Timestamp(end)) & (m["valid_to"] >= pd.Timestamp(start))]
    m["instrument_token"] = m["instrument_token"].astype(str)
    return m.sort_values(["tradingsymbol", "valid_from"])


def fetch_daily(kite: KiteConnect, token: str, start: date, end: date) -> pd.DataFrame:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{token}_{start}_{end}.csv"
    if path.exists():
        d = pd.read_csv(path, parse_dates=["date"])
        return d.set_index("date").sort_index()
    chunks = []
    cursor = start
    # Kite's historical endpoint has request-window limits. Keep requests well below them.
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=1500), end)
        rows = kite.historical_data(
            instrument_token=int(token),
            from_date=datetime.combine(cursor, datetime.min.time()),
            to_date=datetime.combine(chunk_end, datetime.max.time()),
            interval="day",
            continuous=False,
            oi=False,
        )
        if rows:
            part = pd.DataFrame(rows)
            part["date"] = pd.to_datetime(part["date"]).dt.tz_localize(None).dt.normalize()
            chunks.append(part)
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)
    if not chunks:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"]).rename_axis("date")
    d = pd.concat(chunks).drop_duplicates("date").set_index("date").sort_index()
    d = d[["open", "high", "low", "close", "volume"]].astype(float)
    d.to_csv(path, index_label="date")
    return d


def weekly_bars(daily: pd.DataFrame) -> pd.DataFrame:
    # Friday-ending weekly bars; holiday-shortened weeks naturally end on the last session.
    return daily.resample("W-FRI", label="right", closed="right").agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum"
    }).dropna(subset=["open", "high", "low", "close"])


def generate_trades(symbol: str, membership: pd.DataFrame, daily_all: pd.DataFrame,
                    cost_side_pct: float, slippage_side_pct: float):
    d = daily_all.copy().sort_index()
    d = d[~d.index.duplicated(keep="last")]
    # Use daily history for indicators, but only permit entries/exits during membership.
    d = supertrend(d)
    w = supertrend(weekly_bars(daily_all))
    # Only completed weekly bars: weekly labels after the final daily session are not used.
    # A weekly signal on week W is executed at the first daily open after W's last session.
    entries = []
    for stamp, row in w[w["flip_bull"].fillna(False)].iterrows():
        prior_sessions = d.index[d.index <= stamp]
        if len(prior_sessions) == 0:
            continue
        signal_session = prior_sessions[-1]
        next_sessions = d.index[d.index > signal_session]
        if len(next_sessions) == 0:
            continue
        entry_day = next_sessions[0]
        if not ((membership["valid_from"].iloc[0] <= entry_day) and
                (membership["valid_to"].iloc[0] >= entry_day)):
            continue
        entries.append((signal_session, entry_day, float(d.loc[entry_day, "open"])))

    trades = []
    used_entry_days = set()
    for signal_day, entry_day, entry_price in entries:
        if entry_day in used_entry_days:
            continue
        used_entry_days.add(entry_day)
        after_entry = d.index[d.index > entry_day]
        exit_day = None
        exit_signal_day = None
        for day in after_entry:
            if bool(d.loc[day, "flip_bear"]):
                future = d.index[d.index > day]
                if len(future):
                    exit_signal_day = day
                    exit_day = future[0]
                break
        # Open position is marked to final close for reporting; not a completed trade.
        if exit_day is None:
            if len(d.index) == 0:
                continue
            exit_day = d.index[-1]
            exit_price = float(d.loc[exit_day, "close"])
            status = "OPEN_MARKED_TO_CLOSE"
        else:
            exit_price = float(d.loc[exit_day, "open"])
            status = "CLOSED"
        if exit_day < entry_day:
            continue
        gross = (exit_price / entry_price - 1) * 100
        round_trip_cost = 2 * (cost_side_pct + slippage_side_pct)
        net = gross - round_trip_cost
        trades.append({
            "symbol": symbol, "entry_signal_week_end": signal_day.date(),
            "entry_date": entry_day.date(), "entry_price": entry_price,
            "exit_signal_date": exit_signal_day.date() if exit_signal_day is not None else None,
            "exit_date": exit_day.date(), "exit_price": exit_price,
            "holding_calendar_days": (exit_day - entry_day).days,
            "gross_return_pct": gross, "estimated_round_trip_cost_pct": round_trip_cost,
            "net_return_pct": net, "status": status,
        })
    return trades


def main():
    parser = argparse.ArgumentParser(description="Oldpal Swing V1 historical backtest")
    parser.add_argument("--from", dest="start", default="2016-01-01")
    parser.add_argument("--to", dest="end", default=date.today().isoformat())
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if start >= end:
        parser.error("--from must be earlier than --to")

    load_dotenv(ROOT / ".env")
    api_key = os.getenv("KITE_API_KEY", "").strip()
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip()
    if not api_key or not access_token:
        sys.exit("Set KITE_API_KEY and KITE_ACCESS_TOKEN in oldpal_swing/.env (never share them in chat).")

    membership = read_membership(DATA / "universe_membership.csv", start, end)
    if membership.empty:
        sys.exit("No historical membership rows overlap the requested period.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    cost = float(os.getenv("COST_PER_SIDE_PCT", "0"))
    slip = float(os.getenv("SLIPPAGE_PER_SIDE_PCT", "0"))
    all_trades, coverage = [], []

    for _, row in membership.iterrows():
        symbol, token = row["tradingsymbol"], row["instrument_token"]
        lo = max(start, row["valid_from"].date())
        hi = min(end, row["valid_to"].date())
        if lo >= hi:
            continue
        try:
            daily = fetch_daily(kite, token, lo - timedelta(days=60), hi)
            if daily.empty or len(daily) < 30:
                coverage.append({"symbol": symbol, "token": token, "status": "INSUFFICIENT_DATA", "rows": len(daily)})
                continue
            # Restrict execution window to membership interval; retain pre-window history for ATR warm-up.
            exec_daily = daily[(daily.index.date >= lo) & (daily.index.date <= hi)]
            if exec_daily.empty:
                coverage.append({"symbol": symbol, "token": token, "status": "NO_EXECUTION_BARS", "rows": len(daily)})
                continue
            # Pass complete daily history for indicator warm-up; membership interval controls entries.
            interval = pd.DataFrame([{
                "valid_from": pd.Timestamp(lo), "valid_to": pd.Timestamp(hi)
            }])
            trades = generate_trades(symbol, interval, daily, cost, slip)
            # Keep trades whose entry falls within this specific membership interval.
            trades = [t for t in trades if lo <= t["entry_date"] <= hi]
            all_trades.extend(trades)
            coverage.append({"symbol": symbol, "token": token, "status": "OK", "rows": len(daily), "trades": len(trades)})
        except Exception as exc:
            coverage.append({"symbol": symbol, "token": token, "status": f"ERROR: {type(exc).__name__}: {exc}", "rows": 0})

    if not all_trades:
        print("No trades generated. Check historical membership/token mappings and API data coverage.")
    trades_df = pd.DataFrame(all_trades)
    coverage_df = pd.DataFrame(coverage)
    OUTPUT.mkdir(parents=True, exist_ok=True)

    if not trades_df.empty:
        closed = trades_df[trades_df["status"] == "CLOSED"].copy()
        returns = closed["net_return_pct"]
        summary = pd.DataFrame([
            ["Total trades", len(trades_df)],
            ["Completed trades", len(closed)],
            ["Open positions marked to close", int((trades_df["status"] != "CLOSED").sum())],
            ["Win rate (completed, net)", float((returns > 0).mean() * 100) if len(returns) else np.nan],
            ["Average trade return % (completed, net)", float(returns.mean()) if len(returns) else np.nan],
            ["Median trade return % (completed, net)", float(returns.median()) if len(returns) else np.nan],
            ["Best trade % (completed, net)", float(returns.max()) if len(returns) else np.nan],
            ["Worst trade % (completed, net)", float(returns.min()) if len(returns) else np.nan],
            ["Average holding days (completed)", float(closed["holding_calendar_days"].mean()) if len(closed) else np.nan],
            ["Cost per side %", cost], ["Slippage per side %", slip],
        ], columns=["metric", "value"])
        trades_df["entry_year"] = pd.to_datetime(trades_df["entry_date"]).dt.year
        yearly = trades_df.groupby("entry_year").agg(
            trades=("symbol", "count"),
            avg_net_trade_return_pct=("net_return_pct", "mean"),
            median_net_trade_return_pct=("net_return_pct", "median"),
            win_rate_pct=("net_return_pct", lambda s: (s > 0).mean() * 100),
        ).reset_index()
    else:
        summary = pd.DataFrame(columns=["metric", "value"])
        yearly = pd.DataFrame()

    trades_df.to_csv(OUTPUT / "oldpal_swing_trades.csv", index=False)
    with pd.ExcelWriter(OUTPUT / "oldpal_swing_trades.xlsx", engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        trades_df.to_excel(writer, sheet_name="Trades", index=False)
        yearly.to_excel(writer, sheet_name="Yearly", index=False)
        coverage_df.to_excel(writer, sheet_name="Data Coverage", index=False)
    print(f"Finished. Trades: {len(trades_df)}; membership rows: {len(membership)}")
    print(f"Workbook: {OUTPUT / 'oldpal_swing_trades.xlsx'}")
    print(f"Coverage: {OUTPUT / 'oldpal_swing_trades.csv'}")


if __name__ == "__main__":
    main()
