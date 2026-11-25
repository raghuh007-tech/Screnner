#!/usr/bin/env python3
"""Build dated Nifty 500 membership + current Kite token mapping.

The membership history is a community-maintained reconstruction, not an NSE API.
Its own coverage notes disclose weaker pre-April-2018 accuracy. Review the source
and unresolved symbol report before interpreting results.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kiteconnect import KiteConnect

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
SOURCE = (
    "https://raw.githubusercontent.com/aditya-jha/nse-historical-membership/"
    "main/index_history/data/index_membership_history.csv"
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", default="2016-01-01")
    ap.add_argument("--to", dest="end", default=pd.Timestamp.today().date().isoformat())
    args = ap.parse_args()
    start, end = pd.Timestamp(args.start), pd.Timestamp(args.end)

    load_dotenv(ROOT / ".env")
    key, token = os.getenv("KITE_API_KEY", "").strip(), os.getenv("KITE_ACCESS_TOKEN", "").strip()
    if not key or not token:
        sys.exit("Set KITE_API_KEY and KITE_ACCESS_TOKEN in oldpal_swing/.env.")

    print("Downloading point-in-time Nifty 500 membership reconstruction...")
    hist = pd.read_csv(SOURCE)
    hist = hist[hist["index_name"].eq("Nifty 500")].copy()
    hist["valid_from"] = pd.to_datetime(hist["valid_from"], errors="coerce")
    hist["valid_to"] = pd.to_datetime(hist["valid_to"], errors="coerce")
    hist = hist[hist["valid_from"].le(end) & (hist["valid_to"].isna() | hist["valid_to"].gt(start))]
    hist = hist[["symbol", "valid_from", "valid_to", "source", "source_url", "notes"]].dropna(subset=["symbol", "valid_from"])
    hist = hist.drop_duplicates(["symbol", "valid_from", "valid_to"])

    kite = KiteConnect(api_key=key)
    kite.set_access_token(token)
    print("Loading current NSE instrument master from Kite...")
    instruments = kite.instruments("NSE")
    master = pd.DataFrame(instruments)
    master = master[["tradingsymbol", "instrument_token"]].drop_duplicates("tradingsymbol")
    master["tradingsymbol"] = master["tradingsymbol"].astype(str).str.upper()
    token_map = dict(zip(master["tradingsymbol"], master["instrument_token"]))

    hist["tradingsymbol"] = hist["symbol"].astype(str).str.upper()
    hist["instrument_token"] = hist["tradingsymbol"].map(token_map)
    # Source table uses half-open intervals [valid_from, valid_to).
    # Convert to inclusive valid_to for the backtest engine.
    hist["valid_to_inclusive"] = hist["valid_to"] - pd.Timedelta(days=1)
    hist.loc[hist["valid_to"].isna(), "valid_to_inclusive"] = end
    hist["valid_to_inclusive"] = hist["valid_to_inclusive"].fillna(end)
    hist["valid_from"] = hist["valid_from"].clip(lower=start)
    hist["valid_to_inclusive"] = hist["valid_to_inclusive"].clip(upper=end)

    resolved = hist[hist["instrument_token"].notna()].copy()
    unresolved = hist[hist["instrument_token"].isna()].copy()
    DATA.mkdir(parents=True, exist_ok=True)
    out = resolved[["tradingsymbol", "instrument_token", "valid_from", "valid_to_inclusive", "source", "source_url", "notes"]].rename(
        columns={"valid_to_inclusive": "valid_to"}
    )
    out.to_csv(DATA / "universe_membership.csv", index=False)
    unresolved.to_csv(DATA / "unresolved_symbols.csv", index=False)

    print(f"Membership intervals in period: {len(hist)}")
    print(f"Mapped to current Kite NSE token: {len(resolved)}")
    print(f"Unmapped historical symbol intervals: {len(unresolved)}")
    print(f"Membership source types: {hist['source'].value_counts(dropna=False).to_dict()}")
    print(f"Saved: {DATA / 'universe_membership.csv'}")
    print(f"Saved: {DATA / 'unresolved_symbols.csv'}")
    if len(unresolved):
        print("WARNING: This is not a complete survivorship-bias-free run. "
              "Kite's current instrument master does not map every delisted/renamed historical security.")
    print("Review the source coverage notes, especially the pre-2018 membership reconstruction.")


if __name__ == "__main__":
    main()
