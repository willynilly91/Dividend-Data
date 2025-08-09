# dividendhistory_only_scraper.py
"""
Scrapes full historical dividend data from dividendhistory.org for all tickers in the Canadian and US ticker files.
Does NOT use Yahoo Finance at all — only dividendhistory.org.
Outputs: dividendhistory_only_canada.csv, dividendhistory_only_us.csv
"""

import os
import time
import random
from datetime import datetime
import pandas as pd
import requests
from lxml import html

SLEEP_SECS = (0.2, 0.6)
CANADA_TICKERS_FILE = "tickers_canada.txt"
US_TICKERS_FILE = "tickers_us.txt"
OUT_CANADA = "dividendhistory_only_canada.csv"
OUT_US = "dividendhistory_only_us.csv"

# ---------------------------
# Helpers
# ---------------------------
def load_ticker_list(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

def dh_symbol(symbol: str) -> str:
    s = symbol.replace("$", "").split(":")[-1]
    s = s.replace(".TO", "").replace(".NE", "").replace(".UN", "-UN")
    return s

# ---------------------------
# Scraper
# ---------------------------
def fetch_dividends_from_dividendhistory(symbol: str, is_tsx: bool) -> list[dict]:
    try:
        clean = dh_symbol(symbol)
        url = f"https://dividendhistory.org/payout/tsx/{clean}/" if is_tsx else f"https://dividendhistory.org/payout/{clean}/"
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        tree = html.fromstring(r.content)
        dates = [d.strip() for d in tree.xpath('//*[@id="dividend_table"]//tr/td[1]//text()') if d.strip()]
        dividends = [v.strip() for v in tree.xpath('//*[@id="dividend_table"]//tr/td[3]//text()') if v.strip()]
        out = []
        for d, v in zip(dates, dividends):
            out.append({
                "Ticker": symbol,
                "Ex-Div Date": d,
                "Dividend": v,
                "Source": "dividendhistory.org",
                "Scraped At": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
            })
        return out
    except Exception as e:
        print(f"[DIVHIST FAIL] {symbol} ({url}): {e}")
        return []

# ---------------------------
# Processing
# ---------------------------
def process_universe(tickers: list[str], is_tsx: bool, out_csv: str):
    collected = []
    for sym in tickers:
        print(f"Processing {sym} ({'TSX' if is_tsx else 'US'})")
        rows = fetch_dividends_from_dividendhistory(sym, is_tsx)
        collected.extend(rows)
        time.sleep(random.uniform(*SLEEP_SECS))
    if collected:
        pd.DataFrame(collected).to_csv(out_csv, index=False)
        print(f"Saved {out_csv} with {len(collected)} rows.")
    else:
        print(f"No data collected for {out_csv}.")

# ---------------------------
# Main
# ---------------------------
def main():
    ca = load_ticker_list(CANADA_TICKERS_FILE)
    us = load_ticker_list(US_TICKERS_FILE)
    process_universe(ca, True, OUT_CANADA)
    process_universe(us, False, OUT_US)

if __name__ == "__main__":
    main()
