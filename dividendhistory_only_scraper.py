# dividendhistory_only_scraper.py
"""
Scrapes full historical dividend data from dividendhistory.org for EMCL only.
Does NOT use Yahoo Finance at all — only dividendhistory.org.
Outputs: dividendhistory_emcl_only.csv
"""

import os
import time
from datetime import datetime
import pandas as pd
import requests
from lxml import html

SLEEP_SECS = 0.5
OUT_FILE = "dividendhistory_emcl_only.csv"

# ---------------------------
# Helpers
# ---------------------------
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
# Main
# ---------------------------
def main():
    ticker = "EMCL.NE"  # Hardcoded to scrape only EMCL from NEO Exchange
    is_tsx = True  # Treat NEO like other Canadian exchanges for DividendHistory.org
    print(f"Processing {ticker}")
    rows = fetch_dividends_from_dividendhistory(ticker, is_tsx)
    if rows:
        pd.DataFrame(rows).to_csv(OUT_FILE, index=False)
        print(f"Saved {OUT_FILE} with {len(rows)} rows.")
    else:
        print(f"No data collected for {ticker}.")
    time.sleep(SLEEP_SECS)

if __name__ == "__main__":
    main()
