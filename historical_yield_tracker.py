Here's your corrected **historical_yield_tracker.py** with the newline bug fixed and ready to copy/paste:

```python
# historical_yield_tracker.py
"""
Builds raw historical dividend records for Canada and US tickers.

• Dividends: scraped from dividendhistory.org (primary), yfinance (fallback)
• Prices: Yahoo Finance raw Close on the ex-div date (cached)
• Time window: last 5 years only
• Output: historical_yield_canada.csv, historical_yield_us.csv (append, dedupe)
• NOTE: This script intentionally leaves Frequency blank and does NOT compute
  Annualized Yield % or stats. After this finishes, run frequency_inference.py
  (or allow this script to invoke it automatically; see AUTO_RUN_INFERENCE).
"""

import os
import json
import time
import random
from datetime import datetime, timedelta

import pandas as pd
import requests
from lxml import html
import yfinance as yf

# ---------------------------
# Settings
# ---------------------------
PRICE_CACHE = "price_cache.json"
AUTO_RUN_INFERENCE = True  # set False if you want to run frequency_inference.py separately
SLEEP_SECS = (0.2, 0.6)    # jitter between requests to be gentle on sites
YEARS_BACK = 5

CANADA_TICKERS_FILE = "tickers_canada.txt"
US_TICKERS_FILE = "tickers_us.txt"
OUT_CANADA = "historical_yield_canada.csv"
OUT_US = "historical_yield_us.csv"

SINCE_DATE = (datetime.utcnow() - timedelta(days=365 * YEARS_BACK)).strftime("%Y-%m-%d")

# ---------------------------
# Cache helpers
# ---------------------------
if os.path.exists(PRICE_CACHE):
    try:
        with open(PRICE_CACHE, "r") as f:
            PRICE_CACHE_MEM = json.load(f)
    except Exception:
        PRICE_CACHE_MEM = {}
else:
    PRICE_CACHE_MEM = {}


def save_cache():
    with open(PRICE_CACHE, "w") as f:
        json.dump(PRICE_CACHE_MEM, f)

# ---------------------------
# Ticker IO
# ---------------------------

def load_ticker_list(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    seen = set()
    out = []
    for s in lines:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

# ---------------------------
# Symbol sanitization
# ---------------------------

def yf_symbol(symbol: str, is_tsx: bool) -> str:
    s = symbol.replace("$", "").split(":")[-1]
    s = s.replace("-UN", ".UN")
    if s.endswith(".TO") or s.endswith(".NE"):
        if s.endswith(".NE"):
            s = s.replace(".NE", "")
    else:
        if is_tsx:
            s = f"{s}.TO"
    return s


def dh_symbol(symbol: str) -> str:
    s = symbol.replace("$", "").split(":")[-1]
    s = s.replace(".TO", "").replace(".NE", "").replace(".UN", "-UN")
    return s

# ---------------------------
# Scrapers
# ---------------------------

def fetch_dividends_from_dividendhistory(symbol: str, is_tsx: bool) -> list[tuple[str, str]]:
    try:
        clean = dh_symbol(symbol)
        url = f"https://dividendhistory.org/payout/tsx/{clean}/" if is_tsx else f"https://dividendhistory.org/payout/{clean}/"
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        tree = html.fromstring(r.content)
        dates = [d.strip() for d in tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')]
        dividends = [v.strip() for v in tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')]
        return [(d, v) for d, v in zip(dates, dividends) if d and v]
    except Exception as e:
        print(f"[DIVHIST FAIL] {symbol}: {e}")
        return []


def fetch_dividends_from_yf(symbol: str, is_tsx: bool) -> list[tuple[str, str]]:
    try:
        t = yf.Ticker(yf_symbol(symbol, is_tsx))
        ser = t.dividends
        if ser is None or ser.empty:
            return []
        return [(idx.strftime("%Y-%m-%d"), str(val)) for idx, val in ser.items()]
    except Exception as e:
        print(f"[YF DIV FAIL] {symbol}: {e}")
        return []


def get_price_on_date(symbol: str, date_str: str) -> float | None:
    key = f"{symbol}_{date_str}"
    if key in PRICE_CACHE_MEM:
        return PRICE_CACHE_MEM[key]
    try:
        t = yf.Ticker(symbol)
        dt = pd.to_datetime(date_str)
        hist = t.history(start=dt.strftime('%Y-%m-%d'), end=(dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d'))
        if not hist.empty:
            price = float(hist.iloc[0]['Close'])
            PRICE_CACHE_MEM[key] = price
            return price
    except Exception as e:
        print(f"[YF PRICE FAIL] {symbol} {date_str}: {e}")
    return None

# ---------------------------
# CSV helpers
# ---------------------------

def latest_ex_date_in_csv(csv_path: str, ticker: str) -> str:
    if not os.path.exists(csv_path):
        return "1900-01-01"
    try:
        df = pd.read_csv(csv_path)
        df = df[df["Ticker"] == ticker]
        if df.empty:
            return "1900-01-01"
        return str(pd.to_datetime(df["Ex-Div Date"]).max().date())
    except Exception:
        return "1900-01-01"


def append_rows(csv_path: str, rows: list[dict]):
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    if os.path.exists(csv_path):
        try:
            old = pd.read_csv(csv_path)
            combined = pd.concat([old, new_df], ignore_index=True)
            combined.drop_duplicates(subset=["Ticker", "Ex-Div Date"], inplace=True)
            combined.sort_values(["Ticker", "Ex-Div Date"], inplace=True)
            combined.to_csv(csv_path, index=False)
            return
        except Exception as e:
            print(f"[CSV MERGE WARN] {csv_path}: {e}\nFalling back to overwrite.")
    new_df.sort_values(["Ticker", "Ex-Div Date"], inplace=True)
    new_df.to_csv(csv_path, index=False)

# ---------------------------
# Core processing
# ---------------------------

def process_universe(tickers: list[str], is_tsx: bool, out_csv: str):
    since = pd.to_datetime(SINCE_DATE)
    collected = []
    for sym in tickers:
        print(f"Processing {sym} ({'TSX' if is_tsx else 'US'})")
        try:
            last_in_csv = pd.to_datetime(latest_ex_date_in_csv(out_csv, sym))
            rows = fetch_dividends_from_dividendhistory(sym, is_tsx)
            source = "dividendhistory.org"
            if not rows:
                rows = fetch_dividends_from_yf(sym, is_tsx)
                source = "yfinance"
            if not rows:
                continue
            for ex_date_str, div_str in rows:
                try:
                    ex_dt = pd.to_datetime(ex_date_str)
                except Exception:
                    continue
                if ex_dt < since or ex_dt <= last_in_csv:
                    continue
                yf_sym = yf_symbol(sym, is_tsx)
                price = get_price_on_date(yf_sym, ex_dt.strftime("%Y-%m-%d"))
                if price is None:
                    continue
                collected.append({
                    "Ticker": sym,
                    "Ex-Div Date": ex_dt.strftime("%Y-%m-%d"),
                    "Dividend": _to_float(div_str),
                    "Price on Ex-Date": round(price, 6),
                    "Frequency": "",
                    "Source": source,
                    "Scraped At": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
                })
                time.sleep(random.uniform(*SLEEP_SECS))
        except Exception as e:
            print(f"[PROCESS WARN] {sym}: {e}")
    append_rows(out_csv, collected)

# ---------------------------
# Main
# ---------------------------

def main():
    ca = load_ticker_list(CANADA_TICKERS_FILE)
    us = load_ticker_list(US_TICKERS_FILE)
    process_universe(ca, True, OUT_CANADA)
    process_universe(us, False, OUT_US)
    save_cache()
    if AUTO_RUN_INFERENCE and os.path.exists("frequency_inference.py"):
        print("\n[INFO] Running frequency_inference.py to fill Frequency and recalc stats…\n")
        try:
            exit_code = os.system("python frequency_inference.py")
            print(f"[INFO] frequency_inference.py exit code: {exit_code}")
        except Exception as e:
            print(f"[WARN] Could not run frequency_inference.py: {e}")

def _to_float(x):
    s = str(x).strip().replace(",", "").replace("$", "")
    if s in ("", "-", "—", "None", "nan", "NaN"):
        return float("nan")
    try:
        return float(s)
    except Exception:
        s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
        return float(s) if s else float("nan")

if __name__ == "__main__":
    main()
```
