# historical_yield_tracker.py
"""
Fetches dividend history from dividendhistory.org,
gets historical prices from Yahoo Finance on ex-dividend dates,
calculates annualized yields per entry based on frequency,
and saves historical data to CSV.

- Appends new entries (no overwrite)
- Skips existing data based on most recent date in CSV
- Generates a second CSV with mean and standard deviation per ticker
- Falls back to yfinance if dividendhistory.org fails
- Implements caching to reduce rate limits and repeated requests
- Designed to be run periodically (e.g. quarterly)
"""

import os
import json
import requests
import pandas as pd
from lxml import html
import yfinance as yf
from datetime import datetime

# ---------------------------
# Constants and Caching
# ---------------------------
CACHE_FILE = "price_cache.json"

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        price_cache = json.load(f)
else:
    price_cache = {}

def save_cache():
    with open(CACHE_FILE, "w") as f:
        json.dump(price_cache, f)

# ---------------------------
# Load tickers
# ---------------------------
def load_ticker_list(filename: str) -> list[str]:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except Exception:
        return []

# ---------------------------
# Ticker sanitization
# ---------------------------
def sanitize_symbol_for_yfinance(symbol: str, is_tsx: bool) -> str:
    clean = symbol.replace("$", "").split(":")[-1].replace("-UN", ".UN").replace(".TO", "").replace(".NE", "")
    return f"{clean}.TO" if is_tsx else clean

def sanitize_symbol_for_dividendhistory(symbol: str) -> str:
    return symbol.replace("$", "").split(":")[-1].replace(".TO", "").replace(".NE", "").replace(".UN", "-UN")

# ---------------------------
# Get historical dividend data
# ---------------------------
def scrape_dividends(symbol: str, is_tsx: bool):
    try:
        clean_symbol = sanitize_symbol_for_dividendhistory(symbol)
        url = f"https://dividendhistory.org/payout/tsx/{clean_symbol}/" if is_tsx else f"https://dividendhistory.org/payout/{clean_symbol}/"
        response = requests.get(url, timeout=10)
        tree = html.fromstring(response.content)
        dates = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')
        dividends = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')
        frequencies = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[4]/text()')
        return list(zip(dates, dividends, frequencies))
    except Exception as e:
        print(f"[DIV ERROR] {symbol}: {e}")
        return []

# ---------------------------
# Get historical price from Yahoo Finance (with caching)
# ---------------------------
def get_price_on_date(symbol: str, date_str: str) -> float | None:
    key = f"{symbol}_{date_str}"
    if key in price_cache:
        return price_cache[key]
    try:
        t = yf.Ticker(symbol)
        date = pd.to_datetime(date_str)
        hist = t.history(start=date.strftime('%Y-%m-%d'), end=(date + pd.Timedelta(days=1)).strftime('%Y-%m-%d'))
        if not hist.empty:
            price = float(hist.iloc[0]['Close'])
            price_cache[key] = price
            return price
    except Exception as e:
        print(f"[PRICE ERROR] {symbol} on {date_str}: {e}")
    return None

# ---------------------------
# Frequency factor
# ---------------------------
def freq_to_multiplier(freq: str) -> int:
    mapping = {
        "annual": 1,
        "annually": 1,
        "quarterly": 4,
        "monthly": 12,
        "semi-monthly": 24,
        "bi-weekly": 26,
        "weekly": 52
    }
    return mapping.get(freq.strip().lower(), 12)  # default to monthly

# ---------------------------
# Load most recent ex-date from history
# ---------------------------
def get_latest_ex_date(path: str, symbol: str) -> str:
    if os.path.exists(path):
        df = pd.read_csv(path)
        df = df[df["Ticker"] == symbol]
        if not df.empty:
            return df["Ex-Div Date"].max()
    return "1900-01-01"

# ---------------------------
# Process a single ticker
# ---------------------------
def process_ticker_history(symbol: str, is_tsx: bool, existing_csv: str) -> pd.DataFrame:
    print(f"Processing historical yield: {symbol}")
    records = []
    latest_date = get_latest_ex_date(existing_csv, symbol)
    entries = scrape_dividends(symbol, is_tsx)
    source = "dividendhistory.org"

    if not entries:
        try:
            yf_symbol = sanitize_symbol_for_yfinance(symbol, is_tsx)
            t = yf.Ticker(yf_symbol)
            divs = t.dividends
            if not divs.empty:
                entries = [(d.strftime("%Y-%m-%d"), a, "monthly") for d, a in divs.items()]  # fallback default freq
                source = "yfinance"
        except Exception as e:
            print(f"[YF FALLBACK ERROR] {symbol}: {e}")
            entries = []

    for ex_date, div, freq in entries:
        try:
            if pd.to_datetime(ex_date) <= pd.to_datetime(latest_date):
                continue  # skip old entries
            yf_symbol = sanitize_symbol_for_yfinance(symbol, is_tsx)
            price = get_price_on_date(yf_symbol, ex_date)
            div = float(div)
            if price:
                multiplier = freq_to_multiplier(freq)
                annual_yield = (div * multiplier / price) * 100
                records.append({
                    "Ticker": symbol,
                    "Ex-Div Date": ex_date,
                    "Dividend": div,
                    "Price on Ex-Date": round(price, 3),
                    "Annualized Yield %": round(annual_yield, 3),
                    "Frequency": freq,
                    "Source": source
                })
        except Exception as e:
            print(f"[RECORD ERROR] {symbol} on {ex_date}: {e}")

    return pd.DataFrame(records)

# ---------------------------
# Append new data to master CSV
# ---------------------------
def update_history_csv(df: pd.DataFrame, path: str):
    if os.path.exists(path):
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df])
        combined.drop_duplicates(subset=["Ticker", "Ex-Div Date"], inplace=True)
        combined.to_csv(path, index=False)
    else:
        df.to_csv(path, index=False)

# ---------------------------
# Generate summary stats
# ---------------------------
def generate_summary_stats(stats_output_path: str, history_csv: str):
    df = pd.read_csv(history_csv)
    if "Annualized Yield %" not in df.columns or df.empty:
        print(f"[SKIP STATS] {history_csv} is empty or malformed.")
        return
    stats = df.groupby("Ticker")["Annualized Yield %"].agg(["mean", "std"]).reset_index()
    stats.columns = ["Ticker", "Mean Yield %", "Std Dev %"]
    stats = stats.sort_values(by="Mean Yield %", ascending=False)
    stats.to_csv(stats_output_path, index=False)
    print(f"Saved summary: {stats_output_path}")

# ---------------------------
# Main
# ---------------------------
def main():
    for filename, is_tsx, stats_outfile, history_outfile in [
        ("tickers_canada.txt", True, "historical_yield_canada.csv", "yield_stats_canada.csv"),
        ("tickers_us.txt", False, "historical_yield_us.csv", "yield_stats_us.csv")
    ]:
        tickers = load_ticker_list(filename)
        for symbol in tickers:
            df = process_ticker_history(symbol, is_tsx, stats_outfile)
            if not df.empty:
                update_history_csv(df, stats_outfile)

        generate_summary_stats(history_outfile, stats_outfile)

    save_cache()

if __name__ == "__main__":
    main()
