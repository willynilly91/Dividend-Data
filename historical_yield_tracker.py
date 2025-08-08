# historical_yield_tracker.py
"""
Fetches dividend history from dividendhistory.org,
gets historical prices from Yahoo Finance on ex-dividend dates,
calculates annualized yields per entry,
and saves historical data to CSV.

- Appends new entries (no overwrite)
- Generates a second CSV with mean and standard deviation per ticker
- Falls back to yfinance if dividendhistory.org fails
- Designed to be run periodically (e.g. quarterly)
"""

import os
import requests
import pandas as pd
from lxml import html
import yfinance as yf
from datetime import datetime

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
    clean = symbol.split(":")[-1].replace("-UN", ".UN").replace(".TO", "").replace(".NE", "")
    if is_tsx:
        return f"{clean}.TO"
    return clean

def sanitize_symbol_for_dividendhistory(symbol: str) -> str:
    return symbol.split(":")[-1].replace(".TO", "").replace(".NE", "")

# ---------------------------
# Get historical dividend data
# ---------------------------
def scrape_dividends(symbol: str, is_tsx: bool):
    try:
        clean_symbol = sanitize_symbol_for_dividendhistory(symbol)
        if is_tsx:
            url = f"https://dividendhistory.org/payout/tsx/{clean_symbol}/"
        else:
            url = f"https://dividendhistory.org/payout/{clean_symbol}/"

        response = requests.get(url, timeout=10)
        tree = html.fromstring(response.content)

        dates = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')
        dividends = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')

        return list(zip(dates, dividends))

    except Exception as e:
        print(f"[DIV ERROR] {symbol}: {e}")
        return []

# ---------------------------
# Get historical price from Yahoo Finance
# ---------------------------
def get_price_on_date(symbol: str, date_str: str) -> float | None:
    try:
        t = yf.Ticker(symbol)
        date = pd.to_datetime(date_str)
        hist = t.history(start=date.strftime('%Y-%m-%d'), end=(date + pd.Timedelta(days=1)).strftime('%Y-%m-%d'))
        if not hist.empty:
            return float(hist.iloc[0]['Close'])
    except Exception as e:
        print(f"[PRICE ERROR] {symbol} on {date_str}: {e}")
    return None

# ---------------------------
# Process a single ticker
# ---------------------------
def process_ticker_history(symbol: str, is_tsx: bool) -> pd.DataFrame:
    print(f"Processing historical yield: {symbol}")
    records = []

    entries = scrape_dividends(symbol, is_tsx)
    source = "dividendhistory.org"

    if not entries:
        try:
            yf_symbol = sanitize_symbol_for_yfinance(symbol, is_tsx)
            t = yf.Ticker(yf_symbol)
            divs = t.dividends

            if not divs.empty:
                entries = [(d.strftime("%Y-%m-%d"), a) for d, a in divs.items()]
                source = "yfinance"
        except Exception as e:
            print(f"[YF FALLBACK ERROR] {symbol}: {e}")
            entries = []

    for ex_date, div in entries:
        try:
            yf_symbol = sanitize_symbol_for_yfinance(symbol, is_tsx)
            price = get_price_on_date(yf_symbol, ex_date)
            div = float(div)
            if price:
                annual_yield = (div * 12 / price) * 100
                records.append({
                    "Ticker": symbol,
                    "Ex-Div Date": ex_date,
                    "Dividend": div,
                    "Price on Ex-Date": round(price, 3),
                    "Annualized Yield %": round(annual_yield, 3),
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
def generate_summary_stats(history_csv: str, output_csv: str):
    df = pd.read_csv(history_csv)
    stats = df.groupby("Ticker")["Annualized Yield %"].agg(["mean", "std"]).reset_index()
    stats.columns = ["Ticker", "Mean Yield %", "Std Dev %"]
    stats = stats.sort_values(by="Mean Yield %", ascending=False)
    stats.to_csv(output_csv, index=False)
    print(f"Saved summary: {output_csv}")

# ---------------------------
# Main
# ---------------------------
def main():
    for filename, is_tsx, outfile in [
        ("tickers_canada.txt", True, "historical_yield_canada.csv"),
        ("tickers_us.txt", False, "historical_yield_us.csv")
    ]:
        tickers = load_ticker_list(filename)
        for symbol in tickers:
            df = process_ticker_history(symbol, is_tsx)
            if not df.empty:
                update_history_csv(df, outfile)

    generate_summary_stats("historical_yield_canada.csv", "yield_stats_canada.csv")
    generate_summary_stats("historical_yield_us.csv", "yield_stats_us.csv")

if __name__ == "__main__":
    main()
