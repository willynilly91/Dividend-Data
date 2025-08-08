# daily_etf_yield_tracker.py
"""
Fetches ETF live prices from Yahoo Finance
and dividend data (amount, date, frequency) from dividendhistory.org,
calculates forward yield, and exports to CSV.

Sorted by yield. US and Canadian tickers are separated.
"""

import requests
import pandas as pd
from lxml import html
import yfinance as yf
from datetime import datetime

# ---------------------------
# Load tickers from external text files
# ---------------------------
def load_ticker_list(filename: str) -> list[str]:
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except Exception:
        return []

# ---------------------------
# Frequency map from text
# ---------------------------
FREQ_MULTIPLIER = {
    "Weekly": 52,
    "Bi-Weekly": 26,
    "Semi-Monthly": 24,
    "Monthly": 12,
    "Quarterly": 4,
    "Semi-Annual": 2,
    "Annual": 1
}

def get_dividend_data_from_dho(symbol: str, is_tsx: bool):
    if is_tsx:
        url = f"https://dividendhistory.org/payout/tsx/{symbol.replace('.TO', '')}/"
    else:
        url = f"https://dividendhistory.org/payout/{symbol}/"

    try:
        response = requests.get(url, timeout=10)
        tree = html.fromstring(response.content)

        freq_texts = tree.xpath('//div[contains(@class,"col")]/text()')
        frequency = None
        for line in freq_texts:
            if "Frequency:" in line:
                frequency = line.strip().split("Frequency:")[-1].strip()
                break

        dividend_dates = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')
        dividend_values = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')

        if dividend_dates and dividend_values:
            last_dividend_date = dividend_dates[0].strip()
            last_dividend = float(dividend_values[0].strip())
        else:
            last_dividend_date, last_dividend = None, None

        return last_dividend, last_dividend_date, frequency, "dividendhistory.org"

    except Exception as e:
        print(f"[DHO ERROR] {symbol}: {e}")
        return None, None, None, None

def get_dividend_data_from_yf(ticker_obj: yf.Ticker):
    try:
        divs = ticker_obj.dividends
        if divs is not None and not divs.empty:
            return float(divs.iloc[-1]), str(divs.index[-1].date()), "yfinance"
    except Exception as e:
        print(f"[YF DIV ERROR] {ticker_obj.ticker}: {e}")
    return None, None, None

def get_price(ticker_obj: yf.Ticker) -> float | None:
    try:
        return float(ticker_obj.fast_info.get("last_price") or ticker_obj.info.get("regularMarketPrice"))
    except Exception as e:
        print(f"[PRICE ERROR] {ticker_obj.ticker}: {e}")
        return None

def process_ticker(symbol: str, is_tsx: bool) -> dict:
    try:
        print(f"Processing: {symbol}")
        t = yf.Ticker(symbol)
        name = t.info.get("shortName") or t.info.get("longName")
        price = get_price(t)
        currency = t.info.get("currency")

        last_div, last_date, freq_text, remarks = get_dividend_data_from_dho(symbol, is_tsx)

        if last_div is None:
            last_div, last_date, fallback_remarks = get_dividend_data_from_yf(t)
            remarks = fallback_remarks
            if last_div is not None:
                freq_text = "Monthly"  # Default fallback

        multiplier = FREQ_MULTIPLIER.get(freq_text, None)
        forward_yield = ((last_div * multiplier) / price * 100) if last_div and multiplier and price else None

        return {
            "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
            "Ticker": symbol,
            "Name": name,
            "Price": price,
            "Currency": currency,
            "Last Dividend": last_div,
            "Last Dividend Date": last_date,
            "Frequency": freq_text,
            "Yield (Forward) %": round(forward_yield, 3) if forward_yield else None,
            "Remarks": remarks
        }

    except Exception as e:
        print(f"[ERROR] Failed to process {symbol}: {e}")
        return {
            "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
            "Ticker": symbol,
            "Name": None,
            "Price": None,
            "Currency": None,
            "Last Dividend": None,
            "Last Dividend Date": None,
            "Frequency": None,
            "Yield (Forward) %": None,
            "Remarks": "Error"
        }

def build_csv(ticker_file: str, is_tsx: bool, output_file: str):
    tickers = load_ticker_list(ticker_file)
    data = [process_ticker(t, is_tsx) for t in tickers]
    df = pd.DataFrame(data)
    df = df.sort_values(by="Yield (Forward) %", ascending=False)
    df.to_csv(output_file, index=False)
    print(f"Saved {output_file}")

def main():
    try:
        build_csv("tickers_canada.txt", is_tsx=True, output_file="etf_yields_canada.csv")
        build_csv("tickers_us.txt", is_tsx=False, output_file="etf_yields_us.csv")
    except Exception as e:
        print(f"[FATAL] Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
