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
# CONFIG: List your tickers here
# ---------------------------
CANADIAN_TICKERS = ["HYLD.TO", "RY.TO"]
US_TICKERS = ["JEPI", "QYLD", "XYLD"]

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
    """Scrape dividendhistory.org for last dividend and frequency."""
    if is_tsx:
        url = f"https://dividendhistory.org/payout/tsx/{symbol.replace('.TO', '')}/"
    else:
        url = f"https://dividendhistory.org/payout/{symbol}/"

    try:
        response = requests.get(url, timeout=10)
        tree = html.fromstring(response.content)

        # Get frequency
        freq_texts = tree.xpath('//div[contains(@class,"col")]/text()')
        frequency = None
        for line in freq_texts:
            if "Frequency:" in line:
                frequency = line.strip().split("Frequency:")[-1].strip()
                break

        # Get last dividend
        dividend_dates = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')
        dividend_values = tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')

        if dividend_dates and dividend_values:
            last_dividend_date = dividend_dates[0].strip()
            last_dividend = float(dividend_values[0].strip())
        else:
            last_dividend_date, last_dividend = None, None

        return last_dividend, last_dividend_date, frequency

    except Exception:
        return None, None, None


def get_dividend_data_from_yf(ticker_obj: yf.Ticker):
    try:
        divs = ticker_obj.dividends
        if divs is not None and not divs.empty:
            return float(divs.iloc[-1]), str(divs.index[-1].date())
    except Exception:
        pass
    return None, None


def get_price(ticker_obj: yf.Ticker) -> float | None:
    try:
        return float(ticker_obj.fast_info.get("last_price") or ticker_obj.info.get("regularMarketPrice"))
    except Exception:
        return None


def process_ticker(symbol: str, is_tsx: bool) -> dict:
    t = yf.Ticker(symbol)
    name = t.info.get("shortName") or t.info.get("longName")
    price = get_price(t)
    currency = t.info.get("currency")

    # Try DHO first for dividend
    last_div, last_date, freq_text = get_dividend_data_from_dho(symbol, is_tsx)

    if last_div is None:
        # Fallback to Yahoo Finance
        last_div, last_date = get_dividend_data_from_yf(t)
        freq_text = None  # unknown frequency

    multiplier = FREQ_MULTIPLIER.get(freq_text, None)
    forward_div = last_div * multiplier if last_div and multiplier else None
    forward_yield = (forward_div / price * 100) if forward_div and price else None

    return {
        "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
        "Ticker": symbol,
        "Name": name,
        "Price": price,
        "Currency": currency,
        "Last Dividend": last_div,
        "Last Dividend Date": last_date,
        "Frequency": freq_text,
        "Forward Dividend (Annualized)": forward_div,
        "Yield (Forward) %": round(forward_yield, 3) if forward_yield else None
    }


def build_csv(tickers, is_tsx: bool, filename: str):
    data = [process_ticker(t, is_tsx) for t in tickers]
    df = pd.DataFrame(data)
    df = df.sort_values(by="Yield (Forward) %", ascending=False)
    df.to_csv(filename, index=False)
    print(f"Saved {filename}")


def main():
    build_csv(CANADIAN_TICKERS, is_tsx=True, filename="etf_yields_canada.csv")
    build_csv(US_TICKERS, is_tsx=False, filename="etf_yields_us.csv")


if __name__ == "__main__":
    main()
