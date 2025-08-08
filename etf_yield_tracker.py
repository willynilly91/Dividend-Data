# daily_etf_yield_tracker.py
"""
Fetches ETF live prices, latest dividends, and payout frequency from Yahoo Finance
and dividendhistory.org, calculates forward yield, and exports to CSV.

Designed to run daily via GitHub Actions (free) and be imported dynamically into
Google Sheets via =IMPORTDATA("<raw GitHub CSV URL>")
"""

import requests
import pandas as pd
from lxml import html
import yfinance as yf
from datetime import datetime

# ---------------------------
# CONFIG: List your tickers here
# ---------------------------
TICKERS = [
    "HYLD.TO",  # TSX
    "JEPI",     # NYSE
    "QYLD",     # NASDAQ
    "XYLD",     # NASDAQ
    "RY.TO"     # Canadian bank
]

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


def get_frequency_from_dividendhistory(ticker: str, is_tsx: bool) -> str | None:
    if is_tsx:
        url = f"https://dividendhistory.org/payout/tsx/{ticker.replace('.TO', '')}/"
    else:
        url = f"https://dividendhistory.org/payout/{ticker}/"

    try:
        response = requests.get(url, timeout=10)
        tree = html.fromstring(response.content)
        freq_texts = tree.xpath('//div[contains(@class,"col")]/text()')
        for line in freq_texts:
            if "Frequency:" in line:
                return line.strip().split("Frequency:")[-1].strip()
    except Exception:
        return None
    return None


def get_last_dividend(ticker_obj: yf.Ticker) -> tuple[float | None, str | None]:
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


def process_ticker(symbol: str) -> dict:
    is_tsx = symbol.endswith(".TO")
    t = yf.Ticker(symbol)
    name = t.info.get("shortName") or t.info.get("longName")
    price = get_price(t)
    last_div, last_date = get_last_dividend(t)
    freq_text = get_frequency_from_dividendhistory(symbol, is_tsx)
    multiplier = FREQ_MULTIPLIER.get(freq_text, None)

    forward_div = last_div * multiplier if last_div and multiplier else None
    forward_yield = (forward_div / price * 100) if forward_div and price else None

    return {
        "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
        "Ticker": symbol,
        "Name": name,
        "Price": price,
        "Currency": t.info.get("currency"),
        "Last Dividend": last_div,
        "Last Dividend Date": last_date,
        "Frequency": freq_text,
        "Forward Dividend (Annualized)": forward_div,
        "Yield (Forward) %": round(forward_yield, 3) if forward_yield else None
    }


def main():
    data = [process_ticker(t) for t in TICKERS]
    df = pd.DataFrame(data)
    df = df.sort_values(by="Yield (Forward) %", ascending=False)
    df.to_csv("etf_yields.csv", index=False)
    print("Saved etf_yields.csv")


if __name__ == "__main__":
    main()
