# daily_etf_yield_tracker.py
"""
Fetches ETF live prices from Yahoo Finance
and dividend data (amount, date, frequency) from dividendhistory.org,
calculates forward yield, and exports to CSV.

Now also merges historical stats (median/mean/std) and labels valuation:
  - Underpriced   if Forward Yield % > Median + 1*Std
  - Overpriced    if Forward Yield % < Median - 1*Std
  - Fair Price    otherwise

Sorted by forward yield. US and Canadian tickers are separated.
"""

import os
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
    "Annual": 1,
}

# ---------------------------
# Scrapers
# ---------------------------
def get_dividend_data_from_dho(symbol: str, is_tsx: bool):
    # dividendhistory.org symbol format
    dho_symbol = symbol.replace(".TO", "") if is_tsx else symbol
    url = f"https://dividendhistory.org/payout/tsx/{dho_symbol}/" if is_tsx else f"https://dividendhistory.org/payout/{dho_symbol}/"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        tree = html.fromstring(response.content)

        # Attempt to read a Frequency: line if present
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
            last_dividend = float(str(dividend_values[0]).strip().replace("$", "").replace(",", ""))
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

# ---------------------------
# Stats merge + valuation
# ---------------------------
def _merge_stats_and_valuation(daily_df: pd.DataFrame, stats_csv: str) -> pd.DataFrame:
    """
    Merge in historical stats and compute Valuation based on Forward Yield % vs (median ± 1*std).
    Adds columns:
      - Median Annualized Yield %
      - Mean Annualized Yield %
      - Std Dev %
      - Valuation  (Underpriced / Overpriced / Fair Price / Unknown)
    """
    out = daily_df.copy()

    # Ensure the key column exists
    if "Ticker" not in out.columns:
        raise ValueError("daily_df missing required column: 'Ticker'")

    # If stats CSV missing, add empty columns and bail gracefully
    if not os.path.exists(stats_csv):
        out["Median Annualized Yield %"] = pd.NA
        out["Mean Annualized Yield %"] = pd.NA
        out["Std Dev %"] = pd.NA
        out["Valuation"] = "Unknown"
        return out

    stats = pd.read_csv(stats_csv)

    # Normalize possible legacy column names -> new names
    rename_map = {}
    if "Average Yield %" in stats.columns and "Median Annualized Yield %" not in stats.columns:
        rename_map["Average Yield %"] = "Median Annualized Yield %"
    if "Mean Yield %" in stats.columns and "Mean Annualized Yield %" not in stats.columns:
        rename_map["Mean Yield %"] = "Mean Annualized Yield %"
    if "Std Deviation" in stats.columns and "Std Dev %" not in stats.columns:
        rename_map["Std Deviation"] = "Std Dev %"
    if rename_map:
        stats = stats.rename(columns=rename_map)

    needed = {"Ticker", "Median Annualized Yield %", "Mean Annualized Yield %", "Std Dev %"}
    if not needed.issubset(stats.columns):
        # Stats file exists but layout is unexpected; fail safe
        out["Median Annualized Yield %"] = pd.NA
        out["Mean Annualized Yield %"] = pd.NA
        out["Std Dev %"] = pd.NA
        out["Valuation"] = "Unknown"
        return out

    out = out.merge(
        stats[["Ticker", "Median Annualized Yield %", "Mean Annualized Yield %", "Std Dev %"]],
        on="Ticker",
        how="left"
    )

    # Compute valuation
    # Using Forward Yield % from this daily sheet vs median ± 1*std
    def _label(row):
        cur = row.get("Yield (Forward) %")
        med = row.get("Median Annualized Yield %")
        sd  = row.get("Std Dev %")
        try:
            if pd.isna(cur) or pd.isna(med) or pd.isna(sd):
                return "Unknown"
            low  = med - sd
            high = med + sd
            if cur > high:
                return "Underpriced"
            if cur < low:
                return "Overpriced"
            return "Fair Price"
        except Exception:
            return "Unknown"

    out["Valuation"] = out.apply(_label, axis=1)
    return out

# ---------------------------
# Per-ticker processing
# ---------------------------
def process_ticker(symbol: str, is_tsx: bool) -> dict:
    try:
        print(f"Processing: {symbol}")
        t = yf.Ticker(symbol)
        name = t.info.get("shortName") or t.info.get("longName")
        price = get_price(t)
        currency = t.info.get("currency")

        last_div, last_date, freq_text, remarks = get_dividend_data_from_dho(symbol, is_tsx)

        # Fallback if DHO fails
        if last_div is None:
            last_div, last_date, fallback_remarks = get_dividend_data_from_yf(t)
            remarks = fallback_remarks
            if last_div is not None and not freq_text:
                freq_text = "Monthly"  # conservative default

        multiplier = FREQ_MULTIPLIER.get(freq_text, None)
        forward_yield = ((last_div * multiplier) / price * 100) if (last_div and multiplier and price) else None

        return {
            "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
            "Ticker": symbol,
            "Name": name,
            "Price": price,
            "Currency": currency,
            "Last Dividend": last_div,
            "Last Dividend Date": last_date,
            "Frequency": freq_text,
            "Yield (Forward) %": round(forward_yield, 3) if forward_yield is not None else None,
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

# ---------------------------
# Build and save per region
# ---------------------------
def build_csv(ticker_file: str, is_tsx: bool, output_file: str, stats_csv: str):
    tickers = load_ticker_list(ticker_file)
    data = [process_ticker(t, is_tsx) for t in tickers]
    df = pd.DataFrame(data)

    # Merge stats, compute Valuation, then sort by forward yield
    df = _merge_stats_and_valuation(df, stats_csv)
    if "Yield (Forward) %" in df.columns:
        df = df.sort_values(by="Yield (Forward) %", ascending=False, na_position="last")

    df.to_csv(output_file, index=False)
    print(f"Saved {output_file}")

# ---------------------------
# Main
# ---------------------------
def main():
    try:
        build_csv(
            "tickers_canada.txt",
            is_tsx=True,
            output_file="etf_yields_canada.csv",
            stats_csv="yield_stats_canada.csv",
        )
        build_csv(
            "tickers_us.txt",
            is_tsx=False,
            output_file="etf_yields_us.csv",
            stats_csv="yield_stats_us.csv",
        )
    except Exception as e:
        print(f"[FATAL] Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
