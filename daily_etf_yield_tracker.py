# daily_etf_yield_tracker.py
"""
Fetches ETF live prices from Yahoo Finance
and dividend data (amount, date, frequency) from dividendhistory.org,
calculates CURRENT yield, and exports to CSV.

Also merges historical stats (median/mean/std) and labels valuation:
  - Underpriced   if Current Yield (%) > Median + 1*Std
  - Overpriced    if Current Yield (%) < Median - 1*Std
  - Fair Price    otherwise

Frequency is now sourced primarily from historical_yield_*.csv.
If not found there, we try to read it from dividendhistory.org.
If still unknown, we leave it as "Unknown" (no default to Monthly).

Sorted by current yield. US and Canadian tickers are separated.
"""

import os
import requests
import pandas as pd
from lxml import html
import yfinance as yf
from datetime import datetime
from typing import Optional, Dict

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
    # "Unknown": None  # handled by code when multiplier lookup fails
}

# ---------------------------
# Frequency helpers
# ---------------------------
def normalize_frequency(freq: Optional[str]) -> Optional[str]:
    """Normalize arbitrary frequency text to canonical keys used in FREQ_MULTIPLIER."""
    if not freq or pd.isna(freq):
        return None
    f = str(freq).strip().lower()

    # strip punctuation and extra spaces
    f = f.replace("_", " ").replace("-", " ").replace("/", " ").replace(".", " ").strip()
    f = " ".join(f.split())

    aliases = {
        "weekly": "Weekly",
        "bi weekly": "Bi-Weekly",
        "biweekly": "Bi-Weekly",
        "semi monthly": "Semi-Monthly",
        "semimonthly": "Semi-Monthly",
        "monthly": "Monthly",
        "quarterly": "Quarterly",
        "qtr": "Quarterly",
        "semi annual": "Semi-Annual",
        "semiannual": "Semi-Annual",
        "yearly": "Annual",
        "annual": "Annual",
    }
    return aliases.get(f, None)

def load_frequency_map(hist_file: str) -> Dict[str, str]:
    """
    Load {Ticker -> Frequency} from historical_yield_*.csv.
    Accepts case-insensitive columns 'Ticker' (or 'Symbol') and 'Frequency' (or 'Freq').
    Unknown/invalid entries are skipped.
    """
    freq_map: Dict[str, str] = {}
    if not os.path.exists(hist_file):
        return freq_map

    df = pd.read_csv(hist_file)
    # find columns case-insensitively
    cols = {c.lower(): c for c in df.columns}
    ticker_col = cols.get("ticker") or cols.get("symbol")
    freq_col = cols.get("frequency") or cols.get("freq")

    if not ticker_col or not freq_col:
        return freq_map

    for _, row in df[[ticker_col, freq_col]].dropna().iterrows():
        ticker = str(row[ticker_col]).strip()
        nf = normalize_frequency(row[freq_col])
        if ticker and nf:
            freq_map[ticker] = nf
    return freq_map

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
            raw_val = str(dividend_values[0]).strip().replace("$", "").replace(",", "")
            last_dividend = float(raw_val)
        else:
            last_dividend_date, last_dividend = None, None

        return last_dividend, last_dividend_date, frequency

    except Exception as e:
        print(f"[DHO ERROR] {symbol}: {e}")
        return None, None, None

def get_dividend_data_from_yf(ticker_obj: yf.Ticker):
    try:
        divs = ticker_obj.dividends
        if divs is not None and not divs.empty:
            return float(divs.iloc[-1]), str(divs.index[-1].date())
    except Exception as e:
        print(f"[YF DIV ERROR] {ticker_obj.ticker}: {e}")
    return None, None

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
    Merge in historical stats and compute Valuation based on Current Yield (%) vs (median ± 1*std).
    Adds columns:
      - Median Annualized Yield %
      - Mean Annualized Yield %
      - Std Dev %
      - Valuation  (Underpriced / Overpriced / Fair Price / Unknown)
    """
    out = daily_df.copy()

    if "Ticker" not in out.columns:
        raise ValueError("daily_df missing required column: 'Ticker'")

    if not os.path.exists(stats_csv):
        out["Median Annualized Yield %"] = pd.NA
        out["Mean Annualized Yield %"] = pd.NA
        out["Std Dev %"] = pd.NA
        out["Valuation"] = "Unknown"
        return out

    stats = pd.read_csv(stats_csv)

    # Normalize legacy headers if needed
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
    def _label(row):
        cur = row.get("Current Yield (%)")
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
def process_ticker(symbol: str, is_tsx: bool, frequency_map: dict | None = None) -> dict:
    try:
        print(f"Processing: {symbol}")
        t = yf.Ticker(symbol)
        info = t.info or {}
        name = info.get("shortName") or info.get("longName")
        price = get_price(t)
        currency = info.get("currency")

        # 1) Prefer frequency from historical_yield_*.csv via frequency_map
        freq_text = None
        if frequency_map:
            # exact match
            freq_text = frequency_map.get(symbol)
            # try dropping .TO if not present in the CSV
            if not freq_text and is_tsx and symbol.endswith(".TO"):
                freq_text = frequency_map.get(symbol[:-3])
            # try adding .TO if CSV stored with suffix but tickers file didn't
            if not freq_text and is_tsx and not symbol.endswith(".TO"):
                freq_text = frequency_map.get(symbol + ".TO")
        if freq_text:
            freq_text = normalize_frequency(freq_text)

        # 2) Get dividend amount/date (DHO, then YF), but do not override freq if we already have it
        last_div, last_date, dho_freq = get_dividend_data_from_dho(symbol, is_tsx)
        if last_div is None:
            last_div, last_date = get_dividend_data_from_yf(t)

        # 3) If we still don't have a frequency, try DHO's reported frequency
        if not freq_text:
            freq_text = normalize_frequency(dho_freq)

        # 4) Final fallback: Unknown (no default to Monthly)
        if not freq_text:
            freq_text = "Unknown"

        multiplier = FREQ_MULTIPLIER.get(freq_text, None)
        current_yield = ((last_div * multiplier) / price * 100) if (last_div and multiplier and price) else None

        return {
            "Last Updated (UTC)": datetime.utcnow().isoformat() + "Z",
            "Ticker": symbol,
            "Name": name,
            "Price": price,
            "Currency": currency,
            "Last Dividend": last_div,
            "Last Dividend Date": last_date,
            "Frequency": freq_text,
            "Current Yield (%)": round(current_yield, 3) if current_yield is not None else None,
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
            "Current Yield (%)": None,
        }

# ---------------------------
# Build and save per region
# ---------------------------
def build_csv(ticker_file: str, is_tsx: bool, output_file: str, stats_csv: str, frequency_map: dict | None = None):
    tickers = load_ticker_list(ticker_file)
    data = [process_ticker(t, is_tsx, frequency_map=frequency_map) for t in tickers]
    df = pd.DataFrame(data)

    # Merge stats, compute Valuation, then sort by current yield
    df = _merge_stats_and_valuation(df, stats_csv)
    if "Current Yield (%)" in df.columns:
        df = df.sort_values(by="Current Yield (%)", ascending=False, na_position="last")

    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Saved {output_file}")

# ---------------------------
# Main
# ---------------------------
def main():
    try:
        # Load frequency maps from historical yield files
        freq_map_can = load_frequency_map("historical_yield_canada.csv")
        freq_map_us  = load_frequency_map("historical_yield_us.csv")

        build_csv(
            "tickers_canada.txt",
            is_tsx=True,
            output_file="current_etf_yields_canada.csv",
            stats_csv="yield_stats_canada.csv",
            frequency_map=freq_map_can,
        )
        build_csv(
            "tickers_us.txt",
            is_tsx=False,
            output_file="current_etf_yields_us.csv",
            stats_csv="yield_stats_us.csv",
            frequency_map=freq_map_us,
        )
    except Exception as e:
        print(f"[FATAL] Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
