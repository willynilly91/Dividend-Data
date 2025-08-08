import os
import csv
import yfinance as yf
import pandas as pd
import requests
from lxml import html
from datetime import datetime

def get_dividend_history(ticker, is_cad):
    if is_cad:
        url = f"https://dividendhistory.org/payout/tsx/{ticker}/"
    else:
        url = f"https://dividendhistory.org/payout/{ticker}/"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        tree = html.fromstring(response.content)

        rows = tree.xpath('//*[@id="dividend_table"]/tbody/tr')
        history = []
        for row in rows:
            columns = row.xpath("td")
            if len(columns) >= 3:
                ex_date = columns[0].text_content().strip()
                amount = columns[2].text_content().strip().replace('$', '')
                try:
                    history.append({
                        "ex_date": datetime.strptime(ex_date, "%b %d, %Y").strftime("%Y-%m-%d"),
                        "amount": float(amount)
                    })
                except:
                    continue
        return history
    except Exception as e:
        print(f"Error fetching dividend data for {ticker}: {e}")
        return []

def get_price_on_date(ticker, date):
    try:
        df = yf.download(ticker, start=date, end=date, progress=False)
        if not df.empty:
            return float(df["Close"].iloc[0])
    except Exception as e:
        print(f"Error fetching price for {ticker} on {date}: {e}")
    return None

def load_existing_data(csv_file):
    if os.path.exists(csv_file):
        return pd.read_csv(csv_file)
    return pd.DataFrame(columns=["Ticker", "Ex-Date", "Dividend", "Price", "Yield (%)"])

def append_new_data(df, new_data):
    combined = pd.concat([df, new_data], ignore_index=True)
    combined.drop_duplicates(subset=["Ticker", "Ex-Date"], keep="last", inplace=True)
    return combined

def process_tickers(ticker_list_path, output_csv, stats_csv, is_cad):
    tickers = []
    with open(ticker_list_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                parts = line.split(":")
                if len(parts) == 2:
                    tickers.append(parts[1] if is_cad else parts[1])

    existing_df = load_existing_data(output_csv)
    new_rows = []

    for ticker in tickers:
        print(f"Processing {ticker}...")
        history = get_dividend_history(ticker, is_cad)

        for record in history:
            ex_date = record["ex_date"]
            dividend = record["amount"]
            price = get_price_on_date(f"{ticker}.TO" if is_cad else ticker, ex_date)

            if price:
                annual_yield = round((dividend * 12 / price) * 100, 2)  # Assuming monthly
                new_rows.append({
                    "Ticker": ticker,
                    "Ex-Date": ex_date,
                    "Dividend": dividend,
                    "Price": price,
                    "Yield (%)": annual_yield
                })

    new_df = pd.DataFrame(new_rows)
    final_df = append_new_data(existing_df, new_df)
    final_df.to_csv(output_csv, index=False)

    # Stats
    stats = final_df.groupby("Ticker")["Yield (%)"].agg(["mean", "std"]).reset_index()
    stats.rename(columns={"mean": "Mean Yield (%)", "std": "Std Dev (%)"}, inplace=True)
    stats.sort_values(by="Mean Yield (%)", ascending=False, inplace=True)
    stats.to_csv(stats_csv, index=False)

# Main execution
if __name__ == "__main__":
    process_tickers(
        ticker_list_path="tickers_us.txt",
        output_csv="historical_yield_us.csv",
        stats_csv="yield_stats_us.csv",
        is_cad=False
    )

    process_tickers(
        ticker_list_path="tickers_canada.txt",
        output_csv="historical_yield_canada.csv",
        stats_csv="yield_stats_canada.csv",
        is_cad=True
    )
