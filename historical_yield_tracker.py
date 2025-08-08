import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from utils import get_dividend_history  # assuming you already modularized scraping

def ensure_csv_exists(path, headers):
    if not os.path.exists(path):
        pd.DataFrame(columns=headers).to_csv(path, index=False)

def append_new_data(csv_path, new_data_df):
    existing_df = pd.read_csv(csv_path)
    combined_df = pd.concat([existing_df, new_data_df]).drop_duplicates()
    combined_df.to_csv(csv_path, index=False)

def main():
    tickers = {
        "canada": "tickers_canada.txt",
        "us": "tickers_us.txt"
    }

    for region, file_path in tickers.items():
        output_path = f"historical_yield_{region}.csv"
        ensure_csv_exists(output_path, [
            "Ticker", "Ex-Div Date", "Div Amount", "Ex-Div Price",
            "Annualized Yield", "Source"
        ])

        with open(file_path, "r") as f:
            symbols = [line.strip() for line in f if line.strip()]

        all_data = []

        for ticker in symbols:
            try:
                records = get_dividend_history(ticker)  # Returns list of dicts
                for record in records:
                    ex_date = record["Ex-Div Date"]
                    ex_price = yf.Ticker(ticker).history(start=ex_date, end=ex_date).get("Close")
                    price = ex_price.iloc[0] if not ex_price.empty else None

                    if price and record["Div Amount"]:
                        yield_annualized = (float(record["Div Amount"]) * 12 / price) * 100
                        all_data.append({
                            "Ticker": ticker,
                            "Ex-Div Date": ex_date,
                            "Div Amount": record["Div Amount"],
                            "Ex-Div Price": round(price, 2),
                            "Annualized Yield": round(yield_annualized, 2),
                            "Source": "DividendHistory.org"
                        })
            except Exception as e:
                print(f"{ticker} failed: {e}")

        df = pd.DataFrame(all_data)
        append_new_data(output_path, df)

if __name__ == "__main__":
    main()
