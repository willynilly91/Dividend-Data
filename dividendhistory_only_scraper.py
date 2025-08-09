# dividendhistory_emcl_only.py
import requests
import pandas as pd
from lxml import html
from datetime import datetime

# Settings
TICKER = "EMCL"   # The symbol for dividendhistory.org (no .NE suffix)
IS_TSX = True
OUT_CSV = "dividendhistory_emcl_only.csv"

def fetch_dividends_from_dividendhistory(symbol: str, is_tsx: bool):
    """Scrape all dividend history from dividendhistory.org for a given ticker."""
    url = f"https://dividendhistory.org/payout/tsx/{symbol}/" if is_tsx else f"https://dividendhistory.org/payout/{symbol}/"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        tree = html.fromstring(r.content)
        dates = [d.strip() for d in tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[1]/text()')]
        dividends = [v.strip() for v in tree.xpath('//*[@id="dividend_table"]/tbody/tr/td[3]/text()')]
        return [(d, v) for d, v in zip(dates, dividends) if d and v]
    except Exception as e:
        print(f"[DIVHIST FAIL] {symbol} ({url}): {e}")
        return []

def main():
    rows = fetch_dividends_from_dividendhistory(TICKER, IS_TSX)
    if not rows:
        print(f"No data scraped for {TICKER}")
        return

    df = pd.DataFrame(rows, columns=["Ex-Div Date", "Dividend"])
    df.insert(0, "Ticker", TICKER)
    df["Scraped At"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    df.sort_values("Ex-Div Date", inplace=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"✅ Saved {OUT_CSV} with {len(df)} rows.")

if __name__ == "__main__":
    main()
