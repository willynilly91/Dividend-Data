# emcl_dividends_dho.py
"""
Scrape ALL dividend history for a single ticker (EMCL) from dividendhistory.org
and write it to a CSV named `emcl_dividends_dho.csv`.

This script ONLY touches EMCL and ONLY uses dividendhistory.org.
It always writes the CSV (even if empty) so a CI workflow can commit it.
"""

from datetime import datetime
from lxml import html
import pandas as pd
import requests

# -------- Settings --------
TICKER_REPO_STYLE = "EMCL.NE"   # how it appears in your lists
EMIT_TICKER = "EMCL"            # how it should appear in the CSV
IS_TSX = True                    # Canadian route on dividendhistory.org
OUT_CSV = "emcl_dividends_dho.csv"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36"
    )
}

# -------- Helpers --------
def dh_symbol(symbol: str) -> str:
    """Normalize repo-style symbols to dividendhistory.org slug."""
    s = symbol.replace("$", "").split(":")[-1]
    s = s.replace(".TO", "").replace(".NE", "").replace(".UN", "-UN")
    return s

# -------- Scraper --------
def fetch_dividends_from_dividendhistory(symbol_repo_style: str, is_tsx: bool):
    clean = dh_symbol(symbol_repo_style)
    url = (
        f"https://dividendhistory.org/payout/tsx/{clean}/"
        if is_tsx
        else f"https://dividendhistory.org/payout/{clean}/"
    )
    rows = []
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
        tree = html.fromstring(r.content)
        # iterate row-by-row to keep columns aligned even if structure shifts
        for tr in tree.xpath('//*[@id="dividend_table"]//tr[td]'):
            date_txt = "".join(tr.xpath('./td[1]//text()')).strip()
            div_txt  = "".join(tr.xpath('./td[3]//text()')).strip()
            if date_txt and div_txt:
                rows.append((date_txt, div_txt))
    except Exception as e:
        print(f"[DIVHIST FAIL] {clean} ({url}): {e}")
    return rows, url

# -------- Main --------
def main():
    rows, url = fetch_dividends_from_dividendhistory(TICKER_REPO_STYLE, IS_TSX)

    # Build DataFrame (always) so CI can commit the CSV
    if rows:
        df = pd.DataFrame(rows, columns=["Ex-Div Date", "Dividend"]).sort_values("Ex-Div Date")
        df.insert(0, "Ticker", EMIT_TICKER)
        df["Source"] = "dividendhistory.org"
        df["Scraped At"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    else:
        df = pd.DataFrame(columns=["Ticker", "Ex-Div Date", "Dividend", "Source", "Scraped At"])  # empty with headers

    df.to_csv(OUT_CSV, index=False)
    print(f"Saved {OUT_CSV} with {len(rows)} rows from {url}")

if __name__ == "__main__":
    main()
