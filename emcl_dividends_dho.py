import subprocess
import sys

# Ensure playwright is installed
try:
    from playwright.async_api import async_playwright
except ImportError:
    print("📦 Installing Playwright...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "pandas"])
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
    from playwright.async_api import async_playwright

import asyncio
import pandas as pd

# -----------------
# SETTINGS
# -----------------
TICKER = "EMCL"  # Ticker symbol
IS_TSX = True    # True = TSX path, False = US path
OUTPUT_CSV = "dividendhistory_emcl_only.csv"

async def scrape_dividend_history():
    base_url = "https://dividendhistory.org/payout/"
    url = f"{base_url}{'tsx/' if IS_TSX else ''}{TICKER}/"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"🌐 Loading {url}")
        await page.goto(url)

        # Try clicking "Show All" if present
        try:
            await page.click("button:has-text('Show All')")
            await page.wait_for_timeout(2000)  # Wait for the table to refresh
            print("📄 Clicked 'Show All'")
        except Exception as e:
            print(f"ℹ️ No 'Show All' button found: {e}")

        # Wait until table rows are loaded
        await page.wait_for_selector("#dividend_table tbody tr")

        # Extract table into pandas DataFrame
        html = await page.inner_html("#dividend_table")
        df = pd.read_html(f"<table>{html}</table>")[0]

        # Save to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Saved {len(df)} rows to {OUTPUT_CSV}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_dividend_history())
