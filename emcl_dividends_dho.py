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
        await page.goto(url, timeout=60000)  # allow up to 60 seconds to load

        # Wait for the dropdown to appear
        try:
            await page.wait_for_selector("select[name='dividend_table_length']", timeout=30000)
            # Select the maximum available (usually "100")
            await page.select_option("select[name='dividend_table_length']", "100")
            await page.wait_for_timeout(2000)  # wait for table refresh
            print("📄 Selected 100 entries per page")
        except Exception as e:
            print(f"⚠️ Could not set page size: {e}")

        # Wait until table rows are loaded (max 60s)
        try:
            await page.wait_for_selector("#dividend_table tbody tr", timeout=60000)
        except Exception as e:
            print(f"❌ Table rows not found: {e}")
            await browser.close()
            return

        # Extract table into pandas DataFrame
        html = await page.inner_html("#dividend_table")
        df = pd.read_html(f"<table>{html}</table>")[0]

        # Save to CSV
        df.to_csv(OUTPUT_CSV, index=False)
        print
