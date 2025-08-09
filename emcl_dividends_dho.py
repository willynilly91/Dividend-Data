import subprocess
import sys

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("📦 Installing Playwright...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright", "pandas"])
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
    from playwright.async_api import async_playwright

import asyncio
import pandas as pd

TICKER = "EMCL"
IS_TSX = True
OUTPUT_CSV = "emcl_dividends_dho.csv"

async def scrape_dividend_history():
    base_url = "https://dividendhistory.org/payout/"
    url = f"{base_url}{'tsx/' if IS_TSX else ''}{TICKER}/"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"🌐 Loading {url}")
        await page.goto(url, timeout=60000)

        # Set dropdown to 100
        try:
            await page.wait_for_selector("select[name='dividend_table_length']", timeout=30000)
            await page.select_option("select[name='dividend_table_length']", "100")
            await page.wait_for_timeout(2000)
            print("📄 Selected 100 entries per page")
        except Exception as e:
            print(f"⚠️ Could not set page size: {e}")

        # Try scraping table
        try:
            await page.wait_for_selector("#dividend_table tbody tr", timeout=60000)
            html = await page.inner_html("#dividend_table")
            df = pd.read_html(f"<table>{html}</table>")[0]
        except Exception as e:
            print(f"❌ Failed to load table rows: {e}")
            df = p
