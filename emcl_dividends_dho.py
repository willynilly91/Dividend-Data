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
            await page
