import asyncio
import pandas as pd
from playwright.async_api import async_playwright

# -----------------
# SETTINGS
# -----------------
TICKER = "EMCL"  # Change this if you want another ticker
IS_TSX = True    # True = TSX path, False = US path

async def scrape_dividend_history():
    base_url = "https://dividendhistory.org/payout/"
    url = f"{base_url}{'tsx/' if IS_TSX else ''}{TICKER}/"
    output_csv = f"{TICKER.lower()}_dividendhistory_full.csv"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"🌐 Loading {url}")
        await page.goto(url)

        # Try clicking "Show All" if it exists
        try:
            await page.click("button:has-text('Show All')")
            await page.wait_for_timeout(2000)  # wait for table update
            print("📄 Clicked 'Show All'")
        except:
            print("ℹ️ No 'Show All' button found")

        # Wait until table rows are loaded
        await page.wait_for_selector("#dividend_table tbody tr")

        # Extract table into pandas DataFrame
        html = await page.inner_html("#dividend_table")
        df = pd.read_html(f"<table>{html}</table>")[0]

        # Save to CSV
        df.to_csv(output_csv, index=False)
        print(f"✅ Saved {len(df)} rows to {output_csv}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_dividend_history())
