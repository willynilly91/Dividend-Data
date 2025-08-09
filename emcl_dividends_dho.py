# emcl_dividends_dho.py
import asyncio
import pandas as pd
from playwright.async_api import async_playwright

async def scrape_dividend_history():
    ticker = "EMCL"
    url = f"https://dividendhistory.org/payout/tsx/{ticker}/"

    print(f"🌐 Loading {url} ...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, timeout=60000)

        # Set page size to maximum (usually 100)
        try:
            await page.select_option("select[name='dividend_table_length']", "100")
            print("✅ Set page size to maximum")
            await page.wait_for_timeout(1500)  # wait for table to reload
        except Exception as e:
            print(f"⚠️ Could not change page size: {e}")

        # Wait for table rows to load
        await page.wait_for_selector("#dividend_table tbody tr")

        # Extract table rows
        rows = await page.query_selector_all("#dividend_table tbody tr")
        data = []
        for row in rows:
            cells = await row.query_selector_all("td")
            if not cells:
                continue
            row_data = [await cell.inner_text() for cell in cells]
            data.append(row_data)

        await browser.close()

    if not data:
        print("⚠️ No data found.")
    else:
        headers = ["Ex-Div Date", "Type", "Cash Amount", "Declaration Date", "Record Date", "Payment Date"]
        df = pd.DataFrame(data, columns=headers[:len(data[0])])
        df.to_csv("emcl_dividends_dho.csv", index=False)
        print(f"💾 Saved emcl_dividends_dho.csv with {len(df)} rows.")

if __name__ == "__main__":
    asyncio.run(scrape_dividend_history())
