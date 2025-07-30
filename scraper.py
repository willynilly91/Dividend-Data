import requests
from lxml import html

def get_price_yahoo(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}  # Needed to avoid blocking

    response = requests.get(url, headers=headers)
    tree = html.fromstring(response.content)

    # XPath to the current price element
    price_xpath = '//fin-streamer[@data-field="regularMarketPrice"]//text()'
    price = tree.xpath(price_xpath)

    if price:
        try:
            return float(price[0].replace(',', ''))
        except ValueError:
            return None
    return None

if __name__ == "__main__":
    ticker = "YNVD.TO"  # Change to your desired ticker
    price = get_price_yahoo(ticker)
    if price:
        print(f"Live price for {ticker} is: {price}")
    else:
        print(f"Could not fetch price for {ticker}")
