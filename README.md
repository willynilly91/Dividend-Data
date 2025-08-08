# ETF Yield Tracker

This repository includes Python tools for tracking and analyzing the yields of Canadian and U.S.-listed ETFs. It includes two primary components:

1. **Live Yield Tracker** — tracks current ETF yield based on most recent distribution and price.
2. **Historical Yield Tracker** — archives past yields and calculates mean and standard deviation for statistical analysis.

---

## 1. Live Yield Tracker

Tracks current yield metrics across your watchlist.

### Features

* Pulls **latest market price** using Yahoo Finance
* Scrapes **most recent dividend and distribution frequency** from DividendHistory.org
* Calculates **forward annualized yield**
* Outputs sorted CSVs by yield (Canada and U.S. separately)

### Files

* `daily_etf_yield_tracker.py`: Script for current yield tracking
* `tickers_canada.txt`, `tickers_us.txt`: Input tickers
* `etf_yields_canada.csv`, `etf_yields_us.csv`: Output CSVs

### GitHub Automation

The GitHub Actions workflow:

* Runs daily
* Automatically updates the CSVs
* Commits and pushes them to the repo

---

## 2. Historical Yield Tracker

Analyzes yield trends over time.

### Features

* Scrapes dividend history from DividendHistory.org
* Fetches historical **price on ex-div date** via Yahoo Finance
* Calculates **annualized yield** at each ex-div date
* Appends new entries without overwriting
* Computes **mean** and **standard deviation** of yield per ETF

### Files

* `historical_yield_tracker.py`: Main script
* `historical_yield_canada.csv`, `historical_yield_us.csv`: Yield history
* `yield_stats_canada.csv`, `yield_stats_us.csv`: Summary stats

### Manual Execution

Run manually a few times per year:

```bash
pip install requests pandas yfinance lxml
python historical_yield_tracker.py
```

GitHub Actions workflow is available but disabled by default.

---

## Use Cases

* 📈 Spot ETFs yielding above historical norms ("on sale")
* 💹 Monitor income trends from dividend ETFs
* 📊 Build automated dashboards in Google Sheets using CSV imports

---

## License

MIT
