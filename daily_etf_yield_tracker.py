name: Run Daily ETF Script

on:
  schedule:
    - cron: '0 13 * * *'  # Runs every day at 9am Eastern (13 UTC)
  workflow_dispatch:

permissions:
  contents: write

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install yfinance pandas lxml requests

      - name: Run Python script
        run: python daily_etf_yield_tracker.py

      - name: Commit and push results
        run: |
          git config user.name "github-actions"
          git config user.email "github-actions@github.com"
          git add etf_yields_canada.csv etf_yields_us.csv
          git commit -m "Auto-update ETF yield data" || echo "No changes to commit"
          git push
