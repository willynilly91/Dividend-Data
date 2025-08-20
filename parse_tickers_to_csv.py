#!/usr/bin/env python3
import argparse
import csv
import sys
from pathlib import Path
from datetime import datetime

HEADER = ["Ticker", "Company Name", "Yield", "Ex-Dividend Date", "Yield_Value"]

def parse_args():
    p = argparse.ArgumentParser(description="Parse TSX tickers list into a sorted CSV.")
    p.add_argument(
        "-i", "--input",
        type=Path,
        required=True,
        help="Path to the raw text file (Ticker/Name/Yield/Date in 4-line blocks)."
    )
    p.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("tsx_dividends.csv"),
        help="Output CSV path (default: tsx_dividends.csv)"
    )
    return p.parse_args()

def clean_yield(y):
    """Return (as_text, numeric_value_or_empty)."""
    y_txt = y.strip()
    core = y_txt.replace(",", "").replace(" ", "").rstrip("%")
    if core == "":
        return (y_txt, None)
    try:
        return (y_txt, float(core))
    except ValueError:
        return (y_txt, None)

def clean_date(d):
    """Normalize to YYYY-MM-DD if possible; otherwise return original."""
    s = d.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return s

def read_lines(path: Path):
    raw = path.read_text(encoding="utf-8", errors="replace")
    return [ln.strip() for ln in raw.splitlines() if ln.strip()]

def group_in_fours(lines):
    return [lines[i:i+4] for i in range(0, len(lines), 4)]

def main():
    args = parse_args()
    lines = read_lines(args.input)
    blocks = group_in_fours(lines)

    rows = []
    for block in blocks:
        if len(block) != 4:
            print(f"Skipping malformed block: {block}", file=sys.stderr)
            continue
        ticker, company, yld, ex_date = block
        y_text, y_num = clean_yield(yld)
        ex_norm = clean_date(ex_date)
        rows.append([ticker, company, y_text, ex_norm, y_num])

    # Sort by Yield_Value descending (highest first)
    rows.sort(key=lambda r: (r[4] if r[4] is not None else -1), reverse=True)

    with args.output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(rows)

    print(f"✅ Wrote {len(rows)} rows sorted by Yield to {args.output}")

if __name__ == "__main__":
    main()
