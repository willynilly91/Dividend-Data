import pandas as pd
import os

# Map days-between to a frequency label (lowercase to match your main script)
def infer_frequency(days: int) -> str:
    if pd.isna(days) or days <= 0:
        return "monthly"  # safe default if spacing can't be inferred
    if days < 10:
        return "weekly"
    if days < 20:
        return "bi-weekly"
    if days < 25:
        return "semi-monthly"
    if days < 60:
        return "monthly"
    if days < 130:
        return "quarterly"
    if days < 250:
        return "semi-annual"
    return "annual"

def update_file(path: str):
    if not os.path.exists(path):
        print(f"[SKIP] {path} not found.")
        return

    df = pd.read_csv(path)
    if df.empty:
        print(f"[SKIP] {path} is empty.")
        return

    # Required columns check
    for col in ("Ticker", "Ex-Div Date"):
        if col not in df.columns:
            print(f"[SKIP] {path} missing required column: {col}")
            return

    # Work on a copy
    df["Ex-Div Date"] = pd.to_datetime(df["Ex-Div Date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["Ex-Div Date"])

    # Sort and compute frequency per-ticker
    out = []
    for ticker, g in df.sort_values(["Ticker", "Ex-Div Date"]).groupby("Ticker", sort=False):
        g = g.copy().reset_index(drop=True)
        g["next_date"] = g["Ex-Div Date"].shift(-1)
        g["prev_date"] = g["Ex-Div Date"].shift(1)

        # Prefer spacing to NEXT payment; if missing (last row), use spacing from PREVIOUS
        days_to_next = (g["next_date"] - g["Ex-Div Date"]).dt.days
        days_from_prev = (g["Ex-Div Date"] - g["prev_date"]).dt.days

        # If days_to_next is NA or <=0, use days_from_prev
        days = days_to_next.where(days_to_next.notna() & (days_to_next > 0), days_from_prev)

        g["Frequency"] = days.apply(infer_frequency)
        g = g.drop(columns=["next_date", "prev_date"])
        out.append(g)

    result = pd.concat(out, ignore_index=True)

    # Keep original column order where possible (place Frequency after Ex-Div Date if not present)
    if "Frequency" not in df.columns:
        cols = list(result.columns)
        # move Frequency after Ex-Div Date
        cols.remove("Frequency")
        insert_at = cols.index("Ex-Div Date") + 1 if "Ex-Div Date" in cols else len(cols)
        cols = cols[:insert_at] + ["Frequency"] + cols[insert_at:]
        result = result[cols]

    result.to_csv(path, index=False)
    print(f"✅ Updated frequencies in {path} (using next-date spacing; fallback to previous for last rows).")

if __name__ == "__main__":
    update_file("historical_yield_canada.csv")
    update_file("historical_yield_us.csv")
