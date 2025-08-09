import pandas as pd
from collections import defaultdict

def infer_frequency(days):
    if days < 10:
        return "weekly"
    elif days < 20:
        return "bi-weekly"
    elif days < 25:
        return "semi-monthly"
    elif days < 60:
        return "monthly"
    elif days < 130:
        return "quarterly"
    elif days < 250:
        return "semi-annual"
    else:
        return "annual"

def update_frequency(df: pd.DataFrame) -> pd.DataFrame:
    updated = []
    grouped = df.sort_values("Ex-Div Date").groupby("Ticker")

    for ticker, group in grouped:
        group = group.sort_values("Ex-Div Date").reset_index(drop=True)
        inferred_freqs = []

        for i in range(len(group)):
            if i == 0:
                inferred_freqs.append(group.loc[i, "Frequency"])  # keep original for first entry
                continue
            prev_date = pd.to_datetime(group.loc[i - 1, "Ex-Div Date"])
            curr_date = pd.to_datetime(group.loc[i, "Ex-Div Date"])
            days_between = (curr_date - prev_date).days
            inferred = infer_frequency(days_between)
            inferred_freqs.append(inferred)

        group["Frequency"] = inferred_freqs
        updated.append(group)

    return pd.concat(updated, ignore_index=True)

def process_file(path: str):
    print(f"Updating: {path}")
    df = pd.read_csv(path)
    if "Ex-Div Date" not in df.columns:
        print(f"Skipping {path} — no 'Ex-Div Date' column")
        return
    df_updated = update_frequency(df)
    df_updated.to_csv(path, index=False)
    print(f"✅ Updated frequency in {path}")

if __name__ == "__main__":
    process_file("historical_yield_canada.csv")
    process_file("historical_yield_us.csv")
