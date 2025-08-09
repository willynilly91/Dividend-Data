import os
import pandas as pd

# ----------------------------
# Config
# ----------------------------
FREQ_MULTIPLIER = {
    "weekly": 52,
    "bi-weekly": 26,
    "biweekly": 26,        # normalized to bi-weekly
    "semi-monthly": 24,
    "semimonthly": 24,     # normalized to semi-monthly
    "monthly": 12,
    "quarterly": 4,
    "semi-annual": 2,
    "semiannual": 2,       # normalized to semi-annual
    "annual": 1,
    "annually": 1,         # normalized to annual
}

HISTORY_FILES = [
    ("historical_yield_canada.csv", "yield_stats_canada.csv"),
    ("historical_yield_us.csv",     "yield_stats_us.csv"),
]


# ----------------------------
# Helpers
# ----------------------------
def _to_float(x):
    s = str(x).strip().replace(",", "").replace("$", "")
    if s in ("", "-", "—", "None", "nan", "NaN"):
        return float("nan")
    try:
        return float(s)
    except Exception:
        s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
        return float(s) if s else float("nan")


def infer_frequency_from_days(days: float) -> str:
    """Map days-between to a frequency label."""
    if pd.isna(days) or days <= 0:
        return "monthly"              # safe default
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


def normalize_freq_label(label: str) -> str:
    if pd.isna(label):
        return ""
    s = str(label).strip().lower().replace("_", "").replace(" ", "")
    if s == "biweekly":
        return "bi-weekly"
    if s == "semimonthly":
        return "semi-monthly"
    if s == "semiannual":
        return "semi-annual"
    return s


# ----------------------------
# Core steps
# ----------------------------
def update_frequencies_inplace(path: str) -> pd.DataFrame | None:
    """Update/insert the Frequency column using next-date spacing (fallback to prev for last row)."""
    if not os.path.exists(path):
        print(f"[SKIP] {path} not found.")
        return None

    df = pd.read_csv(path)
    if df.empty:
        print(f"[SKIP] {path} is empty.")
        return None

    for col in ("Ticker", "Ex-Div Date"):
        if col not in df.columns:
            print(f"[SKIP] {path} missing required column: {col}")
            return None

    df["Ex-Div Date"] = pd.to_datetime(df["Ex-Div Date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["Ex-Div Date"])

    out_frames = []
    for ticker, g in df.sort_values(["Ticker", "Ex-Div Date"]).groupby("Ticker", sort=False):
        g = g.copy().reset_index(drop=True)
        g["next_date"] = g["Ex-Div Date"].shift(-1)
        g["prev_date"] = g["Ex-Div Date"].shift(1)

        days_to_next = (g["next_date"] - g["Ex-Div Date"]).dt.days
        days_from_prev = (g["Ex-Div Date"] - g["prev_date"]).dt.days

        # Prefer spacing to NEXT payment; if missing or invalid, use spacing from PREVIOUS
        days = days_to_next.where(days_to_next.notna() & (days_to_next > 0), days_from_prev)

        g["Frequency"] = days.apply(infer_frequency_from_days)
        g.drop(columns=["next_date", "prev_date"], inplace=True)
        out_frames.append(g)

    result = pd.concat(out_frames, ignore_index=True)

    # Place Frequency after Ex-Div Date if it didn't exist before
    if "Frequency" not in df.columns:
        cols = list(result.columns)
        cols.remove("Frequency")
        try:
            insert_at = cols.index("Ex-Div Date") + 1
        except ValueError:
            insert_at = len(cols)
        cols = cols[:insert_at] + ["Frequency"] + cols[insert_at:]
        result = result[cols]

    result.to_csv(path, index=False)
    print(f"✅ Updated Frequency in {path}")
    return result


def recalc_yields_inplace(path: str):
    """Recalculate Annualized Yield % in-place using the (now corrected) Frequency."""
    if not os.path.exists(path):
        print(f"[SKIP] {path} not found.")
        return

    df = pd.read_csv(path)
    if df.empty:
        print(f"[SKIP] {path} is empty.")
        return

    required = {"Dividend", "Price on Ex-Date", "Frequency"}
    if not required.issubset(df.columns):
        print(f"[SKIP] {path} missing required columns: {required - set(df.columns)}")
        return

    df["Dividend"] = df["Dividend"].apply(_to_float)
    df["Price on Ex-Date"] = df["Price on Ex-Date"].apply(_to_float)
    df["Frequency"] = df["Frequency"].apply(normalize_freq_label)

    def calc_row(freq: str, div: float, price: float) -> float:
        try:
            mult = FREQ_MULTIPLIER.get(freq, 12)  # default monthly
            if pd.isna(div) or pd.isna(price) or price == 0:
                return float("nan")
            return round((div * mult / price) * 100.0, 4)
        except Exception:
            return float("nan")

    df["Annualized Yield %"] = [
        calc_row(f, d, p) for f, d, p in zip(df["Frequency"], df["Dividend"], df["Price on Ex-Date"])
    ]

    df.to_csv(path, index=False)
    print(f"✅ Recalculated Annualized Yield % in {path}")


def regenerate_stats(history_csv: str, stats_csv: str):
    if not os.path.exists(history_csv):
        print(f"[SKIP STATS] {history_csv} not found.")
        return
    df = pd.read_csv(history_csv)
    if df.empty or "Annualized Yield %" not in df.columns:
        print(f"[SKIP STATS] {history_csv} empty or missing 'Annualized Yield %'.")
        return

    good = df.dropna(subset=["Annualized Yield %"])
    if good.empty:
        print(f"[NOTE] No valid yield rows to summarize in {history_csv}")
        return

    g = good.groupby("Ticker")["Annualized Yield %"]
    out = pd.DataFrame({
        "Ticker": g.mean().index,
        "Mean Yield %": g.mean().values,
        "Median Yield %": g.median().values,
        "Std Dev %": g.std().values,
    }).sort_values(by="Mean Yield %", ascending=False)

    out.to_csv(stats_csv, index=False)
    print(f"✅ Updated stats: {stats_csv}")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    for hist_path, stats_path in HISTORY_FILES:
        # 1) Fix / infer frequency using next-date spacing
        updated = update_frequencies_inplace(hist_path)
        # 2) Recalculate yields using frequency multipliers
        recalc_yields_inplace(hist_path)
        # 3) Regenerate summary stats (mean/median/std)
        regenerate_stats(hist_path, stats_path)
