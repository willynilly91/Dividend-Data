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

# How we map day-gaps to frequency labels (unchanged)
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

def rel_diff(a: float, b: float) -> float:
    """Relative difference in [0, 1]; NaN if either is NaN."""
    if pd.isna(a) or pd.isna(b):
        return float("nan")
    m = max(abs(a), abs(b))
    if m == 0:
        return 0.0
    return abs(a - b) / m

# ----------------------------
# Core steps (improved logic)
# ----------------------------

def update_frequencies_inplace(path: str) -> pd.DataFrame | None:
    """
    Update/insert the Frequency column using a *hybrid* approach:
      1) For each row, compute spacing to NEXT and PREV ex-div dates.
      2) Decide whether to use NEXT or PREV spacing based on which neighbor's
         dividend AMOUNT is more similar to the current one (lower relative diff),
         with a slight bias toward NEXT (to keep original behavior when amounts are close).
      3) Map the chosen spacing to a frequency label.

    This fixes cases like a final monthly (large amount) followed by new weekly cadence (smaller amount):
    we will prefer the PREV spacing because the current amount is closer to the previous regime.
    """
    if not os.path.exists(path):
        print(f"[SKIP] {path} not found.")
        return None

    df = pd.read_csv(path)
    if df.empty:
        print(f"[SKIP] {path} is empty.")
        return None

    for col in ("Ticker", "Ex-Div Date", "Dividend"):
        if col not in df.columns:
            print(f"[SKIP] {path} missing required column: {col}")
            return None

    # Normalize types
    df["Ex-Div Date"] = pd.to_datetime(df["Ex-Div Date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["Ex-Div Date"])
    df["Dividend"] = df["Dividend"].apply(_to_float)

    out_frames = []
    for ticker, g in df.sort_values(["Ticker", "Ex-Div Date"]).groupby("Ticker", sort=False):
        g = g.copy().reset_index(drop=True)

        # Neighbor dates & amounts
        g["next_date"]_]()
