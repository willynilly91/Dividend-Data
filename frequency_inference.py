#!/usr/bin/env python3
"""
Infer and (re)write cash distribution Frequency per row using a hybrid neighbor-based approach.

Logic per Ticker (sorted by Ex-Div Date ascending):
  1) Compute spacing in days to the NEXT and PREV ex-div dates.
  2) Compare how similar the current Dividend AMOUNT is to the neighbor's amount:
       - Use relative difference elementwise.
       - Prefer the neighbor whose amount is closer to the current amount,
         with a small bias toward NEXT to preserve forward-looking behavior.
  3) Map the chosen day-gap to a frequency label.
  4) (Optional) If 'Annualized Yield %' exists, recompute it using:
       annualized_yield = (Dividend * FREQ_MULTIPLIER[Frequency]) / Price_on_Ex_Date * 100

Files updated IN-PLACE:
  - historical_yield_canada.csv
  - historical_yield_us.csv
"""

from __future__ import annotations
import os
import sys
import pandas as pd
import numpy as np

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
# Frequency mapping from day-gaps
# ----------------------------
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
def _to_float(x) -> float:
    s = str(x).strip().replace(",", "").replace("$", "")
    if s in ("", "-", "—", "None", "nan", "NaN"):
        return float("nan")
    try:
        return float(s)
    except Exception:
        s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
        return float(s) if s else float("nan")

def normalize_freq_key(series: pd.Series) -> pd.Series:
    """Normalize frequency labels to keys for FREQ_MULTIPLIER."""
    s = (
        series.astype(str)
        .str.lower()
        .str.strip()
        .str.replace("_", "", regex=False)
        .str.replace(" ", "", regex=False)
        .replace({"biweekly":"bi-weekly", "semimonthly":"semi-monthly", "semiannual":"semi-annual"})
    )
    return s

def rel_diff_series(a: pd.Series, b: pd.Series) -> pd.Series:
    """
    Elementwise relative difference in [0,1]: |a-b| / max(|a|,|b|)
    Returns NaN where either side is NaN.
    """
    a = a.astype(float)
    b = b.astype(float)
    m = np.maximum(np.abs(a), np.abs(b))
    out = np.where(m == 0, 0.0, np.abs(a - b) / m)
    out = pd.Series(out, index=a.index, dtype="float64")
    # set NaN where either input is NaN
    out[pd.isna(a) | pd.isna(b)] = np.nan
    return out

# ----------------------------
# Core
# ----------------------------
def update_frequencies_inplace(path: str) -> pd.DataFrame | None:
    """
    Update/insert the Frequency column using a *hybrid* approach (NEXT vs PREV neighbor).
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
    df = df.dropna(subset=["Ex-Div Date"]).copy()
    df["Dividend"] = df["Dividend"].apply(_to_float)

    original_cols = list(df.columns)
    out_frames = []

    for ticker, g in df.sort_values(["Ticker", "Ex-Div Date"]).groupby("Ticker", sort=False):
        g = g.copy().reset_index(drop=True)

        # Neighbor dates & amounts
        g["next_date"] = g["Ex-Div Date"].shift(-1)
        g["prev_date"] = g["Ex-Div Date"].shift(1)
        g["next_amt"]  = g["Dividend"].shift(-1)
        g["prev_amt"]  = g["Dividend"].shift(1)

        # Day gaps
        g["days_to_next"] = (g["next_date"] - g["Ex-Div Date"]).dt.days
        g["days_to_prev"] = (g["Ex-Div Date"] - g["prev_date"]).dt.days

        # Relative diffs (vectorized)
        g["reldiff_next"] = rel_diff_series(g["Dividend"], g["next_amt"])
        g["reldiff_prev"] = rel_diff_series(g["Dividend"], g["prev_amt"])

        # Prefer PREV only if it's clearly closer by >5% than NEXT
        prefer_prev = (g["reldiff_prev"] + 0.05) < g["reldiff_next"]

        # Choose spacing to use
        g["freq_days"] = g["days_to_next"]
        g.loc[prefer_prev, "freq_days"] = g.loc[prefer_prev, "days_to_prev"]

        # Map to frequency labels
        g["Frequency"] = g["freq_days"].apply(infer_frequency_from_days)

        # Drop helper cols
        g = g.drop(columns=[
            "next_date","prev_date","next_amt","prev_amt",
            "days_to_next","days_to_prev","reldiff_next","reldiff_prev","freq_days"
        ])

        out_frames.append(g)

    out = pd.concat(out_frames, ignore_index=True)

    # Optional: recompute Annualized Yield % if present and price exists
    if "Annualized Yield %" in out.columns:
        if "Price on Ex-Date" in out.columns:
            price = out["Price on Ex-Date"].apply(_to_float)
            div   = out["Dividend"].apply(_to_float)
            freq_key = normalize_freq_key(out["Frequency"])
            mult = freq_key.map(FREQ_MULTIPLIER).fillna(12)  # default to monthly if unknown
            with pd.option_context("mode.use_inf_as_na", True):
                out["Annualized Yield %"] = (div * mult / price) * 100
        else:
            print("[WARN] Missing 'Price on Ex-Date'; skipping Annualized Yield % recompute.")

    # Preserve original column order where possible
    if "Frequency" not in original_cols:
        cols = [c for c in original_cols if c in out.columns] + [c for c in out.columns if c not in original_cols]
    else:
        cols = original_cols

    out = out[cols]

    out.to_csv(path, index=False)
    print(f"[OK] Updated frequencies in {path} (rows: {len(out)})")
    return out

# ----------------------------
# Main
# ----------------------------
def main(argv: list[str]) -> int:
    # Allow optional CLI arguments for custom CSV paths
    if len(argv) > 1:
        targets = [(p, None) for p in argv[1:]]
    else:
        targets = HISTORY_FILES

    any_success = False
    for hist_csv, _ in targets:
        try:
            res = update_frequencies_inplace(hist_csv)
            any_success = any_success or (res is not None and not res.empty)
        except Exception as e:
            print(f"Error:  Failed to update {hist_csv}: {e}")

    return 0 if any_success else 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
