#!/usr/bin/env python3
"""
Infer and (re)write cash distribution Frequency per row using a hybrid neighbor-based approach.

Logic per Ticker (sorted by Ex-Div Date ascending):
  1) Compute spacing in days to the NEXT and PREV ex-div dates.
  2) Compare how similar the current Dividend AMOUNT is to the neighbor's amount:
       - Use relative difference (|a-b| / max(|a|,|b|)).
       - Prefer the neighbor whose amount is closer to the current amount,
         with a small bias toward NEXT to preserve forward-looking behavior.
  3) Map the chosen day-gap to a frequency label.
  4) (Optional) If 'Annualized Yield %' exists, recompute it using:
       annualized_yield = (Dividend * FREQ_MULTIPLIER[Frequency]) / Price_on_Ex_Date * 100

Input files are updated IN-PLACE.

Files covered (in repo root by default):
  - historical_yield_canada.csv
  - historical_yield_us.csv
"""

from __future__ import annotations
import os
import sys
import math
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

# (stats files are listed but not modified here; kept for symmetry with your project layout)
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
# Core
# ----------------------------
def update_frequencies_inplace(path: str) -> pd.DataFrame | None:
    """
    Update/insert the Frequency column using a *hybrid* approach:
      - Consider spacing to NEXT and PREV ex-div dates.
      - Choose the neighbor whose dividend amount is closer to the current amount,
        with a small bias toward NEXT (keeps forward-looking behavior when amounts are similar).
      - Map chosen spacing (days) to a frequency label.
      - Recompute 'Annualized Yield %' if present.
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

    # Keep original columns to preserve ordering when we write back
    original_cols = list(df.columns)

    out_frames = []
    for ticker, g in df.sort_values(["Ticker", "Ex-Div Date"]).groupby("Ticker", sort=False):
        g = g.copy().reset_index(drop=True)

        # Neighbor dates & amounts
        g["next_date"] = g["Ex-Div Date"].shift(-1)
        g["prev_date"] = g["Ex-Div Date"].shift(1)
        g["next_amt"] = g["Dividend"].shift(-1)
        g["prev_amt"] = g["Dividend"].shift(1)

        # Day gaps
        g["days_to_next"] = (g["next_date"] - g["Ex-Div Date"]).dt.days
        g["days_to_prev"] = (g["Ex-Div Date"] - g["prev_date"]).dt.days

        # Relative diffs to neighbor amounts
        g["reldiff_next"] = rel_diff(g["Dividend"], g["next_amt"])
        g["reldiff_prev"] = rel_diff(g["Dividend"], g["prev_amt"])

        # Decide whether to prefer PREV spacing when it's clearly closer in amount
        # Bias: require prev to be closer by > 5% than next to override.
        prefer_prev = (g["reldiff_prev"] + 0.05) < g["reldiff_next"]

        # Pick the spacing to use
        g["freq_days"] = g["days_to_next"]
        g.loc[prefer_prev, "freq_days"] = g.loc[prefer_prev, "days_to_prev"]

        # Map to frequency labels
        g["Frequency"] = g["freq_days"].apply(infer_frequency_from_days)

        # Clean up helper cols before concatenation
        g = g.drop(columns=[
            "next_date", "prev_date", "next_amt", "prev_amt",
            "days_to_next", "days_to_prev", "reldiff_next", "reldiff_prev", "freq_days"
        ])

        out_frames.append(g)

    out = pd.concat(out_frames, ignore_index=True)

    # Optional: recompute 'Annualized Yield %' if column exists and required inputs are available
    if "Annualized Yield %" in out.columns:
        # normalize label to multiplier key
        freq_key = out["Frequency"].astype(str).str.lower().str.strip()
        freq_key = (
            freq_key.str.replace("_", "", regex=False)
                    .str.replace(" ", "", regex=False)
        ).replace({
            "biweekly": "bi-weekly",
            "semimonthly": "semi-monthly",
            "semiannual": "semi-annual",
        })
        mult = freq_key.map(FREQ_MULTIPLIER).fillna(12)  # conservative default = monthly
        price_col = "Price on Ex-Date" if "Price on Ex-Date" in out.columns else None

        if price_col is not None:
            # safe numeric
            price = out[price_col].apply(_to_float)
            div = out["Dividend"].apply(_to_float)
            with pd.option_context('mode.use_inf_as_na', True):
                out["Annualized Yield %"] = (div * mult / price) * 100
        else:
            print("[WARN] 'Price on Ex-Date' column missing; skipping Annualized Yield % recompute.")

    # Restore original column order where possible; ensure Frequency present
    if "Frequency" not in original_cols:
        # append Frequency at the end if it wasn’t present originally
        cols = [c for c in original_cols if c in out.columns] + [c for c in out.columns if c not in original_cols]
    else:
        cols = original_cols  # overwrite existing Frequency in-place

    out = out[cols]

    # Write back IN-PLACE
    out.to_csv(path, index=False)
    print(f"[OK] Updated frequencies in {path} (rows: {len(out)})")
    return out

# ----------------------------
# Main
# ----------------------------
def main(argv: list[str]) -> int:
    # Allow optional CLI paths; otherwise use defaults
    if len(argv) > 1:
        # user can pass specific CSVs to update
        targets = [(p, None) for p in argv[1:]]
    else:
        targets = HISTORY_FILES

    any_success = False
    for hist_csv, _stats_csv in targets:
        try:
            res = update_frequencies_inplace(hist_csv)
            any_success = any_success or (res is not None and not res.empty)
        except Exception as e:
            print(f"[ERROR] Failed to update {hist_csv}: {e}")

    return 0 if any_success else 1

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
