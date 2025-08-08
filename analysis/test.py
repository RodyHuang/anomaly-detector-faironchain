#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature Distribution Stats (Raw + Log1p + Nonzero-Log1p diagnostics)
--------------------------------------------------------------------
For each feature, compute distribution stats for:
- Raw
- Log1p (all samples)
- Nonzero subset after Log1p (for sparse features)
And produce scaling recommendations for highly sparse features.

Usage:
  python feature_stats_raw_log1p.py --csv path/to/features.csv \
      --features col1 col2 col3 --outdir ./out

Outputs:
  diagnostics_raw_log1p.csv (if --outdir specified)
"""

import argparse
import os
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
from scipy.stats import skew, kurtosis


def _safe_numeric(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return s.replace([np.inf, -np.inf], np.nan)


def _dist_stats(x: pd.Series, prefix: str) -> Dict[str, Any]:
    x = _safe_numeric(x)
    out: Dict[str, Any] = {}
    out[f"n_{prefix}"] = int(x.shape[0])
    out[f"n_missing_{prefix}"] = int(x.isna().sum())
    out[f"pct_missing_{prefix}"] = float(x.isna().mean())
    nn = x.dropna()
    out[f"n_unique_{prefix}"] = int(nn.nunique())
    if nn.shape[0] == 0:
        out[f"min_{prefix}"] = out[f"max_{prefix}"] = out[f"mean_{prefix}"] = out[f"std_{prefix}"] = None
        out[f"skew_{prefix}"] = out[f"kurtosis_{prefix}"] = None
        out[f"pct_zeros_{prefix}"] = None
        return out
    out[f"min_{prefix}"] = float(np.nanmin(nn))
    out[f"max_{prefix}"] = float(np.nanmax(nn))
    out[f"mean_{prefix}"] = float(np.nanmean(nn))
    out[f"std_{prefix}"] = float(np.nanstd(nn, ddof=1)) if nn.shape[0] > 1 else 0.0
    try:
        out[f"skew_{prefix}"] = float(skew(nn)) if nn.shape[0] > 2 else 0.0
    except Exception:
        out[f"skew_{prefix}"] = None
    try:
        out[f"kurtosis_{prefix}"] = float(kurtosis(nn, fisher=False)) if nn.shape[0] > 3 else 3.0
    except Exception:
        out[f"kurtosis_{prefix}"] = None
    out[f"pct_zeros_{prefix}"] = float((nn == 0).mean()) if nn.shape[0] else None
    return out


def _nonzero_log1p_stats(x: pd.Series) -> Dict[str, Any]:
    """
    Compute stats on non-zero subset after log1p.
    Also returns Q1, Q3, IQR for IQR==0 detection.
    """
    x = _safe_numeric(x)
    mask = (x != 0) & (~x.isna())
    nnz = x[mask]
    out: Dict[str, Any] = {
        "n_nonzero": int(mask.sum())
    }
    if nnz.shape[0] == 0:
        out.update({
            "skew_nz_log1p": None, "kurtosis_nz_log1p": None,
            "q1_nz_log1p": None, "q3_nz_log1p": None, "iqr_nz_log1p": None
        })
        return out

    nz_log = np.log1p(nnz)
    out["q1_nz_log1p"] = float(np.nanpercentile(nz_log, 25)) if nz_log.size > 0 else None
    out["q3_nz_log1p"] = float(np.nanpercentile(nz_log, 75)) if nz_log.size > 0 else None
    out["iqr_nz_log1p"] = (
        out["q3_nz_log1p"] - out["q1_nz_log1p"]
        if (out["q3_nz_log1p"] is not None and out["q1_nz_log1p"] is not None)
        else None
    )
    try:
        out["skew_nz_log1p"] = float(skew(nz_log)) if nz_log.size > 2 else 0.0
    except Exception:
        out["skew_nz_log1p"] = None
    try:
        out["kurtosis_nz_log1p"] = float(kurtosis(nz_log, fisher=False)) if nz_log.size > 3 else 3.0
    except Exception:
        out["kurtosis_nz_log1p"] = None
    return out


def _recommend_scaling(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Recommend scaling for sparse counts based on:
      - pct_zeros_raw
      - n_nonzero
      - skew_nz_log1p / kurtosis_nz_log1p
      - iqr_nz_log1p
    Returns: (recommendation, reason)
    """
    pct_zero = row.get("pct_zeros_raw", None)
    nnz = row.get("n_nonzero", 0)
    skew_nz = row.get("skew_nz_log1p", None)
    kurt_nz = row.get("kurtosis_nz_log1p", None)
    iqr_nz = row.get("iqr_nz_log1p", None)

    # Default
    rec = "no_special_treatment"
    reason = "Distribution not highly sparse; use normal log1p + standardization."

    if pct_zero is None:
        return rec, reason

    # Very sparse
    if pct_zero >= 0.95:
        # IQR==0 or too few nonzero -> binary (presence) or binary + log channel
        if (iqr_nz is None) or (iqr_nz == 0) or (nnz < 50):
            rec = "binary_presence_or_binary_plus_log"
            reason = (
                f"pct_zero={pct_zero:.2%}, n_nonzero={nnz}, IQR={iqr_nz}. "
                "Nonzero distribution too thin/degenerate; recommend binary presence. "
                "Optionally add a log-strength channel without scaling."
            )
        else:
            # decide standard vs robust on nz_log1p
            if (skew_nz is not None and kurt_nz is not None) and (skew_nz < 2 and kurt_nz < 10):
                rec = "log1p_then_standardize_nonzero_only"
                reason = (
                    f"pct_zero={pct_zero:.2%}, n_nonzero={nnz}, skew={skew_nz:.2f}, kurt={kurt_nz:.2f}, IQR={iqr_nz:.4g}. "
                    "Nonzero log1p approx normal; standardize only the nonzero part (keep zeros as 0)."
                )
            else:
                rec = "log1p_then_robust_scale_nonzero_only"
                reason = (
                    f"pct_zero={pct_zero:.2%}, n_nonzero={nnz}, skew={skew_nz:.2f}, kurt={kurt_nz:.2f}, IQR={iqr_nz:.4g}. "
                    "Nonzero log1p still heavy-tailed; robust-scale only the nonzero part (keep zeros as 0)."
                )
    else:
        # Not extremely sparse: treat normally
        if (skew_nz is not None and kurt_nz is not None) and (skew_nz < 2 and kurt_nz < 10):
            rec = "log1p_then_standardize"
            reason = (
                f"pct_zero={pct_zero:.2%}; overall OK after log1p. Use standard scaling."
            )
        else:
            rec = "log1p_then_robust_scale"
            reason = (
                f"pct_zero={pct_zero:.2%}; overall still heavy-tailed after log1p. Use robust scaling."
            )
    return rec, reason


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to your features CSV")
    ap.add_argument("--features", nargs="+", required=True, help="Feature column names to analyze")
    ap.add_argument("--outdir", default=None, help="If set, write diagnostics_raw_log1p.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    missing = [f for f in args.features if f not in df.columns]
    if missing:
        raise SystemExit(f"Features not in CSV: {missing}")

    rows = []
    for feat in args.features:
        s_raw = df[feat]
        s_log_all = np.log1p(_safe_numeric(s_raw).clip(lower=0))

        stats_raw = _dist_stats(s_raw, "raw")
        stats_log = _dist_stats(s_log_all, "log1p")
        stats_nz = _nonzero_log1p_stats(s_raw)

        row = {"feature": feat}
        row.update(stats_raw)
        row.update(stats_log)
        row.update(stats_nz)

        # Recommendation block
        rec, reason = _recommend_scaling({**row})
        row["scaling_recommendation"] = rec
        row["scaling_reason"] = reason

        rows.append(row)

        # Console summary
        print(f"\n==== {feat} ====")
        print(f"Raw:    skew={stats_raw['skew_raw']:.3f}, kurtosis={stats_raw['kurtosis_raw']:.3f}, %zeros={stats_raw['pct_zeros_raw']:.2%}")
        print(f"Log1p:  skew={stats_log['skew_log1p']:.3f}, kurtosis={stats_log['kurtosis_log1p']:.3f}, %zeros={stats_log['pct_zeros_log1p']:.2%}")
        print(f"NZ+Log: n_nonzero={stats_nz['n_nonzero']}, skew={stats_nz['skew_nz_log1p']}, kurt={stats_nz['kurtosis_nz_log1p']}, "
              f"Q1={stats_nz['q1_nz_log1p']}, Q3={stats_nz['q3_nz_log1p']}, IQR={stats_nz['iqr_nz_log1p']}")
        print(f"↳ Recommend: {rec}\n   Reason: {reason}")

    if args.outdir:
        os.makedirs(args.outdir, exist_ok=True)
        out_csv = os.path.join(args.outdir, "diagnostics_raw_log1p.csv")
        pd.DataFrame(rows).to_csv(out_csv, index=False)
        print(f"\n[OK] Wrote summary CSV: {out_csv}")


if __name__ == "__main__":
    main()
