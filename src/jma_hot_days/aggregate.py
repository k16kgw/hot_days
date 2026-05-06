"""Aggregations and report figures for JMA hot-day counts."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def restrict_to_window(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    return df[(df["year"] >= start) & (df["year"] <= end)].copy()


def per_station_summary(df_window: pd.DataFrame, n_years: int) -> pd.DataFrame:
    g = df_window.groupby(
        ["prec_no", "block_no", "name", "kind", "pref_name"], dropna=False
    )
    summary = g.agg(
        total_hot_days=("hot_days", "sum"),
        mean_hot_days=("hot_days", "mean"),
        max_hot_days=("hot_days", "max"),
        years_with_data=("hot_days", lambda s: s.notna().sum()),
    ).reset_index()
    summary["coverage"] = summary["years_with_data"] / n_years
    return summary.sort_values("total_hot_days", ascending=False)


def per_prefecture_summary(df_window: pd.DataFrame) -> pd.DataFrame:
    return (
        df_window.groupby(["prec_no", "pref_name"], dropna=False)["hot_days"]
        .agg(["sum", "mean", "max", "count"])
        .reset_index()
        .rename(
            columns={
                "sum": "total_hot_days",
                "mean": "mean_hot_days_per_station_year",
                "max": "max_station_year",
                "count": "station_year_obs",
            }
        )
        .sort_values("total_hot_days", ascending=False)
    )


def national_annual_trend(df_window: pd.DataFrame) -> pd.DataFrame:
    return (
        df_window.dropna(subset=["hot_days"])
        .groupby("year")["hot_days"]
        .agg(
            stations_reporting="count",
            total_station_days="sum",
            mean_per_station="mean",
            median_per_station="median",
        )
        .reset_index()
    )
