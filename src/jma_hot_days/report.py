"""Generate plots and a Markdown report from aggregated hot-day data."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

def _configure_cjk_font() -> None:
    """Pick the first available CJK-capable font for matplotlib labels."""
    import matplotlib.font_manager as fm

    available = {f.name for f in fm.fontManager.ttflist}
    for cand in (
        "Hiragino Sans",
        "Hiragino Maru Gothic Pro",
        "Yu Gothic",
        "Noto Sans CJK JP",
        "IPAexGothic",
        "TakaoGothic",
        "Meiryo",
        "Osaka",
    ):
        if cand in available:
            matplotlib.rcParams["font.family"] = cand
            matplotlib.rcParams["axes.unicode_minus"] = False
            return


_configure_cjk_font()

logger = logging.getLogger(__name__)


def plot_national_trend(trend: pd.DataFrame, out_path: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax1.bar(trend["year"], trend["total_station_days"], color="#cc4125", alpha=0.85)
    ax1.set_ylabel("Total station-days (>=35C, summed across stations)")
    ax1.set_xlabel("Year")
    ax2 = ax1.twinx()
    ax2.plot(
        trend["year"],
        trend["mean_per_station"],
        color="#1f77b4",
        marker="o",
        linewidth=2,
        label="mean per station",
    )
    ax2.set_ylabel("Mean hot days per station")
    ax1.set_title("Japan: annual hot days (>=35C) - last 10 years")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_top_stations(summary: pd.DataFrame, out_path: Path, n: int = 25) -> None:
    top = summary.head(n).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, max(5, 0.32 * n)))
    labels = top["name"] + " (" + top["pref_name"] + ")"
    ax.barh(labels, top["total_hot_days"], color="#cc4125")
    ax.set_xlabel(f"Total hot days over window ({n} stations shown)")
    ax.set_title(f"Top {n} hottest stations (10-year total of >=35C days)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def make_station_map(
    summary_with_coords: pd.DataFrame,
    out_path: Path,
    metric: str = "mean_hot_days",
) -> None:
    """Create an HTML folium map; circle radius scaled by ``metric``."""
    import folium

    df = summary_with_coords.dropna(subset=["lat", "lon", metric]).copy()
    if df.empty:
        logger.warning("no stations with coords; skipping map")
        return
    m = folium.Map(location=[37.5, 137.0], zoom_start=5, tiles="cartodbpositron")

    vmax = max(df[metric].max(), 1.0)
    for _, row in df.iterrows():
        v = float(row[metric])
        if v <= 0:
            color = "#3b82f6"  # blue for zero
            radius = 2.0
        else:
            ratio = min(v / vmax, 1.0)
            # red intensity proportional to value
            r = int(180 + 75 * ratio)
            g = int(80 - 50 * ratio)
            color = f"#{r:02x}{g:02x}30"
            radius = 2.5 + 8.5 * ratio
        popup = (
            f"<b>{row['name']}</b> ({row['pref_name']})<br>"
            f"mean hot days/yr: {row['mean_hot_days']:.1f}<br>"
            f"total: {row['total_hot_days']:.0f} "
            f"(years with data: {int(row['years_with_data'])})"
        )
        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            weight=0.6,
            popup=folium.Popup(popup, max_width=260),
        ).add_to(m)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))


def plot_prefecture_summary(pref: pd.DataFrame, out_path: Path) -> None:
    df = pref.sort_values("mean_hot_days_per_station_year", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 11))
    ax.barh(
        df["pref_name"],
        df["mean_hot_days_per_station_year"],
        color="#cc4125",
    )
    ax.invert_yaxis()
    ax.set_xlabel("Mean hot days per station-year (window)")
    ax.set_title("Prefecture-level mean hot days per station-year")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def write_markdown_report(
    *,
    window: tuple[int, int],
    n_stations_total: int,
    n_stations_with_data: int,
    summary: pd.DataFrame,
    pref: pd.DataFrame,
    trend: pd.DataFrame,
    figures_dir: Path,
    out_path: Path,
) -> None:
    start, end = window
    lines: list[str] = []
    lines.append(f"# JMA extreme hot days (>=35C) - {start}-{end}\n")
    lines.append("")
    lines.append("## Coverage")
    lines.append(
        f"- Stations in catalog: **{n_stations_total}**"
    )
    lines.append(
        f"- Stations with at least one parsed hot-day value in window: "
        f"**{n_stations_with_data}**"
    )
    lines.append("")
    lines.append("## National annual trend")
    lines.append("")
    lines.append(
        "![national trend](" + str(figures_dir.name) + "/national_trend.png)"
    )
    lines.append("")
    lines.append(trend.to_markdown(index=False, floatfmt=".2f"))
    lines.append("")

    lines.append("## Top 25 hottest stations (10-year total)")
    lines.append("")
    lines.append(
        "![top stations](" + str(figures_dir.name) + "/top_stations.png)"
    )
    lines.append("")
    cols = [
        "name",
        "pref_name",
        "kind",
        "total_hot_days",
        "mean_hot_days",
        "max_hot_days",
        "years_with_data",
    ]
    lines.append(summary[cols].head(25).to_markdown(index=False, floatfmt=".2f"))
    lines.append("")

    lines.append("## Prefecture-level summary")
    lines.append("")
    lines.append(
        "![prefecture summary](" + str(figures_dir.name) + "/prefecture_summary.png)"
    )
    lines.append("")
    lines.append(pref.to_markdown(index=False, floatfmt=".2f"))
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Source: 気象庁 過去の気象データ "
        "(https://www.data.jma.go.jp/obd/stats/etrn/)."
    )
    lines.append(
        "- '猛暑日' is JMA's official term for a day with daily maximum "
        "temperature >= 35 degC."
    )
    lines.append(
        "- AMeDAS-only stations and stations newly installed within the "
        "window may have shorter coverage; see `years_with_data` per station."
    )
    lines.append(
        "- Values flagged with ')' or similar in JMA tables indicate "
        "incomplete monthly aggregation; they are kept as-is in this dataset "
        "with the original flag preserved in the `flag` column."
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")
