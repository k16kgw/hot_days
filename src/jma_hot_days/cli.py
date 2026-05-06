"""Command-line entry point for the JMA hot-day pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from . import aggregate, annual, coords, report, stations

logger = logging.getLogger("jma_hot_days")

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RAW = DATA / "raw"
INTERIM = DATA / "interim"
PROCESSED = DATA / "processed"
REPORTS = ROOT / "reports"
FIGURES = REPORTS / "figures"


def cmd_catalog(_: argparse.Namespace) -> None:
    out_csv = PROCESSED / "stations.csv"
    df = stations.build_catalog(cache_dir=RAW / "pref", out_csv=out_csv)
    print(f"[ok] {len(df)} stations -> {out_csv}")
    print(df["kind"].value_counts().to_string())


def cmd_fetch(args: argparse.Namespace) -> None:
    catalog_path = PROCESSED / "stations.csv"
    if not catalog_path.exists():
        raise SystemExit("Run `catalog` first.")
    df = pd.read_csv(catalog_path, dtype={"prec_no": str, "block_no": str})
    if args.limit:
        df = df.head(args.limit)
    if args.kind:
        df = df[df["kind"].isin(args.kind.split(","))]
    raw_dir = RAW / "annual"
    parsed = annual.collect_all(df, cache_dir=raw_dir)
    out = INTERIM / "annual_hot_days.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    parsed.to_parquet(out)
    parsed.to_csv(INTERIM / "annual_hot_days.csv", index=False)
    print(f"[ok] {len(parsed)} (station, year) rows -> {out}")


def cmd_report(args: argparse.Namespace) -> None:
    parsed = pd.read_parquet(INTERIM / "annual_hot_days.parquet")
    parsed["prec_no"] = parsed["prec_no"].astype(str)
    parsed["block_no"] = parsed["block_no"].astype(str)

    start, end = args.start, args.end
    n_years = end - start + 1
    window = aggregate.restrict_to_window(parsed, start, end)
    summary = aggregate.per_station_summary(window, n_years)
    pref = aggregate.per_prefecture_summary(window)
    trend = aggregate.national_annual_trend(window)

    PROCESSED.mkdir(parents=True, exist_ok=True)
    FIGURES.mkdir(parents=True, exist_ok=True)
    summary.to_csv(PROCESSED / "summary_by_station.csv", index=False)
    pref.to_csv(PROCESSED / "summary_by_prefecture.csv", index=False)
    trend.to_csv(PROCESSED / "national_annual_trend.csv", index=False)
    window.to_csv(PROCESSED / "hot_days_long.csv", index=False)
    window.to_parquet(PROCESSED / "hot_days_long.parquet")

    report.plot_national_trend(trend, FIGURES / "national_trend.png")
    report.plot_top_stations(summary, FIGURES / "top_stations.png")
    report.plot_prefecture_summary(pref, FIGURES / "prefecture_summary.png")

    # Resolve station coordinates from JMA AMeDAS table and build national map
    try:
        amedas = coords.fetch_amedas_table(RAW / "amedas" / "amedastable.json")
        amedas_df = coords.amedas_table_to_df(amedas)
        merged = coords.merge_coords(summary, amedas_df)
        merged.to_csv(PROCESSED / "summary_by_station_with_coords.csv", index=False)
        report.make_station_map(merged, REPORTS / "hot_days_map.html")
        n_with_coords = merged.dropna(subset=["lat", "lon"]).shape[0]
        print(f"[ok] map: {n_with_coords}/{len(merged)} stations geocoded")
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] map generation failed: {exc}")

    n_total = parsed[["prec_no", "block_no"]].drop_duplicates().shape[0]
    n_with_data = (
        window.dropna(subset=["hot_days"])[["prec_no", "block_no"]]
        .drop_duplicates()
        .shape[0]
    )
    report.write_markdown_report(
        window=(start, end),
        n_stations_total=n_total,
        n_stations_with_data=n_with_data,
        summary=summary,
        pref=pref,
        trend=trend,
        figures_dir=FIGURES,
        out_path=REPORTS / "hot_days_report.md",
    )
    print("[ok] report written to", REPORTS / "hot_days_report.md")


def cmd_all(args: argparse.Namespace) -> None:
    cmd_catalog(args)
    cmd_fetch(args)
    cmd_report(args)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    p = argparse.ArgumentParser(prog="jma-hot-days")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_cat = sub.add_parser("catalog", help="build station catalog")
    p_cat.set_defaults(func=cmd_catalog)

    p_fetch = sub.add_parser("fetch", help="fetch and parse annual stats")
    p_fetch.add_argument("--limit", type=int, default=0)
    p_fetch.add_argument("--kind", type=str, default="", help="comma-separated: s,a")
    p_fetch.set_defaults(func=cmd_fetch)

    p_rep = sub.add_parser("report", help="aggregate and write report")
    p_rep.add_argument("--start", type=int, default=2016)
    p_rep.add_argument("--end", type=int, default=2025)
    p_rep.set_defaults(func=cmd_report)

    p_all = sub.add_parser("all", help="run catalog + fetch + report")
    p_all.add_argument("--limit", type=int, default=0)
    p_all.add_argument("--kind", type=str, default="")
    p_all.add_argument("--start", type=int, default=2016)
    p_all.add_argument("--end", type=int, default=2025)
    p_all.set_defaults(func=cmd_all)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
