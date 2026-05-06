# hot_days

日本全国の観測地点における **猛暑日（日最高気温 ≥ 35°C）** の年間日数を、
気象庁の公開データから集計するパイプライン。

直近10年（2016–2025年）について、気象官署 (160地点) と AMeDAS (1519地点) を
すべて対象とする。

## Output (after running)

- `data/processed/stations.csv` – station catalog (1679 entries)
- `data/processed/hot_days_long.{csv,parquet}` – tidy table: (station × year → hot_days)
- `data/processed/summary_by_station.csv` – 10-year totals & means per station
- `data/processed/summary_by_prefecture.csv` – per-prefecture aggregates
- `data/processed/national_annual_trend.csv` – national totals per year
- `reports/hot_days_report.md` – Markdown report
- `reports/figures/{national_trend,top_stations,prefecture_summary}.png`
- `reports/hot_days_map.html` – interactive station map (folium)

## Usage

```bash
uv sync
uv run jma-hot-days catalog                       # build station list
uv run jma-hot-days fetch                         # download + parse (~30 min, polite 1 req/s)
uv run jma-hot-days report --start 2016 --end 2025
```

`uv run jma-hot-days all` runs the three stages back-to-back.

## Source

気象庁 過去の気象データ検索 https://www.data.jma.go.jp/obd/stats/etrn/  
Year-by-year detail page (view=a2) → column `各階級の日数（最高） ≧35℃`.
Coordinates from `https://www.jma.go.jp/bosai/amedas/const/amedastable.json`.
