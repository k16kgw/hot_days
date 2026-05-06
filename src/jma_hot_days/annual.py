"""Fetch and parse JMA annual (year-by-year) statistics pages.

The page used is the 'detail (temperature/vapor/humidity)' view (view=a2):

  気象官署 (5-digit block_no):
    https://www.data.jma.go.jp/obd/stats/etrn/view/annually_s.php?prec_no=&block_no=&view=a2
  AMeDAS-only (4-digit block_no):
    https://www.data.jma.go.jp/obd/stats/etrn/view/annually_a.php?prec_no=&block_no=&view=a2

That table has a multi-level header. The column we want lives at:
    気温(C)  -> 各階級の日数（最高） -> >=35C   (a.k.a. 猛暑日)

Header rows have ``rowspan``/``colspan`` attributes; we expand them into a
2D grid and label each leaf column by joining its ancestor labels.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from .fetch import fetch_cached
from .stations import Station, _decode

logger = logging.getLogger(__name__)

BASE = "https://www.data.jma.go.jp/obd/stats/etrn/view"

YEAR_RE = re.compile(r"^\s*(\d{4})\s*$")

# Column we want: nested under '各階級の日数（最高）' and labelled with '35'.
HOT_HIGH_GROUP_RE = re.compile(r"各階級の日数[（(]最高[）)]")
HOT_THRESHOLD_RE = re.compile(r"35")


@dataclass(frozen=True)
class AnnualHotDays:
    prec_no: str
    block_no: str
    year: int
    hot_days: float | None
    flag: str = ""


def annual_url(station: Station) -> str:
    page = "annually_s.php" if station.is_full_station else "annually_a.php"
    return (
        f"{BASE}/{page}?prec_no={station.prec_no}&block_no={station.block_no}"
        f"&year=&month=&day=&view=a2"
    )


def _clean_value(text: str) -> tuple[float | None, str]:
    """Parse a JMA cell. Trailing ']', ')', '*', and '#' indicate flags."""
    s = text.strip().replace("\xa0", "")
    if not s or s in {"--", "///", "×"}:
        return None, s
    flag = ""
    while s and s[-1] in "])》*#":
        flag = s[-1] + flag
        s = s[:-1].strip()
    if not s or s in {"--", "///"}:
        return None, flag
    try:
        return float(s), flag
    except ValueError:
        return None, s


def _expand_header_grid(table) -> tuple[list[list[str]], int]:
    """Expand the table's <th> header into a rectangular grid.

    Returns (grid, n_header_rows). ``grid[r][c]`` holds the label assigned to
    the (r, c) cell after rowspan/colspan expansion. Non-header rows are
    skipped: header rows are detected as those that contain only ``<th>``
    elements.
    """
    grid: list[list[str]] = []
    n_header_rows = 0
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells or any(c.name == "td" for c in cells):
            break
        # Ensure we have a row in the grid for this header row
        while len(grid) <= n_header_rows:
            grid.append([])
        # Walk left-to-right, skipping cells already filled by a rowspan from above
        col = 0
        for cell in cells:
            while col < len(grid[n_header_rows]) and grid[n_header_rows][col] is not None:
                col += 1
            cs = int(cell.get("colspan", 1))
            rs = int(cell.get("rowspan", 1))
            label = cell.get_text(" ", strip=True)
            for dr in range(rs):
                while len(grid) <= n_header_rows + dr:
                    grid.append([])
                row = grid[n_header_rows + dr]
                # Pad row to length col + cs with None
                while len(row) < col + cs:
                    row.append(None)
                for dc in range(cs):
                    row[col + dc] = label
            col += cs
        n_header_rows += 1
    # Make all rows the same width by padding with None
    width = max((len(r) for r in grid[:n_header_rows]), default=0)
    for r in range(n_header_rows):
        while len(grid[r]) < width:
            grid[r].append(None)
    return grid, n_header_rows


def _find_hot_day_column(grid: list[list[str]], n_header_rows: int) -> int | None:
    """Locate the column that lives under '各階級の日数（最高）' and labelled '35'.

    Returns the 0-based column index in the body row (which equals the grid
    column index — the year cell sits at column 0).
    """
    if n_header_rows == 0:
        return None
    width = len(grid[0])
    for c in range(width):
        labels = [grid[r][c] or "" for r in range(n_header_rows)]
        joined = " | ".join(labels)
        if HOT_HIGH_GROUP_RE.search(joined) and HOT_THRESHOLD_RE.search(joined):
            return c
    return None


def fetch_annual_html(station: Station, cache_dir: Path, client: httpx.Client) -> bytes:
    url = annual_url(station)
    sub = "kishou" if station.is_full_station else "amedas"
    cache_path = cache_dir / sub / f"{station.prec_no}_{station.block_no}.html"
    return fetch_cached(url, cache_path, client)


def parse_annual_hot_days(
    html_bytes: bytes, prec_no: str, block_no: str
) -> list[AnnualHotDays]:
    html = _decode(html_bytes)
    soup = BeautifulSoup(html, "lxml")

    # The data table is the first <table class="data2_*"> with our header content.
    tables = soup.find_all("table", class_=re.compile(r"data2_"))
    for table in tables:
        text = table.get_text(" ", strip=True)
        if not HOT_HIGH_GROUP_RE.search(text):
            continue
        grid, n_header = _expand_header_grid(table)
        col = _find_hot_day_column(grid, n_header)
        if col is None:
            logger.debug("hot-day column not found for %s/%s", prec_no, block_no)
            continue

        out: list[AnnualHotDays] = []
        # Iterate body rows (start after the header rows)
        body_started = False
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            if not body_started:
                # Body rows are those with at least one <td>
                if any(c.name == "td" for c in cells):
                    body_started = True
                else:
                    continue
            year_text = cells[0].get_text(strip=True)
            m = YEAR_RE.match(year_text)
            if not m:
                continue
            year = int(m.group(1))
            if col >= len(cells):
                continue
            value, flag = _clean_value(cells[col].get_text(" ", strip=True))
            out.append(
                AnnualHotDays(
                    prec_no=prec_no,
                    block_no=block_no,
                    year=year,
                    hot_days=value,
                    flag=flag,
                )
            )
        return out

    return []


def collect_all(
    catalog: pd.DataFrame,
    cache_dir: Path,
    progress: bool = True,
) -> pd.DataFrame:
    from tqdm import tqdm

    from .fetch import make_client

    rows: list[dict] = []
    iterable = catalog.to_dict("records")
    if progress:
        iterable = tqdm(iterable, desc="fetch+parse", unit="station")

    n_no_table = 0
    n_errors = 0
    with make_client() as client:
        for rec in iterable:
            station = Station(
                prec_no=str(rec["prec_no"]),
                block_no=str(rec["block_no"]),
                name=rec["name"],
                kind=rec["kind"],
                pref_name=rec["pref_name"],
            )
            try:
                html = fetch_annual_html(station, cache_dir, client)
                parsed = parse_annual_hot_days(html, station.prec_no, station.block_no)
            except Exception as exc:  # noqa: BLE001
                n_errors += 1
                logger.warning(
                    "fetch/parse failed for %s/%s (%s): %s",
                    station.prec_no, station.block_no, station.name, exc,
                )
                continue
            if not parsed:
                n_no_table += 1
                continue
            for r in parsed:
                rows.append(
                    {
                        "prec_no": r.prec_no,
                        "block_no": r.block_no,
                        "name": station.name,
                        "kind": station.kind,
                        "pref_name": station.pref_name,
                        "year": r.year,
                        "hot_days": r.hot_days,
                        "flag": r.flag,
                    }
                )
    logger.info(
        "collect_all done: %d (station,year) rows; %d stations w/o threshold table; %d errors",
        len(rows), n_no_table, n_errors,
    )
    return pd.DataFrame(rows)
