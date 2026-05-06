"""Build the JMA station catalog by crawling prefecture maps.

Source: https://www.data.jma.go.jp/obd/stats/etrn/select/prefecture00.php
The prefecture page links to per-prefecture pages whose imagemap encodes
(prec_no, block_no) for each station, plus the station kind ('s' = full
weather station / 気象官署, 'a' = AMeDAS).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from .fetch import fetch_cached, make_client

logger = logging.getLogger(__name__)

BASE = "https://www.data.jma.go.jp/obd/stats/etrn/select"
INDEX_URL = f"{BASE}/prefecture00.php"

# href example:
#   prefecture.php?prec_no=44&block_no=&year=&month=&day=&view=
PREF_LINK_RE = re.compile(r"prefecture\.php\?prec_no=(\d+)")

# point.php?prec_no=44&block_no=47662&year=&month=&day=&view=
# point.php?prec_no=44&block_no=0379&year=&month=&day=&view=  (AMeDAS)
POINT_HREF_RE = re.compile(r"prec_no=(\d+)&block_no=(\d+)")


@dataclass(frozen=True)
class Station:
    prec_no: str
    block_no: str
    name: str
    kind: str  # 's' for 気象官署 (5-digit block), 'a' for AMeDAS (4-digit block)
    pref_name: str

    @property
    def is_full_station(self) -> bool:
        return self.kind == "s"


def _decode(html: bytes) -> str:
    """JMA pages are UTF-8 (modern site); fall back to Shift_JIS for legacy."""
    try:
        return html.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return html.decode("shift_jis")
        except UnicodeDecodeError:
            return html.decode("cp932", errors="replace")


def crawl_prefecture_index(cache_dir: Path) -> list[tuple[str, str]]:
    """Return list of (prec_no, prefecture_name)."""
    with make_client() as client:
        raw = fetch_cached(INDEX_URL, cache_dir / "prefecture00.html", client)
    html = _decode(raw)
    soup = BeautifulSoup(html, "lxml")

    pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for area in soup.find_all("area"):
        href = area.get("href", "")
        m = PREF_LINK_RE.search(href)
        if not m:
            continue
        prec_no = m.group(1)
        if prec_no in seen:
            continue
        seen.add(prec_no)
        # alt attribute carries the prefecture / region label
        alt = (area.get("alt") or "").strip()
        pairs.append((prec_no, alt))
    pairs.sort(key=lambda x: x[0])
    return pairs


def crawl_prefecture_stations(
    prec_no: str, pref_name: str, cache_dir: Path
) -> list[Station]:
    url = f"{BASE}/prefecture.php?prec_no={prec_no}&block_no=&year=&month=&day=&view="
    cache = cache_dir / f"prefecture_{prec_no}.html"
    with make_client() as client:
        raw = fetch_cached(url, cache, client)
    html = _decode(raw)
    soup = BeautifulSoup(html, "lxml")

    stations: dict[tuple[str, str], Station] = {}
    for area in soup.find_all("area"):
        href = area.get("href", "")
        m = POINT_HREF_RE.search(href)
        if not m:
            continue
        block_no = m.group(2)
        # Skip empty / index links and 2-digit '00' pseudo-entries
        # ('地方全地点' summary links share the prefecture imagemap).
        if not block_no or len(block_no) <= 2:
            continue
        # alt holds the station name
        name = (area.get("alt") or "").strip()
        if not name:
            continue
        # 5-digit block_no -> 気象官署 (annually_s.php),
        # 4-digit -> AMeDAS-only (annually_a.php).
        kind = "s" if len(block_no) >= 5 else "a"
        key = (prec_no, block_no)
        if key in stations:
            continue
        stations[key] = Station(
            prec_no=prec_no,
            block_no=block_no,
            name=name,
            kind=kind,
            pref_name=pref_name,
        )
    return list(stations.values())


def build_catalog(cache_dir: Path, out_csv: Path) -> pd.DataFrame:
    pairs = crawl_prefecture_index(cache_dir)
    logger.info("Found %d prefecture / region pages", len(pairs))
    rows: list[Station] = []
    for prec_no, pref_name in pairs:
        sts = crawl_prefecture_stations(prec_no, pref_name, cache_dir)
        logger.info("prec %s (%s): %d stations", prec_no, pref_name, len(sts))
        rows.extend(sts)

    df = pd.DataFrame([s.__dict__ for s in rows])
    df = df.drop_duplicates(subset=["prec_no", "block_no"]).sort_values(
        ["prec_no", "block_no"]
    )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    return df
