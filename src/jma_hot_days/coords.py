"""Resolve station latitude/longitude from public JMA metadata."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import httpx
import pandas as pd

from .fetch import fetch_cached, make_client

logger = logging.getLogger(__name__)

AMEDAS_TABLE_URL = "https://www.jma.go.jp/bosai/amedas/const/amedastable.json"


def fetch_amedas_table(cache_path: Path) -> dict:
    """Fetch the JMA AMeDAS station table (JSON) and return as dict.

    The keys are 5-character AMeDAS codes (e.g. '11001'); each value contains
    name, kana, prefecture index, lat/lon as [deg, min], elevation, etc.
    """
    with make_client() as client:
        raw = fetch_cached(AMEDAS_TABLE_URL, cache_path, client)
    return json.loads(raw.decode("utf-8"))


def _dms_to_deg(pair: list[float]) -> float:
    deg, minutes = pair
    return deg + minutes / 60.0


def amedas_table_to_df(table: dict) -> pd.DataFrame:
    rows = []
    for code, info in table.items():
        try:
            lat = _dms_to_deg(info["lat"])
            lon = _dms_to_deg(info["lon"])
        except (KeyError, TypeError):
            continue
        # The first 2 digits of the AMeDAS code match the JMA prec_no
        # (zero-padded). We expose this so name collisions across regions
        # can be disambiguated at merge time.
        rows.append(
            {
                "amedas_code": code,
                "amedas_prec": code[:2],
                "kjName": info.get("kjName", ""),
                "knName": info.get("knName", ""),
                "lat": lat,
                "lon": lon,
                "alt": info.get("alt"),
            }
        )
    return pd.DataFrame(rows)


_TRAILING_PARENS = re.compile(r"[（(].*?[)）]\s*$")
_TRAILING_BRACKET = re.compile(r"\s*[\[［].*?[\]］]\s*$")


def _normalize_name(name: str) -> str:
    n = _TRAILING_PARENS.sub("", name)
    n = _TRAILING_BRACKET.sub("", n)
    return n.strip()


_PREC_FALLBACK_MAP = {
    # JMA stations.csv groups all of Okinawa under prec_no=91, but the AMeDAS
    # JSON spreads it across 91 (沖縄本島), 93 (大東島), and 94 (先島諸島).
    "91": ("91", "93", "94"),
    # 渡島・檜山地方 (stations.csv prec_no=23) → AMeDAS prefix 24
    "23": ("23", "24"),
    # 宗谷地方 (stations.csv prec_no=11) → AMeDAS prefix 13
    "11": ("11", "13"),
}


def merge_coords(stations: pd.DataFrame, amedas: pd.DataFrame) -> pd.DataFrame:
    """Merge station catalog with AMeDAS coordinates.

    Many station names recur across regions (山口, 高松, 大津, 金山, 新城, ...).
    Joining on name alone causes Hokkaido AMeDAS codes (1xxxx, which iterate
    first in the JMA JSON) to win every collision, so we disambiguate by also
    matching a prefecture key: the first 2 digits of the AMeDAS code equal
    the JMA `prec_no` (zero-padded) for most regions. A small map handles the
    handful of regions where the two data sources use different prefixes
    (Okinawa, 渡島, 宗谷). Anything still unresolved falls back to name-only.
    """
    s = stations.copy().reset_index(drop=True)
    s["name_norm"] = s["name"].apply(_normalize_name)
    s["prec_key"] = s["prec_no"].astype(str).str.zfill(2)

    a = amedas.copy()
    a["name_norm"] = a["kjName"].apply(_normalize_name)
    a["prec_key"] = a["amedas_prec"].astype(str).str.zfill(2)
    a = a.drop_duplicates(subset=["name_norm", "prec_key"], keep="first")

    coord_cols = ["amedas_code", "kjName", "knName", "lat", "lon", "alt"]

    by_strict = a.set_index(["name_norm", "prec_key"])[coord_cols]
    by_name = a.drop_duplicates(subset=["name_norm"], keep="first").set_index(
        "name_norm"
    )[coord_cols]

    def _lookup(name_norm: str, prec_key: str) -> pd.Series:
        for candidate in _PREC_FALLBACK_MAP.get(prec_key, (prec_key,)):
            try:
                return by_strict.loc[(name_norm, candidate)]
            except KeyError:
                continue
        try:
            return by_name.loc[name_norm]
        except KeyError:
            return pd.Series({c: pd.NA for c in coord_cols})

    resolved = s.apply(
        lambda row: _lookup(row["name_norm"], row["prec_key"]),
        axis=1,
    )
    resolved = resolved.reindex(columns=coord_cols)

    out = pd.concat([s.drop(columns=["prec_key"]), resolved], axis=1)
    return out
