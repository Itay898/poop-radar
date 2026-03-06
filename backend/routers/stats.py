import re
import json
import time as _time
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from typing import Optional
from urllib.parse import quote

import httpx

from ..services.alert_store import store

router = APIRouter()

# In-memory cache: city name -> {alert_count, shelter_time_sec}
_mako_cache: dict[str, dict] = {}


async def _fetch_mako_stats(city: str) -> dict | None:
    """Fetch pre-computed ORef stats from Mako's shelter time page (Iran2026 op)."""
    if city in _mako_cache:
        return _mako_cache[city]
    try:
        url = f"https://sheltertime.mako.co.il/share?c={quote(city)}"
        async with httpx.AsyncClient(timeout=10, verify=False) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        match = re.search(r'window\.__ctx\s*=\s*(\{.*?\})\s*;?\s*</script>', resp.text, re.DOTALL)
        if not match:
            return None
        ctx = json.loads(match.group(1))
        all_cities = ctx.get("initialStats", {}).get("Iran2026", {})
        city_data = all_cities.get(city)
        if not city_data or len(city_data) < 2:
            return None
        # Compute rank among all cities sorted by shelter time descending (same as Mako)
        sorted_cities = sorted(all_cities.items(), key=lambda x: x[1][0] if x[1] else 0, reverse=True)
        rank = next((i + 1 for i, (c, _) in enumerate(sorted_cities) if c == city), None)
        result = {
            "alert_count": int(city_data[1]),
            "shelter_time_sec": int(city_data[0]) // 1000,
            "rank": rank,
            "total_cities": len(all_cities),
        }
        _mako_cache[city] = result
        return result
    except Exception:
        return None


@router.get("/stats")
async def get_stats(
    location: str = Query(..., description="Pipe-delimited area names"),
    since_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (overrides window_days)"),
    window_days: int = Query(30, ge=1, le=90, description="History window in days"),
):
    """Return alert count, shelter time (Mako/Tzofar formula), and city ranking."""
    areas = [a.strip() for a in location.split("|") if a.strip()]

    if since_date:
        try:
            dt = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            cutoff_ts = dt.timestamp()
            since_days = (_time.time() - cutoff_ts) / 86400
        except ValueError:
            since_days = window_days
    else:
        since_days = window_days

    # Use Mako/Tzofar authoritative data when querying for the current operation
    # (since_date=2026-02-28 corresponds to שאגת הארי / Iran2026)
    our_stats = store.get_stats_for_areas(areas, since_days)
    if since_date == "2026-02-28" and areas:
        mako = await _fetch_mako_stats(areas[0])
        if mako:
            return {**mako, "window_days": since_days}

    return our_stats
