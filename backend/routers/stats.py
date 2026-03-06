import time as _time
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from typing import Optional

from ..services.alert_store import store

router = APIRouter()


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

    return store.get_stats_for_areas(areas, since_days)
