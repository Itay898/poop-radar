from fastapi import APIRouter, Query

from ..services.alert_store import store

router = APIRouter()


@router.get("/stats")
async def get_stats(
    location: str = Query(..., description="Pipe-delimited area names"),
    window_days: int = Query(30, ge=1, le=90, description="History window in days"),
):
    """Return alert count, shelter time (Mako/Tzofar formula), and city ranking."""
    areas = [a.strip() for a in location.split("|") if a.strip()]
    return store.get_stats_for_areas(areas, window_days)
