from fastapi import APIRouter, Query

from ..services.alert_store import store
from .locations import _load_coords

router = APIRouter()


@router.get("/stats")
async def get_stats(
    location: str = Query(..., description="Pipe-delimited area names"),
    window_days: int = Query(30, ge=1, le=90, description="History window in days"),
):
    """Return alert count, shelter time, and city ranking for given areas."""
    areas = [a.strip() for a in location.split("|") if a.strip()]
    stats = store.get_stats_for_areas(areas, window_days)

    # 10 minutes per alert = Home Front Command minimum shelter stay
    SHELTER_SECONDS_PER_ALERT = 600
    stats["shelter_time_sec"] = stats["alert_count"] * SHELTER_SECONDS_PER_ALERT
    return stats
