import json
import math
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from ..config import (
    WEIGHT_RECENCY, WEIGHT_BURST, WEIGHT_VOLUME,
    WEIGHT_PATTERN, WEIGHT_PROXIMITY,
    WEIGHT_ESCALATION, WEIGHT_CLUSTER, WEIGHT_DAY_OF_WEEK,
    LEVEL_GREEN_MAX, LEVEL_YELLOW_MAX, LEVEL_ORANGE_MAX,
)
from ..models import PredictResponse, Reasoning, LocalizedText
from ..services.alert_store import store

# Trend tracking: keyed by frozenset of areas -> deque of (timestamp, risk)
_trend_buffer: dict[frozenset, deque] = {}

# Area proximity map
_area_groups: dict[str, list[str]] | None = None


def _load_area_groups() -> dict[str, list[str]]:
    global _area_groups
    if _area_groups is None:
        path = Path(__file__).parent.parent / "data" / "area_groups.json"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                _area_groups = json.load(f)
        else:
            _area_groups = {}
    return _area_groups


def _recency_module(areas: set[str]) -> Reasoning:
    if store.is_currently_active(areas):
        risk = 1.0
        explanation_he = "התרעה פעילה כרגע באזור שלך!"
        explanation_en = "There is an active alert in your area right now!"
    else:
        minutes = store.get_minutes_since_last_alert(areas)
        if minutes is None:
            risk = 0.0
            explanation_he = "לא היו התרעות באזור שלך ב-48 השעות האחרונות"
            explanation_en = "No alerts in your area in the last 48 hours"
        else:
            risk = math.exp(-0.03 * minutes)
            mins_int = int(minutes)
            if mins_int < 60:
                time_he = f"{mins_int} דקות"
                time_en = f"{mins_int} minutes"
            elif mins_int < 1440:
                hours = mins_int // 60
                remaining = mins_int % 60
                time_he = f"{hours} שעות ו-{remaining} דקות" if remaining else f"{hours} שעות"
                time_en = f"{hours}h {remaining}m" if remaining else f"{hours} hours"
            else:
                days = mins_int // 1440
                hours = (mins_int % 1440) // 60
                time_he = f"{days} ימים ו-{hours} שעות" if hours else f"{days} ימים"
                time_en = f"{days}d {hours}h" if hours else f"{days} days"
            explanation_he = f"ההתרעה האחרונה באזור שלך הייתה לפני {time_he}"
            explanation_en = f"Last alert in your area was {time_en} ago"

    return Reasoning(
        id="recency_decay",
        label=LocalizedText(en="Recency of last alert", he="עדכניות ההתרעה האחרונה"),
        weight=WEIGHT_RECENCY,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_RECENCY, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _burst_module(areas: set[str]) -> Reasoning:
    salvo_timestamps = store.get_salvo_timestamps(areas, window_hours=2)
    salvo_count = len(salvo_timestamps)

    if salvo_count == 0:
        risk = 0.0
        explanation_he = "לא זוהו מטחים באזור שלך בשעתיים האחרונות"
        explanation_en = "No salvos detected in your area in the last 2 hours"
    else:
        base_risk = min(1.0, salvo_count * 0.25)
        # Check for acceleration
        if salvo_count >= 2:
            gaps = [salvo_timestamps[i+1] - salvo_timestamps[i] for i in range(len(salvo_timestamps)-1)]
            if len(gaps) >= 2 and gaps[-1] < gaps[-2]:
                base_risk = min(1.0, base_risk * 1.3)
                explanation_he = f"{salvo_count} מטחים בשעתיים האחרונות - קצב מואץ!"
                explanation_en = f"{salvo_count} salvos in last 2 hours - accelerating!"
            else:
                explanation_he = f"{salvo_count} מטחים בשעתיים האחרונות"
                explanation_en = f"{salvo_count} salvos in the last 2 hours"
        else:
            explanation_he = f"מטח אחד בשעתיים האחרונות"
            explanation_en = "1 salvo in the last 2 hours"
        risk = base_risk

    return Reasoning(
        id="burst_detection",
        label=LocalizedText(en="Salvo frequency detection", he="זיהוי מטחי ירי"),
        weight=WEIGHT_BURST,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_BURST, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _volume_module() -> Reasoning:
    all_alerts = store.get_all_alerts_since(since_minutes=360)  # 6 hours
    total = len(all_alerts)
    risk = min(1.0, total / 50)

    if total == 0:
        explanation_he = "לא היו התרעות ארציות ב-6 השעות האחרונות"
        explanation_en = "No nationwide alerts in the last 6 hours"
    else:
        explanation_he = f"{total} התרעות ארציות ב-6 השעות האחרונות"
        explanation_en = f"{total} nationwide alerts in the last 6 hours"

    return Reasoning(
        id="volume_intensity",
        label=LocalizedText(en="Alert volume intensity", he="עוצמת נפח ההתרעות"),
        weight=WEIGHT_VOLUME,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_VOLUME, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _pattern_module(areas: set[str]) -> Reasoning:
    freq = store.get_alert_frequency_by_hour(areas)
    total = sum(freq.values())

    if total == 0:
        risk = 0.0
        explanation_he = "אין מספיק נתונים היסטוריים לניתוח דפוסים"
        explanation_en = "Not enough historical data for pattern analysis"
    else:
        avg = total / 24
        israel_tz = timedelta(hours=2)
        current_hour = datetime.now(tz=timezone(israel_tz)).hour
        hour_count = freq.get(current_hour, 0)
        ratio = hour_count / max(1, avg)
        risk = min(1.0, ratio * 0.5)

        explanation_he = f"השעה הנוכחית ({current_hour}:00) - {hour_count} התרעות (ממוצע: {avg:.1f})"
        explanation_en = f"Current hour ({current_hour}:00) - {hour_count} alerts (avg: {avg:.1f})"

    return Reasoning(
        id="historical_pattern",
        label=LocalizedText(en="Time-of-day pattern analysis", he="ניתוח דפוסי זמן"),
        weight=WEIGHT_PATTERN,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_PATTERN, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _proximity_module(areas: set[str]) -> Reasoning:
    from ..routers.locations import _load_coords, _haversine_km

    coords = _load_coords()
    # Find coordinates of user's areas
    user_lats, user_lons = [], []
    for area in areas:
        if area in coords:
            user_lats.append(coords[area]["lat"])
            user_lons.append(coords[area]["lon"])

    if not user_lats:
        risk = 0.0
        explanation_he = "אין נתוני מיקום לאזורים שלך"
        explanation_en = "No location data for your areas"
    else:
        # Find all areas within 30km of user's areas
        center_lat = sum(user_lats) / len(user_lats)
        center_lon = sum(user_lons) / len(user_lons)
        neighbors: set[str] = set()
        for name, c in coords.items():
            if name not in areas and _haversine_km(center_lat, center_lon, c["lat"], c["lon"]) <= 30:
                neighbors.add(name)

        neighbor_alerts = store.get_alerts_for_areas(neighbors, since_minutes=30)
        count = len(neighbor_alerts)
        risk = min(1.0, count * 0.2)

        if count == 0:
            explanation_he = "אין התרעות באזורים סמוכים (30 ק\"מ) ב-30 הדקות האחרונות"
            explanation_en = "No alerts within 30km in the last 30 minutes"
        else:
            explanation_he = f"{count} התרעות באזורים סמוכים (30 ק\"מ) ב-30 הדקות האחרונות"
            explanation_en = f"{count} alerts within 30km in the last 30 minutes"

    return Reasoning(
        id="area_proximity",
        label=LocalizedText(en="Nearby area spillover", he="זליגה מאזורים סמוכים"),
        weight=WEIGHT_PROXIMITY,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_PROXIMITY, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _escalation_module() -> Reasoning:
    duration = store.get_escalation_duration_hours()

    if duration is None:
        risk = 0.0
        explanation_he = "אין הסלמה פעילה כרגע"
        explanation_en = "No active escalation detected"
    else:
        # Escalation risk rises with duration, peaks around 24h+
        risk = min(1.0, duration / 24)
        hours_int = int(duration)
        if hours_int < 1:
            mins = int(duration * 60)
            explanation_he = f"הסלמה פעילה כבר {mins} דקות"
            explanation_en = f"Active escalation for {mins} minutes"
        else:
            explanation_he = f"הסלמה פעילה כבר {hours_int} שעות"
            explanation_en = f"Active escalation for {hours_int} hours"

    return Reasoning(
        id="escalation_duration",
        label=LocalizedText(en="Escalation duration", he="משך הסלמה"),
        weight=WEIGHT_ESCALATION,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_ESCALATION, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _cluster_module() -> Reasoning:
    distinct = store.get_distinct_area_count(since_minutes=30)

    if distinct == 0:
        risk = 0.0
        explanation_he = "לא זוהו התרעות ב-30 הדקות האחרונות"
        explanation_en = "No alerts detected in the last 30 minutes"
    else:
        # 20+ distinct areas in 30min = max cluster risk
        risk = min(1.0, distinct / 20)
        explanation_he = f"{distinct} אזורים שונים הותרעו ב-30 הדקות האחרונות"
        explanation_en = f"{distinct} distinct areas alerted in the last 30 minutes"

    return Reasoning(
        id="multi_area_cluster",
        label=LocalizedText(en="Multi-area cluster attack", he="מתקפה רב-אזורית"),
        weight=WEIGHT_CLUSTER,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_CLUSTER, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _day_of_week_module(areas: set[str]) -> Reasoning:
    freq = store.get_alert_frequency_by_day(areas)
    total = sum(freq.values())

    day_names_he = ["שני", "שלישי", "רביעי", "חמישי", "שישי", "שבת", "ראשון"]
    day_names_en = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    if total == 0:
        risk = 0.0
        explanation_he = "אין מספיק נתונים לניתוח דפוסי ימים"
        explanation_en = "Not enough data for day-of-week analysis"
    else:
        avg = total / 7
        israel_tz = timedelta(hours=2)
        current_day = datetime.now(tz=timezone(israel_tz)).weekday()
        day_count = freq.get(current_day, 0)
        ratio = day_count / max(1, avg)
        risk = min(1.0, ratio * 0.5)

        explanation_he = f"יום {day_names_he[current_day]} - {day_count} התרעות (ממוצע: {avg:.1f})"
        explanation_en = f"{day_names_en[current_day]} - {day_count} alerts (avg: {avg:.1f})"

    return Reasoning(
        id="day_of_week_pattern",
        label=LocalizedText(en="Day-of-week pattern", he="דפוס יום בשבוע"),
        weight=WEIGHT_DAY_OF_WEEK,
        risk=round(risk, 4),
        contribution=round(risk * WEIGHT_DAY_OF_WEEK, 4),
        explanation=LocalizedText(en=explanation_en, he=explanation_he),
    )


def _classify_level(risk: float) -> str:
    if risk <= LEVEL_GREEN_MAX:
        return "GREEN"
    if risk <= LEVEL_YELLOW_MAX:
        return "YELLOW"
    if risk <= LEVEL_ORANGE_MAX:
        return "ORANGE"
    return "RED"


def _compute_trend(areas: set[str], current_risk: float) -> str:
    key = frozenset(areas)
    if key not in _trend_buffer:
        _trend_buffer[key] = deque(maxlen=6)
    buf = _trend_buffer[key]
    buf.append((time.time(), current_risk))

    if len(buf) < 3:
        return "stable"

    recent = [r for _, r in list(buf)[-3:]]
    older = [r for _, r in list(buf)[:-3]] if len(buf) > 3 else recent
    avg_recent = sum(recent) / len(recent)
    avg_older = sum(older) / len(older)

    if avg_recent - avg_older > 0.05:
        return "increasing"
    elif avg_older - avg_recent > 0.05:
        return "decreasing"
    return "stable"


def calculate_risk(areas: list[str], duration_min: int) -> PredictResponse:
    areas_set = store.expand_with_regions(set(areas))

    modules = [
        _recency_module(areas_set),
        _burst_module(areas_set),
        _volume_module(),
        _pattern_module(areas_set),
        _proximity_module(set(areas)),  # proximity uses original areas for coordinates
        _escalation_module(),
        _cluster_module(),
        _day_of_week_module(areas_set),
    ]

    total_risk = min(1.0, sum(m.contribution for m in modules))

    # Scale risk to the requested shower duration.
    # The base modules assume a ~10-minute window; apply compound probability scaling.
    base_duration = 10
    if duration_min != base_duration and 0 < total_risk < 1:
        per_minute = 1 - (1 - total_risk) ** (1 / base_duration)
        total_risk = min(1.0, 1 - (1 - per_minute) ** duration_min)

    level = _classify_level(total_risk)
    trend = _compute_trend(areas_set, total_risk)

    minutes_since = store.get_minutes_since_last_alert(areas_set)
    last_alert_ts = store.get_last_alert_timestamp(areas_set)

    return PredictResponse(
        risk=round(total_risk, 4),
        level=level,
        minutesSinceLastAlert=int(minutes_since) if minutes_since is not None else None,
        lastAlertTimestamp=last_alert_ts,
        salvoCount=store.get_salvo_count(areas_set),
        trend=trend,
        reasonings=modules,
    )
