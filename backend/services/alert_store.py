import asyncio
import time
from typing import Optional

from ..config import ALERT_HISTORY_WINDOW_HOURS, MAX_HISTORY_RECORDS, ROCKETALERT_HISTORY_DAYS


class AlertStore:
    def __init__(self):
        self._history: list[dict] = []
        self._current_active: list[dict] = []
        self._connected: bool = False
        self._lock = asyncio.Lock()
        self._city_to_region: dict[str, str] = {}

    def register_region(self, city: str, region: str):
        """Map a city name to its region (e.g. 'גבעתיים' → 'דן')."""
        if city and region and city != region:
            self._city_to_region[city] = region

    def expand_with_regions(self, areas: set[str]) -> set[str]:
        """Expand a set of area names with their regions for broader matching."""
        expanded = set(areas)
        for area in areas:
            region = self._city_to_region.get(area)
            if region:
                expanded.add(region)
        return expanded

    async def add_alert(self, alert_data: dict, timestamp: float):
        async with self._lock:
            areas = alert_data.get("data", [])
            if isinstance(areas, str):
                areas = [areas]
            record = {
                "id": str(alert_data.get("id", "")),
                "cat": alert_data.get("cat", 0),
                "title": alert_data.get("title", ""),
                "areas": areas,
                "timestamp": timestamp,
            }
            self._history.append(record)
            if len(self._history) > MAX_HISTORY_RECORDS:
                self._history = self._history[-MAX_HISTORY_RECORDS:]

    def set_current_active(self, alerts: list[dict]):
        self._current_active = alerts

    def get_current_active(self) -> list[dict]:
        return list(self._current_active)

    def set_connected(self, connected: bool):
        self._connected = connected

    def is_connected(self) -> bool:
        return self._connected

    def _cutoff(self) -> float:
        return time.time() - (ALERT_HISTORY_WINDOW_HOURS * 3600)

    def _pattern_cutoff(self) -> float:
        """Cutoff for pattern analysis — uses full available history."""
        return time.time() - (ROCKETALERT_HISTORY_DAYS * 24 * 3600)

    def get_alerts_for_areas(self, areas: set[str], since_minutes: float) -> list[dict]:
        cutoff = time.time() - (since_minutes * 60)
        return [
            a for a in self._history
            if a["timestamp"] >= cutoff and set(a["areas"]) & areas
        ]

    def get_all_alerts_since(self, since_minutes: float) -> list[dict]:
        cutoff = time.time() - (since_minutes * 60)
        return [a for a in self._history if a["timestamp"] >= cutoff]

    def get_minutes_since_last_alert(self, areas: set[str]) -> Optional[float]:
        for a in reversed(self._history):
            if set(a["areas"]) & areas:
                return (time.time() - a["timestamp"]) / 60
        return None

    def is_currently_active(self, areas: set[str]) -> bool:
        for a in self._current_active:
            alert_areas = a.get("areas", a.get("data", []))
            if isinstance(alert_areas, str):
                alert_areas = [alert_areas]
            if set(alert_areas) & areas:
                return True
        return False

    def get_salvo_count(self, areas: set[str], window_hours: int = 24) -> int:
        cutoff = time.time() - (window_hours * 3600)
        matching = sorted(
            [a for a in self._history if a["timestamp"] >= cutoff and set(a["areas"]) & areas],
            key=lambda x: x["timestamp"],
        )
        if not matching:
            return 0
        salvos = 1
        last_ts = matching[0]["timestamp"]
        for a in matching[1:]:
            if a["timestamp"] - last_ts > 300:  # >5 min gap = new salvo
                salvos += 1
            last_ts = a["timestamp"]
        return salvos

    def get_salvo_timestamps(self, areas: set[str], window_hours: int = 2) -> list[float]:
        """Return the timestamp of each salvo start in the window."""
        cutoff = time.time() - (window_hours * 3600)
        matching = sorted(
            [a for a in self._history if a["timestamp"] >= cutoff and set(a["areas"]) & areas],
            key=lambda x: x["timestamp"],
        )
        if not matching:
            return []
        salvos = [matching[0]["timestamp"]]
        last_ts = matching[0]["timestamp"]
        for a in matching[1:]:
            if a["timestamp"] - last_ts > 300:
                salvos.append(a["timestamp"])
            last_ts = a["timestamp"]
        return salvos

    def get_alert_frequency_by_hour(self, areas: set[str]) -> dict[int, int]:
        from datetime import datetime, timezone, timedelta
        israel_tz = timedelta(hours=2)
        freq: dict[int, int] = {h: 0 for h in range(24)}
        cutoff = self._pattern_cutoff()
        for a in self._history:
            if a["timestamp"] >= cutoff and set(a["areas"]) & areas:
                dt = datetime.fromtimestamp(a["timestamp"], tz=timezone(israel_tz))
                freq[dt.hour] += 1
        return freq

    def get_escalation_duration_hours(self, gap_threshold_hours: float = 4) -> float | None:
        """Return how many hours the current escalation has been going on.
        An escalation is a continuous stream of alerts with gaps < threshold."""
        if not self._history:
            return None
        now = time.time()
        threshold = gap_threshold_hours * 3600
        # Walk backwards from most recent alert
        last_ts = self._history[-1]["timestamp"]
        # If last alert is older than threshold, no active escalation
        if now - last_ts > threshold:
            return None
        escalation_start = last_ts
        for a in reversed(self._history[:-1]):
            if escalation_start - a["timestamp"] > threshold:
                break
            escalation_start = a["timestamp"]
        return (now - escalation_start) / 3600

    def get_distinct_area_count(self, since_minutes: float) -> int:
        """Count how many distinct areas had alerts in the window."""
        cutoff = time.time() - (since_minutes * 60)
        areas: set[str] = set()
        for a in self._history:
            if a["timestamp"] >= cutoff:
                areas.update(a["areas"])
        return len(areas)

    def get_alert_frequency_by_day(self, areas: set[str]) -> dict[int, int]:
        """Return alert count per day-of-week (0=Mon, 6=Sun) for given areas."""
        from datetime import datetime, timezone, timedelta
        israel_tz = timedelta(hours=2)
        freq: dict[int, int] = {d: 0 for d in range(7)}
        cutoff = self._pattern_cutoff()
        for a in self._history:
            if a["timestamp"] >= cutoff and set(a["areas"]) & areas:
                dt = datetime.fromtimestamp(a["timestamp"], tz=timezone(israel_tz))
                freq[dt.weekday()] += 1
        return freq

    def get_stats_for_areas(self, areas: list[str], window_days: float = 30) -> dict:
        """Return alert count, city ranking, and total cities for the given areas."""
        cutoff = time.time() - window_days * 86400
        exact_areas = set(areas)
        # RocketAlert stores bare city names ("רמת גן") while area_coords uses
        # sub-area names ("רמת גן - מערב"). Match both exact and bare-city forms,
        # but NOT region names or other sub-areas of the same city.
        bare_cities = {a.split(" - ")[0] for a in exact_areas}

        def _alert_matches(alert_areas: list[str]) -> bool:
            for a in alert_areas:
                if a in exact_areas:
                    return True
                # Bare city name with no sub-area suffix (RocketAlert format)
                if " - " not in a and a in bare_cities:
                    return True
            return False

        city_counts: dict[str, int] = {}
        user_alert_ids: set[str] = set()
        user_timestamps: list[float] = []

        for a in self._history:
            if a["timestamp"] < cutoff:
                continue
            for area in a["areas"]:
                city_counts[area] = city_counts.get(area, 0) + 1
            if _alert_matches(a["areas"]):
                if a["id"] not in user_alert_ids:
                    user_alert_ids.add(a["id"])
                    user_timestamps.append(a["timestamp"])

        # Deduplicate: RocketAlert stores one record per city per broadcast,
        # so the same ORef event yields multiple records with identical (or nearly
        # identical) timestamps. Cluster within 5 minutes → count unique events.
        EVENT_DEDUP_SEC = 300
        unique_event_timestamps: list[float] = []
        if user_timestamps:
            user_timestamps.sort()
            last_event_ts = user_timestamps[0]
            unique_event_timestamps.append(last_event_ts)
            for ts in user_timestamps[1:]:
                if ts - last_event_ts > EVENT_DEDUP_SEC:
                    unique_event_timestamps.append(ts)
                    last_event_ts = ts

        alert_count = len(unique_event_timestamps)

        # Shelter time using Mako/Tzofar formula:
        # 17 min for a standalone alert, 27 min from the last alert in a salvo.
        # Salvo gap threshold: 30 minutes.
        SINGLE_SEC = 17 * 60
        CONSECUTIVE_SEC = 27 * 60
        SALVO_GAP_SEC = 30 * 60
        shelter_time_sec = 0
        if unique_event_timestamps:
            salvo_start = unique_event_timestamps[0]
            salvo_last = unique_event_timestamps[0]
            for ts in unique_event_timestamps[1:]:
                if ts - salvo_last <= SALVO_GAP_SEC:
                    salvo_last = ts
                else:
                    shelter_time_sec += (salvo_last - salvo_start) + CONSECUTIVE_SEC if salvo_last > salvo_start else SINGLE_SEC
                    salvo_start = ts
                    salvo_last = ts
            shelter_time_sec += (salvo_last - salvo_start) + CONSECUTIVE_SEC if salvo_last > salvo_start else SINGLE_SEC

        sorted_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)
        rank = None
        for i, (city, _) in enumerate(sorted_cities):
            if city in exact_areas or (" - " not in city and city in bare_cities):
                rank = i + 1
                break

        return {
            "alert_count": alert_count,
            "shelter_time_sec": int(shelter_time_sec),
            "rank": rank,
            "total_cities": len(city_counts),
            "window_days": window_days,
        }

    async def prune_old(self):
        async with self._lock:
            cutoff = self._pattern_cutoff()
            self._history = [a for a in self._history if a["timestamp"] >= cutoff]


store = AlertStore()
