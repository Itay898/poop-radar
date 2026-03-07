from pydantic import BaseModel
from typing import Optional


class LocalizedText(BaseModel):
    en: str
    he: str


class Reasoning(BaseModel):
    id: str
    label: LocalizedText
    weight: float
    risk: float
    contribution: float
    explanation: LocalizedText


class PredictResponse(BaseModel):
    risk: float
    level: str
    minutesSinceLastAlert: Optional[int]
    lastAlertTimestamp: Optional[float]
    salvoCount: int
    trend: str
    reasonings: list[Reasoning]


class StoredAlert(BaseModel):
    id: str
    cat: int
    title: str
    areas: list[str]
    timestamp: float


class CurrentAlertsResponse(BaseModel):
    active: bool
    alerts: list[StoredAlert]
    connected: bool
