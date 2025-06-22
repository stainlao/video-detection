from typing import Literal, TypedDict, Optional
from datetime import datetime


class ScenarioEvent(TypedDict):
    event_type: Literal["scenario_status_changed", "heartbeat", "error"]
    scenario_id: str
    new_state: Optional[str]
    timestamp: str  # ISO 8601


class ScenarioCommand(TypedDict):
    event_type: Literal["create_scenario", "trigger_scenario"]
    scenario_id: str
    trigger: Optional[str]
    initial_state: Optional[str]
    timestamp: str  # ISO 8601
