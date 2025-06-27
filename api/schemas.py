from pydantic import BaseModel
from datetime import datetime


class ScenarioCreateResponse(BaseModel):
    id: str
    state: str


class ScenarioResponse(BaseModel):
    id: str
    state: str
    updated_at: datetime
