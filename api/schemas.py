from pydantic import BaseModel, Field


class ScenarioCreateResponse(BaseModel):
    id: str
    state: str


class ScenarioTriggerRequest(BaseModel):
    trigger: str = Field(..., example="activate")
