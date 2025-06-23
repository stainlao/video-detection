from pydantic import BaseModel

class ScenarioCreateResponse(BaseModel):
    id: str
    state: str
