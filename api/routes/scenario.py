from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID, uuid4

from db import async_session
from models import ScenarioModel, OutboxModel, OutboxStatus
from schemas import ScenarioCreateResponse

router = APIRouter()

# Dependency для сессии
async def get_db():
    async with async_session() as session:
        yield session

# Создание сценария (инициализация)
@router.post("/scenario/", response_model=ScenarioCreateResponse)
async def create_scenario(session: AsyncSession = Depends(get_db)):
    scenario_id = uuid4()
    scenario = ScenarioModel(id=scenario_id, state="init_startup")
    outbox = OutboxModel(
        event_type="create_scenario",
        payload={
            "scenario_id": str(scenario_id),
            "initial_state": "init_startup"
        },
        status=OutboxStatus.PENDING
    )
    async with session.begin():
        session.add(scenario)
        session.add(outbox)
    return ScenarioCreateResponse(id=str(scenario_id), state="init_startup")

# Инициировать остановку сценария (init_shutdown)
@router.post("/scenario/{scenario_id}/")
async def shutdown_scenario(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_db)
):
    async with session.begin():
        scenario = await session.get(ScenarioModel, scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")
        # Shutdown допускается только из active!
        if scenario.state != "active":
            raise HTTPException(
                status_code=409,
                detail=f"Shutdown allowed only from 'active' state, current state: {scenario.state}"
            )
        outbox = OutboxModel(
            event_type="trigger_scenario",
            payload={
                "scenario_id": str(scenario_id),
                "trigger": "init_shutdown"
            },
            status=OutboxStatus.PENDING
        )
        session.add(outbox)
    return {"status": "accepted"}
