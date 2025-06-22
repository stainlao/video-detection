from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID, uuid4

from db import async_session
from models import ScenarioModel, OutboxModel, OutboxStatus
from schemas import ScenarioCreateResponse, ScenarioTriggerRequest

router = APIRouter()


# Асинхронный dependency для получения сессии
async def get_db():
    async with async_session() as session:
        yield session


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


@router.post("/scenario/{scenario_id}/")
async def trigger_scenario(
    scenario_id: UUID,
    request: ScenarioTriggerRequest,
    session: AsyncSession = Depends(get_db)
):
    async with session.begin():
        # Получаем сценарий внутри транзакции
        scenario = await session.get(ScenarioModel, scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")

        # Создаем новую запись в outbox
        outbox = OutboxModel(
            event_type="trigger_scenario",
            payload={
                "scenario_id": str(scenario_id),
                "trigger": request.trigger
            },
            status=OutboxStatus.PENDING
        )
        session.add(outbox)

    return {"status": "accepted"}

