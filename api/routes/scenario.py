from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID, uuid4

from db import async_session
from models import ScenarioModel, OutboxModel, OutboxStatus
from schemas import ScenarioCreateResponse, ScenarioResponse

router = APIRouter()


# Dependency для получения сессии
async def get_db():
    async with async_session() as session:
        yield session


# Создание сценария
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


# Переключение статуса сценария
@router.post("/scenario/{scenario_id}/")
async def toggle_scenario_state(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_db)
):
    async with session.begin():
        scenario = await session.get(ScenarioModel, scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")

        # Объявим группы состояний (теперь ACTIVE и INACTIVE только финальные состояния)
        INACTIVE_STATES = {"inactive"}
        ACTIVE_STATES = {"active"}
        # Явно все транзакционные/промежуточные состояния (init*, in_*processing)
        TRANSITION_STATES = {
            "init_startup", "in_startup_processing", "init_shutdown", "in_shutdown_processing"
        }

        if scenario.state in TRANSITION_STATES:
            # === Вот теперь эта проверка строго ловит все промежуточные состояния ===
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Scenario is in a transition state: {scenario.state}. Status change not allowed."
            )
        elif scenario.state in INACTIVE_STATES:
            outbox = OutboxModel(
                event_type="trigger_scenario",
                payload={
                    "scenario_id": str(scenario_id),
                    "trigger": "init_startup"
                },
                status=OutboxStatus.PENDING
            )
            session.add(outbox)
            return {"status": "activation_requested", "current_state": scenario.state}
        elif scenario.state in ACTIVE_STATES:
            outbox = OutboxModel(
                event_type="trigger_scenario",
                payload={
                    "scenario_id": str(scenario_id),
                    "trigger": "init_shutdown"
                },
                status=OutboxStatus.PENDING
            )
            session.add(outbox)
            return {"status": "shutdown_requested", "current_state": scenario.state}
        else:
            # На случай появления неизвестных/ошибочных статусов
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Scenario in unexpected state: {scenario.state}"
            )


# Получение текущего статуса сценария
@router.get("/scenario/{scenario_id}/", response_model=ScenarioResponse)
async def get_scenario(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_db)
):
    scenario = await session.get(ScenarioModel, scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return ScenarioResponse(
        id=str(scenario.id),
        state=scenario.state,
        updated_at=scenario.updated_at
    )
