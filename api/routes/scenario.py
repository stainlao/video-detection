from fastapi import APIRouter, Depends, HTTPException, status
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


@router.post("/scenario/{scenario_id}/")
async def toggle_scenario_state(
    scenario_id: UUID,
    session: AsyncSession = Depends(get_db)
):
    async with session.begin():
        scenario = await session.get(ScenarioModel, scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail="Scenario not found")

        # Для читаемости: выделим возможные статусы
        INACTIVE_STATES = {"inactive", "in_shutdown_processing"}
        ACTIVE_STATES = {"active", "in_startup_processing"}
        INIT_STATES = {"init_startup", "init_shutdown"}

        # Если сценарий в процессе смены состояния — возвращаем ошибку
        if scenario.state in INIT_STATES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Scenario is already in transition: {scenario.state}"
            )

        # Если сценарий не активен — инициируем активацию
        if scenario.state in INACTIVE_STATES:
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

        # Если сценарий активен — инициируем деактивацию
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
            # Любой неожиданный статус — ошибка (можно заменить на обработку по нужной бизнес-логике)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Scenario in unexpected state: {scenario.state}"
            )

