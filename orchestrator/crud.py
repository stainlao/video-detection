import uuid
import datetime
from sqlalchemy import select, update, desc
from sqlalchemy.ext.asyncio import AsyncSession
from models import ScenarioRunModel


async def create_scenario_run(session: AsyncSession, scenario_id: uuid.UUID, state: str) -> uuid.UUID:
    run_id = uuid.uuid4()
    run = ScenarioRunModel(
        id=run_id,
        scenario_id=scenario_id,
        state=state
    )
    session.add(run)
    await session.commit()
    return run_id


async def get_active_run(session: AsyncSession, scenario_id: uuid.UUID) -> ScenarioRunModel | None:
    # Ищет run НЕ в состоянии "inactive"
    stmt = (
        select(ScenarioRunModel)
        .where(
            ScenarioRunModel.scenario_id == scenario_id,
            ScenarioRunModel.state != "inactive"
        )
        .order_by(desc(ScenarioRunModel.created_at))
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def update_run_state(session: AsyncSession, run_id: uuid.UUID, new_state: str) -> None:
    stmt = (
        update(ScenarioRunModel)
        .where(ScenarioRunModel.id == run_id)
        .values(state=new_state, updated_at=datetime.datetime.utcnow())
    )
    await session.execute(stmt)
    await session.commit()


async def get_all_runs(session: AsyncSession, scenario_id: uuid.UUID):
    stmt = (
        select(ScenarioRunModel)
        .where(ScenarioRunModel.scenario_id == scenario_id)
        .order_by(desc(ScenarioRunModel.created_at))
    )
    result = await session.execute(stmt)
    return result.scalars().all()
