import uuid
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from models import ScenarioModel
from db import async_session


async def create_scenario(scenario_id: uuid.UUID, state: str) -> uuid.UUID:
    async with async_session() as session:
        async with session.begin():
            scenario = ScenarioModel(id=scenario_id, state=state)
            session.add(scenario)
    return scenario_id


async def get_scenario(scenario_id: uuid.UUID) -> ScenarioModel | None:
    async with async_session() as session:
        stmt = select(ScenarioModel).where(ScenarioModel.id == scenario_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()


async def update_scenario_state(session: AsyncSession, scenario_id: uuid.UUID, new_state: str) -> None:
    stmt = (
        update(ScenarioModel)
        .where(ScenarioModel.id == scenario_id)
        .values(state=new_state)
    )
    await session.execute(stmt)
    await session.commit()