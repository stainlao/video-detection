import uuid
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from models import ScenarioModel
from db import async_session


async def create_scenario(state: str) -> uuid.UUID:
    new_id = uuid.uuid4()
    async with async_session() as session:
        async with session.begin():
            scenario = ScenarioModel(id=new_id, state=state)
            session.add(scenario)
    return new_id


async def get_scenario(scenario_id: uuid.UUID) -> ScenarioModel:
    async with async_session() as session:
        stmt = select(ScenarioModel).where(ScenarioModel.id == scenario_id)
        result = await session.execute(stmt)
        scenario = result.scalar_one_or_none()
        if scenario is None:
            raise NoResultFound(f"Scenario {scenario_id} not found")
        return scenario


async def update_scenario_state(session: AsyncSession, scenario_id: uuid.UUID, new_state: str) -> None:
    stmt = (
        update(ScenarioModel)
        .where(ScenarioModel.id == scenario_id)
        .values(state=new_state)
    )
    await session.execute(stmt)
    await session.commit()