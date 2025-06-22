from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from models import Base

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@db_orchestrator:5432/orchestrator"

engine = create_async_engine(DATABASE_URL, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)
