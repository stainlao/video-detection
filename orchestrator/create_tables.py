import asyncio
from db import engine
from models import Base


async def create():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    print("[create_tables] creating started...")
    asyncio.run(create())
    print("[create_tables] tables_created...")
