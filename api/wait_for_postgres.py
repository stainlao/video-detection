import asyncio
import os
import asyncpg
import time
import re


async def wait_for_postgres():
    raw_url = os.getenv("DATABASE_URL")

    if not raw_url:
        raise ValueError("DATABASE_URL not set")

    # sanitize sqlalchemy-style URL for asyncpg
    DB_URL = re.sub(r"^\w+\+\w+://", "postgresql://", raw_url)

    print("Waiting for PostgreSQL...")

    timeout = 10
    for _ in range(timeout):
        try:
            conn = await asyncpg.connect(DB_URL)
            await conn.close()
            print("PostgreSQL is ready")
            return
        except Exception as e:
            print("PostgreSQL not ready yet...")
            await asyncio.sleep(1)

    print("PostgreSQL not ready within timeout")
    raise e

if __name__ == "__main__":
    asyncio.run(wait_for_postgres())
