import asyncio
import os
import re
import socket

async def wait_for_tcp_service(host: str, port: int, timeout: int = 60):
    print(f"Waiting for service at {host}:{port} ...")
    for _ in range(timeout):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=2)
            writer.close()
            await writer.wait_closed()
            print(f"Service at {host}:{port} is ready.")
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError(f"Service at {host}:{port} not ready after {timeout} seconds")

async def main():
    # --- POSTGRES ---
    raw_pg_url = os.getenv("DATABASE_URL")
    if not raw_pg_url:
        raise ValueError("DATABASE_URL not set")
    pg_url = re.sub(r"^\w+\+\w+://", "postgresql://", raw_pg_url)
    pg_host, pg_port = None, None
    match = re.match(r"postgresql://[^:]+:[^@]+@([a-zA-Z0-9_\-.]+):(\d+)/", pg_url)
    if match:
        pg_host, pg_port = match.group(1), int(match.group(2))
    else:
        raise ValueError(f"Can't parse DATABASE_URL: {raw_pg_url}")
    await wait_for_tcp_service(pg_host, pg_port)

    # --- KAFKA ---
    kafka_host = os.getenv("KAFKA_HOST", "kafka")
    kafka_port = int(os.getenv("KAFKA_PORT", "9092"))
    await wait_for_tcp_service(kafka_host, kafka_port)

    print("All services are ready.")

if __name__ == "__main__":
    asyncio.run(main())
