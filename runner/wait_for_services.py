import asyncio
import os
import socket

async def wait_for_tcp_service(host: str, port: int, timeout: int = 60):
    print(f"[Waiter] Waiting for service at {host}:{port} ...")
    for _ in range(timeout):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=2)
            writer.close()
            await writer.wait_closed()
            print(f"[Waiter] Service at {host}:{port} is ready.")
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError(f"[Waiter] Service at {host}:{port} not ready after {timeout} seconds")

async def main():
    kafka_host = os.getenv("KAFKA_HOST", "kafka")
    kafka_port = int(os.getenv("KAFKA_PORT", "9092"))

    minio_host = os.getenv("MINIO_HOST", "minio")
    minio_port = int(os.getenv("MINIO_PORT", "9000"))

    await wait_for_tcp_service(kafka_host, kafka_port)
    await wait_for_tcp_service(minio_host, minio_port)
    print("[Waiter] All services are ready.")

if __name__ == "__main__":
    asyncio.run(main())
