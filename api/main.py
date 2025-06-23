from fastapi import FastAPI
from routes.scenario import router as scenario_router
import asyncio
from events.outbox_worker import process_outbox_events
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS

app = FastAPI()
app.include_router(scenario_router)


worker_task = None


@app.on_event("startup")
async def startup_event():
    global worker_task
    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    # Запуск в фоне
    loop = asyncio.get_event_loop()
    worker_task = loop.create_task(process_outbox_events(producer))


@app.on_event("shutdown")
async def shutdown_event():
    global worker_task
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
