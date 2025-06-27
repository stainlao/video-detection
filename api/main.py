from fastapi import FastAPI
from routes.scenario import router as scenario_router
import asyncio
from events.outbox_worker import process_outbox_events
from events.status_consumer import process_status_events
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS

app = FastAPI()
app.include_router(scenario_router)


worker_task = None
status_consumer_task = None


@app.on_event("startup")
async def startup_event():
    global worker_task, status_consumer_task
    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    loop = asyncio.get_event_loop()
    worker_task = loop.create_task(process_outbox_events(producer))
    status_consumer_task = loop.create_task(process_status_events())


@app.on_event("shutdown")
async def shutdown_event():
    global worker_task, status_consumer_task
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    if status_consumer_task:
        status_consumer_task.cancel()
        try:
            await status_consumer_task
        except asyncio.CancelledError:
            pass
