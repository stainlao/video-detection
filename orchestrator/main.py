import sys
import os
import asyncio

from fsm_task import ScenarioFSM
from db import async_session
from crud import get_scenario, create_scenario
from kafka_to.consumer import KafkaConsumerWrapper
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCENARIO_COMMANDS_TOPIC,
    SCENARIO_EVENTS_TOPIC,
    KAFKA_GROUP_ID,
)

sys.path.append(os.path.abspath(os.path.dirname(__file__)))


fsm_tasks = {}
event_publish_queue = asyncio.Queue()
kafka_producer = None  # инициализируется на старте


# --- FSM manager: получение или запуск таски FSM по scenario_id ---
async def get_or_create_fsm(scenario_id, initial_state=None):
    if scenario_id not in fsm_tasks:
        async with async_session() as session:
            model = await get_scenario(scenario_id)
            if model is None:
                if initial_state is None:
                    initial_state = "init_startup"
                new_id = await create_scenario(initial_state)
                model = await get_scenario(new_id)
            fsm = ScenarioFSM(
                model=model,
                session=session,
                kafka_producer=kafka_producer,
                event_publish_queue=event_publish_queue,
            )
            task = asyncio.create_task(fsm.run())
            fsm_tasks[str(model.id)] = fsm
            print(f"[Orchestrator] FSM for scenario_id {model.id} CREATED (state={model.state})")
            return fsm
    return fsm_tasks[scenario_id]




# --- Kafka consumer event handler ---
async def handle_command(event):
    scenario_id = event.get("scenario_id")
    event_type = event.get("event_type")
    initial_state = event.get("initial_state")
    if not scenario_id:
        print("[Orchestrator] Event without scenario_id, ignoring:", event)
        return
    print(f"[Orchestrator] Received event from Kafka: {event}")  # ← вот эта строка!
    if event_type == "create_scenario":
        await get_or_create_fsm(scenario_id, initial_state=initial_state)
    else:
        fsm = await get_or_create_fsm(scenario_id)
        await fsm.queue.put(event)


# --- Event publisher worker (с ретраями) ---
async def event_publisher_worker():
    while True:
        event = await event_publish_queue.get()
        # До 5 попыток отправить в Kafka, иначе просто лог
        for attempt in range(5):
            try:
                await kafka_producer.send(SCENARIO_EVENTS_TOPIC, event)
                print(f"[Publisher] Sent event: {event}")
                break
            except Exception as e:
                print(f"[Publisher] Error sending event: {e}. Retrying...")
                await asyncio.sleep(1)
        else:
            print(f"[Publisher] Failed to send event after retries: {event}")

async def main():
    print("[Orchestrator] main started...")
    global kafka_producer
    kafka_producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
    await kafka_producer.start()
    print("[Orchestrator] kafka producer started...")
    consumer = KafkaConsumerWrapper(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=SCENARIO_COMMANDS_TOPIC,
        group_id=KAFKA_GROUP_ID + "_orch",
    )
    await consumer.start()

    # Стартуем воркеры
    publisher_task = asyncio.create_task(event_publisher_worker())
    consumer_task = asyncio.create_task(consumer.consume(handle_command))

    print("[Orchestrator] Service started. Listening for scenario commands...")

    # Ждём завершения задач (обычно бесконечно)
    await asyncio.gather(publisher_task, consumer_task)

if __name__ == "__main__":
    asyncio.run(main())
