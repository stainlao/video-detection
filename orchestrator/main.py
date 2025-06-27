import sys
import os
import asyncio
import uuid
from fsm_task import ScenarioFSM, fsm_registry
from db import async_session
from crud import get_active_run, create_scenario_run
from models import ScenarioRunModel
from kafka_to.consumer import KafkaConsumerWrapper
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCENARIO_COMMANDS_TOPIC,
    SCENARIO_EVENTS_TOPIC,
    KAFKA_GROUP_ID,
)

fsm_tasks = {}  # run_id -> FSM
event_publish_queue = asyncio.Queue()
kafka_producer = None


# -- FSM запуск только по run_id! --
async def start_fsm_for_run(run: ScenarioRunModel):
    run_id = str(run.id)
    fsm = ScenarioFSM(
        model=run,
        kafka_producer=kafka_producer,
        event_publish_queue=event_publish_queue,
    )
    fsm_registry.tasks[run_id] = fsm
    asyncio.create_task(fsm.run())
    print(f"[Orchestrator] FSM for run_id={run_id} (scenario_id={run.scenario_id}) started.")
    return fsm



async def handle_create_scenario(scenario_id: str, initial_state: str = "init_startup"):
    scenario_uuid = uuid.UUID(scenario_id)
    async with async_session() as session:
        # Проверить, есть ли вообще записи с этим scenario_id (run-ы)
        from sqlalchemy import select
        from models import ScenarioRunModel
        stmt = select(ScenarioRunModel).where(ScenarioRunModel.scenario_id == scenario_uuid)
        result = await session.execute(stmt)
        existing_runs = result.scalars().all()
        if existing_runs:
            print(f"[Orchestrator] Scenario {scenario_id} already exists, not creating duplicate.")
            return
        # Нет записей — можно создать первый run
        new_run_id = await create_scenario_run(session, scenario_uuid, initial_state)
        new_run = await session.get(ScenarioRunModel, new_run_id)
        fsm = await start_fsm_for_run(new_run)
        await fsm.queue.put({"event_type": "trigger_scenario", "trigger": "start"})
        print(f"[Orchestrator] Scenario {scenario_id}: first run created (run_id={new_run_id}), started FSM.")


async def handle_toggle_scenario(scenario_id: str):
    scenario_uuid = uuid.UUID(scenario_id)
    async with async_session() as session:
        # Находим единственный не-inactive run (если есть)
        current_run = await get_active_run(session, scenario_uuid)
        # Определяем состояния
        TRANSITION_STATES = {"init_startup", "in_startup_processing", "in_shutdown_processing", "init_shutdown"}
        ACTIVE_STATES = {"active"}
        INACTIVE_STATES = {"inactive"}
        if current_run:
            # Проверка статуса текущего run
            state = current_run.state
            run_id = str(current_run.id)
            if state in TRANSITION_STATES:
                print(f"[Orchestrator] Scenario {scenario_id} is in transition state: {state}. Command ignored.")
                # Здесь можно отправить событие об ошибке или просто логировать
                return
            if state in ACTIVE_STATES:
                # Инициируем shutdown через FSM
                fsm = fsm_tasks.get(run_id)
                if not fsm:
                    fsm = await start_fsm_for_run(current_run)
                await fsm.queue.put({"event_type": "trigger_scenario", "trigger": "shutdown"})
                print(f"[Orchestrator] Shutdown initiated for run_id={run_id}")
                return
            if state in INACTIVE_STATES:
                # Можно создавать новый run — ниже!
                pass
        else:
            # Никогда не запускался — по ТЗ это ошибка, не создаём run
            print(f"[Orchestrator] Scenario {scenario_id} not found, cannot start.")
            return

        # Если мы здесь, значит либо не было текущего run, либо он inactive (разрешено создавать новый run)
        new_run_id = await create_scenario_run(session, scenario_uuid, "init_startup")
        # Получаем только что созданный run
        new_run = await session.get(ScenarioRunModel, new_run_id)
        fsm = await start_fsm_for_run(new_run)
        # Автоматически инициируем старт процесса
        await fsm.queue.put({"event_type": "trigger_scenario", "trigger": "start"})
        print(f"[Orchestrator] Activation initiated for new run_id={new_run_id}")


async def handle_command(event):
    scenario_id = event.get("scenario_id")
    event_type = event.get("event_type")
    trigger = event.get("trigger")
    initial_state = event.get("initial_state") or "init_startup"
    if not scenario_id:
        print("[Orchestrator] Event without scenario_id, ignoring:", event)
        return
    print(f"[Orchestrator] Received event from Kafka: {event}")

    if event_type == "create_scenario":
        await handle_create_scenario(scenario_id, initial_state)
    elif event_type == "trigger_scenario":
        await handle_toggle_scenario(scenario_id)
    else:
        print(f"[Orchestrator] Unknown event_type {event_type}, ignored.")


async def event_publisher_worker():
    while True:
        event = await event_publish_queue.get()
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
    publisher_task = asyncio.create_task(event_publisher_worker())
    consumer_task = asyncio.create_task(consumer.consume(handle_command))
    print("[Orchestrator] Service started. Listening for scenario commands...")
    await asyncio.gather(publisher_task, consumer_task)

if __name__ == "__main__":
    asyncio.run(main())
