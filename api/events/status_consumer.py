import asyncio
from datetime import datetime
from sqlalchemy import select
from db import async_session
from models import ScenarioModel
from kafka_to.consumer import KafkaConsumerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_EVENTS_TOPIC, KAFKA_GROUP_ID


async def handle_status_event(event):
    # Логируем приходящее сообщение
    print(f"[status_consumer] Received event: {event}")

    # Проверяем тип события
    if event.get("event_type") != "scenario_status_changed":
        print(f"[status_consumer] Ignored event type: {event.get('event_type')}")
        return
    scenario_id = event.get("scenario_id")
    new_state = event.get("new_state")
    timestamp = event.get("timestamp")
    if not scenario_id or not new_state or not timestamp:
        print(f"[status_consumer] Malformed event: {event}")
        return

    try:
        new_timestamp = datetime.fromisoformat(timestamp)
    except Exception as e:
        print(f"[status_consumer] Invalid timestamp format: {timestamp} ({e})")
        return

    async with async_session() as session:
        async with session.begin():
            stmt = select(ScenarioModel).where(ScenarioModel.id == scenario_id)
            result = await session.execute(stmt)
            scenario = result.scalar_one_or_none()
            if not scenario:
                print(f"[status_consumer] Scenario {scenario_id} not found")
                return

            # Idempotency + порядок: обновлять только если новое событие свежее
            if scenario.updated_at is None or new_timestamp > scenario.updated_at:
                scenario.state = new_state
                scenario.updated_at = new_timestamp
                print(f"[status_consumer] Scenario {scenario_id} state updated to {new_state} at {new_timestamp}")
            else:
                print(f"[status_consumer] Scenario {scenario_id} event ignored (older or duplicate)")


async def process_status_events():
    consumer = KafkaConsumerWrapper(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=SCENARIO_EVENTS_TOPIC,
        group_id=KAFKA_GROUP_ID + "_api_status"
    )
    await consumer.start()
    print(f"[status_consumer] Listening for scenario status events...")
    await consumer.consume(handle_status_event)
