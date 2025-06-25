import asyncio
from datetime import datetime
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db import async_session
from models import OutboxModel, OutboxStatus

from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_COMMANDS_TOPIC

POLL_INTERVAL = 5  # seconds


async def process_outbox_events(producer: KafkaProducerWrapper):
    while True:
        async with async_session() as session:
            # Получаем только pending
            stmt = select(OutboxModel).where(OutboxModel.status == OutboxStatus.PENDING)
            result = await session.execute(stmt)
            events = result.scalars().all()

            for event in events:
                try:
                    # Определи нужный топик
                    topic = SCENARIO_COMMANDS_TOPIC
                    # Для гибкости: если нужны разные топики по типу, можно здесь добавить if

                    # Пишем в Kafka
                    await producer.send(topic, {
                        "event_type": event.event_type,
                        **event.payload,
                        "timestamp": event.created_at.isoformat(),
                    })

                    # Отмечаем как SENT
                    stmt_update = (
                        update(OutboxModel)
                        .where(OutboxModel.id == event.id)
                        .values(status=OutboxStatus.SENT, sent_at=datetime.utcnow())
                    )
                    await session.execute(stmt_update)
                    await session.commit()
                    print(f"Outbox event {event.id} sent to Kafka.")

                except Exception as e:
                    print(f"Ошибка отправки события {event.id}: {e}")
                    stmt_update = (
                        update(OutboxModel)
                        .where(OutboxModel.id == event.id)
                        .values(status=OutboxStatus.FAILED)
                    )
                    await session.execute(stmt_update)
                    await session.commit()
        await asyncio.sleep(POLL_INTERVAL)


async def main():
    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        await process_outbox_events(producer)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
