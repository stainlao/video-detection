import asyncio
from kafka_to.consumer import KafkaConsumerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_EVENTS_TOPIC, KAFKA_GROUP_ID

async def print_event(event):
    print(f"Received: {event}")

async def main():
    consumer = KafkaConsumerWrapper(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=SCENARIO_EVENTS_TOPIC,
        group_id=KAFKA_GROUP_ID + "_test",
    )
    await consumer.start()
    print(f"Listening on {SCENARIO_EVENTS_TOPIC} ...")
    await consumer.consume(print_event)

if __name__ == "__main__":
    asyncio.run(main())
