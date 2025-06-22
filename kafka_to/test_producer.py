import asyncio
import uuid
from datetime import datetime
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import KAFKA_BOOTSTRAP_SERVERS, SCENARIO_EVENTS_TOPIC

async def main():
    producer = KafkaProducerWrapper(KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    test_event = {
        "event_type": "scenario_status_changed",
        "scenario_id": str(uuid.uuid4()),
        "new_state": "active",
        "timestamp": datetime.utcnow().isoformat(),
    }

    await producer.send(SCENARIO_EVENTS_TOPIC, test_event)
    print(f"Sent: {test_event}")
    await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
