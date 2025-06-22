import json
from aiokafka import AIOKafkaConsumer

class KafkaConsumerWrapper:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        )
        await self._consumer.start()

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()

    async def consume(self, handler):
        try:
            async for msg in self._consumer:
                await handler(msg.value)
        finally:
            await self.stop()
