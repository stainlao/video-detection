import os
import asyncio
import json
import uuid
import aioboto3
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime
from typing import Optional

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCENARIO_EVENTS_TOPIC = os.getenv("SCENARIO_EVENTS_TOPIC", "scenario_events")
RUNNER_COMMANDS_TOPIC = os.getenv("SCENARIO_EVENTS_TOPIC", "scenario_events")  # используем тот же топик
RUNNER_ID = os.getenv("RUNNER_ID", f"runner-{uuid.uuid4()}")
S3_BUCKET = os.getenv("S3_BUCKET", "mybucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
HEARTBEAT_INTERVAL = 2  # seconds
PROGRESS_MARKER_KEY = "scenarios/{scenario_id}/progress.json"
FRAME_KEY = "scenarios/{scenario_id}/frames/frame_{frame_idx:06d}.jpg"

def s3_client():
    return aioboto3.client(
        "s3",
        region_name=S3_REGION,
        endpoint_url=AWS_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

class Runner:
    def __init__(self):
        self.scenario_id: Optional[str] = None
        self.processing: bool = False
        self.frame_idx: int = 0
        self.heartbeat_task = None
        self.kafka_producer = None

    async def run(self):
        consumer = AIOKafkaConsumer(
            RUNNER_COMMANDS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{RUNNER_ID}_commands",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=True
        )
        await consumer.start()

        self.kafka_producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.kafka_producer.start()

        print(f"[{RUNNER_ID}] Started listening for commands...")
        try:
            async for msg in consumer:
                event = msg.value
                await self.handle_command(event)
        finally:
            await consumer.stop()
            await self.kafka_producer.stop()

    async def handle_command(self, event: dict):
        event_type = event.get("event_type")
        scenario_id = event.get("scenario_id")
        runner_id = event.get("runner_id")

        # Стартовать обработку сценария только если runner_id совпадает с нашим
        if event_type == "runner_start" and runner_id == RUNNER_ID:
            print(f"[{RUNNER_ID}] Received start command for scenario {scenario_id}")
            if self.processing:
                print(f"[{RUNNER_ID}] Already processing {self.scenario_id}, ignoring new start.")
                return
            self.scenario_id = scenario_id
            await self.start_processing()
        elif event_type == "runner_stop" and runner_id == RUNNER_ID:
            print(f"[{RUNNER_ID}] Received stop command.")
            await self.stop_processing()
        elif runner_id and runner_id != RUNNER_ID:
            # Если приходит команда для другого runner, а мы что-то делаем — остановить работу
            if self.processing:
                print(f"[{RUNNER_ID}] Another runner ({runner_id}) assigned. Stopping my work.")
                await self.stop_processing()

    async def start_processing(self):
        self.processing = True
        self.frame_idx = await self.read_progress_marker(self.scenario_id)
        print(f"[{RUNNER_ID}] Will continue from frame_idx={self.frame_idx}")
        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())
        try:
            while self.processing:
                await asyncio.sleep(1)  # эмуляция обработки кадра
                await self.save_frame(self.scenario_id, self.frame_idx)
                await self.write_progress_marker(self.scenario_id, self.frame_idx)
                print(f"[{RUNNER_ID}] Frame {self.frame_idx} processed and saved.")
                self.frame_idx += 1
                # Для теста — завершить работу после 10 кадров
                if self.frame_idx > 10:
                    print(f"[{RUNNER_ID}] Finished scenario {self.scenario_id}")
                    await self.stop_processing()
        except Exception as ex:
            print(f"[{RUNNER_ID}] Processing exception: {ex}")
            await self.stop_processing()

    async def stop_processing(self):
        self.processing = False
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None
        print(f"[{RUNNER_ID}] Stopped processing scenario {self.scenario_id}")
        self.scenario_id = None

    async def heartbeat_loop(self):
        while self.processing:
            await self.send_heartbeat()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def send_heartbeat(self):
        if not self.scenario_id:
            return
        msg = {
            "event_type": "heartbeat",
            "scenario_id": self.scenario_id,
            "runner_id": RUNNER_ID,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await self.kafka_producer.send_and_wait(SCENARIO_EVENTS_TOPIC, msg)
        print(f"[{RUNNER_ID}] Sent heartbeat for scenario {self.scenario_id}")

    async def save_frame(self, scenario_id: str, frame_idx: int):
        async with s3_client() as s3:
            key = FRAME_KEY.format(scenario_id=scenario_id, frame_idx=frame_idx)
            await s3.put_object(Bucket=S3_BUCKET, Key=key, Body=f"frame_{frame_idx}".encode())

    async def write_progress_marker(self, scenario_id: str, frame_idx: int):
        async with s3_client() as s3:
            key = PROGRESS_MARKER_KEY.format(scenario_id=scenario_id)
            marker = {"last_frame_idx": frame_idx, "timestamp": datetime.utcnow().isoformat()}
            await s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(marker).encode())

    async def read_progress_marker(self, scenario_id: str) -> int:
        async with s3_client() as s3:
            key = PROGRESS_MARKER_KEY.format(scenario_id=scenario_id)
            try:
                obj = await s3.get_object(Bucket=S3_BUCKET, Key=key)
                data = await obj["Body"].read()
                marker = json.loads(data)
                return marker.get("last_frame_idx", 0) + 1
            except Exception as e:
                # Если файл не найден — начинаем с 0 кадра
                return 0

if __name__ == "__main__":
    asyncio.run(Runner().run())
