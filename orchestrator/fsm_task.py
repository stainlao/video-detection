import asyncio
import yaml
from datetime import datetime
from transitions import Machine
from crud import update_scenario_state
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import SCENARIO_EVENTS_TOPIC

HEARTBEAT_TIMEOUT = 10  # seconds

# Загрузка состояния FSM из workflow.yaml
with open("workflow.yaml", "r") as f:
    workflow = yaml.safe_load(f)

states = workflow["states"]
transitions = workflow["transitions"]


class ScenarioFSM:
    def __init__(self, model, session, kafka_producer: KafkaProducerWrapper, event_publish_queue: asyncio.Queue):
        self.model = model
        self.session = session
        self.producer = kafka_producer
        self.event_publish_queue = event_publish_queue
        self.queue = asyncio.Queue()
        self.last_heartbeat = datetime.utcnow()
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT
        self.scenario_id = str(model.id)
        self.machine = Machine(
            model=self,
            states=states,
            transitions=transitions,
            initial=model.state,
        )

    async def process_event(self, event):
        event_type = event.get("event_type")
        if event_type == "heartbeat":
            self.last_heartbeat = datetime.utcnow()
            print(f"[FSM:{self.scenario_id}] Heartbeat received.")
        elif event_type == "trigger":
            trigger = event["trigger"]
            try:
                # transitions делает только sync смену state (если переход валиден)
                getattr(self, trigger)()
                await self.on_state_change()
            except Exception as e:
                print(f"[FSM:{self.scenario_id}] FSM transition error: {e}")
        elif event_type == "runner_lost":
            print(f"[FSM:{self.scenario_id}] Runner lost, initiating restart.")
            await self.initiate_restart()
        else:
            print(f"[FSM:{self.scenario_id}] Unknown event: {event}")

    async def on_state_change(self):
        # Обновляем состояние в БД
        await update_scenario_state(self.session, self.model.id, self.state)
        # Отправляем событие в Kafka (через общую очередь)
        await self.event_publish_queue.put({
            "event_type": "scenario_status_changed",
            "scenario_id": self.scenario_id,
            "new_state": self.state,
            "timestamp": datetime.utcnow().isoformat(),
        })
        print(f"[FSM:{self.scenario_id}] State changed to {self.state}.")

    async def heartbeat_watcher(self):
        try:
            while True:
                await asyncio.sleep(self.heartbeat_timeout)
                if (datetime.utcnow() - self.last_heartbeat).total_seconds() > self.heartbeat_timeout:
                    print(f"[FSM:{self.scenario_id}] Heartbeat timeout. Triggering restart.")
                    await self.queue.put({"event_type": "runner_lost"})
        except asyncio.CancelledError:
            print(f"[FSM:{self.scenario_id}] Heartbeat watcher stopped (FSM inactive).")

    async def initiate_restart(self):
        # Корректный shutdown (через автомат)
        try:
            if self.state not in ["in_shutdown_processing", "inactive"]:
                self.shutdown()
                await self.on_state_change()
                self.stop()
                await self.on_state_change()
        except Exception as e:
            print(f"[FSM:{self.scenario_id}] Error during shutdown: {e}")

        # Перезапуск (init_startup → in_startup_processing → active)
        try:
            self.start()
            await self.on_state_change()
            self.activate()
            await self.on_state_change()
        except Exception as e:
            print(f"[FSM:{self.scenario_id}] Error during restart: {e}")

    async def run(self):
        heartbeat_task = asyncio.create_task(self.heartbeat_watcher())
        while True:
            event = await self.queue.get()
            await self.process_event(event)
            if self.state == "inactive":
                print(f"[FSM:{self.scenario_id}] FSM task finished (inactive).")
                break
        # После завершения — отменить watcher
        heartbeat_task.cancel()
        await heartbeat_task
        print(f"[FSM:{self.scenario_id}] Task is exiting, should be cleaned up.")
