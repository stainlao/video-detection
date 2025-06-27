import asyncio
import yaml
from datetime import datetime
from transitions import Machine
from crud import update_run_state
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import SCENARIO_EVENTS_TOPIC
from db import async_session

HEARTBEAT_TIMEOUT = 10

with open("workflow.yaml", "r") as f:
    workflow = yaml.safe_load(f)
states = workflow["states"]
transitions = workflow["transitions"]


# Вспомогательный singleton для хранения FSM тасков
# Можно импортировать этот объект в main.py и fsm_task.py для управления
class FSMRegistry:
    _instance = None

    def __init__(self):
        self.tasks = {}

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = FSMRegistry()
        return cls._instance


fsm_registry = FSMRegistry.instance()


class ScenarioFSM:
    def __init__(self, model, kafka_producer: KafkaProducerWrapper, event_publish_queue: asyncio.Queue):
        self.model = model                  # ScenarioRunModel (run)
        self.kafka_producer = kafka_producer
        self.event_publish_queue = event_publish_queue
        self.queue = asyncio.Queue()
        self.last_heartbeat = datetime.utcnow()
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT
        self.scenario_id = str(model.scenario_id)
        self.run_id = str(model.id)
        self.machine = Machine(
            model=self,
            states=states,
            transitions=transitions,
            initial=model.state,
        )
        self._running = True

    async def process_event(self, event):
        event_type = event.get("event_type")
        if event_type == "heartbeat":
            self.last_heartbeat = datetime.utcnow()
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat received.")
        elif event_type == "trigger_scenario":
            trigger = event["trigger"]
            if hasattr(self, trigger):
                try:
                    getattr(self, trigger)()
                    await self.on_state_change()
                except Exception as e:
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM transition error: {e}")
            else:
                print(f"[FSM:{self.scenario_id}/{self.run_id}] Unknown trigger: {trigger}")
        elif event_type == "runner_lost":
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Runner lost, initiating restart.")
            await self.initiate_restart()
        else:
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Unknown event: {event}")

    async def on_state_change(self):
        # Теперь session создаём локально!
        async with async_session() as session:
            await update_run_state(session, self.model.id, self.state)
        await self.event_publish_queue.put({
            "event_type": "scenario_status_changed",
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "new_state": self.state,
            "timestamp": datetime.utcnow().isoformat(),
        })
        print(f"[FSM:{self.scenario_id}/{self.run_id}] State changed to {self.state}.")

    async def heartbeat_watcher(self):
        try:
            while self._running:
                await asyncio.sleep(self.heartbeat_timeout)
                if (datetime.utcnow() - self.last_heartbeat).total_seconds() > self.heartbeat_timeout:
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat timeout. Triggering runner_lost event.")
                    await self.queue.put({"event_type": "runner_lost"})
        except asyncio.CancelledError:
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher stopped (FSM inactive).")

    async def initiate_restart(self):
        try:
            # Корректный shutdown текущего run, если не завершён
            if self.state not in ["in_shutdown_processing", "inactive"]:
                if hasattr(self, "shutdown"):
                    self.shutdown()
                    await self.on_state_change()
                if hasattr(self, "stop"):
                    self.stop()
                    await self.on_state_change()
        except Exception as e:
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Error during shutdown: {e}")

        try:
            # Старт нового запуска (FSM не создаёт новый run — только переводит свой state)
            if hasattr(self, "start"):
                self.start()
                await self.on_state_change()
            if hasattr(self, "activate"):
                self.activate()
                await self.on_state_change()
        except Exception as e:
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Error during restart: {e}")

    async def run(self):
        heartbeat_task = None
        try:
            while self._running:
                event = await self.queue.get()
                await self.process_event(event)

                # === Запуск heartbeat только при переходе в active ===
                if self.state == "active" and heartbeat_task is None:
                    heartbeat_task = asyncio.create_task(self.heartbeat_watcher())
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher started.")

                # === Остановка heartbeat, если выходим из active ===
                if self.state != "active" and heartbeat_task is not None:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
                    heartbeat_task = None
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher stopped.")

                # === Финал, если сценарий завершён ===
                if self.state == "inactive":
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM task finished (inactive).")
                    break
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass
            # Удаление FSM из registry
            if self.run_id in fsm_registry.tasks:
                del fsm_registry.tasks[self.run_id]
                print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM removed from registry.")
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Task is exiting, should be cleaned up.")
