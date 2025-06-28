import asyncio
import yaml
from datetime import datetime
from transitions import Machine
from crud import update_run_state
from kafka_to.producer import KafkaProducerWrapper
from kafka_to.settings import SCENARIO_EVENTS_TOPIC, RUNNER_COMMANDS_TOPIC
from db import async_session

HEARTBEAT_TIMEOUT = 10

with open("workflow.yaml", "r") as f:
    workflow = yaml.safe_load(f)
states = workflow["states"]
transitions = workflow["transitions"]


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
        self.active_runner = "runner-1"
        self.failover_runner = "runner-2"
        self.pending_shutdown = False
        self.machine = Machine(
            model=self,
            states=states,
            transitions=transitions,
            initial=model.state,
        )
        self._running = True

    async def process_event(self, event):
        event_type = event.get("event_type")

        if self.state == "inactive":
            print(f"[FSM:{self.scenario_id}/{self.run_id}] State is inactive. Ignoring event: {event}")
            return

        if event_type == "heartbeat":
            self.last_heartbeat = datetime.utcnow()
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat received.")
            if self.state == "in_startup_processing":
                if hasattr(self, "activate"):
                    self.activate()
                    await self.on_state_change()

        elif event_type == "runner_finished":
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Runner finished work.")
            if self.pending_shutdown:
                print(
                    f"[FSM:{self.scenario_id}/{self.run_id}] Pending manual shutdown: transitioning to init_shutdown.")
                self.pending_shutdown = False
                if hasattr(self, "init_shutdown"):
                    self.init_shutdown()
                    await self.on_state_change()
            else:
                if self.state == "active":
                    if hasattr(self, "init_shutdown"):
                        self.init_shutdown()
                        await self.on_state_change()
                elif self.state == "init_shutdown":
                    if hasattr(self, "shutdown"):
                        self.shutdown()
                        await self.on_state_change()
                elif self.state == "in_shutdown_processing":
                    if hasattr(self, "stop"):
                        self.stop()
                        await self.on_state_change()
                else:
                    print(
                        f"[FSM:{self.scenario_id}/{self.run_id}] Received runner_finished in state {self.state} (ignored)")

        elif event_type == "trigger_scenario":
            trigger = event["trigger"]
            # Для ручного shutdown
            if trigger == "init_shutdown" and self.state in ["active", "init_shutdown"]:
                stop_event = {
                    "event_type": "runner_stop",
                    "scenario_id": self.scenario_id,
                    "run_id": self.run_id,
                    "runner_id": self.active_runner,
                }
                await self.kafka_producer.send(RUNNER_COMMANDS_TOPIC, stop_event)
                print(
                    f"[FSM:{self.scenario_id}/{self.run_id}] Sent runner_stop to {self.active_runner} (shutdown requested by user/API)")
                self.pending_shutdown = True
                return  # <-- ОЧЕНЬ ВАЖНО: здесь return, дальше не делаем переходы по init_shutdown, ждем runner_finished

            # --- обработка остальных триггеров ---
            if hasattr(self, trigger) and trigger in self.machine.get_triggers(self.state):
                try:
                    getattr(self, trigger)()
                    await self.on_state_change()
                except Exception as e:
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM transition error: {e}")
            else:
                print(f"[FSM:{self.scenario_id}/{self.run_id}] Trigger '{trigger}' not allowed from state {self.state}")

        elif event_type == "runner_lost":
            print(
                f"[FSM:{self.scenario_id}/{self.run_id}] Runner {self.active_runner} lost! Checking state for failover/restart...")
            if self.state == "in_startup_processing":
                if self.active_runner == "runner-1":
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Failover: switching to {self.failover_runner}.")
                    self.active_runner, self.failover_runner = self.failover_runner, self.active_runner
                    event = {
                        "event_type": "runner_start",
                        "scenario_id": self.scenario_id,
                        "run_id": self.run_id,
                        "runner_id": self.active_runner,
                        "video_path": "/input_videos/7048885897979.mp4"
                    }
                    await self.kafka_producer.send(RUNNER_COMMANDS_TOPIC, event)
                    print(f"[FSM:{self.scenario_id}] Sent runner_start for {self.active_runner}: {event}")
                    self.last_heartbeat = datetime.utcnow()
                else:
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Both runners failed to start. Stopping scenario.")
                    if hasattr(self, "stop"):
                        self.stop()
                        await self.on_state_change()
            elif self.state == "active":
                print(f"[FSM:{self.scenario_id}/{self.run_id}] Active runner died, will restart with failover runner.")
                self.next_active_runner = self.failover_runner
                await self.initiate_restart()
            else:
                print(f"[FSM:{self.scenario_id}/{self.run_id}] runner_lost in state {self.state} (ignored)")

    async def on_state_change(self):
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

        if self.state == "inactive":
            # Всё, финал — никаких последующих триггеров
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Now inactive. Final cleanup done.")
            return

        if self.state == "in_startup_processing":
            event = {
                "event_type": "runner_start",
                "scenario_id": self.scenario_id,
                "run_id": self.run_id,
                "runner_id": self.active_runner,
                "video_path": "/input_videos/7048885897979.mp4"
            }
            await self.kafka_producer.send(RUNNER_COMMANDS_TOPIC, event)
            print(f"[FSM:{self.scenario_id}] Sent runner_start for {self.active_runner}: {event}")

        elif self.state == "init_shutdown":
            # Останавливаем watchdog
            if hasattr(self, "heartbeat_task") and self.heartbeat_task is not None:
                print(f"[FSM:{self.scenario_id}/{self.run_id}] Cancelling heartbeat watcher in init_shutdown...")
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            print(
                f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher cancelled. Triggering shutdown transition...")
            # Только если действительно в init_shutdown
            if hasattr(self, "shutdown") and self.state == "init_shutdown":
                self.shutdown()
                await self.on_state_change()

        elif self.state == "in_shutdown_processing":
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Shutdown processing done, going to inactive.")
            # Только если действительно в in_shutdown_processing
            if hasattr(self, "stop") and self.state == "in_shutdown_processing":
                self.stop()
                await self.on_state_change()

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
            # Только если FSM в active, сначала корректно переводим в init_shutdown
            if self.state == "active":
                if hasattr(self, "init_shutdown"):
                    self.init_shutdown()
                    await self.on_state_change()
                return  # Ждём runner_finished и shutdown-цепочку
            # После runner_finished FSM перейдёт в init_shutdown
            if self.state == "init_shutdown":
                if hasattr(self, "shutdown"):
                    self.shutdown()
                    await self.on_state_change()
                return
            # После этого FSM окажется в in_shutdown_processing
            if self.state == "in_shutdown_processing":
                if hasattr(self, "stop"):
                    self.stop()
                    await self.on_state_change()
                return
            # Только теперь можно реально перезапускать run — в состоянии inactive
            if self.state == "inactive":
                # Если нужен failover, переключаем runner
                if hasattr(self, "next_active_runner"):
                    print(
                        f"[FSM:{self.scenario_id}/{self.run_id}] Switching active runner to {self.next_active_runner} for restart.")
                    self.active_runner, self.failover_runner = self.next_active_runner, self.active_runner
                    del self.next_active_runner
                if hasattr(self, "restart"):
                    self.restart()
                    await self.on_state_change()
                return
        except Exception as e:
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Error during restart: {e}")

    async def run(self):
        self.heartbeat_task = None
        try:
            while self._running:
                event = await self.queue.get()
                await self.process_event(event)
                # Стартуем watchdog только в in_startup_processing и active
                if self.state in ["in_startup_processing", "active"] and self.heartbeat_task is None:
                    self.heartbeat_task = asyncio.create_task(self.heartbeat_watcher())
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher started.")
                # Остановить watchdog, если мы больше не в in_startup_processing и не в active
                if self.state not in ["in_startup_processing", "active"] and self.heartbeat_task is not None:
                    self.heartbeat_task.cancel()
                    try:
                        await self.heartbeat_task
                    except asyncio.CancelledError:
                        pass
                    self.heartbeat_task = None
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] Heartbeat watcher stopped.")
                if self.state == "inactive":
                    print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM task finished (inactive).")
                    break
        finally:
            # Остановить heartbeat_task, если он ещё жив
            if self.heartbeat_task is not None:
                self.heartbeat_task.cancel()
                try:
                    await self.heartbeat_task
                except asyncio.CancelledError:
                    pass
                self.heartbeat_task = None
            if self.run_id in fsm_registry.tasks:
                del fsm_registry.tasks[self.run_id]
                print(f"[FSM:{self.scenario_id}/{self.run_id}] FSM removed from registry.")
            print(f"[FSM:{self.scenario_id}/{self.run_id}] Task is exiting, should be cleaned up.")

