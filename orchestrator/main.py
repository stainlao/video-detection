import asyncio
import yaml
from transitions import Machine
from crud import create_scenario, get_scenario, update_scenario_state
from models import ScenarioModel
from db import async_session  # обязательно импортируй сессию
import uuid

# Загрузка состояния FSM из workflow.yaml
with open("workflow.yaml", "r") as f:
    workflow = yaml.safe_load(f)

states = workflow["states"]
transitions = workflow["transitions"]


class ScenarioFSM:
    def __init__(self, model: ScenarioModel, session):
        self.model = model
        self.session = session
        self.machine = Machine(
            model=self,
            states=states,
            transitions=transitions,
            initial=model.state,
        )

    # Асинхронные методы-обработчики переходов (вызываем отдельно)
    async def on_start(self):
        print(f"FSM({self.model.id}): start → {self.state}")
        await update_scenario_state(self.session, self.model.id, self.state)

    async def on_activate(self):
        print(f"FSM({self.model.id}): activate → {self.state}")
        await update_scenario_state(self.session, self.model.id, self.state)

    async def on_shutdown(self):
        print(f"FSM({self.model.id}): shutdown → {self.state}")
        await update_scenario_state(self.session, self.model.id, self.state)

    async def on_stop(self):
        print(f"FSM({self.model.id}): stop → {self.state}")
        await update_scenario_state(self.session, self.model.id, self.state)


async def run_fsm_lifecycle():
    print("Создание нового FSM в состоянии 'init_startup'")

    async with async_session() as session:
        scenario_id = await create_scenario("init_startup")
        print(f"scenario_id = {scenario_id}")

        model = await get_scenario(scenario_id)
        fsm = ScenarioFSM(model, session)

        print("Переход: start")
        fsm.start()           # синхронный переход
        await fsm.on_start()  # async обработчик

        print("Переход: activate")
        fsm.activate()
        await fsm.on_activate()

        print("Переход: shutdown")
        fsm.shutdown()
        await fsm.on_shutdown()

        print("Переход: stop")
        fsm.stop()
        await fsm.on_stop()

        final_model = await get_scenario(scenario_id)
        print(f"Финальное состояние в БД: {final_model.state}")


if __name__ == "__main__":
    asyncio.run(run_fsm_lifecycle())
