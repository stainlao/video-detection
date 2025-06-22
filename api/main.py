from fastapi import FastAPI
from routes.scenario import router as scenario_router

app = FastAPI()
app.include_router(scenario_router)
