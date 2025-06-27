import uuid
import datetime
from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import declarative_base

Base = declarative_base(cls=AsyncAttrs)


class ScenarioRunModel(Base):
    __tablename__ = "scenario_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)  # run_id / task_id
    scenario_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    state = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
