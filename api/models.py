import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Enum, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class OutboxStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class OutboxModel(Base):
    __tablename__ = "outbox"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(String, default=OutboxStatus.PENDING, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    sent_at = Column(DateTime, nullable=True)

    
class ScenarioModel(Base):
    __tablename__ = "scenario"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state = Column(String, nullable=False, default="init_startup")
    created_at = Column(DateTime, default=datetime.utcnow)

