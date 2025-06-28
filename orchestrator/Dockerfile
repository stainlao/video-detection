FROM python:3.10-slim

WORKDIR /app

COPY orchestrator/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY orchestrator /app
COPY kafka_to /app/kafka_to

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "python wait_for_services.py && python create_tables.py && python main.py"]
