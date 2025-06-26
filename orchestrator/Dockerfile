FROM python:3.10-slim

WORKDIR /app

COPY orchestrator /app
COPY kafka_to /app/kafka_to

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "python wait_for_services.py && python create_tables.py && python main.py"]
