FROM python:3.10-slim

WORKDIR /app

COPY api /app
COPY kafka_to /app/kafka_to
COPY api/wait_for_services.py /wait_for_services.py

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["bash", "-c", "python /wait_for_services.py && python create_tables.py && uvicorn main:app --host 0.0.0.0 --port 8000"]