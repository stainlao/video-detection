FROM python:3.10-slim

WORKDIR /app

COPY api /app
COPY kafka_to /app/kafka_to

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
