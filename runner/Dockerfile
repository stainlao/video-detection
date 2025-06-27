FROM python:3.10-slim

WORKDIR /app

COPY runner /app

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "python wait_for_services.py && python main.py"]
