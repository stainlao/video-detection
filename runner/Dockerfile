FROM python:3.10-slim

WORKDIR /app

COPY runner/requirements.txt /app/requirements.txt

RUN apt-get update && apt-get install -y ffmpeg libsm6 libxext6 && \
    pip install --no-cache-dir -r requirements.txt

COPY runner /app

ENV PYTHONUNBUFFERED=1

CMD ["bash", "-c", "python wait_for_services.py && python main.py"]
