FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта orchestrator (включая kafka_to)
COPY . .

# Ждём PostgreSQL, затем запускаем orchestrator
CMD ["bash", "-c", "python wait_for_postgres.py && python main.py"]
