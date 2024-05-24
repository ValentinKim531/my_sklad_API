FROM python:3.10.2

WORKDIR /app

COPY . /app

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y redis-server

# Создание виртуальной среды и установка зависимостей
RUN python3 -m venv /app/.venv
RUN . /app/.venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# Копирование скриптов для запуска
COPY start_uvicorn.sh /app/start_uvicorn.sh

# Установка прав на выполнение скриптов
RUN chmod +x /app/start_uvicorn.sh

EXPOSE 6379
EXPOSE 8000

# Запуск всех служб
CMD service redis-server start && . /app/.venv/bin/activate && uvicorn main:app --host 0.0.0.0 --port 8000 & celery -A celery_worker worker --beat --loglevel=info
