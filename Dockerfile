FROM python:3.10.2

WORKDIR /app

COPY . /app

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y redis-server

# Создание виртуальной среды и установка зависимостей
RUN python3 -m venv /app/.venv
RUN /app/.venv/bin/pip install --upgrade pip && /app/.venv/bin/pip install -r requirements.txt

# Копирование скриптов для запуска
COPY start_uvicorn.sh /app/start_uvicorn.sh
COPY wait-for-redis.sh /app/wait-for-redis.sh

# Установка прав на выполнение скриптов
RUN chmod +x /app/start_uvicorn.sh
RUN chmod +x /app/wait-for-redis.sh

EXPOSE 6379
EXPOSE 8000

CMD /app/.venv/bin/uvicorn main:fastapi_app --host 0.0.0.0 --port 8000 --reload
