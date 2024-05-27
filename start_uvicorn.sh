#!/bin/bash
source /app/.venv/bin/activate
uvicorn main:fastapi_app --host 0.0.0.0 --port $PORT --reload
