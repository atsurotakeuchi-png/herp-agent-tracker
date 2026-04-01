FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ backend/
COPY herp_agent_tracker.html .
COPY .env* ./

RUN mkdir -p /app/data

ENV DB_PATH=/app/data/herp_tracker.db

EXPOSE 8080

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8080"]
