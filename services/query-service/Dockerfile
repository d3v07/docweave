FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/query-service/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/query-service /app/services/query_service

ENV PYTHONPATH=/app
EXPOSE 8005

CMD ["uvicorn", "services.query_service.main:app", "--host", "0.0.0.0", "--port", "8005"]
