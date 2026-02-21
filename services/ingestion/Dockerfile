FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY connectors /app/connectors
COPY services/ingestion/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/ingestion /app/services/ingestion

ENV PYTHONPATH=/app
EXPOSE 8001

CMD ["uvicorn", "services.ingestion.main:app", "--host", "0.0.0.0", "--port", "8001"]
