FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc tesseract-ocr poppler-utils \
    && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/parser/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/parser /app/services/parser

ENV PYTHONPATH=/app
EXPOSE 8002

CMD ["uvicorn", "services.parser.main:app", "--host", "0.0.0.0", "--port", "8002"]
