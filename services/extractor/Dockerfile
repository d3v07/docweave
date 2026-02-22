FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/extractor/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm

COPY services/extractor /app/services/extractor

ENV PYTHONPATH=/app
EXPOSE 8003

CMD ["uvicorn", "services.extractor.main:app", "--host", "0.0.0.0", "--port", "8003"]
