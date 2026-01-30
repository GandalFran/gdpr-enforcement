FROM python:3.9-slim

WORKDIR /app

COPY code/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY code/ code/

ENV PYTHONPATH=/app/code
ENV KAFKA_BROKER=broker:29092
ENV PG_HOST=postgres
ENV PYTHONUNBUFFERED=1

CMD ["python", "code/src/ingestion/main.py"]
