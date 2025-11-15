FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY relay.py /app/relay.py

RUN pip3 install --no-cache-dir \
    redis==5.0.1 \
    clickhouse-driver==0.2.6 \
    prometheus_client==0.16.0

ENV PYTHONUNBUFFERED=1

CMD ["python3", "-u", "/app/relay.py"]
