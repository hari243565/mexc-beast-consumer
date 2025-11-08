FROM python:3.11-slim

# --- patched for protobuf 5.27.3 (source build, cleans build deps) ---
RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ make git && \
    pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --no-binary=protobuf protobuf==5.27.3 && \
    pip install --no-cache-dir websockets aiohttp redis prometheus_client && \
    apt-get purge -y gcc g++ make && \
    apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
CMD ["python3", "ws_consumer.py"]
