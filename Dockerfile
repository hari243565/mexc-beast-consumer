FROM python:3.11-slim
WORKDIR /app
COPY ws_consumer.py /app/ws_consumer.py
RUN pip install --no-cache-dir websockets aiohttp
# Create the ingest data directory *inside* the container
RUN mkdir /app/ingest
# The command to run when the container starts
CMD ["python","/app/ws_consumer.py"]

# --- ADD THIS SECTION NEAR THE BOTTOM OF YOUR EXISTING Dockerfile ---
# ensure protobuf runtime is installed
RUN pip install --no-cache-dir protobuf
# ------------------------------------------------------------
