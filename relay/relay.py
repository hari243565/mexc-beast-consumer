#!/usr/bin/env python3
# -----------------------------------------------------------
#   OMEGA-BUILD RELAY: Redis.asyncio  ->  ClickHouse
#   Fully Self-Healing, HFT-Grade Ingestion Layer
# -----------------------------------------------------------

import os
import json
import time
import asyncio
import traceback
import datetime
from concurrent.futures import ThreadPoolExecutor

# -----------------------------------------------------------
# ENV CONFIG
# -----------------------------------------------------------
CLICK_HOST = os.getenv("CLICK_HOST", "127.0.0.1")
CLICK_PORT = int(os.getenv("CLICK_PORT", "9000"))
CLICK_USER = os.getenv("CLICK_USER", "relay")
CLICK_PASS = os.getenv("CLICK_PASS", "2310antvb5s")
DB = os.getenv("CLICK_DB", "mexc")
TABLE = os.getenv("CLICK_TABLE", "ticks")

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
LIST_KEY = os.getenv("LIST_KEY", "ingest:queue")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_MS = float(os.getenv("FLUSH_MS", "200"))
SPILL_DIR = os.getenv("SPILL_DIR", "/opt/mexc/consumer/relay/spill")

METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

os.makedirs(SPILL_DIR, exist_ok=True)

# -----------------------------------------------------------
# DYNAMIC DEP IMPORT (redis.asyncio + clickhouse-driver)
# -----------------------------------------------------------
async def import_deps():
    global redis_async, clickhouse_driver, PrometheusServer, Counter, Gauge
    import importlib

    redis_async = importlib.import_module("redis.asyncio")
    clickhouse_driver = importlib.import_module("clickhouse_driver")

    from prometheus_client import start_http_server, Counter, Gauge
    PrometheusServer = start_http_server

# -----------------------------------------------------------
# CLICKHOUSE INSERT (runs in threadpool)
# -----------------------------------------------------------
def clickhouse_insert_blocking(cfg, rows):
    client = clickhouse_driver.Client(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        secure=False,
        connect_timeout=5,
        send_receive_timeout=10
    )
    client.execute(
        f"INSERT INTO {cfg['database']}.{TABLE} "
        "(event_time,exchange_ts,local_ts,lag_ms,symbol,price,quantity,tradeType) VALUES",
        rows
    )

# -----------------------------------------------------------
# CLASSIFY EXCEPTIONS (HEALING DECISION MAKER)
# -----------------------------------------------------------
def classify_exception(e):
    msg = str(e).lower()
    if "timeout" in msg:
        return "TimeoutError"
    if "connection" in msg or "refused" in msg or "unreachable" in msg:
        return "ConnectionError"
    if "schema" in msg or "type" in msg or "tzinfo" in msg:
        return "SchemaError"
    return "Other"

# -----------------------------------------------------------
# DURABLE SPILL (ZERO DATA LOSS)
# -----------------------------------------------------------
def spill_rows(rows):
    fn = os.path.join(SPILL_DIR, f"spill-{int(time.time())}.ndjson")
    with open(fn, "a", encoding="utf-8") as f:
        for r in rows:
            d = list(r)
            if isinstance(d[0], datetime.datetime):
                d[0] = d[0].isoformat()
            f.write(json.dumps(d) + "\n")
    print(f"🔴 SPILLED {len(rows)} rows to {fn}", flush=True)

# -----------------------------------------------------------
# MAP JSON -> TYPED ROW
# -----------------------------------------------------------
def msg_to_row(msg):
    ts = int(msg.get("exchange_ts", 0))
    dt = datetime.datetime.fromtimestamp(ts/1000, tz=datetime.timezone.utc)

    return (
        dt,
        ts,
        int(msg.get("local_ts", 0)),
        float(msg.get("lag_ms", 0.0)),
        str(msg.get("symbol", "")),
        float(msg.get("price", 0.0)),
        float(msg.get("quantity", 0.0)),
        int(msg.get("tradeType", 0)),
    )

# -----------------------------------------------------------
# MAIN LOOP (OMEGA-BUILD SELF-HEALING ENGINE)
# -----------------------------------------------------------
async def main():
    await import_deps()

    # Start Prometheus
    try:
        PrometheusServer(METRICS_PORT)
        print(f"📊 Prometheus metrics on :{METRICS_PORT}", flush=True)
    except Exception as e:
        print("⚠ Prometheus unavailable:", e, flush=True)

    cfg = dict(host=CLICK_HOST, port=CLICK_PORT, user=CLICK_USER, password=CLICK_PASS, database=DB)

    # -------------------------
    # SELF-HEALING REDIS CONNECT
    # -------------------------
    redis_client = None
    backoff = 0.5
    while redis_client is None:
        try:
            redis_client = redis_async.from_url(REDIS_URL)
            await redis_client.ping()
            print("🔌 Connected to Redis", flush=True)
        except Exception as e:
            print("⚠ Redis connect failed:", e, flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10)

    # Threadpool for ClickHouse writes
    pool = ThreadPoolExecutor(max_workers=2)
    loop = asyncio.get_event_loop()

    buf = []
    last_flush = time.time()

    print("🚀 Relay main loop started", flush=True)

    # -------------------------
    # INFINITE LOOP
    # -------------------------
    while True:
        try:
            # BRPOP with timeout
            res = await redis_client.brpop(LIST_KEY, timeout=1)
            if res:
                _, payload = res
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode("utf-8")
                msg = json.loads(payload)
                buf.append(msg_to_row(msg))

            now = time.time()
            if buf and ((now - last_flush) * 1000 >= FLUSH_MS or len(buf) >= BATCH_SIZE):
                rows = buf[:BATCH_SIZE]
                buf = buf[BATCH_SIZE:]
                last_flush = now

                tries = 0
                while True:
                    try:
                        await loop.run_in_executor(pool, clickhouse_insert_blocking, cfg, rows)
                        print(f"✅ Inserted {len(rows)} rows", flush=True)
                        break
                    except Exception as exc:
                        etype = classify_exception(exc)
                        print(f"❌ Insert failed ({etype}): {exc}", flush=True)
                        tries += 1

                        if etype == "ConnectionError" and tries < 6:
                            await asyncio.sleep(min(5, 0.5*(2**tries)))
                            continue
                        elif etype == "SchemaError":
                            spill_rows(rows)
                            break
                        else:
                            spill_rows(rows)
                            break

        except Exception as e:
            print("🔥 Loop exception:", e, flush=True)
            traceback.print_exc()
            await asyncio.sleep(1)

# -----------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("✋ Relay stopped by user", flush=True)
    except Exception as e:
        print("❌ Fatal exit:", e, flush=True)
        traceback.print_exc()
