#!/usr/bin/env python3
"""
Hardened asyncio consumer + writer + optional Redis router.
Writes atomic NDJSON and pushes JSON to Redis list 'ingest:queue' if REDIS_URL set.
Robust proto decode: converts wrapper to dict and finds 'deals'.
"""

import os, sys, time, json, asyncio, random, tempfile
from collections import deque

# ensure proto_gen is on path
sys.path.insert(0, '/opt/mexc/consumer/proto_gen')

from google.protobuf import json_format
import PushDataV3ApiWrapper_pb2

# modern asyncio redis (redis>=4.5)
REDIS_URL = os.environ.get("REDIS_URL")
if REDIS_URL:
    try:
        import redis.asyncio as aioredis
    except Exception:
        aioredis = None

# parameters
URL = os.environ.get("MEXC_WS_URL", "wss://wbs-api.mexc.com/ws")
SYMBOL = os.environ.get("MEXC_SYMBOL", "BTC_USDT")
CHAN = f"spot@public.aggre.deals.v3.api.pb@100ms@{SYMBOL.replace('_','').upper()}"
OUT_DIR = "/opt/mexc/ingest"
os.makedirs(OUT_DIR, exist_ok=True)

QUEUE_MAXSIZE = int(os.environ.get("QUEUE_MAXSIZE", "20000"))
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY", "50"))
FLUSH_INTERVAL_S = float(os.environ.get("FLUSH_INTERVAL_S", "2.0"))

q = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

drops = 0

def _atomic_write(path, lines):
    """
    Atomically write lines (iterable of strings) to path.
    Uses tempfile in same dir and os.replace for atomicity.
    """
    dirn = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".tmp.", dir=dirn)
    try:
        with os.fdopen(fd, "w") as fh:
            for ln in lines:
                fh.write(ln + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception as e:
        # cleanup tmp
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise

async def writer_loop(redis_url=None):
    """
    Pop items off queue, write to NDJSON files atomically, and optionally push to Redis.
    """
    redis = None
    if redis_url:
        if aioredis is None:
            print("⚠️ WARNING: REDIS_URL set but redis.asyncio import failed; Redis disabled.", flush=True)
        else:
            try:
                redis = aioredis.from_url(redis_url, max_connections=8)
                await redis.ping()
                print("✅ Redis router connected.", flush=True)
            except Exception as e:
                print("⚠️ Redis connection failed:", e, flush=True)
                redis = None

    buf = []
    last_flush = time.time()
    file_idx = 0
    while True:
        try:
            try:
                item = await asyncio.wait_for(q.get(), timeout=FLUSH_INTERVAL_S)
            except asyncio.TimeoutError:
                item = None

            if item is not None:
                buf.append(item)
                if redis:
                    try:
                        # push as JSON str
                        await redis.rpush("ingest:queue", json.dumps(item))
                    except Exception as e:
                        print("⚠️ Redis push error:", e, flush=True)
                q.task_done()

            # flush by size or time
            if len(buf) >= FLUSH_EVERY or (time.time() - last_flush) >= FLUSH_INTERVAL_S:
                if buf:
                    fname = os.path.join(OUT_DIR, f"feed-decoded-{file_idx}.ndjson")
                    try:
                        _atomic_write(fname, [json.dumps(x) for x in buf])
                        print("Flushed decoded JSON ->", fname, flush=True)
                    except Exception as e:
                        print("⚠️ Writer failed to flush:", e, flush=True)
                    file_idx += 1
                    buf.clear()
                    last_flush = time.time()
            await asyncio.sleep(0)  # cooperative yield
        except Exception as e:
            print("⚠️ writer_loop exception:", e, flush=True)
            await asyncio.sleep(1)

def extract_deals_from_wrapper(wrapper):
    """
    Convert the wrapper protobuf to dict and find any 'deals' lists inside.
    Returns list of deal dicts.
    """
    try:
        d = json_format.MessageToDict(wrapper, preserving_proto_field_name=True)
    except Exception as e:
        # fallback: return empty
        return []
    # depth-first search for 'deals' keys
    stack = [d]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "deals" in node and isinstance(node["deals"], list):
                return node["deals"]
            for v in node.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return []

async def handle_message_bytes(msg_bytes):
    """
    Parse proto bytes and enqueue trade dicts with timestamp fields.
    """
    local_ts_ns = time.time_ns()
    try:
        wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
        wrapper.ParseFromString(msg_bytes)
    except Exception as e:
        print("⚠️ Proto parse failed:", e, flush=True)
        return

    deals = extract_deals_from_wrapper(wrapper)
    for d in deals:
        try:
            # many feeds use ms; if value looks like seconds convert.
            raw_ts = d.get("time", None)
            if raw_ts is None:
                continue
            exch_ts = int(raw_ts)
            # guess units: if < 1e12 then seconds -> convert to ms, else assume ms
            exch_ms = exch_ts if exch_ts > 1_000_000_000_000 else int(exch_ts * 1000)
            local_ms = local_ts_ns / 1e6
            lag_ms = (local_ms - exch_ms)
            out = {
                "exchange_ts": int(exch_ms),
                "local_ts": int(local_ts_ns),
                "lag_ms": round(lag_ms, 3),
                "symbol": SYMBOL.replace("_",""),
                "price": d.get("price"),
                "quantity": d.get("quantity"),
                "tradeType": d.get("tradeType")
            }
            try:
                q.put_nowait(out)
            except asyncio.QueueFull:
                # drop oldest, then push; count drops
                global drops
                drops += 1
                try:
                    _ = q.get_nowait()
                    q.put_nowait(out)
                except Exception:
                    # if still failing, skip
                    pass
        except Exception as e:
            print("⚠️ Error building out dict:", e, flush=True)

async def consumer_loop():
    import websockets
    from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
    attempt = 0
    while True:
        try:
            async with websockets.connect(URL, ping_interval=25, ping_timeout=10, max_size=None) as ws:
                print("CONNECTED", flush=True)
                sub = {"method":"SUBSCRIPTION","params":[CHAN],"id":random.randint(1,1_000_000)}
                await ws.send(json.dumps(sub))
                print("✅ Sent subscription:", sub, flush=True)
                while True:
                    msg = await ws.recv()
                    if isinstance(msg, bytes):
                        await handle_message_bytes(msg)
                    else:
                        print("TXT MSG:", msg, flush=True)
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            print("⚠️ Websocket closed:", e, flush=True)
            await asyncio.sleep(1)
        except Exception as e:
            print("⚠️ Consumer error:", e, flush=True)
            await asyncio.sleep(min(5, 0.5 * (2 ** attempt)))
            attempt += 1

async def main():
    redis_url = REDIS_URL
    writer = asyncio.create_task(writer_loop(redis_url))
    consumer = asyncio.create_task(consumer_loop())
    await asyncio.gather(writer, consumer)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down", flush=True)
