#!/usr/bin/env python3
import os, sys, time, json, asyncio, random
from collections import deque
sys.path.insert(0, '/opt/mexc/consumer/proto_gen')
from google.protobuf import json_format
import PushDataV3ApiWrapper_pb2
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

URL = "wss://wbs-api.mexc.com/ws"
SYMBOL = os.environ.get("MEXC_SYMBOL", "BTC_USDT")
CHAN = f"spot@public.aggre.deals.v3.api.pb@100ms@{SYMBOL.replace('_','').upper()}"
OUT_DIR = "/opt/mexc/ingest"
os.makedirs(OUT_DIR, exist_ok=True)
json_buffer = deque()
PROTO_FLUSH_EVERY = 20

def flush_json():
    if not json_buffer: return
    fname = os.path.join(OUT_DIR, f"feed-decoded-{int(time.time())%10}.ndjson")
    with open(fname, "a") as fh:
        while json_buffer:
            fh.write(json.dumps(json_buffer.popleft()) + "\n")
    print("Flushed decoded JSON ->", fname, flush=True)

async def run():
    attempt = 0
    while True:
        try:
            async with websockets.connect(URL, ping_interval=25, ping_timeout=10, max_size=None) as ws:
                print("CONNECTED", flush=True)
                sub = {"method":"SUBSCRIPTION","params":[CHAN],"id":random.randint(1,1_000_000)}
                await ws.send(json.dumps(sub))
                print("✅ Sent subscription:", sub, flush=True)

                idx, last_flush = 0, time.time()
                while True:
                    msg = await ws.recv()
                    local_ts = time.time_ns()
                    if isinstance(msg, bytes):
                        try:
                            wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            wrapper.ParseFromString(msg)
                            if hasattr(wrapper, "publicAggreDeals"):
                                obj = wrapper.publicAggreDeals
                                deals = json_format.MessageToDict(obj, preserving_proto_field_name=True).get("deals", [])
                                for d in deals:
                                    exch_ts = int(d.get("time", 0))
                                    lag_ms = (local_ts/1e6) - (exch_ts if exch_ts < 1e15 else exch_ts/1e3)
                                    json_buffer.append({
                                        "exchange_ts": exch_ts,
                                        "local_ts": local_ts,
                                        "lag_ms": round(lag_ms, 3),
                                        "symbol": SYMBOL.replace("_",""),
                                        "price": d.get("price"),
                                        "quantity": d.get("quantity"),
                                        "tradeType": d.get("tradeType")
                                    })
                                print(f"Decoded {len(deals)} deals -> buffered", flush=True)
                        except Exception as e:
                            print("⚠️ Proto undecoded ->", e, flush=True)
                    idx += 1
                    if idx % PROTO_FLUSH_EVERY == 0 or (time.time() - last_flush) > 3:
                        flush_json(); last_flush = time.time()
        except (ConnectionClosedOK, ConnectionClosedError) as e:
            print("⚠️ Websocket closed:", e, flush=True); flush_json(); await asyncio.sleep(2)
        except Exception as e:
            print("⚠️ Consumer error:", e, flush=True); flush_json(); await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(run())
