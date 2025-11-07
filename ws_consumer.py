#!/usr/bin/env python3
import os, sys, time, random, json
from collections import deque
import asyncio

# ensure proto_gen is importable
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

# buffers
json_buffer = deque()
PROTO_FLUSH_EVERY = 20

def flush_json():
    if not json_buffer:
        return
    fname = os.path.join(OUT_DIR, f"feed-decoded-{int(time.time())%10}.ndjson")
    with open(fname, "a") as fh:
        while json_buffer:
            fh.write(json.dumps(json_buffer.popleft()) + "\n")
    print("Flushed decoded JSON ->", fname, flush=True)

def persist_raw(msg_bytes, idx):
    fname = os.path.join(OUT_DIR, f"feed-proto-raw-{idx%10}.bin")
    with open(fname, "ab") as fh:
        fh.write(msg_bytes)
    print("Wrote raw proto ->", fname, flush=True)

async def run():
    attempt = 0
    while True:
        try:
            async with websockets.connect(URL, ping_interval=25, ping_timeout=10, max_size=None) as ws:
                print("CONNECTED", flush=True)
                sub = {"method":"SUBSCRIPTION","params":[CHAN],"id": random.randint(1,1_000_000)}
                await ws.send(json.dumps(sub))
                print("✅ Sent subscription:", sub, flush=True)

                idx = 0
                last_flush = time.time()
                while True:
                    msg = await ws.recv()
                    # default: no wrapper until parse succeeds
                    wrapper = None
                    if isinstance(msg, bytes):
                        try:
                            wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                            wrapper.ParseFromString(msg)
                        except Exception as e:
                            # parse failure — persist raw and continue
                            print("⚠️ Parse failed for incoming proto:", str(e)[:200], flush=True)
                            persist_raw(msg, idx)
                            idx += 1
                            # flush occasionally to keep persistence timely
                            if idx % PROTO_FLUSH_EVERY == 0 or (time.time() - last_flush) > 3:
                                flush_json()
                                last_flush = time.time()
                            continue

                        # parse succeeded — examine wrapper safely
                        try:
                            # prefer the explicit publicAggreDeals path we discovered
                            if hasattr(wrapper, "publicAggreDeals"):
                                obj = wrapper.publicAggreDeals
                                # convert to dict
                                out = {
                                    "channel": getattr(wrapper, "channel", str(CHAN)),
                                    "symbol": getattr(wrapper, "symbol", SYMBOL.replace('_','')),
                                    "sendTime": str(getattr(wrapper, "sendTime", "")),
                                    "deals": json_format.MessageToDict(obj, preserving_proto_field_name=True).get("deals", [])
                                }
                                json_buffer.append(out)
                                print("Decoded proto -> buffered JSON", flush=True)
                            else:
                                # fallback: serialize entire wrapper for future inspection
                                out_full = json_format.MessageToDict(wrapper, preserving_proto_field_name=True)
                                json_buffer.append({"channel": getattr(wrapper, "channel", str(CHAN)), "wrapper": out_full})
                                print("Decoded wrapper (fallback) -> buffered full JSON", flush=True)
                        except Exception as e:
                            # If converting or accessing nested fields fails, persist raw and log
                            print("⚠️ Post-parse handling error:", str(e)[:300], flush=True)
                            persist_raw(msg, idx)
                    else:
                        # handle text/control messages
                        try:
                            j = json.loads(msg)
                            print("TXT MSG:", j, flush=True)
                        except Exception:
                            print("UNK TEXT MSG", str(msg)[:200], flush=True)

                    idx += 1
                    if idx % PROTO_FLUSH_EVERY == 0 or (time.time() - last_flush) > 3:
                        flush_json()
                        last_flush = time.time()

        except (ConnectionClosedOK, ConnectionClosedError) as e:
            print("⚠️ Websocket closed:", e, flush=True)
            flush_json()
            attempt += 1
            await asyncio.sleep(min(60, 2**attempt) + random.uniform(0,1))
        except Exception as e:
            print("⚠️ Consumer error:", e, flush=True)
            flush_json()
            attempt += 1
            await asyncio.sleep(min(60, 2**attempt) + random.uniform(0,1))

if __name__ == "__main__":
    asyncio.run(run())
