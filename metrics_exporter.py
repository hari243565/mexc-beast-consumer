#!/usr/bin/env python3
from prometheus_client import start_http_server, Summary
import json, os, time, glob

LAG = Summary('ingest_lag_ms', 'Lag between exchange and local timestamps')

def watch_files():
    seen = set()
    while True:
        files = sorted(glob.glob("/opt/mexc/ingest/feed-decoded-*.ndjson"), key=os.path.getmtime)
        if not files:
            time.sleep(2); continue
        with open(files[-1]) as f:
            for line in f:
                if line in seen: continue
                seen.add(line)
                try:
                    data = json.loads(line)
                    lag = float(data.get("lag_ms", 0))
                    if lag: LAG.observe(lag)
                except: pass
        time.sleep(2)

if __name__ == "__main__":
    start_http_server(9200)
    watch_files()
