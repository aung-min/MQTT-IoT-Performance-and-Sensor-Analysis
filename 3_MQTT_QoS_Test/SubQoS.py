#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SubQoS.py — MQTT Subscriber that logs each received message to CSV.

CSV columns:
  run_id,qos,msg_id,topic_name,msg_context,pub_ts,recv_ts,delay_s,delay_ms,
  declared_size_bytes,wire_size_bytes,pub_device,sub_device

Usage:
  python3 SubQoS.py --broker 192.168.1.10 --port 1883 --topic test/qos \
    --qos 2 --out mqtt_qos_log.csv --subdev RaspberryPi5
"""

import argparse
import csv
import json
import os
import sys
import time
from typing import Optional

import paho.mqtt.client as mqtt

CSV_HEADER = [
    "run_id", "qos", "msg_id", "topic_name", "msg_context", "pub_ts", "recv_ts",
    "delay_s", "delay_ms", "declared_size_bytes", "wire_size_bytes",
    "pub_device", "sub_device"
]


def ensure_header(path: str):
    newfile = not os.path.exists(path)
    if newfile:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="127.0.0.1", help="MQTT broker hostname/IP")
    ap.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    ap.add_argument("--topic", default="test/qos_analyzer", help="MQTT topic to subscribe")
    ap.add_argument("--qos", type=int, default=2, choices=[0, 1, 2], help="Subscribe max QoS")
    ap.add_argument("--out", default="mqtt_qos_log.csv", help="CSV output path")
    ap.add_argument("--subdev", default="RaspberryPi5", help="Subscriber device label")
    args = ap.parse_args()

    ensure_header(args.out)

    client = mqtt.Client(client_id=f"SubQoS-{int(time.time())}", clean_session=True)
    client.enable_logger()

    def on_connect(cli, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"[SUB] Connected. Subscribing {args.topic} with QoS={args.qos}", flush=True)
            cli.subscribe(args.topic, qos=args.qos)
        else:
            print(f"[ERROR] Connect failed with rc={rc}", file=sys.stderr, flush=True)

    def on_message(cli, userdata, msg):
        recv_ts = time.time()
        raw = msg.payload  # bytes
        wire_size = len(raw)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            print(f"[WARN] Non-JSON payload (ignored): {e}", file=sys.stderr, flush=True)
            return

        rid = data.get("rid", "")
        q = int(data.get("q", -1))
        mid = int(data.get("id", -1))
        pub_ts = float(data.get("t", 0.0))
        ctx = str(data.get("ctx", ""))
        pub_dev = str(data.get("pub", ""))
        declared = int(data.get("sz", wire_size))

        delay_s = recv_ts - pub_ts
        delay_ms = delay_s * 1000.0

        row = [
            rid, q, mid, msg.topic, ctx, f"{pub_ts:.6f}", f"{recv_ts:.6f}",
            f"{delay_s:.6f}", f"{delay_ms:.3f}", declared, wire_size,
            pub_dev, args.subdev
        ]

        with open(args.out, "a", newline="") as f:
            csv.writer(f).writerow(row)

        # light progress printing
        if mid % 10 == 0:
            print(f"[SUB] QoS={q} received msg_id={mid} delay_ms={delay_ms:.1f} (declared/wire={declared}/{wire_size})",
                  flush=True)

    def on_disconnect(cli, userdata, rc, properties=None):
        print(f"[SUB] Disconnected (rc={rc})", flush=True)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(args.broker, args.port, keepalive=30)
    except Exception as e:
        print(f"[ERROR] Cannot connect to MQTT broker {args.broker}:{args.port} -> {e}", file=sys.stderr)
        sys.exit(2)

    try:
        print(f"[SUB] Writing CSV to: {args.out}", flush=True)
        client.loop_forever()
    except KeyboardInterrupt:
        print("[SUB] Interrupted by user.", flush=True)
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
