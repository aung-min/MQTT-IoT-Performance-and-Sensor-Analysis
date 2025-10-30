#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PubQoS.py — MQTT Publisher that sends N messages per QoS (0,1,2),
with JSON payload including size (`sz`) and context.

Usage:
  python3 PubQoS.py --broker 192.168.1.10 --port 1883 --topic test/qos \
    --per 100 --interval 0.05 --runid runA --pubdev RaspberryPi5 \
    --ctx "sensor=desk,temp=24.6,hum=47.2"
"""

import argparse
import json
import time
import sys
from typing import Optional

import paho.mqtt.client as mqtt


def build_payload(rid: str, qos: int, seq_id: int, ctx: str, pub: str) -> bytes:
    """Build a compact JSON payload and include its byte size `sz`."""
    obj = {"rid": rid, "q": qos, "id": seq_id, "t": time.time(), "ctx": ctx, "pub": pub}
    tmp = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    obj["sz"] = len(tmp)  # declared size (bytes of compact JSON)
    payload_bytes = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return payload_bytes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="127.0.0.1", help="MQTT broker hostname/IP")
    ap.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    ap.add_argument("--topic", default="test/qos_analyzer", help="MQTT topic")
    ap.add_argument("--per", type=int, default=100, help="Messages per QoS")
    ap.add_argument("--interval", type=float, default=0.05, help="Seconds between messages")
    ap.add_argument("--runid", default="runA", help="Run ID")
    ap.add_argument("--pubdev", default="RaspberryPi5", help="Publisher device label")
    ap.add_argument("--ctx", default="", help="Message context template/text")
    args = ap.parse_args()

    # If no explicit context, make a compact default (keeps payloads consistent)
    if not args.ctx:
        args.ctx = "sensor=desk,temp=24.5,hum=47.0"

    client = mqtt.Client(client_id=f"PubQoS-{int(time.time())}", clean_session=True)
    client.enable_logger()  # logs to stderr (GUI will capture)
    try:
        client.connect(args.broker, args.port, keepalive=30)
    except Exception as e:
        print(f"[ERROR] Cannot connect to MQTT broker {args.broker}:{args.port} -> {e}", file=sys.stderr)
        sys.exit(2)

    client.loop_start()
    try:
        for qos in (0, 1, 2):
            print(f"[PUB] QoS={qos} starting batch ({args.per} msgs)...", flush=True)
            for i in range(args.per):
                payload = build_payload(args.runid, qos, i, args.ctx, args.pubdev)
                info = client.publish(args.topic, payload=payload, qos=qos)
                # Optional: wait for mid completion on qos>0
                if qos > 0:
                    info.wait_for_publish(timeout=10)
                if i % 10 == 0:
                    print(f"[PUB] QoS={qos} sent {i+1}/{args.per}", flush=True)
                time.sleep(args.interval)
            print(f"[PUB] QoS={qos} batch complete.", flush=True)

        print("[PUB] All QoS batches complete.", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
