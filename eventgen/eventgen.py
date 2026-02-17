import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        linger_ms=5,
        retries=3,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def send(producer: KafkaProducer, topic: str, event: dict, force_event_id: str | None = None):
    key = force_event_id or event.get("eventId") or str(uuid4())
    event["eventId"] = key
    producer.send(topic, key=key, value=event)


def event_noise():
    return {
        "eventId": str(uuid4()),
        "timestamp": now_iso(),
        "category": "ops",
        "type": "app.error",
        "service": "catalog",
        "env": "prod",
        "signals": {"error_rate": 0.002, "latency_p95_ms": 220},
        "context": {"endpoint": "/search", "region": "eu-west-1"},
        "message": "Read timeout sporadique sur /search (retry ok)"
    }


def event_incident_vip():
    return {
        "eventId": str(uuid4()),
        "timestamp": now_iso(),
        "category": "ops",
        "type": "slo.burn",
        "service": "payment",
        "env": "prod",
        "signals": {"error_rate": 0.12, "latency_p95_ms": 1800, "slo_burn": 6.5},
        "context": {"customerTier": "VIP", "orderId": "D-10492", "region": "eu-west-1"},
        "message": "Checkout failures spike (VIP impacted)"
    }


def event_remediate_lag():
    return {
        "eventId": str(uuid4()),
        "timestamp": now_iso(),
        "category": "ops",
        "type": "consumer.lag",
        "service": "events-router",
        "env": "prod",
        "signals": {"lag": 250000, "lag_growth_per_min": 40000},
        "context": {"runbookHint": "scale-consumer", "consumerGroup": "router-v1", "region": "eu-west-1"},
        "message": "Lag runaway; risk of delayed processing"
    }


def cmd_noise(args):
    for _ in range(args.count):
        send(args.producer, args.topic, event_noise())
        time.sleep(args.sleep)


def cmd_incident_vip(args):
    send(args.producer, args.topic, event_incident_vip())


def cmd_remediate_lag(args):
    send(args.producer, args.topic, event_remediate_lag())


def cmd_replay(args):
    if args.template == "incident-vip":
        e = event_incident_vip()
    elif args.template == "remediate-lag":
        e = event_remediate_lag()
    else:
        e = event_noise()
    send(args.producer, args.topic, e, force_event_id=args.event_id)


def main():
    parser = argparse.ArgumentParser(prog="eventgen")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events.v1"))

    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("noise", help="Send noise burst events")
    p1.add_argument("--count", type=int, default=10)
    p1.add_argument("--sleep", type=float, default=0.05)
    p1.set_defaults(fn=cmd_noise)

    p2 = sub.add_parser("incident-vip", help="Send a VIP incident event")
    p2.set_defaults(fn=cmd_incident_vip)

    p3 = sub.add_parser("remediate-lag", help="Send a remediate lag event")
    p3.set_defaults(fn=cmd_remediate_lag)

    p4 = sub.add_parser("replay", help="Replay a scenario with a fixed eventId")
    p4.add_argument("--template", choices=["noise", "incident-vip", "remediate-lag"], default="incident-vip")
    p4.add_argument("--event-id", required=True)
    p4.set_defaults(fn=cmd_replay)

    args = parser.parse_args()

    producer = make_producer(args.bootstrap)
    args.producer = producer

    try:
        args.fn(args)
        producer.flush(10)
    finally:
        producer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
