import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4
from kafka import KafkaProducer

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"


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


def send(producer: KafkaProducer, topic: str, event: dict, label: str, force_id: str = None):
    key = force_id or event.get("eventId") or str(uuid4())
    event["eventId"] = key
    print(f"{BLUE}[SEND]{RESET} {label.ljust(30)} | ID: {key[:8]}...")
    producer.send(topic, key=key, value=event)


SCENARIOS = {
    "noise": {
        "func": lambda: {
            "timestamp": now_iso(),
            "category": "ops",
            "type": "app.error",
            "service": "catalog",
            "env": "prod",
            "signals": {"error_rate": 0.002, "latency_p95_ms": 220},
            "context": {"endpoint": "/search", "region": "eu-west-1"},
            "message": "Sporadic read timeout on /search"
        },
        "label": f"{YELLOW}Noise (Catalog){RESET}"
    },
    "maintenance": {
        "func": lambda: {
            "timestamp": now_iso(),
            "category": "ops",
            "type": "service.down",
            "service": "catalog",
            "env": "prod",
            "signals": {"status": "unreachable", "ping_loss": "100%"},
            "message": "Catalog API heartbeat failed" # "Connection refused on port 443"
            # The AI must discover via MCP server that this is a scheduled maintenance.
        },
        "label": f"{YELLOW}Maintenance (Catalog){RESET}"
    },
    "regression": {
        "func": lambda: {
            "timestamp": now_iso(),
            "category": "ops",
            "type": "app.error",
            "service": "payment",
            "env": "prod",
            "signals": {"error_rate": 0.05},
            "message": "NullPointerException in ProductController"
            # The AI will see via 'get_recent_investigation_data' that a deployment occurred < 5 min ago.
            # Expected decision: suggest rollback or alert the commit author.
        },
        "label": f"{RED}Regression Post-Deploy (Payment){RESET}"
    },
    "vip": {
        "func": lambda: {
            "timestamp": now_iso(),
            "category": "ops",
            "type": "slo.burn",
            "service": "payment",
            "env": "prod",
            "signals": {"error_rate": 0.15, "slo_burn": 8.2},
            "context": {"customerTier": "VIP", "region": "eu-west-1"},
            "message": "Critical checkout failure spike (VIP impacted)"
        },
        "label": f"{RED}VIP Incident (Payment){RESET}"
    },
    "lag": {
        "func": lambda: {
            "timestamp": now_iso(),
            "category": "ops",
            "type": "consumer.lag",
            "service": "events-router",
            "env": "prod",
            "signals": {"lag": 350000},
            "context": {"runbookHint": "scale-consumer"},
            "message": "Lag runaway; risk of delayed processing"
        },
        "label": f"{GREEN}Auto-Remediation (Lag){RESET}"
    }
}


def cmd_demo_sequence(args):
    sequence = ["noise", "maintenance", "regression", "vip", "lag"]
    for s_key in sequence:
        scenario = SCENARIOS[s_key]
        send(args.producer, args.topic, scenario["func"](), scenario["label"])
        print(f"   {BLUE}-->{RESET} Waiting processing...")
        time.sleep(args.interval)


def cmd_replay(args):
    if args.template not in SCENARIOS:
        print(f"{RED}Error: unknown template '{args.template}'.{RESET}")
        return
    scenario = SCENARIOS[args.template]
    send(args.producer, args.topic, scenario["func"](), f"Replay: {args.template}", force_id=args.event_id)


def main():
    parser = argparse.ArgumentParser(prog="eventgen")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "events.all"))

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_seq = sub.add_parser("demo-sequence", help="Run several commands")
    p_seq.add_argument("--interval", type=int, default=10)
    p_seq.set_defaults(fn=cmd_demo_sequence)

    p_rep = sub.add_parser("replay", help="Replay an event with a specific ID")
    p_rep.add_argument("--template", choices=list(SCENARIOS.keys()), required=True)
    p_rep.add_argument("--event-id", required=True, help="Original event ID to replay")
    p_rep.set_defaults(fn=cmd_replay)

    for key in SCENARIOS.keys():
        p = sub.add_parser(key)
        p.set_defaults(fn=lambda a, k=key: send(a.producer, a.topic, SCENARIOS[k]["func"](), SCENARIOS[k]["label"]))

    args = parser.parse_args()
    producer = make_producer(args.bootstrap)
    args.producer = producer

    try:
        args.fn(args)
        producer.flush(5)
    finally:
        producer.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
