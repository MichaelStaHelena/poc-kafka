"""Helpers compartilhados pelos scripts de demo."""
from __future__ import annotations

import json
import os
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from rich.console import Console

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.environ.get("KAFKA_TOPIC", "demo.events")

console = Console()


def build_producer(**overrides: Any) -> KafkaProducer:
    kwargs: dict[str, Any] = {
        "bootstrap_servers": BOOTSTRAP,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        "acks": "all",
        "linger_ms": 5,
    }
    kwargs.update(overrides)
    return KafkaProducer(**kwargs)


def build_consumer(group: str | None, **overrides: Any) -> KafkaConsumer:
    topics = overrides.pop("topics", [TOPIC])
    kwargs: dict[str, Any] = {
        "bootstrap_servers": BOOTSTRAP,
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
        "key_deserializer": lambda k: k.decode("utf-8") if k else None,
        "enable_auto_commit": True,
        "auto_offset_reset": "earliest",
        "group_id": group,
    }
    kwargs.update(overrides)
    consumer = KafkaConsumer(**kwargs)
    consumer.subscribe(topics)
    return consumer


def pretty_record(msg) -> None:
    console.print(
        f"[bold cyan]part={msg.partition}[/] "
        f"[bold yellow]offset={msg.offset}[/] "
        f"[bold magenta]key={msg.key!r}[/] "
        f"value={msg.value}"
    )


def banner(title: str) -> None:
    console.rule(f"[bold green]{title}[/] — bootstrap={BOOTSTRAP} topic={TOPIC}")
