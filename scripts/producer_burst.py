"""Producer em rajada: envia muitos eventos rapido para ver o log crescer.

Use no Cenario 8 -- combinado com `ls kafka-data/demo.events-0/` durante e
depois do burst para ver varios segmentos `.log` / `.index` / `.timeindex`
sendo criados (o broker esta configurado com segmentos de 1 MB).
"""
from __future__ import annotations

import argparse
import time

from _common import TOPIC, banner, build_producer, console


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--count", type=int, default=50_000)
    parser.add_argument("--size", type=int, default=512, help="bytes do payload")
    args = parser.parse_args()

    banner(f"producer_burst count={args.count} size={args.size}B")
    producer = build_producer(linger_ms=20, batch_size=64 * 1024)

    payload = {"blob": "x" * args.size}
    start = time.perf_counter()
    for i in range(args.count):
        payload["id"] = i
        producer.send(args.topic, value=payload)
        if i % 5000 == 0 and i:
            console.print(f"[cyan]...enviados {i}[/]")
    producer.flush()
    producer.close()
    elapsed = time.perf_counter() - start
    console.print(f"[green]{args.count} mensagens em {elapsed:.2f}s[/]")


if __name__ == "__main__":
    main()
