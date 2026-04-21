"""Producer com key: demonstra particionamento por hash(key).

Mesmo `user_id` sempre cai na mesma particao -> garante ordenacao por usuario.
Use este script no Cenario 3.
"""
from __future__ import annotations

import argparse
import random
import time
from datetime import datetime

from _common import TOPIC, banner, build_producer, console


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--keys", default="alice,bob,carol,dan")
    parser.add_argument("--interval", type=float, default=0.3)
    parser.add_argument("--count", type=int, default=0)
    args = parser.parse_args()

    keys = [k.strip() for k in args.keys.split(",") if k.strip()]
    banner(f"producer_keyed keys={keys}")
    producer = build_producer()

    i = 0
    try:
        while args.count == 0 or i < args.count:
            key = random.choice(keys)
            event = {
                "id": i,
                "ts": datetime.utcnow().isoformat(),
                "user": key,
                "action": random.choice(["login", "click", "purchase", "logout"]),
            }
            metadata = producer.send(args.topic, key=key, value=event).get(timeout=10)
            console.print(
                f"[green]enviado[/] key=[magenta]{key}[/] "
                f"-> part={metadata.partition} offset={metadata.offset}"
            )
            i += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        console.print("[yellow]interrompido[/]")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
