"""Producer basico: envia eventos SEM key.

Sem key, o particionador faz round-robin entre as particoes disponiveis
(em clientes recentes, 'sticky' por batch). Use este script no Cenario 2.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime

from faker import Faker

from _common import TOPIC, banner, build_producer, console

fake = Faker("pt_BR")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--interval", type=float, default=0.5, help="segundos entre eventos")
    parser.add_argument("--count", type=int, default=0, help="0 = infinito")
    args = parser.parse_args()

    banner("producer_simple (sem key)")
    producer = build_producer()

    i = 0
    try:
        while args.count == 0 or i < args.count:
            event = {
                "id": i,
                "ts": datetime.utcnow().isoformat(),
                "msg": fake.sentence(nb_words=6),
            }
            future = producer.send(args.topic, value=event)
            metadata = future.get(timeout=10)
            console.print(
                f"[green]enviado[/] part={metadata.partition} "
                f"offset={metadata.offset} value={event}"
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
