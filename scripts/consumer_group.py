"""Consumer que participa de um grupo compartilhado.

Rode varias instancias deste script com o MESMO --group em terminais
diferentes para ver o rebalance ao vivo (Cenarios 4, 5 e 6).

O handler `on_assign` / `on_revoke` imprime as particoes atribuidas
sempre que o grupo reequilibra.
"""
from __future__ import annotations

import argparse
import socket

from kafka.consumer.subscription_state import ConsumerRebalanceListener

from _common import TOPIC, banner, build_consumer, console, pretty_record

HOST_TAG = f"{socket.gethostname()}/{__import__('os').getpid()}"


class LoggingRebalanceListener(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked) -> None:
        console.print(f"[red]\\[{HOST_TAG}] revogadas:[/] {sorted(str(p) for p in revoked)}")

    def on_partitions_assigned(self, assigned) -> None:
        console.print(f"[green]\\[{HOST_TAG}] atribuidas:[/] {sorted(str(p) for p in assigned)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", required=True)
    args = parser.parse_args()

    banner(f"consumer_group group={args.group} pid={HOST_TAG}")
    consumer = build_consumer(args.group, topics=[args.topic])
    consumer.subscribe([args.topic], listener=LoggingRebalanceListener())

    try:
        for msg in consumer:
            pretty_record(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
