"""Cria (ou recria) o topico da demo com N particoes."""
from __future__ import annotations

import argparse

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from _common import BOOTSTRAP, TOPIC, console


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=TOPIC)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication", type=int, default=1)
    parser.add_argument("--recreate", action="store_true", help="apaga antes de criar")
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="create-topics")

    if args.recreate:
        try:
            admin.delete_topics([args.name])
            console.print(f"[yellow]topico {args.name} apagado[/]")
        except UnknownTopicOrPartitionError:
            pass

    topic = NewTopic(
        name=args.name,
        num_partitions=args.partitions,
        replication_factor=args.replication,
    )
    try:
        admin.create_topics([topic])
        console.print(
            f"[green]criado[/] {args.name} "
            f"particoes={args.partitions} replicacao={args.replication}"
        )
    except TopicAlreadyExistsError:
        console.print(f"[yellow]topico {args.name} ja existe[/]")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
