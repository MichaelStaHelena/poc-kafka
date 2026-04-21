"""Mostra committed offsets, end offsets e lag de um consumer group.

Tambem permite resetar o grupo (apenas quando nao ha membros ativos) para
`earliest` ou `latest` -- util no Cenario 7.
"""
from __future__ import annotations

import argparse

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from rich.table import Table

from _common import BOOTSTRAP, TOPIC, console


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", required=True)
    parser.add_argument(
        "--reset-to",
        choices=["earliest", "latest"],
        help="reseta offsets do grupo (grupo precisa estar sem membros ativos)",
    )
    args = parser.parse_args()

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=args.group)
    partitions = [TopicPartition(args.topic, p) for p in consumer.partitions_for_topic(args.topic) or []]

    if args.reset_to:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        if args.reset_to == "earliest":
            target = consumer.beginning_offsets(partitions)
        else:
            target = consumer.end_offsets(partitions)
        from kafka.structs import OffsetAndMetadata

        consumer_for_commit = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP, group_id=args.group, enable_auto_commit=False
        )
        consumer_for_commit.assign(partitions)
        consumer_for_commit.commit(
            {tp: OffsetAndMetadata(off, "") for tp, off in target.items()}
        )
        consumer_for_commit.close()
        admin.close()
        console.print(f"[yellow]offsets do grupo {args.group} resetados para {args.reset_to}[/]")

    end_offsets = consumer.end_offsets(partitions)
    committed = {tp: consumer.committed(tp) for tp in partitions}
    consumer.close()

    table = Table(title=f"grupo={args.group} topico={args.topic}")
    table.add_column("partition", justify="right")
    table.add_column("committed", justify="right")
    table.add_column("end", justify="right")
    table.add_column("lag", justify="right", style="bold")

    for tp in sorted(partitions, key=lambda p: p.partition):
        end = end_offsets[tp]
        comm = committed[tp]
        comm_s = "-" if comm is None else str(comm)
        lag = "-" if comm is None else str(max(0, end - comm))
        table.add_row(str(tp.partition), comm_s, str(end), lag)

    console.print(table)


if __name__ == "__main__":
    main()
