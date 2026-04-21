"""Consumer unico, sem grupo (ou grupo proprio): le tudo e imprime."""
from __future__ import annotations

import argparse
import uuid

from _common import TOPIC, banner, build_consumer, pretty_record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=f"simple-{uuid.uuid4().hex[:6]}")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="le desde o inicio (auto_offset_reset=earliest, default)",
    )
    args = parser.parse_args()

    banner(f"consumer_simple group={args.group}")
    reset = "earliest" if args.from_beginning else "latest"
    consumer = build_consumer(args.group, auto_offset_reset=reset, topics=[args.topic])
    try:
        for msg in consumer:
            pretty_record(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
