"""Cria o topic de forma explícita, escolhendo partições e replicação."""

import argparse
import os

from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "pedidos"))
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    futuros = admin.create_topics(
        [NewTopic(args.topic, args.partitions, args.replication_factor)]
    )
    for topic, futuro in futuros.items():
        try:
            futuro.result()
            print(f"Topic '{topic}' criado ({args.partitions} partições).")
        except Exception as e:
            print(f"Não foi possível criar '{topic}': {e}")

    meta = admin.list_topics(args.topic, timeout=10).topics.get(args.topic)
    if meta and not meta.error:
        for p in sorted(meta.partitions.values(), key=lambda p: p.id):
            print(f"  partição {p.id}: líder={p.leader} réplicas={p.replicas} isr={p.isrs}")


if __name__ == "__main__":
    main()
