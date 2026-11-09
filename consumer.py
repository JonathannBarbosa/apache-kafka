"""Consumer de exemplo: lê pedidos de um topic do Kafka dentro de um consumer group."""

import argparse
import json
import os
import signal

from confluent_kafka import Consumer, KafkaError

CLIENTES_VALIDOS = ["ana", "bruno", "carla", "diego", "elisa"]


def on_assign(consumer, partitions):
    print(f"[rebalance] partições atribuídas: {sorted(p.partition for p in partitions)}")


def on_revoke(consumer, partitions):
    print(f"[rebalance] partições revogadas: {sorted(p.partition for p in partitions)}")


def processar(pedido):
    """Aqui entraria a lógica de negócio. Use --falhar-em para simular um erro."""
    return pedido


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "pedidos"))
    parser.add_argument("--group-id", default="processador-pedidos", help="consumer group")
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="grupo novo começa do início do topic (auto.offset.reset=earliest)",
    )
    parser.add_argument(
        "--falhar-em",
        choices=CLIENTES_VALIDOS,
        help="simula falha ao processar pedidos desse cliente, ANTES do commit",
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            # Commit manual só depois de processar: garante at-least-once
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
    print(f"Consumindo {args.topic} no grupo '{args.group_id}' (Ctrl+C para parar)")

    rodando = True

    def parar(*_):
        nonlocal rodando
        rodando = False

    signal.signal(signal.SIGINT, parar)
    signal.signal(signal.SIGTERM, parar)

    try:
        while rodando:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Erro: {msg.error()}")
                continue

            pedido = json.loads(msg.value())
            if args.falhar_em and pedido["cliente"] == args.falhar_em:
                # Sem commit: ao reiniciar, o consumer relê esta mensagem (reprocessamento)
                raise RuntimeError(f"falha simulada no pedido {pedido['id']}")

            processar(pedido)
            print(
                f"[partição {msg.partition()} offset {msg.offset()}] "
                f"{pedido['cliente']} pediu {pedido['quantidade']}x {pedido['produto']}"
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()  # sai do grupo de forma limpa e dispara o rebalance nos demais
        print("Consumer encerrado.")


if __name__ == "__main__":
    main()
