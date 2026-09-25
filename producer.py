"""Producer de exemplo: publica pedidos falsos em JSON num topic do Kafka."""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

CLIENTES = ["ana", "bruno", "carla", "diego", "elisa"]
PRODUTOS = ["camiseta", "caneca", "livro", "mouse", "teclado"]


def on_delivery(err, msg):
    if err is not None:
        print(f"Falha ao entregar: {err}")
        return
    print(
        f"Entregue em {msg.topic()} [partição {msg.partition()}] "
        f"offset {msg.offset()} chave={msg.key().decode()}"
    )


def novo_pedido():
    return {
        "id": str(uuid.uuid4()),
        "cliente": random.choice(CLIENTES),
        "produto": random.choice(PRODUTOS),
        "quantidade": random.randint(1, 5),
        "criado_em": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "pedidos"))
    parser.add_argument("--interval", type=float, default=1.0, help="segundos entre mensagens")
    parser.add_argument("--count", type=int, default=0, help="quantas mensagens enviar (0 = até Ctrl+C)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    print(f"Enviando para {args.topic} em {args.bootstrap_servers} (Ctrl+C para parar)")

    enviadas = 0
    try:
        while args.count == 0 or enviadas < args.count:
            pedido = novo_pedido()
            # A chave define a partição: pedidos do mesmo cliente ficam na mesma partição, em ordem
            producer.produce(
                args.topic,
                key=pedido["cliente"],
                value=json.dumps(pedido),
                on_delivery=on_delivery,
            )
            producer.poll(0)  # dispara os callbacks de entrega pendentes
            enviadas += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nInterrompido.")
    finally:
        pendentes = producer.flush(10)
        if pendentes:
            print(f"{pendentes} mensagem(ns) não foram entregues.")


if __name__ == "__main__":
    main()
