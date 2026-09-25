# Apache Kafka com Docker

Projeto de estudo: Kafka com Zookeeper, Kafka UI e um producer em Python.

## O que foi feito até aqui

- `docker-compose.yml` com três serviços:
  - **Zookeeper** (`confluentinc/cp-zookeeper:7.9.10`)
  - **Kafka** (`confluentinc/cp-kafka:7.9.10`, que é o Kafka 3.9) em `localhost:9092`
  - **Kafka UI** (`kafbat/kafka-ui:v1.5.0`) em http://localhost:8080
- `producer.py`: producer em Python (`confluent-kafka`) que envia pedidos falsos em JSON para o topic `pedidos`, usando o cliente como chave da mensagem.
- Ambiente virtual Python criado com a dependência instalada (`requirements.txt`).

Escolhi o Kafka 3.9 porque o Zookeeper foi removido no Kafka 4.x, e a imagem oficial `apache/kafka` só roda em KRaft.

## Como usar

```bash
docker compose up -d                                  # sobe Zookeeper, Kafka e Kafka UI
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # só na primeira vez
.venv/bin/python producer.py                          # envia 1 pedido por segundo (Ctrl+C para parar)
docker compose down                                   # para tudo (down -v também apaga os dados)
```

Opções do producer: `--topic`, `--interval` (segundos entre mensagens) e `--count` (quantidade de mensagens, 0 = infinito).

Para ver as mensagens: Kafka UI, em **Topics → pedidos → Messages**.

## Próximos passos

- [ ] Instalar o Docker Desktop (`brew install --cask docker-desktop`, precisa da senha do `sudo`).
- [ ] Subir os containers e confirmar que o Kafka fica `healthy`.
- [ ] Rodar o `producer.py` e ver as mensagens no Kafka UI (ainda **não foi testado**, pois o Docker não estava instalado).
- [ ] Criar um consumer em Python.
