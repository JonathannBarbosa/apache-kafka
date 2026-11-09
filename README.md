# Apache Kafka com Docker

Projeto de estudo: Kafka com Zookeeper, Kafka UI, um producer e um consumer em Python.

## O que tem aqui

- `docker-compose.yml` com três serviços:
  - **Zookeeper** (`confluentinc/cp-zookeeper:7.9.10`)
  - **Kafka** (`confluentinc/cp-kafka:7.9.10`, que é o Kafka 3.9) em `localhost:9092`
  - **Kafka UI** (`kafbat/kafka-ui:v1.5.0`) em http://localhost:8080
- `create_topic.py`: cria o topic `pedidos` com o número de partições que você escolher e mostra líder/réplicas/ISR.
- `producer.py`: envia pedidos falsos em JSON, usando o cliente como chave (mesmo cliente → mesma partição → ordem garantida).
- `consumer.py`: lê os pedidos dentro de um consumer group, com commit manual depois do processamento e logs de rebalance.
- `Makefile`: atalhos para os comandos (`up`, `topic`, `produce`, `consume`, `groups`, `describe`…).

Escolhi o Kafka 3.9 porque o Zookeeper foi removido no Kafka 4.x, e a imagem oficial `apache/kafka` só roda em KRaft.

## Como usar

```bash
make setup      # só na primeira vez: venv + dependências
make up         # sobe os containers (espere o Kafka ficar healthy)
make topic      # cria o topic 'pedidos' com 3 partições
make produce    # terminal 1: envia 1 pedido por segundo
make consume    # terminal 2: consome no grupo 'processador-pedidos'
make down       # para tudo (make reset também apaga os dados)
```

Sem `make`, é só rodar `.venv/bin/python producer.py` e `.venv/bin/python consumer.py` diretamente.

Opções do producer: `--topic`, `--interval` (segundos entre mensagens), `--count` (0 = infinito).
Opções do consumer: `--topic`, `--group-id`, `--from-beginning`, `--falhar-em <cliente>`.

Para ver tudo visualmente: Kafka UI → **Topics → pedidos → Messages** e **Consumers**.

## Experimentos

Todos foram testados neste projeto.

**1. Consumer group dividindo partições.** Rode `make consume` em dois terminais. Cada um recebe parte das partições (ex.: `[0, 1]` e `[2]`). Mate um com Ctrl+C e o outro assume as três (rebalance).

**2. Grupos diferentes recebem tudo.** `make consume GROUP=a` e `make consume GROUP=b` em paralelo: os dois leem todas as mensagens, cada grupo com seus próprios offsets.

**3. Retomada pelo offset.** Pare o consumer e rode de novo no mesmo grupo: ele continua de onde parou, sem reler nada. Veja com `make describe GROUP=...` (colunas `CURRENT-OFFSET` e `LAG`).

**4. Ordem por chave.** No output do consumer, todos os pedidos de um mesmo cliente aparecem na mesma partição, com offsets crescentes.

**5. At-least-once (reprocessamento).** Rode com `--falhar-em carla --from-beginning` num grupo novo. Ao chegar num pedido da carla o consumer quebra *antes* do commit. Rode de novo sem a flag e a mensagem é reprocessada.

> **Armadilha do `auto.offset.reset`:** ele só vale para partições em que o grupo **ainda não tem offset commitado**. No experimento 5, só as partições que chegaram a ter commit retomam de onde pararam. As outras caem em `latest` e pulam as mensagens antigas, a menos que você passe `--from-beginning`.

## Próximos passos (opcionais)

- [ ] Segundo consumer que agrega dados (ex.: total de itens por cliente).
- [ ] Dead letter topic para mensagens inválidas.
- [ ] Producer idempotente (`enable.idempotence`) e `acks=all`.
- [ ] Cluster com 3 brokers para estudar replicação, ISR e eleição de líder.
- [ ] Migrar para KRaft (`apache/kafka`).
- [ ] Schema Registry com Avro.
