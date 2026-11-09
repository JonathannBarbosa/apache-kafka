PY = .venv/bin/python

.PHONY: up down reset setup topic produce consume groups describe

up:        ## sobe Zookeeper, Kafka e Kafka UI
	docker compose up -d

down:      ## para tudo (mantém os dados)
	docker compose down

reset:     ## para tudo e apaga os dados (volumes)
	docker compose down -v

setup:     ## cria o venv e instala as dependências
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

topic:     ## cria o topic 'pedidos' com 3 partições
	$(PY) create_topic.py

produce:   ## envia 1 pedido por segundo
	$(PY) producer.py

consume:   ## consome no grupo padrão (use GROUP=outro para mudar)
	$(PY) consumer.py --group-id $(or $(GROUP),processador-pedidos)

groups:    ## lista os consumer groups
	docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

describe:  ## mostra offsets e lag de um grupo (GROUP=...)
	docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $(or $(GROUP),processador-pedidos)
