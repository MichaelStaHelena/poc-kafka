# poc-kafka

Demo acompanhando a apresentacao **"Apache Kafka na pratica"** — slides + docker-compose (Kafka KRaft + Kafka-UI) + scripts Python mostrando topicos, particoes, offsets, consumer groups e segmentos crescendo em disco.

## Pre-requisitos

- Docker Desktop (ou docker engine + compose v2)
- Python 3.11+
- Opcional: [`uv`](https://docs.astral.sh/uv/) (recomendado) ou `pip`
- Opcional, para exportar slides em PDF: Node.js (para `npx @marp-team/marp-cli`)

## Subindo o cluster

```bash
docker compose up -d
docker compose ps   # esperar ambos "healthy"
```

Servicos:

| Servico    | URL / porta               | Para que serve                      |
|------------|---------------------------|-------------------------------------|
| kafka      | `localhost:9094` (host)   | bootstrap para clientes Python      |
| kafka-ui   | http://localhost:8080     | UI para topicos/particoes/mensagens |

Os dados do broker ficam em `./kafka-data/` no seu disco — e la que voce vai olhar os arquivos `.log` crescerem durante a palestra.

## Instalando as dependencias Python

Com `uv` (recomendado):
```bash
uv sync
uv run python scripts/create_topics.py
```

Com `pip`:
```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -e .
python scripts/create_topics.py
```

## Cenarios da demo

| # | O que mostra                                  | Comando                                                                 |
|---|-----------------------------------------------|-------------------------------------------------------------------------|
| 1 | Topico nascendo, 3 pastas em `kafka-data/`    | `python scripts/create_topics.py --recreate`                            |
| 2 | Producer + 1 consumer, offset crescendo       | `python scripts/producer_simple.py` **e** `python scripts/consumer_simple.py --from-beginning` |
| 3 | Particionamento por `key` (hash)              | `python scripts/producer_keyed.py --keys alice,bob,carol,dan`           |
| 4 | Consumer group + rebalance ao vivo            | 3 terminais com `python scripts/consumer_group.py --group g1`           |
| 5 | Mais consumers que particoes (idle)           | 4o terminal com `python scripts/consumer_group.py --group g1`           |
| 6 | Multiplos grupos, fan-out                     | `--group g1` **e** `--group g2` simultaneos                             |
| 7 | Lag e reset de offsets                        | `python scripts/show_offsets.py --group g1 [--reset-to earliest]`       |
| 8 | Burst -> varios segmentos `.log`              | `python scripts/producer_burst.py --count 50000`                        |

Para olhar o que ta no disco durante a demo:
```bash
ls kafka-data/demo.events-0/
# 00000000000000000000.log  00000000000000000000.index  ...
```

## Variaveis de ambiente

Opcional — os defaults ja funcionam com o `docker-compose.yml` deste repo.

```bash
cp .env.example .env
# KAFKA_BOOTSTRAP=localhost:9094
# KAFKA_TOPIC=demo.events
```

Os scripts leem essas variaveis via `os.environ`; se nao existirem, usam os defaults.

## Slides

Os slides sao [Marp](https://marp.app/) em Markdown puro.

```bash
cd slides
make pdf          # gera deck.pdf
make serve        # preview ao vivo em http://localhost:8080
```

Tambem pode ser aberto dentro do VS Code com a extensao "Marp for VS Code".

## Limpando tudo

```bash
docker compose down -v
rm -rf kafka-data/
```

## Estrutura do repo

```
.
|-- docker-compose.yml      # Kafka (KRaft single-node) + Kafka-UI
|-- pyproject.toml          # kafka-python, rich, faker
|-- scripts/                # producers, consumers, admin (ver tabela acima)
|-- slides/
|   |-- deck.md             # slides Marp em PT-BR
|   |-- images/
|   `-- Makefile            # make pdf / make serve
|-- kafka-data/             # [gitignored] volume do broker
`-- README.md
```
