# Pipeline de pagamentos em streaming | Confluent Cloud

Projeto de referência para um pipeline de pagamentos orientado a eventos, com ingestão de dados em PostgreSQL, captura de mudanças com Debezium, processamento em Flink SQL e detecção de fraude em tempo real.

Este repositório demonstra uma arquitetura simples e funcional para portfólio: PostgreSQL (Neon) → Debezium CDC → Kafka/Avro → Flink SQL → alerta de fraude. O foco está em clareza, rastreabilidade e evidências de execução, sem complexidade desnecessária.

## Visão geral

A solução cobre os passos principais do desafio de entrega:

- modelagem de dados e carga inicial em PostgreSQL
- replicação lógica com Debezium CDC
- publicação e consumo de eventos em Kafka
- governança de schema com Schema Registry
- processamento temporal com Flink SQL
- detecção de comportamento suspeito em transações
- consumidor idempotente para persistência local e reentrega segura
- evidências sanitizadas para documentação e apresentação

## Stack

- PostgreSQL com logical replication
- Confluent Cloud: Kafka, Connect, Schema Registry e Flink
- Avro e Schema Registry
- Python 3.10+
- SQLite para armazenamento local do consumidor

## Arquitetura

1. O banco PostgreSQL armazena clientes, contas, cartões, estabelecimentos e transações.
2. O conector Debezium publica mudanças em Kafka via envelopes CDC.
3. O Schema Registry valida e evolui os contratos Avro.
4. O Flink SQL realiza enriquecimento temporal e identifica padrões de fraude.
5. O consumidor decodifica alertas Avro e grava somente eventos únicos em SQLite.

## Requisitos

Antes de iniciar, configure o arquivo `.env` a partir do exemplo:

```bash
cp .env.example .env
```

Em seguida, ajuste as variáveis para o seu ambiente PostgreSQL e Confluent Cloud. Nunca compartilhe o arquivo `.env` no Git.

## Como executar

### 1) Preparar o banco e a massa de dados

```bash
bash setup.sh
```

### 2) Configurar o conector CDC

A configuração do conecto está em `connectors/cdc.json` e deve ser renderizada com variáveis do `.env`.

```bash
set -a
source .env
set +a
envsubst < connectors/cdc.json > /tmp/payments-cdc.json
```

Depois, use o Confluent CLI para criar o connector e validar os tópicos Kafka.

### 3) Processar no Flink SQL

Execute em ordem:

```bash
sql/01_tables.sql
sql/02_fraud_rules.sql
```

A regra de fraude identifica três ou mais transações aprovadas do mesmo cartão em até 60 segundos e publica o alerta em `desafio.fraud.detected`.

### 4) Consumir alertas locais

```bash
python -m pip install -r consumer/requirements.txt
set -a
source .env
set +a
python consumer/consume_alerts.py
```

Para validar a idempotência local:

```bash
python -m unittest discover -s consumer -p "test_*.py" -v
```

## Checklist do desafio

Este projeto atende aos blocos principais esperados pelo desafio:

- [x] Estrutura de ingestão com PostgreSQL e CDC
- [x] Publicação de eventos em Kafka
- [x] Contratos Avro e governança de schema
- [x] Processamento em Flink SQL com temporal join
- [x] Regra de fraude por janela de tempo
- [x] Consumidor idempotente com SQLite
- [x] Evidências sanitizadas do projeto
- [x] Testes locais de reentrega e deduplicação

## Evidências

A pasta `evidencias/` contém registros sanitizados das execuções observadas:

- `01-envelopes-cdc.json`
- `02-compatibilidade-schema.txt`
- `03-alerta-fraude.json`
- `04-seguranca-acls.txt`
- `05-custos-status.txt`
- `06-confiabilidade.txt`

Esses materiais são úteis para apresentação em entrevistas, portfólio e demonstração técnica.

## Estrutura do projeto

- `schemas/`: contratos Avro
- `connectors/`: configuração do connector Debezium
- `sql/`: tabelas e regra de fraude
- `consumer/`: consumidor Kafka/Avro + testes
- `evidencias/`: registros e evidências da execução
- `setup.sh`: preparação do banco e dados
- `teardown.sh`: guia de limpeza de recursos cloud

## Observações finais

Este projeto foi pensado para apresentar uma solução realista, funcional e bem documentada em um portfólio profissional. A implementação foi mantida simples e direta, com foco em demonstrar capacidade de construir fluxo de dados em streaming, governança de schema, processamento em tempo real e resiliência de consumo.

O repositório está preparado para entrega final e preserva a documentação técnica necessária para reprodução e discussão em contexto profissional.

## Git

Para revisar alterações locais e o estado do repositório:

```bash
git status
```
