# Pipeline de pagamentos em streaming | Confluent Cloud

Implementacao de referencia de uma arquitetura de pagamentos orientada a eventos: PostgreSQL (Neon) -> Debezium CDC -> Kafka/Avro -> Flink SQL -> alertas de fraude.

Este repositorio demonstra captura de alteracoes de dados, processamento temporal e deteccao de padroes de transacoes. Inclui scripts SQL, contratos Avro, configuracao-base do conector, um consumidor idempotente e evidencias sanitizadas de execucoes no Confluent Cloud.

> **Escopo:** implementacao de referencia para portfolio, nao um servico de pagamentos em producao. As evidencias documentam execucoes reais; alguns itens de observabilidade, governanca e validacao ponta a ponta ainda estao pendentes e sao identificados abaixo.

## Arquitetura e capacidades

1. PostgreSQL fornece os dados de clientes, contas, cartoes, estabelecimentos e transacoes.
2. Debezium captura INSERT, UPDATE e DELETE por logical replication e publica envelopes CDC em Kafka.
3. Schema Registry aplica contratos Avro e regras de compatibilidade.
4. Flink SQL enriquece transacoes com dados de conta por temporal join e identifica tres ou mais transacoes aprovadas do mesmo cartao em uma janela de 60 segundos.
5. Um consumidor independente decodifica alertas Avro, persiste-os em SQLite e tolera reentregas por meio de uma chave idempotente.

## Tecnologias

- Confluent Cloud: Apache Kafka, Schema Registry, Connect e Flink.
- PostgreSQL compativel com logical replication (ex.: Neon).
- Python 3.10+ e `confluent-kafka[avro]` para o consumidor.
- Git Bash ou WSL, `psql`, `envsubst` e Confluent CLI para configurar e inspecionar recursos cloud.

Recursos cloud podem gerar custos. Revise precos e configuracao da conta antes de criar recursos e acompanhe o billing durante a execucao.

## PostgreSQL e dados

No Git Bash/WSL, entre nesta pasta e crie seu arquivo local de configuracao:

```bash
cp .env.example .env
```

Configure um banco PostgreSQL com logical replication habilitada. Armazene a connection string em `DATABASE_URL`; para o conector, use o hostname de conexao direta em `CDC_DATABASE_HOST` (nao um endpoint pooled) e preencha `CDC_DATABASE_NAME`. Use aspas na connection string se ela tiver caracteres especiais. Nunca compartilhe o arquivo `.env`.

Prepare as tabelas e a massa de dados como proprietario do banco:

```bash
bash setup.sh
```

O resultado esperado e customers=200, accounts=240, cards=320, merchants=60 e transactions=2000. O script tambem aplica `REPLICA IDENTITY FULL`. Se a conexao falhar, confira `DATABASE_URL`, acesso de rede e se `psql` esta instalado.

Como proprietario do banco, crie um usuario dedicado de menor privilegio. Troque a senha pelo valor local de `CDC_DATABASE_PASSWORD`:

```sql
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD 'SENHA_LOCAL';
GRANT CONNECT ON DATABASE payments TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
```

Crie a publication como proprietario do banco:

```sql
CREATE PUBLICATION payments_publication
FOR TABLE customers, accounts, cards, merchants, transactions;
```

Se a publication ja existir, nao a crie novamente. Em um banco ja usado, confira as tabelas antes de rodar a carga: IDs existentes nao sao sobrescritos.

## Identidades e controle de acesso

No Confluent Cloud:

1. Use um environment e um cluster Kafka compativeis com a regiao dos demais recursos.
2. Mantenha identidades distintas para producao/CDC e consumo.
3. Aplique apenas as ACLs necessarias: `CREATE`, `WRITE` e `DESCRIBE` nos topicos CDC para a identidade produtora; `WRITE` e `DESCRIBE` no topico de alertas; `READ` e `DESCRIBE` nos topicos consumidos e `READ` no consumer group para a identidade consumidora.
4. Gere chaves Kafka distintas por service account e credenciais apropriadas para o Schema Registry. Guarde os valores apenas em `.env`, nunca no Git. O conector usa `CONFLUENT_API_KEY`/`CONFLUENT_API_SECRET`; consumidores usam `CONSUMER_KAFKA_API_KEY`/`CONSUMER_KAFKA_API_SECRET`.

Confira no terminal:

```bash
confluent iam service-account list
confluent kafka acl list --service-account ID_DA_SERVICE_ACCOUNT
```

Registre identidades e permissoes aplicadas para permitir auditoria. A identidade do conector precisa publicar nos topicos CDC; nao use uma chave pessoal como credencial permanente de workload. As ACLs verificadas neste ambiente estao em `evidencias/04-seguranca-acls.txt`.

## Contratos e governanca de schemas

Os contratos de dominio estao em `schemas/`. `amount` usa decimal(15,2). Marque `customer_id` e `document_number` como PII e `card_number` como PCI nos schemas CDC das tabelas `accounts`, `customers` e `cards`. `card_id` e apenas a chave de ligacao, nao o numero do cartao. As tags sao metadados do Schema Registry, entao aplique-as no painel do Registry aos campos, nao como texto nos dados.

No Schema Registry, para os subjects usados pelo pipeline:

1. Configure compatibilidade `BACKWARD`.
2. Registre a versao inicial do schema e capture a tela/saida.
3. Teste uma versao que apenas adiciona `risk_level` como union null/string com default null. A validacao deve aceitar.
4. Teste uma versao que remove um campo obrigatorio. A validacao deve rejeitar.
5. Registre os resultados para tornar a evolucao do contrato verificavel.

O conector cria subjects Avro proprios para os envelopes Debezium. Confira os subjects realmente criados e nao registre os arquivos `.avsc` por cima de um subject de envelope. Faca os dois testes BACKWARD em subjects de teste separados. O Flink Cloud pode exigir compatibilidade `FULL` ou `FULL_TRANSITIVE` para schemas que ele mesmo grava; mantenha esse requisito separado dos testes BACKWARD dos schemas CDC.

## Captura de alteracoes com Debezium

No Git Bash/WSL, carregue as variaveis de `.env` e gere um arquivo de configuracao temporario. JSON nao expande variaveis sozinho:

```bash
set -a
source .env
set +a
envsubst < connectors/cdc.json > /tmp/payments-cdc.json
```

Configure o contexto do CLI para o ambiente e cluster criados e envie a configuracao:

```bash
confluent environment use ENVIRONMENT_ID
confluent kafka cluster use CLUSTER_ID
confluent connect cluster create --config-file /tmp/payments-cdc.json
confluent connect cluster list
```

O conector usa `PostgresCdcSourceV2`, `pgoutput`, a publication `payments_publication`, o slot `payments_slot`, Avro e heartbeat de 60000 ms. Ele cria topicos no padrao `payments.public.nome_da_tabela`. Nao publique nem anexe o JSON renderizado, pois ele contem segredos. Se o CLI rejeitar uma propriedade, consulte `confluent connect plugin describe PostgresCdcSourceV2` e ajuste ao plugin disponivel na sua conta.

Espere o status ficar **Running** e os topicos aparecerem. O prefixo deste exemplo e `payments`; nomes esperados incluem `payments.public.accounts` e `payments.public.transactions`.

Gere eventos de teste sem alterar a massa de exemplo:

```sql
INSERT INTO transactions (transaction_id, account_id, card_id, merchant_id, amount, currency, status, occurred_at)
VALUES ('tx-cdc-test-001', 'account-0001', 'card-0001', 'merchant-0001', 25.00, 'BRL', 'APPROVED', now());

UPDATE transactions SET status = 'REVIEW' WHERE transaction_id = 'tx-cdc-test-001';
DELETE FROM transactions WHERE transaction_id = 'tx-cdc-test-001';
```

No topico de transacoes, capture INSERT `op=c` (before nulo), UPDATE `op=u` (before e after preenchidos), DELETE `op=d` (after nulo) e a mensagem tombstone subsequente. A evidencia real do registro `cdc-evidence-20261006-001` esta em `evidencias/01-envelopes-cdc.json`, com os offsets e campos pessoais omitidos.

## Processamento com Flink SQL

Crie um workspace/statement no Flink associado ao cluster. Execute `SHOW TABLES;` e localize as tabelas inferidas `payments.public.accounts` e `payments.public.transactions`. Os nomes de topico com pontos precisam ficar entre crases para serem tratados como identificadores unicos. Rode `SHOW CREATE TABLE` para conferir colunas e chave primaria. O Flink Cloud infere as tabelas CDC pelo topico e schema Avro; nao execute um segundo `CREATE TABLE` para copiar o topico.

Antes do temporal join, configure `payments.public.accounts` com `cleanup.policy=compact` no Kafka. Isso permite ao Flink reconhecer `account_id` como chave primaria da tabela de contas. Execute `sql/01_tables.sql` em ordem. A view usa `$rowtime` e o temporal join `FOR SYSTEM_TIME AS OF t.event_time`; `PROCTIME()` nao e suportado neste workspace Cloud. A regra `MATCH_RECOGNIZE` precisa da tabela de transacoes em append mode, configurado pelo `ALTER TABLE` no arquivo SQL.

Depois execute `sql/02_fraud_rules.sql`. A tabela cria o topico `desafio.fraud.detected` em Avro Registry e a regra particiona por `card_id`, procurando tres ou mais transacoes aprovadas em ate 60 segundos. Configure compatibilidade `FULL` no subject de saida `desafio.fraud.detected-value`, conforme requerido pelo sink Flink; mantenha esse ajuste separado dos testes `BACKWARD` dos schemas de CDC.

**Semantica de append:** o topico CDC de transacoes inclui updates e deletes. `changelog.mode=append` faz cada update ser tratado como um novo evento e descarta deletes para esta regra. Documente essa limitacao ao interpretar os alertas. O topico de contas, por outro lado, deve manter compactacao para o temporal join.

## Operacao, confiabilidade e estado atual

Evidencias reais capturadas ate agora:

- `evidencias/01-envelopes-cdc.json`: INSERT, UPDATE, DELETE e tombstone do mesmo registro CDC, com offsets reais.
- `evidencias/02-compatibilidade-schema.txt`: BACKWARD global e testes com subject FULL isolado.
- `evidencias/03-alerta-fraude.json`: alerta Avro real da carga seed e alerta de reteste correlacionado aos tres eventos aprovados; o primeiro lote `fraud-evidence` nao gerou alerta observavel.
- `evidencias/04-seguranca-acls.txt`: identidades e ACLs observadas no cluster.
- `evidencias/05-custos-status.txt`: consulta diaria de billing e estado dos recursos, ainda sem conciliacao final.
- `evidencias/06-confiabilidade.txt`: fluxo, recuperacao, implementacao do consumidor idempotente e resultado dos testes locais.

### Consumidor idempotente de alertas

O consumidor em `consumer/consume_alerts.py` decodifica Avro via Schema Registry e persiste cada alerta em SQLite. A chave idempotente e um SHA-256 deterministico dos campos do alerta; a escrita SQLite e confirmada antes do commit do offset Kafka. Assim, se o processo cair entre essas duas operacoes, a reentrega nao cria um segundo alerta.

Instale a dependencia e exporte as variaveis locais do `.env` no Git Bash/WSL:

```bash
python -m pip install -r consumer/requirements.txt
set -a
source .env
set +a
python consumer/consume_alerts.py
```

O consumidor permanece ativo ate ser interrompido. Para uma leitura limitada, use `--max-messages 1`. O estado local fica em `data/fraud-alerts.sqlite`, ignorado pelo Git; preserve esse arquivo para manter a deduplicacao após reinicios. Credenciais do consumer e do Schema Registry devem ser dedicadas e nunca publicadas.

Execute os testes locais de idempotencia e reentrega com:

```bash
python -m unittest discover -s consumer -p "test_*.py" -v
```

Os testes simulam falha no commit do offset apos a persistencia e verificam que a reentrega permanece idempotente. Um smoke test real conseguiu ler do Kafka, mas o Schema Registry negou a leitura do schema (HTTP 403, SR 40301); a verificacao cloud permanece pendente ate a credencial do consumer ter permissao de leitura do schema correto.

### Validacoes ainda pendentes neste ambiente

- **Observabilidade:** pico de `received_bytes` e lag do consumer group pela Metrics API. O CLI de lag informa que a operacao exige cluster Dedicated e nao funciona no cluster Basic utilizado; a tentativa na Metrics API retornou HTTP 401. Veja `evidencias/05-custos-status.txt`.
- **Seguranca:** associar as tags PII/PCI ja criadas aos campos de schema indicados na secao 3; a tabela de ACLs e o estado das tags estao em `evidencias/04-seguranca-acls.txt`.
- **Custos:** conciliar o periodo completo do teste e registrar duas alavancas usadas/possiveis (desligar o cluster quando ocioso e reduzir retencao/volume de dados).
- **Confiabilidade:** consumidor implementado e testes locais aprovados; falta corrigir a permissao/credencial de leitura do Schema Registry e repetir o smoke test cloud.
- **Recursos cloud:** o statement de fraude esta parado, mas o conector CDC continua RUNNING e o pool Flink permanece provisionado. `teardown.sh` descreve a remocao manual; nao executa exclusoes automaticamente para evitar afetar recursos compartilhados.

O custo documentado cobre apenas uma janela parcial; nao representa o custo total do periodo. A evidência detalha data, status observado e limitacoes da consulta. Nao remova recursos compartilhados sem verificar a propriedade e o impacto da exclusao.

## Configuracao local e seguranca

O repositorio exclui `.env`, bancos SQLite locais e PDFs de referencia. Para validar a protecao do arquivo de configuracao:

```bash
git check-ignore .env
git status --short
```

## Reproduzir em outro ambiente

1. Copie `.env.example` para `.env` e preencha as configuracoes localmente. O arquivo `.env` e ignorado pelo Git.
2. Ajuste `connectors/cdc.json` para o banco, plugin e topicos usados no seu ambiente.
3. Revise as tabelas e regras em `sql/` para refletir os nomes e formatos do Schema Registry.
4. Provisione explicitamente os recursos necessarios no Confluent Cloud e revise custos e impacto antes de criar ou excluir recursos.
5. Ao registrar novas execucoes, sanitize as evidencias em `evidencias/`; remova dados pessoais e credenciais.

Os scripts `setup.sh` e `teardown.sh` sao guias operacionais: o primeiro prepara o banco e os dados; o segundo lista etapas de limpeza cloud sem executar operacoes destrutivas.

## Estrutura

- `schemas/`: contratos Avro de conta e transacao.
- `connectors/`: modelo de configuracao para o conector CDC.
- `sql/`: DDL Flink e regra de deteccao de fraude.
- `consumer/`: consumidor Kafka/Avro e testes automatizados de idempotencia.
- `evidencias/`: registros sanitizados das execucoes observadas.

## Git

Para revisar alteracoes locais e o estado do working tree, use `git status`.
