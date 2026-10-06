# Pipeline de pagamentos no Confluent Cloud

Projeto do desafio: PostgreSQL (Neon) -> CDC Debezium -> Kafka/Avro -> Flink SQL -> alerta de fraude.

Este repositorio documenta a implementacao do desafio no Confluent Cloud. O `setup.sh` prepara o banco e os dados; os recursos do Confluent Cloud sao criados explicitamente para evitar custos ou exclusoes inesperadas. As evidencias devem ser capturadas no ambiente usado e sanitizadas antes da publicacao.

## O que sera demonstrado

1. Environment e cluster Kafka Basic, com identidades separadas para produtor e consumidor.
2. Schemas Avro e validacao de compatibilidade; os testes estao registrados em `evidencias/02-compatibilidade-schema.txt`.
3. CDC PostgreSQL com INSERT, UPDATE, DELETE e tombstone.
4. Temporal join entre transacoes e contas e alerta para tres transacoes aprovadas do mesmo cartao em 60 segundos.
5. Metricas, ACLs, custo, confiabilidade e limpeza dos recursos, com evidencias do ambiente.

## Pre-requisitos

- Conta Confluent Cloud e acesso ao ambiente do desafio.
- Conta Neon (ou outro PostgreSQL que permita logical replication).
- Git Bash ou WSL, cliente `psql` e `envsubst`.
- Confluent CLI para consultar recursos e custos.

O cluster Basic pode gerar cobranca. Confira a regiao e o preco no painel, anote o horario de inicio e exclua o cluster assim que terminar.

## 1. Preparar o projeto e o PostgreSQL

No Git Bash/WSL, entre nesta pasta e crie seu arquivo local de configuracao:

```bash
cp .env.example .env
```

No painel do Neon, crie um banco `payments`, habilite logical replication nas configuracoes do projeto e reinicie o banco se o painel solicitar. Copie a connection string para `DATABASE_URL` em `.env`. Para o conector, use o hostname de conexao direta do Neon em `CDC_DATABASE_HOST` (nao o endpoint pooled) e preencha `CDC_DATABASE_NAME`. Use aspas na connection string se ela tiver caracteres especiais. Nunca compartilhe o arquivo `.env`.

Prepare as tabelas e a massa de dados como proprietario do banco:

```bash
bash setup.sh
```

O resultado esperado e customers=200, accounts=240, cards=320, merchants=60 e transactions=2000. O script tambem aplica `REPLICA IDENTITY FULL`. Se a conexao falhar, confira `DATABASE_URL`, acesso de rede e se `psql` esta instalado.

Agora, como proprietario, crie o usuario de menor privilegio. Troque a senha pelo valor local de `CDC_DATABASE_PASSWORD`:

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

## 2. Camada 1: ambiente e identidades

No Confluent Cloud:

1. Crie o environment `desafio-final`.
2. Crie um cluster Kafka Basic chamado `desafio-basic`, na regiao escolhida para o desafio (o exemplo do guia usa AWS `us-east-1`).
3. Crie as service accounts `desafio-producer` e `desafio-consumer`.
4. Para `desafio-producer`, aplique `WRITE` e `DESCRIBE` nos recursos `desafio-*`, como pede o desafio. Como essa identidade tambem sera usada pelo conector, aplique `CREATE`, `WRITE` e `DESCRIBE` nos topicos `payments.*` que o CDC cria.
5. Para `desafio-consumer`, aplique `READ` e `DESCRIBE` nos topicos `desafio-*` e `payments.*`, mais `READ` no consumer group `desafio-*`.
6. Gere chaves Kafka separadas para cada service account e uma chave do Schema Registry para o produtor/conector. Guarde os IDs e chaves em `.env`, nunca no Git. O conector usa `CONFLUENT_API_KEY`/`CONFLUENT_API_SECRET`; consumidores usam `CONSUMER_KAFKA_API_KEY`/`CONSUMER_KAFKA_API_SECRET`.

Confira no terminal:

```bash
confluent iam service-account list
confluent kafka acl list --service-account ID_DA_SERVICE_ACCOUNT
```

Salve as saidas reais em `evidencias/` ou no README. Na tabela de seguranca, escreva cada service account, ACL e motivo. A identidade do conector precisa publicar nos topicos CDC; nao use sua chave pessoal como credencial permanente do conector.

## 3. Camada 2: schemas e compatibilidade

Os contratos de dominio estao em `schemas/`. `amount` usa decimal(15,2). Marque `customer_id` e `document_number` como PII e `card_number` como PCI nos schemas CDC das tabelas `accounts`, `customers` e `cards`. `card_id` e apenas a chave de ligacao, nao o numero do cartao. As tags sao metadados do Schema Registry, entao aplique-as no painel do Registry aos campos, nao como texto nos dados.

No Schema Registry, para os subjects usados pelo pipeline:

1. Selecione compatibilidade `BACKWARD`.
2. Registre a versao inicial do schema e capture a tela/saida.
3. Teste uma versao que apenas adiciona `risk_level` como union null/string com default null. A validacao deve aceitar.
4. Teste uma versao que remove um campo obrigatorio. A validacao deve rejeitar.
5. Registre os dois resultados em `evidencias/`.

O conector cria subjects Avro proprios para os envelopes Debezium. Confira os subjects realmente criados e nao registre os arquivos `.avsc` por cima de um subject de envelope. Faca os dois testes BACKWARD em subjects de teste separados. O Flink Cloud pode exigir compatibilidade `FULL` ou `FULL_TRANSITIVE` para schemas que ele mesmo grava; mantenha esse requisito separado do teste BACKWARD do desafio.

## 4. Camada 3: CDC PostgreSQL

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

## 5. Camada 4: Flink SQL

Crie um workspace/statement no Flink associado ao cluster. Execute `SHOW TABLES;` e localize as tabelas inferidas `payments.public.accounts` e `payments.public.transactions`. Os nomes de topico com pontos precisam ficar entre crases para serem tratados como identificadores unicos. Rode `SHOW CREATE TABLE` para conferir colunas e chave primaria. O Flink Cloud infere as tabelas CDC pelo topico e schema Avro; nao execute um segundo `CREATE TABLE` para copiar o topico.

Antes do temporal join, configure `payments.public.accounts` com `cleanup.policy=compact` no Kafka. Isso permite ao Flink reconhecer `account_id` como chave primaria da tabela de contas. Execute `sql/01_tables.sql` em ordem. A view usa `$rowtime` e o temporal join `FOR SYSTEM_TIME AS OF t.event_time`; `PROCTIME()` nao e suportado neste workspace Cloud. A regra `MATCH_RECOGNIZE` precisa da tabela de transacoes em append mode, configurado pelo `ALTER TABLE` no arquivo SQL.

Depois execute `sql/02_fraud_rules.sql`. A tabela cria o topico `desafio.fraud.detected` em Avro Registry e a regra particiona por `card_id`, procurando tres ou mais transacoes aprovadas em ate 60 segundos. Configure compatibilidade `FULL` no subject de saida `desafio.fraud.detected-value`, conforme exigido pelo sink Flink; mantenha esse ajuste separado dos testes `BACKWARD` dos schemas de CDC.

**Semantica de append:** o topico CDC de transacoes inclui updates e deletes. `changelog.mode=append` faz cada update ser tratado como um novo evento e descarta deletes para esta regra. Documente essa limitacao ao interpretar os alertas. O topico de contas, por outro lado, deve manter compactacao para o temporal join.

## 6. Camada 5: operacao e encerramento

Evidencias reais capturadas ate agora:

- `evidencias/01-envelopes-cdc.json`: INSERT, UPDATE, DELETE e tombstone do mesmo registro CDC, com offsets reais.
- `evidencias/02-compatibilidade-schema.txt`: BACKWARD global e testes com subject FULL isolado.
- `evidencias/03-alerta-fraude.json`: alerta Avro real da carga seed; os tres registros sinteticos ainda nao foram correlacionados com a saida.
- `evidencias/04-seguranca-acls.txt`: identidades e ACLs observadas no cluster.
- `evidencias/05-custos-status.txt`: consulta diaria de billing e estado dos recursos, ainda sem conciliacao final.

Antes da entrega, complete as evidencias pendentes abaixo:

- **Observabilidade:** pico de `received_bytes` e lag do consumer group pela Metrics API. O CLI de lag informa que a operacao exige cluster Dedicated e nao funciona no cluster Basic deste desafio; veja `evidencias/05-custos-status.txt`.
- **Seguranca:** associar as tags PII/PCI ja criadas aos campos de schema indicados na secao 3; a tabela de ACLs e o estado das tags estao em `evidencias/04-seguranca-acls.txt`.
- **Custos:** conciliar o periodo completo do teste e registrar duas alavancas usadas/possiveis (desligar o cluster quando ocioso e reduzir retencao/volume de dados).
- **Confiabilidade:** desenhe o caminho Postgres -> conector -> Kafka -> Flink -> consumidor; explique o efeito de falha e como o consumidor evita duplicidade com chave/idempotencia.
- **Teardown:** execute `bash teardown.sh` como checklist, depois exclua statements, conector, topicos e cluster no painel. Exclua o environment somente se estiver vazio e for exclusivo deste desafio.

Nao declare o teardown concluido enquanto statements, conector ou cluster ainda estiverem ativos. Registre a data/hora e o periodo consultado nas evidencias de custo; um valor de uma janela parcial nao representa o custo total do desafio.

Confirme com as listas de statements, conectores e clusters que nada do projeto continua ativo. Cole os resultados reais (por exemplo `None found`) e o custo total. O script de teardown nao apaga recursos automaticamente para impedir exclusao acidental de recursos compartilhados.

## Publicar e entregar

Antes de publicar, confirme que `.env` nao esta sendo rastreado:

```bash
git check-ignore .env
git status --short
```

Crie um repositorio publico no GitHub com nome em minusculas e sem acentos, envie os arquivos do projeto e troque todos os exemplos sinteticos pelas evidencias reais. O checklist final:

- [ ] As cinco camadas tem evidencias reais; nao ha marcador de evidencia pendente.
- [ ] A carga tem as quantidades esperadas e o alerta de fraude foi observado.
- [ ] Compatibilidade aceitou campo novo com default e rejeitou a remocao obrigatoria.
- [ ] Nenhuma senha, chave ou arquivo `.env` foi enviado.
- [ ] Custo total e teardown estao documentados.
- [ ] O link enviado para a DIO e a pagina principal do repositorio publico.

## Comecar

1. Copie `.env.example` para `.env` e preencha as configuracoes localmente. O arquivo `.env` e ignorado pelo Git.
2. Ajuste `connectors/cdc.json` para o banco, plugin e topicos usados no seu ambiente.
3. Complete as tabelas e regras em `sql/` conforme os nomes e formatos registrados no Confluent.
4. Execute os comandos de provisionamento especificos do seu ambiente. Revise recursos e custos antes de criar ou excluir qualquer recurso.
5. Salve em `evidencias/` exemplos sanitizados de envelopes CDC, consultas, logs e capturas do desafio. Remova dados pessoais e credenciais.

Os scripts `setup.sh` e `teardown.sh` sao lembretes seguros: apontam para as etapas que precisam ser adaptadas ao ambiente do bootcamp e nao executam operacoes destrutivas.

## Estrutura

- `schemas/`: contratos Avro de conta e transacao.
- `connectors/`: modelo de configuracao para o conector CDC.
- `sql/`: ponto de partida para DDL Flink e regras de fraude.
- `evidencias/`: exemplos e evidencias sanitizados do projeto.

## Git

O repositorio foi inicializado localmente. Para conferir o estado, use `git status`.
