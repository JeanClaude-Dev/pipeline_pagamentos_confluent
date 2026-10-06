-- O Flink Cloud expoe os topicos CDC como tabelas inferidas com o nome completo do topico.
-- Os identificadores entre crases mantem os pontos como parte do nome da tabela.
SHOW TABLES;

SET 'sql.tables.scan.idle-timeout' = '5s';

SHOW CREATE TABLE `payments.public.accounts`;
SHOW CREATE TABLE `payments.public.transactions`;

-- A tabela de contas precisa estar compactada no Kafka para expor account_id
-- como chave primaria e permitir o temporal join.
-- MATCH_RECOGNIZE requer append; updates passam a ser novos eventos e deletes
-- do topico de transacoes nao sao processados por esta regra.
ALTER TABLE `payments.public.transactions`
SET ('changelog.mode' = 'append');

CREATE VIEW accounts AS
SELECT account_id,
	   customer_id,
	   status,
	   created_at,
	   updated_at,
	   $rowtime AS event_time
FROM `payments.public.accounts`;

CREATE VIEW transactions AS
SELECT transaction_id,
	   account_id,
	   card_id,
	   merchant_id,
	   amount,
	   currency,
	   status,
	   occurred_at,
	   $rowtime AS event_time
FROM `payments.public.transactions`;

CREATE VIEW enriched_transactions AS
SELECT t.transaction_id,
	   t.account_id,
	   a.customer_id,
	   t.card_id,
	   t.amount,
	   t.currency,
	t.status,
	t.occurred_at,
	t.event_time
FROM transactions AS t
JOIN accounts FOR SYSTEM_TIME AS OF t.event_time AS a
  ON t.account_id = a.account_id;
