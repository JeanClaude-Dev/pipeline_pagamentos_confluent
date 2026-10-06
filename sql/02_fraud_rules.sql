-- Publica um alerta quando tres ou mais transacoes aprovadas do mesmo cartao
-- acontecem em uma janela de 60 segundos.
CREATE TABLE `desafio.fraud.detected` (
	card_id STRING,
	account_id STRING,
	customer_id STRING,
	transaction_count BIGINT,
	first_transaction_at TIMESTAMP_LTZ(3),
	last_transaction_at TIMESTAMP_LTZ(3)
) WITH (
	'changelog.mode' = 'append',
	'value.format' = 'avro-registry'
);

INSERT INTO `desafio.fraud.detected`
SELECT card_id,
	   account_id,
	   customer_id,
	   transaction_count,
	   first_transaction_at,
	   last_transaction_at
FROM enriched_transactions
MATCH_RECOGNIZE (
	PARTITION BY card_id
	ORDER BY event_time
	MEASURES
		FIRST(T.account_id) AS account_id,
		FIRST(T.customer_id) AS customer_id,
		COUNT(T.transaction_id) AS transaction_count,
		FIRST(T.event_time) AS first_transaction_at,
		LAST(T.event_time) AS last_transaction_at
	ONE ROW PER MATCH
	AFTER MATCH SKIP PAST LAST ROW
	PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
	DEFINE T AS T.status = 'APPROVED'
);
