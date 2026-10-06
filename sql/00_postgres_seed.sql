-- Execute em um banco PostgreSQL vazio. Os IDs tornam a carga repetivel.
BEGIN;
SELECT setseed(0.42);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    document_number TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(customer_id),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS cards (
    card_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    card_number TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    card_id TEXT NOT NULL REFERENCES cards(card_id),
    merchant_id TEXT NOT NULL REFERENCES merchants(merchant_id),
    amount NUMERIC(15, 2) NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE cards REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

INSERT INTO customers
SELECT 'customer-' || lpad(n::TEXT, 4, '0'),
       'Cliente de teste ' || n,
       'TEST-' || lpad(n::TEXT, 8, '0')
FROM generate_series(1, 200) AS n
ON CONFLICT DO NOTHING;

INSERT INTO accounts
SELECT 'account-' || lpad(n::TEXT, 4, '0'),
       'customer-' || lpad((((n - 1) % 200) + 1)::TEXT, 4, '0'),
       'ACTIVE',
       TIMESTAMPTZ '2025-01-01 00:00:00+00',
       TIMESTAMPTZ '2025-01-01 00:00:00+00'
FROM generate_series(1, 240) AS n
ON CONFLICT DO NOTHING;

INSERT INTO cards
SELECT 'card-' || lpad(n::TEXT, 4, '0'),
       'account-' || lpad((((n - 1) % 240) + 1)::TEXT, 4, '0'),
       'TEST-CARD-' || lpad(n::TEXT, 4, '0'),
       'ACTIVE'
FROM generate_series(1, 320) AS n
ON CONFLICT DO NOTHING;

INSERT INTO merchants
SELECT 'merchant-' || lpad(n::TEXT, 4, '0'),
       'Loja de teste ' || n,
       CASE WHEN n % 2 = 0 THEN 'ONLINE' ELSE 'RETAIL' END
FROM generate_series(1, 60) AS n
ON CONFLICT DO NOTHING;

INSERT INTO transactions
SELECT 'transaction-' || lpad(g.i::TEXT, 5, '0'),
       c.account_id,
       c.card_id,
       'merchant-' || lpad((((g.i - 1) % 60) + 1)::TEXT, 4, '0'),
       (random() * 500 + 1)::NUMERIC(15, 2),
       'BRL',
       'APPROVED',
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g.i * INTERVAL '1 second')
FROM generate_series(1, 2000) AS g(i)
JOIN cards c
  ON c.card_id = 'card-' || lpad((((g.i - 1) % 20) + 1)::TEXT, 4, '0')
ON CONFLICT DO NOTHING;

COMMIT;

SELECT 'customers' AS tabela, count(*) AS quantidade FROM customers
UNION ALL SELECT 'accounts', count(*) FROM accounts
UNION ALL SELECT 'cards', count(*) FROM cards
UNION ALL SELECT 'merchants', count(*) FROM merchants
UNION ALL SELECT 'transactions', count(*) FROM transactions;