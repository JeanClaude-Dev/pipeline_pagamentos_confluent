#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [[ ! -f .env ]]; then
  printf 'Crie .env copiando .env.example e preencha DATABASE_URL.\n' >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

: "${DATABASE_URL:?Preencha DATABASE_URL no arquivo .env}"

if ! command -v psql >/dev/null 2>&1; then
  printf 'Instale o cliente PostgreSQL (psql) e tente novamente.\n' >&2
  exit 1
fi

printf 'Preparando as tabelas e os dados sinteticos no PostgreSQL...\n'
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/00_postgres_seed.sql
printf '\nBanco pronto. As etapas do Confluent Cloud estao descritas no README.md.\n'
printf 'Este script nao cria recursos na nuvem nem inicia cobrancas.\n'
