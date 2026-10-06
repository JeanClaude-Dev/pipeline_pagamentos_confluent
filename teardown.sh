#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

printf 'Teardown do projeto (execute no Confluent Cloud, nesta ordem):\n'
printf '1. Pare e exclua os statements Flink deste projeto.\n'
printf '2. Exclua o conector CDC e confirme que nao ha tarefa ativa.\n'
printf '3. Exclua os topicos payments.* e desafio.fraud.detected.\n'
printf '4. Exclua o cluster Basic desafio-basic para interromper a cobranca.\n'
printf '5. Exclua o environment desafio-final somente se nao tiver outros recursos.\n'
printf '\nConfirme cada exclusao no painel antes de continuar.\n'
printf 'Este script nao apaga recursos automaticamente. Consulte README.md.\n'
