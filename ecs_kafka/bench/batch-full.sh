#!/bin/sh
# Bateria completa: transição, lote principal e estresse (~2h30, 28 rodadas).
# Uso (na raiz do projeto, Mac na tomada e com a tampa aberta):
#   caffeinate -ims bench/batch-full.sh            # rótulo padrão "b"
#   TAG=b3 caffeinate -ims bench/batch-full.sh     # rótulo próprio nas pastas
# Andamento: tail -f runs/$TAG-console.log
set -u
cd "$(dirname "$0")/.."
TAG=${TAG:-b}
PY=.venv/bin/python
LOG=runs/$TAG-console.log
mkdir -p runs
run() { $PY -u bench/deploy.py "$@" >> "$LOG" 2>&1; }

# 1) Transição: o 1º deploy da correção
run T1 --runs 2 --tag "$TAG"
run T2 --runs 2 --tag "$TAG"

# 2) Lote principal (carga do horário de deploy)
for sc in S0 S1 S3 D1 D3; do run $sc --runs 2 --tag "$TAG"; done
for sc in S2 D2; do run $sc --runs 2 --wave-size 2 --tag "$TAG"; done

# 3) Estresse: pico (600 TPS em prod = 60 msg/s local) e dependências 2x lentas
PICO="--rate 60 --dynamo-ms 10"
LENTO="--api-ms 500 --integration-ms 2000"
run S0 --runs 1 $PICO --tag "$TAG-pico"
run S1 --runs 2 $PICO --tag "$TAG-pico"
run S3 --runs 2 $PICO --tag "$TAG-pico"
run S1 --runs 2 $LENTO --tag "$TAG-lento"
run S3 --runs 2 $LENTO --tag "$TAG-lento"
run S1 --runs 1 $PICO $LENTO --tag "$TAG-pico-lento"

echo BATCH_DONE >> "$LOG"
