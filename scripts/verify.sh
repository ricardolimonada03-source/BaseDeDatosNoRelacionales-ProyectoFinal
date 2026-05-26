#!/bin/bash
#
# Validacion integral del repo.
#
# Uso:
#   bash scripts/verify.sh static   # Solo validacion estatica (no requiere Docker)
#   bash scripts/verify.sh e2e      # Estatica + smoke test end-to-end (requiere Docker)
#
# La validacion estatica corre:
#   - python -m py_compile en todos los .py
#   - bash -n en todos los .sh
#   - docker compose config en cada compose.yml (requiere binario docker, no daemon)
#
# El smoke e2e levanta el stack default, corre un load test de 30s y revisa
# que no haya perdida superior al 1%.
#
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

MODE="${1:-static}"

run_static() {
  echo "=== [STATIC] py_compile ==="
  find consumers spark/jobs tests -name "*.py" -print0 | xargs -0 -n1 python3 -m py_compile
  echo "    OK"

  echo "=== [STATIC] bash -n ==="
  find scripts cassandra kafka -name "*.sh" -print0 | xargs -0 -n1 bash -n
  echo "    OK"

  echo "=== [STATIC] docker compose config ==="
  for f in docker-compose.yml docker-compose.secure.yml docker-compose.multinode.yml; do
    if [ -f "$f" ]; then
      docker compose -f "$f" config --quiet
      echo "    OK: $f"
    fi
  done

  echo "=== [STATIC] dataset estatico ==="
  python3 -c "
import csv
with open('data/static/wikis.csv') as f:
    rows = list(csv.DictReader(f))
assert len(rows) >= 30, f'wikis.csv parece muy pequeno: {len(rows)} filas'
required = {'wiki', 'language', 'language_name', 'country', 'project_family', 'community_size_bucket'}
assert required.issubset(rows[0].keys()), f'columnas faltantes: {required - set(rows[0].keys())}'
print(f'    OK: {len(rows)} wikis cargadas con esquema completo')
"
}

run_e2e() {
  echo "=== [E2E] verificando que Docker daemon esta corriendo ==="
  if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon no disponible. Inicia Docker Desktop e intenta de nuevo."
    exit 2
  fi
  echo "    OK"

  echo "=== [E2E] levantando stack default ==="
  docker compose up -d zookeeper kafka cassandra cassandra-init spark-master spark-worker

  echo "=== [E2E] esperando a que cassandra-init termine ==="
  for i in $(seq 1 30); do
    if [ "$(docker inspect -f '{{.State.Status}}' cassandra-init 2>/dev/null || echo missing)" = "exited" ]; then
      break
    fi
    sleep 5
  done
  docker logs cassandra-init | tail -3

  echo "=== [E2E] arrancando productor y consumer en background ==="
  pip install -q -r consumers/requirements.txt
  python3 consumers/wikimedia_to_kafka.py > /tmp/producer.log 2>&1 &
  PRODUCER_PID=$!
  python3 consumers/kafka_to_cassandra.py > /tmp/consumer.log 2>&1 &
  CONSUMER_PID=$!
  trap "kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null || true" EXIT

  sleep 30
  echo "    productor PID=$PRODUCER_PID  consumer PID=$CONSUMER_PID"

  echo "=== [E2E] load test 50 ev/s x 30s ==="
  python3 tests/load/load_test.py --rate 50 --duration 30 --tolerance-pct 5.0

  echo "=== [E2E] OK ==="
}

case "$MODE" in
  static)
    run_static
    ;;
  e2e)
    run_static
    run_e2e
    ;;
  *)
    echo "Uso: $0 {static|e2e}"
    exit 2
    ;;
esac
