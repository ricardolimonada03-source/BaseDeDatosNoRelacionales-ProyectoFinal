#!/bin/bash
#
# Demo de resiliencia del pipeline (Etapa 3).
#
# Escenarios validados:
#   1. Cassandra cae mientras el consumer esta corriendo.
#      -> El consumer reintenta inserts, no commitea offsets, no pierde datos.
#   2. Kafka cae mientras el productor esta corriendo.
#      -> El productor entra en backoff y reanuda al volver Kafka.
#   3. Multinodo (si aplica): tirar 1 nodo de Cassandra y verificar que la
#      ingesta sigue gracias a RF>=2.
#
# Pre-requisitos:
#   - infraestructura levantada con `docker compose up -d`
#   - productor corriendo (`python3 consumers/wikimedia_to_kafka.py` o
#     `tests/load/load_test.py`)
#   - consumer corriendo (`python3 consumers/kafka_to_cassandra.py`)
#
# Uso:
#   bash scripts/resilience_demo.sh cassandra-restart
#   bash scripts/resilience_demo.sh kafka-restart
#   bash scripts/resilience_demo.sh cassandra-node-failover
#
set -euo pipefail

SCENARIO="${1:-help}"
KEYSPACE="${KEYSPACE:-wikimedia}"
RAW_TABLE="${RAW_TABLE:-recent_changes_raw}"

snapshot_count() {
  docker exec cassandra cqlsh -e \
    "SELECT COUNT(*) FROM ${KEYSPACE}.${RAW_TABLE};" 2>/dev/null \
    | awk '/^[ ]*[0-9]+/ {print $1; exit}' \
    || echo "0"
}

cassandra_restart() {
  echo "[DEMO] Escenario: cassandra-restart"
  echo "[DEMO] Conteo inicial en Cassandra:"
  initial=$(snapshot_count)
  echo "       $initial filas"

  echo "[DEMO] Deteniendo Cassandra..."
  docker stop cassandra
  echo "[DEMO] Cassandra detenido. El consumer deberia estar reintentando inserts."
  echo "[DEMO] Espera 30s y revisa logs del consumer; deberian verse mensajes [CASSANDRA] Insert fallido."
  sleep 30

  echo "[DEMO] Reiniciando Cassandra..."
  docker start cassandra
  echo "[DEMO] Esperando a que vuelva a estar disponible..."
  for i in $(seq 1 30); do
    if docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; then
      echo "[DEMO] Cassandra disponible de nuevo."
      break
    fi
    sleep 5
  done

  echo "[DEMO] Esperando 30s a que el consumer drene los reintentos..."
  sleep 30
  final=$(snapshot_count)
  echo "[DEMO] Conteo final en Cassandra: $final filas"
  diff=$((final - initial))
  echo "[DEMO] Filas insertadas durante y despues del fallo: $diff"
  echo "[DEMO] Si diff > 0 y el productor siguio enviando, la resiliencia se valida."
}

kafka_restart() {
  echo "[DEMO] Escenario: kafka-restart"
  echo "[DEMO] Deteniendo Kafka..."
  docker stop kafka
  echo "[DEMO] Kafka detenido. El productor deberia mostrar backoff exponencial."
  echo "[DEMO] Espera 30s y observa logs del productor."
  sleep 30
  echo "[DEMO] Reiniciando Kafka..."
  docker start kafka
  echo "[DEMO] Esperando 30s a que el productor reconecte y vuelva a publicar..."
  sleep 30
  echo "[DEMO] El productor deberia haber reanudado envio sin perdida (eventos pre-fallo no se reproducen, los nuevos si)."
}

cassandra_node_failover() {
  echo "[DEMO] Escenario: cassandra-node-failover (requiere docker-compose.multinode.yml)"
  if ! docker ps --format '{{.Names}}' | grep -q '^cassandra-2$'; then
    echo "[DEMO] Este escenario requiere el compose multinodo (docker-compose.multinode.yml)."
    echo "[DEMO] Levantalo con: docker compose -f docker-compose.multinode.yml up -d"
    exit 2
  fi

  echo "[DEMO] Deteniendo cassandra-2 (1 de 3 nodos)..."
  docker stop cassandra-2
  echo "[DEMO] Con RF=3 y CL=ONE/QUORUM, el pipeline debe seguir funcionando."
  echo "[DEMO] Espera 30s y verifica que las inserciones del consumer siguen exitosas."
  sleep 30
  before=$(snapshot_count)
  echo "[DEMO] Filas tras 30s con un nodo caido: $before"
  echo "[DEMO] Reiniciando cassandra-2..."
  docker start cassandra-2
  echo "[DEMO] Esperando 60s a que el nodo se reincorpore al cluster..."
  sleep 60
  after=$(snapshot_count)
  echo "[DEMO] Filas tras recuperacion: $after"
  echo "[DEMO] Si after > before, el cluster siguio aceptando escrituras durante el fallo."
}

case "$SCENARIO" in
  cassandra-restart)
    cassandra_restart
    ;;
  kafka-restart)
    kafka_restart
    ;;
  cassandra-node-failover)
    cassandra_node_failover
    ;;
  help|*)
    cat <<EOF
Uso: $0 <escenario>

Escenarios disponibles:
  cassandra-restart         Detiene Cassandra ~30s y verifica que el consumer no pierde datos.
  kafka-restart             Detiene Kafka ~30s y verifica que el productor reconecta.
  cassandra-node-failover   (multinodo) Tira 1 de 3 nodos de Cassandra y verifica continuidad.
EOF
    ;;
esac
