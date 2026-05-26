#!/bin/bash
# Ejecuta el job de Spark con las 6 consultas analiticas de la Etapa 5.
# - copia el job y el dataset estatico al contenedor spark-master
# - corre spark-submit con el connector de Cassandra
# - descarga los CSV resultantes al host bajo spark/output/etapa5/
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark-master-proyectoFinal}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}"
SPARK_JOB_LOCAL="${PROJECT_ROOT}/spark/jobs/etapa5_queries.py"
SPARK_JOB_CONTAINER="/tmp/etapa5_queries.py"
STATIC_WIKIS_LOCAL="${PROJECT_ROOT}/data/static/wikis.csv"
STATIC_WIKIS_CONTAINER="/tmp/static/wikis.csv"
CONTAINER_OUTPUT_PATH="/tmp/spark-output/etapa5"
CONTAINER_IVY_PATH="/tmp/.ivy2"
HOST_OUTPUT_PATH="${ETAPA5_HOST_OUTPUT_PATH:-${PROJECT_ROOT}/spark/output/etapa5}"
CASSANDRA_CONNECTOR_PACKAGE="${CASSANDRA_CONNECTOR_PACKAGE:-com.datastax.spark:spark-cassandra-connector_2.13:3.5.1}"

if [ ! -f "${SPARK_JOB_LOCAL}" ]; then
  echo "No existe el job en ${SPARK_JOB_LOCAL}"
  exit 1
fi
if [ ! -f "${STATIC_WIKIS_LOCAL}" ]; then
  echo "No existe el dataset estatico en ${STATIC_WIKIS_LOCAL}"
  exit 1
fi

mkdir -p "$(dirname "${HOST_OUTPUT_PATH}")"
rm -rf "${HOST_OUTPUT_PATH}"
mkdir -p "${HOST_OUTPUT_PATH}"

echo "[ETAPA5] Copiando job y dataset estatico al contenedor ${SPARK_CONTAINER}..."
docker cp "${SPARK_JOB_LOCAL}" "${SPARK_CONTAINER}:${SPARK_JOB_CONTAINER}"
docker exec "${SPARK_CONTAINER}" mkdir -p "$(dirname "${STATIC_WIKIS_CONTAINER}")"
docker cp "${STATIC_WIKIS_LOCAL}" "${SPARK_CONTAINER}:${STATIC_WIKIS_CONTAINER}"

echo "[ETAPA5] Ejecutando spark-submit..."
docker exec \
  -e KEYSPACE="${KEYSPACE:-wikimedia}" \
  -e RAW_TABLE="${RAW_TABLE:-recent_changes_raw}" \
  -e CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}" \
  -e CASSANDRA_PORT="${CASSANDRA_PORT:-9042}" \
  -e ANALYTICS_STATIC_WIKIS_PATH="${STATIC_WIKIS_CONTAINER}" \
  -e ETAPA5_OUTPUT_PATH="${CONTAINER_OUTPUT_PATH}" \
  -e ETAPA5_MIN_EVENTS_BOT_ANOMALY="${ETAPA5_MIN_EVENTS_BOT_ANOMALY:-50}" \
  -e ETAPA5_BOT_THRESHOLD_PCT="${ETAPA5_BOT_THRESHOLD_PCT:-90.0}" \
  "${SPARK_CONTAINER}" \
  bash -lc "mkdir -p '${CONTAINER_IVY_PATH}' && rm -rf '${CONTAINER_OUTPUT_PATH}' && /opt/spark/bin/spark-submit --master '${SPARK_MASTER_URL}' --conf 'spark.jars.ivy=${CONTAINER_IVY_PATH}' --packages '${CASSANDRA_CONNECTOR_PACKAGE}' '${SPARK_JOB_CONTAINER}'"

echo "[ETAPA5] Copiando resultados al host..."
docker cp "${SPARK_CONTAINER}:${CONTAINER_OUTPUT_PATH}/." "${HOST_OUTPUT_PATH}"

echo "[ETAPA5] Salida disponible en ${HOST_OUTPUT_PATH}"
ls "${HOST_OUTPUT_PATH}"
