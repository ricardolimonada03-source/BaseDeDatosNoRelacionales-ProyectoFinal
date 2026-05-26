#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPARK_CONTAINER="${SPARK_CONTAINER:-spark-master-proyectoFinal}"
# Default to local mode inside spark-master so CSV output is written in the same container we copy from.
# Override with SPARK_MASTER_URL=spark://spark-master:7077 if using a shared distributed filesystem.
SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}"
SPARK_JOB_LOCAL="${PROJECT_ROOT}/spark/jobs/recent_changes_analytics.py"
SPARK_JOB_CONTAINER="/tmp/recent_changes_analytics.py"
STATIC_WIKIS_LOCAL="${PROJECT_ROOT}/data/static/wikis.csv"
STATIC_WIKIS_CONTAINER="/tmp/static/wikis.csv"
CONTAINER_OUTPUT_PATH="/tmp/spark-output/changes_by_wiki_hour"
CONTAINER_IVY_PATH="/tmp/.ivy2"
HOST_OUTPUT_PATH="${ANALYTICS_HOST_OUTPUT_PATH:-${PROJECT_ROOT}/spark/output/changes_by_wiki_hour}"
CASSANDRA_CONNECTOR_PACKAGE="${CASSANDRA_CONNECTOR_PACKAGE:-com.datastax.spark:spark-cassandra-connector_2.13:3.5.1}"

if [ ! -f "${SPARK_JOB_LOCAL}" ]; then
  echo "No existe el job Spark en ${SPARK_JOB_LOCAL}"
  exit 1
fi

if [ ! -f "${STATIC_WIKIS_LOCAL}" ]; then
  echo "No existe el dataset estatico en ${STATIC_WIKIS_LOCAL}"
  exit 1
fi

mkdir -p "$(dirname "${HOST_OUTPUT_PATH}")"
rm -rf "${HOST_OUTPUT_PATH}"
mkdir -p "${HOST_OUTPUT_PATH}"

echo "Copiando job y dataset estatico al contenedor ${SPARK_CONTAINER}..."
docker cp "${SPARK_JOB_LOCAL}" "${SPARK_CONTAINER}:${SPARK_JOB_CONTAINER}"
docker exec "${SPARK_CONTAINER}" mkdir -p "$(dirname "${STATIC_WIKIS_CONTAINER}")"
docker cp "${STATIC_WIKIS_LOCAL}" "${SPARK_CONTAINER}:${STATIC_WIKIS_CONTAINER}"

echo "Ejecutando analitica Spark..."
docker exec \
  -e KEYSPACE="${KEYSPACE:-wikimedia}" \
  -e RAW_TABLE="${RAW_TABLE:-recent_changes_raw}" \
  -e AGG_TABLE="${AGG_TABLE:-changes_by_wiki_hour}" \
  -e CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}" \
  -e CASSANDRA_PORT="${CASSANDRA_PORT:-9042}" \
  -e ANALYTICS_OUTPUT_PATH="${CONTAINER_OUTPUT_PATH}" \
  -e ANALYTICS_STATIC_WIKIS_PATH="${STATIC_WIKIS_CONTAINER}" \
  -e ANALYTICS_WRITE_TO_CASSANDRA="${ANALYTICS_WRITE_TO_CASSANDRA:-false}" \
  "${SPARK_CONTAINER}" \
  bash -lc "mkdir -p '${CONTAINER_IVY_PATH}' && rm -rf '${CONTAINER_OUTPUT_PATH}' && /opt/spark/bin/spark-submit --master '${SPARK_MASTER_URL}' --conf 'spark.jars.ivy=${CONTAINER_IVY_PATH}' --packages '${CASSANDRA_CONNECTOR_PACKAGE}' '${SPARK_JOB_CONTAINER}'"

echo "Copiando salida al proyecto..."
docker cp "${SPARK_CONTAINER}:${CONTAINER_OUTPUT_PATH}/." "${HOST_OUTPUT_PATH}"

echo "Salida disponible en ${HOST_OUTPUT_PATH}"
