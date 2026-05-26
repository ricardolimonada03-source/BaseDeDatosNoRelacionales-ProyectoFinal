#!/bin/bash
# Inicializa Cassandra en modo seguro:
# - espera a que el nodo este disponible con el superusuario default
# - aplica schema.cql
# - aplica secure_setup.cql (roles + permisos)
set -euo pipefail

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
SCHEMA_FILE="${SCHEMA_FILE:-/init/schema.cql}"
SECURE_FILE="${SECURE_FILE:-/init/secure_setup.cql}"
BOOTSTRAP_USER="${BOOTSTRAP_USER:-cassandra}"
BOOTSTRAP_PASSWORD="${BOOTSTRAP_PASSWORD:-cassandra}"
MAX_RETRIES="${MAX_RETRIES:-30}"
SLEEP_SECONDS="${SLEEP_SECONDS:-5}"

echo "Esperando a Cassandra (modo seguro) en ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."

for attempt in $(seq 1 "$MAX_RETRIES"); do
  if cqlsh -u "$BOOTSTRAP_USER" -p "$BOOTSTRAP_PASSWORD" "$CASSANDRA_HOST" "$CASSANDRA_PORT" -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
    echo "Cassandra disponible. Aplicando schema..."
    cqlsh -u "$BOOTSTRAP_USER" -p "$BOOTSTRAP_PASSWORD" "$CASSANDRA_HOST" "$CASSANDRA_PORT" -f "$SCHEMA_FILE"
    echo "Aplicando setup de seguridad (roles + permisos)..."
    cqlsh -u "$BOOTSTRAP_USER" -p "$BOOTSTRAP_PASSWORD" "$CASSANDRA_HOST" "$CASSANDRA_PORT" -f "$SECURE_FILE"
    echo "Cassandra inicializado en modo seguro."
    exit 0
  fi
  echo "Intento ${attempt}/${MAX_RETRIES} fallido. Reintentando en ${SLEEP_SECONDS}s..."
  sleep "$SLEEP_SECONDS"
done

echo "No fue posible conectar con Cassandra para aplicar schema seguro."
exit 1
