#!/bin/bash
# Aplica ACLs reales sobre Kafka en modo SASL_PLAINTEXT.
# Diseñado para ejecutarse contra el broker de docker-compose.secure.yml.
set -euo pipefail

BROKER="${BROKER:-localhost:9093}"
TOPIC="${TOPIC:-wikimedia.recentchange}"
COMMAND_CONFIG="${COMMAND_CONFIG:-/etc/kafka/admin_client.properties}"

run_acl() {
  docker exec kafka kafka-acls \
    --bootstrap-server "$BROKER" \
    --command-config "$COMMAND_CONFIG" \
    "$@"
}

echo "[KAFKA] Permitir a User:pipeline_writer producir en $TOPIC..."
run_acl --add \
  --allow-principal "User:pipeline_writer" \
  --operation WRITE --operation DESCRIBE --operation CREATE \
  --topic "$TOPIC"

echo "[KAFKA] Permitir a User:pipeline_writer consumir de $TOPIC (con su group)..."
run_acl --add \
  --allow-principal "User:pipeline_writer" \
  --operation READ --operation DESCRIBE \
  --topic "$TOPIC"
run_acl --add \
  --allow-principal "User:pipeline_writer" \
  --operation READ --operation DESCRIBE \
  --group "wikimedia-cassandra-consumer"

echo "[KAFKA] Permitir a User:analytics_reader consumir de $TOPIC..."
run_acl --add \
  --allow-principal "User:analytics_reader" \
  --operation READ --operation DESCRIBE \
  --topic "$TOPIC"
run_acl --add \
  --allow-principal "User:analytics_reader" \
  --operation READ --operation DESCRIBE \
  --group "wikimedia-analytics-reader"

echo "[KAFKA] ACLs configuradas. Listado actual:"
run_acl --list --topic "$TOPIC"
