import json
import hashlib
import os
import time
from datetime import datetime, timezone

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition


KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wikimedia.recentchange")
CASSANDRA_CONSUMER_GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP_ID", "wikimedia-cassandra-consumer"
)
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("KEYSPACE", "wikimedia")
TABLE_NAME = "recent_changes_raw"
CONNECT_MAX_RETRIES = int(os.getenv("CASSANDRA_CONNECT_MAX_RETRIES", "10"))
CONNECT_RETRY_WAIT_SECONDS = int(os.getenv("CASSANDRA_CONNECT_RETRY_WAIT_SECONDS", "5"))
INSERT_MAX_RETRIES = int(os.getenv("CASSANDRA_INSERT_MAX_RETRIES", "3"))
INSERT_RETRY_WAIT_SECONDS = int(
    os.getenv("CASSANDRA_INSERT_RETRY_WAIT_SECONDS", "2")
)
PROGRESS_EVERY = 100


INSERT_QUERY = f"""
INSERT INTO {KEYSPACE}.{TABLE_NAME} (
    wiki,
    event_date,
    event_hour,
    timestamp_event,
    source_event_id,
    title,
    user_name,
    change_type,
    bot,
    server_name,
    comment,
    raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class InvalidEventError(ValueError):
    """Señala un evento que no puede persistirse de forma segura."""


def parse_timestamp(value):
    """Convierte el timestamp del evento a datetime UTC."""
    if value is None:
        return None

    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError):
        return None


def cassandra_auth_provider():
    """Devuelve un PlainTextAuthProvider si CASSANDRA_USERNAME esta seteado."""
    username = os.getenv("CASSANDRA_USERNAME")
    password = os.getenv("CASSANDRA_PASSWORD")
    if username and password:
        print(f"[CASSANDRA] Auth habilitado para usuario '{username}'.")
        return PlainTextAuthProvider(username=username, password=password)
    return None


def connect_to_cassandra(max_retries=CONNECT_MAX_RETRIES, wait_seconds=CONNECT_RETRY_WAIT_SECONDS):
    """Intenta conectarse a Cassandra con reintentos."""
    auth = cassandra_auth_provider()
    cluster_kwargs = {"port": CASSANDRA_PORT}
    if auth is not None:
        cluster_kwargs["auth_provider"] = auth
    cluster = Cluster([CASSANDRA_HOST], **cluster_kwargs)

    for attempt in range(1, max_retries + 1):
        try:
            session = cluster.connect(KEYSPACE)
            print(f"[CASSANDRA] Conectado a keyspace '{KEYSPACE}'.")
            return cluster, session
        except Exception as exc:
            print(
                f"[CASSANDRA] Intento {attempt}/{max_retries} falló: {exc}. "
                f"Reintentando en {wait_seconds} segundos..."
            )
            time.sleep(wait_seconds)

    raise ConnectionError("No fue posible conectarse a Cassandra.")


def prepare_insert_statement(session):
    """Prepara la insercion hacia la tabla raw."""
    statement = session.prepare(INSERT_QUERY)
    print(f"[CASSANDRA] Prepared statement listo para {KEYSPACE}.{TABLE_NAME}.")
    return statement


def kafka_security_kwargs():
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    if protocol == "PLAINTEXT":
        return {}
    if protocol == "SASL_PLAINTEXT":
        return {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
            "sasl_plain_username": os.environ["KAFKA_SASL_USERNAME"],
            "sasl_plain_password": os.environ["KAFKA_SASL_PASSWORD"],
        }
    raise ValueError(f"KAFKA_SECURITY_PROTOCOL no soportado: {protocol}")


def create_kafka_consumer():
    """Crea el consumidor de Kafka."""
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        group_id=CASSANDRA_CONSUMER_GROUP_ID,
        consumer_timeout_ms=1000,
        **kafka_security_kwargs(),
    )
    print(
        f"[KAFKA] Escuchando topic '{TOPIC_NAME}' en {KAFKA_HOST}:{KAFKA_PORT} "
        f"| group_id={CASSANDRA_CONSUMER_GROUP_ID!r} "
        f"| auto_offset_reset={KAFKA_AUTO_OFFSET_RESET!r}"
    )
    return consumer


def canonical_raw_json(event):
    """Serializa el evento de forma estable para persistencia y fallback."""
    return json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_source_event_id(event, raw_json):
    """Usa meta.id cuando exista; si no, genera un fallback estable."""
    meta = event.get("meta") or {}
    meta_id = meta.get("id")
    if meta_id:
        return str(meta_id)

    return hashlib.sha1(raw_json.encode("utf-8")).hexdigest()


def build_row(event):
    """Valida y transforma el evento a la fila de Cassandra."""
    if not isinstance(event, dict):
        raise InvalidEventError("El mensaje no contiene un JSON tipo objeto.")

    timestamp_event = parse_timestamp(event.get("timestamp"))
    wiki = event.get("wiki")

    if timestamp_event is None:
        raise InvalidEventError("timestamp invalido o ausente.")
    if not wiki:
        raise InvalidEventError("wiki ausente.")

    raw_json = canonical_raw_json(event)
    source_event_id = build_source_event_id(event, raw_json)

    title = event.get("title")
    user_name = event.get("user")
    change_type = event.get("type")
    bot = bool(event.get("bot", False))
    server_name = event.get("server_name")
    comment = event.get("comment")

    event_date = timestamp_event.date()
    event_hour = timestamp_event.hour

    return (
        wiki,
        event_date,
        event_hour,
        timestamp_event,
        source_event_id,
        title,
        user_name,
        change_type,
        bot,
        server_name,
        comment,
        raw_json,
    )


def decode_message_value(raw_value):
    """Decodifica el payload de Kafka a JSON controlando errores."""
    try:
        return json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidEventError(f"payload no es JSON valido: {exc}") from exc


def insert_with_retries(session, prepared_statement, row):
    """Intenta insertar en Cassandra varias veces antes de fallar."""
    for attempt in range(1, INSERT_MAX_RETRIES + 1):
        try:
            session.execute(prepared_statement, row)
            return
        except Exception as exc:
            if attempt == INSERT_MAX_RETRIES:
                raise

            print(
                f"[CASSANDRA] Insert fallido {attempt}/{INSERT_MAX_RETRIES}: {exc}. "
                f"Reintentando en {INSERT_RETRY_WAIT_SECONDS} segundos..."
            )
            time.sleep(INSERT_RETRY_WAIT_SECONDS)


def commit_message(consumer, message):
    """Confirma manualmente el offset del mensaje procesado."""
    offsets = {
        TopicPartition(message.topic, message.partition): OffsetAndMetadata(
            message.offset + 1, None, -1
        )
    }
    consumer.commit(offsets=offsets)


def main():
    cluster = None
    consumer = None
    inserted_count = 0

    try:
        cluster, session = connect_to_cassandra()
        prepared_statement = prepare_insert_statement(session)
        consumer = create_kafka_consumer()

        print("[PIPELINE] Iniciando consumo de Kafka hacia Cassandra...")

        while True:
            has_messages = False

            for message in consumer:
                has_messages = True
                payload_preview = repr(message.value)

                try:
                    event = decode_message_value(message.value)
                    row = build_row(event)
                except InvalidEventError as exc:
                    print(
                        f"[SKIP] Evento invalido en offset {message.offset}: {exc} "
                        f"| payload={payload_preview}"
                    )
                    commit_message(consumer, message)
                    continue

                insert_with_retries(session, prepared_statement, row)
                commit_message(consumer, message)
                inserted_count += 1

                if inserted_count % PROGRESS_EVERY == 0:
                    print(
                        f"[PROGRESS] {inserted_count} eventos insertados en "
                        f"{KEYSPACE}.{TABLE_NAME}."
                    )

            if not has_messages:
                print("[KAFKA] Esperando nuevos mensajes...")
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[PIPELINE] Proceso detenido manualmente.")
    except Exception as exc:
        print(f"[ERROR] Ocurrió un error en el pipeline Kafka → Cassandra: {exc}")
    finally:
        if consumer is not None:
            consumer.close()
            print("[KAFKA] Consumidor cerrado.")
        if cluster is not None:
            cluster.shutdown()
            print("[CASSANDRA] Conexión cerrada.")


if __name__ == "__main__":
    main()