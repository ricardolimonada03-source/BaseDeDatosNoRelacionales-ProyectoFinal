import json
import os
import time

import requests
from kafka import KafkaProducer
from sseclient import SSEClient


STREAM_URL = os.getenv(
    "STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "wikimedia.recentchange")

USER_AGENT = os.getenv(
    "USER_AGENT",
    "ProyectoFinal-Eqp1/1.0 (contacto: equipo1@itam.mx)",
)
FLUSH_EVERY = max(1, int(os.getenv("KAFKA_FLUSH_EVERY", "100")))
SSE_BACKOFF_START_SECONDS = float(os.getenv("SSE_BACKOFF_START_SECONDS", "1"))
SSE_BACKOFF_MAX_SECONDS = float(os.getenv("SSE_BACKOFF_MAX_SECONDS", "30"))

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
}


def kafka_security_kwargs():
    """Configuracion opcional de SASL/ACL via env vars.

    Si KAFKA_SECURITY_PROTOCOL es PLAINTEXT (default), no se anaden kwargs.
    Si es SASL_PLAINTEXT, se requieren KAFKA_SASL_USERNAME y KAFKA_SASL_PASSWORD.
    """
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


def create_producer(max_retries=10, wait_seconds=5):
    sec_kwargs = kafka_security_kwargs()
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                **sec_kwargs,
            )
            print(
                f"[KAFKA] Productor conectado a {KAFKA_HOST}:{KAFKA_PORT} "
                f"(protocol={sec_kwargs.get('security_protocol', 'PLAINTEXT')})"
            )
            return producer
        except Exception as exc:
            print(
                f"[KAFKA] Intento {attempt}/{max_retries} falló: {exc}. "
                f"Reintentando en {wait_seconds} segundos..."
            )
            time.sleep(wait_seconds)

    raise ConnectionError("No fue posible conectarse a Kafka.")


def open_sse_stream():
    print(f"[WIKIMEDIA] Conectando a {STREAM_URL} ...")
    print(f"[WIKIMEDIA] User-Agent usado: {USER_AGENT}")

    response = requests.get(
        STREAM_URL,
        stream=True,
        timeout=60,
        headers=HEADERS,
    )
    response.raise_for_status()
    return response, SSEClient(response)


def main():
    producer = None
    sent_count = 0
    backoff_seconds = SSE_BACKOFF_START_SECONDS

    try:
        producer = create_producer()

        print("[PIPELINE] Iniciando flujo Wikimedia → Kafka...")

        while True:
            response = None
            try:
                response, client = open_sse_stream()
                backoff_seconds = SSE_BACKOFF_START_SECONDS

                for event in client.events():
                    if not event.data:
                        continue

                    try:
                        payload = json.loads(event.data)

                        if not isinstance(payload, dict):
                            continue

                        if payload.get("meta", {}).get("domain") == "canary":
                            continue

                        producer.send(TOPIC_NAME, payload)
                        sent_count += 1

                        if sent_count % FLUSH_EVERY == 0:
                            producer.flush()
                            print(
                                f"[SEND] {sent_count} eventos enviados | "
                                f"ultimo_title={payload.get('title')!r} | "
                                f"ultimo_type={payload.get('type')!r}"
                            )

                    except json.JSONDecodeError:
                        print("[WARN] Evento recibido no es JSON válido.")
                    except Exception as exc:
                        print(f"[ERROR] No se pudo enviar el evento a Kafka: {exc}")

            except KeyboardInterrupt:
                raise
            except Exception as exc:
                if producer is not None:
                    try:
                        producer.flush()
                    except Exception as flush_exc:
                        print(
                            f"[KAFKA] Flush fallido tras error SSE ({flush_exc}). "
                            "Continuando con reconexion..."
                        )

                print(
                    f"[WIKIMEDIA] Stream interrumpido ({type(exc).__name__}: {exc}). "
                    f"Reintentando en {backoff_seconds}s..."
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, SSE_BACKOFF_MAX_SECONDS)
            finally:
                if response is not None:
                    response.close()

    except KeyboardInterrupt:
        print("\n[PIPELINE] Proceso detenido manualmente.")
    except Exception as exc:
        print(f"[ERROR] Ocurrió un error en Wikimedia → Kafka: {type(exc).__name__}: {exc}")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            print("[KAFKA] Productor cerrado.")


if __name__ == "__main__":
    main()
