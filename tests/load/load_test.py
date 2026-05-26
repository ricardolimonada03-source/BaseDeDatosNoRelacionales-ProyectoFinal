"""
Prueba de carga (Etapa 3 - Garantia de caudal).

Inyecta eventos sinteticos al topic Kafka usando el mismo formato que el
stream real de Wikimedia. El consumidor `kafka_to_cassandra.py` los persiste
en Cassandra. Al terminar, el script consulta la tabla raw para verificar
que no hubo perdida (eventos enviados == eventos persistidos a nivel logico).

Uso:
    python3 tests/load/load_test.py \\
        --rate 200 \\
        --duration 30 \\
        --kafka 127.0.0.1:9092 \\
        --topic wikimedia.recentchange \\
        --cassandra 127.0.0.1 \\
        --keyspace wikimedia

Salida: imprime un reporte tabular con eventos enviados, throughput
sostenido y filas vistas en Cassandra. Falla con exit code != 0 si la
diferencia supera la tolerancia indicada por --tolerance-pct.
"""

import argparse
import json
import random
import string
import sys
import time
import uuid
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from kafka import KafkaProducer


def random_word(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))


def synth_event(now_ts: int) -> dict:
    """Genera un evento sintetico con la misma forma que mediawiki/recentchange."""
    event_id = uuid.uuid4().hex
    return {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": f"https://en.wikipedia.org/wiki/{random_word(8)}",
            "id": event_id,
            "dt": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
            "stream": "mediawiki.recentchange",
            "domain": "en.wikipedia.org",
        },
        "id": random.randint(1, 10**9),
        "type": random.choice(["edit", "new", "log", "categorize"]),
        "namespace": random.choice([0, 1, 2, 6, 14]),
        "title": f"Loadtest {random_word(10)}",
        "comment": f"loadtest commit {random_word(4)}",
        "timestamp": now_ts,
        "user": f"loadtest_{random_word(5)}",
        "bot": random.random() < 0.1,
        "server_url": "https://en.wikipedia.org",
        "server_name": "en.wikipedia.org",
        "server_script_path": "/w",
        "wiki": random.choice(["enwiki", "eswiki", "frwiki", "dewiki", "ptwiki"]),
        "_loadtest": True,
        "_loadtest_run_id": LOADTEST_RUN_ID,
    }


LOADTEST_RUN_ID = uuid.uuid4().hex[:12]


def run(args: argparse.Namespace) -> int:
    print(f"[LOADTEST] run_id={LOADTEST_RUN_ID}")
    print(f"[LOADTEST] target_rate={args.rate} ev/s  duration={args.duration}s")
    print(f"[LOADTEST] kafka={args.kafka}  topic={args.topic}")
    print(f"[LOADTEST] cassandra={args.cassandra}  keyspace={args.keyspace}")

    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=5,
    )

    sent = 0
    errors = 0
    start = time.monotonic()
    deadline = start + args.duration
    interval = 1.0 / args.rate if args.rate > 0 else 0
    next_emit = start

    while True:
        now_mono = time.monotonic()
        if now_mono >= deadline:
            break
        if now_mono < next_emit:
            time.sleep(min(0.005, next_emit - now_mono))
            continue

        event = synth_event(int(time.time()))
        try:
            producer.send(args.topic, event)
            sent += 1
        except Exception as exc:
            errors += 1
            print(f"[LOADTEST] error en send: {exc}", file=sys.stderr)
        next_emit += interval

        if sent % max(1, args.rate * 5) == 0 and sent > 0:
            elapsed = time.monotonic() - start
            achieved = sent / elapsed if elapsed > 0 else 0
            print(f"[LOADTEST] t={elapsed:6.1f}s sent={sent} rate_actual={achieved:.1f} ev/s")

    producer.flush()
    producer.close()
    elapsed_total = time.monotonic() - start
    achieved_rate = sent / elapsed_total if elapsed_total > 0 else 0

    print()
    print(f"[LOADTEST] envio terminado: sent={sent}  errors={errors}  achieved={achieved_rate:.1f} ev/s")

    print(f"[LOADTEST] esperando {args.drain_seconds}s a que el consumer drene...")
    time.sleep(args.drain_seconds)

    print("[LOADTEST] consultando Cassandra...")
    cluster = Cluster([args.cassandra], port=args.cassandra_port)
    session = cluster.connect(args.keyspace)
    try:
        rs = session.execute(
            f"SELECT raw_json FROM {args.raw_table} ALLOW FILTERING"
        )
        seen_in_cassandra = 0
        for row in rs:
            raw = row.raw_json or ""
            if LOADTEST_RUN_ID in raw:
                seen_in_cassandra += 1
    finally:
        cluster.shutdown()

    diff = sent - seen_in_cassandra
    diff_pct = (diff / sent * 100) if sent > 0 else 0

    print()
    print("======== REPORTE LOADTEST ========")
    print(f"  run_id                : {LOADTEST_RUN_ID}")
    print(f"  enviados a Kafka      : {sent}")
    print(f"  errores en send       : {errors}")
    print(f"  rate sostenido        : {achieved_rate:.1f} ev/s")
    print(f"  vistos en Cassandra   : {seen_in_cassandra}")
    print(f"  diferencia            : {diff} ({diff_pct:.2f}%)")
    print(f"  tolerancia configurada: {args.tolerance_pct:.2f}%")
    print("==================================")

    if abs(diff_pct) > args.tolerance_pct:
        print(f"[LOADTEST] FAIL: diferencia {diff_pct:.2f}% supera tolerancia {args.tolerance_pct:.2f}%")
        return 1
    print("[LOADTEST] PASS: diferencia dentro de tolerancia.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Prueba de carga del pipeline Wikimedia")
    parser.add_argument("--rate", type=int, default=100, help="Eventos/segundo objetivo")
    parser.add_argument("--duration", type=int, default=30, help="Duracion en segundos")
    parser.add_argument("--kafka", default="127.0.0.1:9092", help="bootstrap servers Kafka")
    parser.add_argument("--topic", default="wikimedia.recentchange")
    parser.add_argument("--cassandra", default="127.0.0.1")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--keyspace", default="wikimedia")
    parser.add_argument("--raw-table", default="recent_changes_raw")
    parser.add_argument(
        "--drain-seconds",
        type=int,
        default=15,
        help="Segundos a esperar tras enviar para que el consumer drene Kafka",
    )
    parser.add_argument(
        "--tolerance-pct",
        type=float,
        default=1.0,
        help="Maxima diferencia porcentual aceptable entre enviados y persistidos",
    )
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
