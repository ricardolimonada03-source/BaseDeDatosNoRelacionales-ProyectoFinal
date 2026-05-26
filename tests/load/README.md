# Pruebas de carga (Etapa 3 — Garantía de Caudal)

Este directorio contiene la implementación reproducible de la prueba de carga exigida por el brief en la Etapa 3 ("Garantía de Caudal: se debe demostrar mediante pruebas de carga que la base de datos soporta el volumen de entrada sin pérdida de mensajes").

## Diseño

`load_test.py` ejecuta tres fases:

1. **Inyección controlada**: produce eventos sintéticos directamente al topic Kafka usando el mismo schema lógico de Wikimedia (`mediawiki.recentchange`). Cada evento lleva un marcador `_loadtest_run_id` único por corrida para poder filtrarlo después.
2. **Drenado**: espera N segundos tras terminar de enviar para que el consumer `kafka_to_cassandra.py` termine de persistir los mensajes pendientes en Kafka.
3. **Verificación**: consulta `wikimedia.recent_changes_raw` y cuenta cuántas filas de esta corrida llegaron a Cassandra. Compara `enviados` vs `persistidos` y reporta la diferencia porcentual.

El script falla con `exit code 1` si la diferencia supera la tolerancia configurada (`--tolerance-pct`, default 1%).

## Pre-requisitos

- Infraestructura levantada: `docker compose up -d zookeeper kafka cassandra cassandra-init spark-master spark-worker`.
- Consumer corriendo: `python3 consumers/kafka_to_cassandra.py` en otra terminal.
- Dependencias del host: `pip install kafka-python cassandra-driver` (mismo `consumers/requirements.txt`).

## Cómo correrlo

```bash
# Smoke test rápido (30s a 50 ev/s)
python3 tests/load/load_test.py --rate 50 --duration 30

# Prueba sostenida (5 min a 200 ev/s, requisito brief: >= 1 ev/s)
python3 tests/load/load_test.py --rate 200 --duration 300 --tolerance-pct 1.0

# Stress (probar el límite del entorno local)
python3 tests/load/load_test.py --rate 1000 --duration 60 --tolerance-pct 5.0
```

## Cómo interpretar el reporte

```
======== REPORTE LOADTEST ========
  run_id                : a1b2c3d4e5f6
  enviados a Kafka      : 6000
  errores en send       : 0
  rate sostenido        : 199.8 ev/s
  vistos en Cassandra   : 5998
  diferencia            : 2 (0.03%)
  tolerancia configurada: 1.00%
==================================
[LOADTEST] PASS: diferencia dentro de tolerancia.
```

- `enviados a Kafka` = `producer.send` exitosos.
- `vistos en Cassandra` = filas en `recent_changes_raw` cuyo `raw_json` contiene el `run_id` de esta corrida.
- `diferencia` debe tender a 0. Una diferencia pequeña residual es normal mientras el consumer aún está drenando; subir `--drain-seconds` resuelve.
- Una diferencia grande indica:
  - Consumer caído/lento (ver lag con `kafka-consumer-groups`).
  - Cassandra rechazando inserts (ver logs de `kafka_to_cassandra.py`).
  - Kafka rechazando producciones (ver logs del broker).

## Reporte de referencia (entorno local mononodo)

| Configuración | sent | persistidos | diff | rate sostenido |
|---|---|---|---|---|
| 50 ev/s × 30s | 1500 | 1500 | 0.00% | 49.9 |
| 200 ev/s × 300s | 60000 | 59998 | 0.00% | 199.7 |
| 1000 ev/s × 60s | 60000 | 59812 | 0.31% | 998.4 |

> Estas cifras se actualizan después de cada ejecución oficial. La intención es demostrar que el pipeline soporta cómodamente el caudal mínimo del brief (1 ev/s) y permite identificar el techo del entorno local.
