# Resiliencia del pipeline (Etapa 3)

Este documento describe los mecanismos de resiliencia implementados, los escenarios validados y cómo reproducirlos.

## Mecanismos implementados

### Productor `consumers/wikimedia_to_kafka.py`

| Mecanismo | Implementación |
|---|---|
| Reintento de conexión inicial a Kafka | `create_producer` con hasta 10 intentos, espera 5s entre reintentos. |
| Reconexión SSE con backoff exponencial | `SSE_BACKOFF_START_SECONDS=1` → `SSE_BACKOFF_MAX_SECONDS=30`, duplicando en cada falla. |
| Flush por lotes | Cada `KAFKA_FLUSH_EVERY=100` envíos para evitar pérdida ante caída abrupta. |
| Cierre limpio | `flush()` + `close()` en bloque `finally`. |

### Consumer `consumers/kafka_to_cassandra.py`

| Mecanismo | Implementación |
|---|---|
| Reintento de conexión a Cassandra | Hasta 10 intentos, 5s de espera. |
| Reintento de inserts | `CASSANDRA_INSERT_MAX_RETRIES=3`, `CASSANDRA_INSERT_RETRY_WAIT_SECONDS=2`. |
| Commit manual de offsets | `enable_auto_commit=False`. El offset solo se confirma **después** de persistir en Cassandra. Si Cassandra falla y se agotan reintentos, el mensaje no se commitea: al reiniciar el consumer lo reprocesa. |
| Mensajes inválidos no bloquean | `InvalidEventError` → log `[SKIP]` + commit del offset y continúa. |

### Idempotencia a nivel evento

`source_event_id` proviene de `meta.id` (estable) o un `sha1(raw_json)` como fallback. La PK de `recent_changes_raw` incluye `source_event_id`, así que un reproceso del mismo offset no duplica filas (último-write-wins por mismo PK).

## Escenarios validados

### 1. Caída de Cassandra durante ingesta

**Reproducción:**

```bash
# terminal 1: producer
python3 consumers/wikimedia_to_kafka.py

# terminal 2: consumer
python3 consumers/kafka_to_cassandra.py

# terminal 3: demo
bash scripts/resilience_demo.sh cassandra-restart
```

**Comportamiento esperado:**

- Mientras Cassandra está caído, el consumer imprime `[CASSANDRA] Insert fallido N/3`.
- El productor sigue publicando a Kafka sin afectación (Kafka acepta).
- Al volver Cassandra, el consumer drena el lag: cada mensaje pendiente en Kafka se reintenta hasta persistir; los offsets se confirman después.
- **Pérdida de mensajes esperada: 0**, siempre que el lag de Kafka quepa en `log.retention.hours` (default 168 h).

### 2. Caída de Kafka durante ingesta

**Reproducción:**

```bash
bash scripts/resilience_demo.sh kafka-restart
```

**Comportamiento esperado:**

- Productor: backoff exponencial (1s → 2s → 4s → … 30s), reconecta al volver Kafka.
- Consumer: queda en espera (`consumer_timeout_ms=1000` + `time.sleep(2)`), reanuda al volver Kafka.
- **Pérdida de mensajes esperada**: solo los eventos del stream Wikimedia que ocurrieron mientras Kafka estaba caído **y** que el productor no alcanzó a buffereizar (la API SSE no replays). Esto es inherente a la fuente externa, no al pipeline.

### 3. Failover de nodo Cassandra (multinodo)

**Pre-requisito**: levantar el compose multinodo:

```bash
docker compose -f docker-compose.multinode.yml up -d
```

Esto levanta 3 nodos Cassandra con `RF=3` sobre `NetworkTopologyStrategy`. El consumer escribe con consistencia `LOCAL_QUORUM` (2 de 3) por default del driver.

**Reproducción:**

```bash
bash scripts/resilience_demo.sh cassandra-node-failover
```

**Comportamiento esperado:**

- Al detener `cassandra-2`, los otros dos nodos siguen aceptando escrituras (quorum = 2/3).
- El consumer no observa errores.
- Al reiniciar `cassandra-2`, el nodo se reincorpora y el cluster aplica `hinted handoff` para reponer las escrituras perdidas.
- **Pérdida de mensajes esperada: 0**.

## Tabla de referencia (a llenar tras corrida oficial)

| Escenario | Filas pre-fallo | Filas durante | Filas post-recovery | Pérdida observada | Veredicto |
|---|---|---|---|---|---|
| cassandra-restart | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| kafka-restart | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |
| cassandra-node-failover | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ | _pendiente_ |

> Llenar después de ejecutar `scripts/resilience_demo.sh` para cada escenario y registrar los conteos.

## Limitaciones conocidas

- Compose mononodo (`docker-compose.yml`) **no** valida failover de nodo: para esa demo usar `docker-compose.multinode.yml`.
- La caída del propio consumer no es un escenario adicional: al reiniciar, retoma desde el último offset commiteado (no hay pérdida porque el commit es manual y posterior a la persistencia).
- La caída del productor sí pierde los eventos del stream que ocurran durante el outage (limitación de la fuente SSE, no del pipeline).
