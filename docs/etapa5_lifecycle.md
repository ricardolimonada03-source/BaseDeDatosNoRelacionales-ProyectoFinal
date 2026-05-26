# Trazabilidad del dato: del evento crudo a la información estratégica

El brief de la Etapa 5 pide demostrar "cómo el evento crudo capturado en la capa de ingesta fue transformado y enriquecido hasta convertirse en información estratégica". Este documento sigue **un evento real** del stream a lo largo de las 4 capas del pipeline.

## Evento ejemplo

Tomamos un evento real capturado durante la corrida de validación:

| Campo | Valor |
|---|---|
| `meta.id` | `c98b245c-d39f-462e-b46f-6c92fcca73c3` |
| `wiki` | `plwiki` |
| `title` | `Marta Kostiuk` |
| `user` | `L.zurawski` |
| `type` | `edit` |
| `bot` | `false` |
| `timestamp` | `1777931508` (epoch) → `2026-05-04T21:51:48Z` |

---

## Capa 1 — Evento crudo (Wikimedia EventStreams → Kafka)

`consumers/wikimedia_to_kafka.py` recibe el evento por SSE y lo publica **sin transformar** en el topic Kafka `wikimedia.recentchange`. El payload es JSON anidado con metadatos del sistema MediaWiki:

```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "bot": false,
  "comment": "ranking",
  "id": 161485684,
  "length": { "new": 18076, "old": 18080 },
  "meta": {
    "domain": "pl.wikipedia.org",
    "dt": "2026-05-04T21:51:50.021Z",
    "id": "c98b245c-d39f-462e-b46f-6c92fcca73c3",
    "offset": 6099073344,
    "partition": 0,
    "request_id": "ba92c47c-a220-4060-9a0f-a6f51d22c21e",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "uri": "https://pl.wikipedia.org/wiki/Marta_Kostiuk"
  },
  "minor": false,
  "namespace": 0,
  "revision": { "new": 79673045, "old": 79666560 },
  "server_name": "pl.wikipedia.org",
  "timestamp": 1777931508,
  "title": "Marta Kostiuk",
  "type": "edit",
  "user": "L.zurawski",
  "wiki": "plwiki"
}
```

Características de esta capa:

- **Schema-on-read.** El payload puede traer campos opcionales (`length`, `revision`) o desconocidos.
- **Timestamps en epoch.** Hay que parsearlos para hacer cualquier análisis temporal.
- **Información mezclada.** Datos de negocio (`title`, `user`) viven junto a metadatos de infraestructura (`offset`, `partition`).
- **No queryable.** Está dentro del payload binario de Kafka; no se puede filtrar por `wiki` sin deserializar.

---

## Capa 2 — Verdad operativa (Cassandra `recent_changes_raw`)

`consumers/kafka_to_cassandra.py` aplica la primera transformación: extrae los campos relevantes, parsea el timestamp, deriva `event_date`/`event_hour` para el modelo de particionamiento y persiste:

| Columna | Valor | Origen |
|---|---|---|
| `event_date` | `2026-05-04` | derivado de `timestamp` |
| `wiki` | `plwiki` | directo |
| `event_hour` | `21` | derivado de `timestamp` |
| `timestamp_event` | `2026-05-04 21:51:48+00:00` | parseado de `timestamp` |
| `source_event_id` | `c98b245c-d39f-462e-b46f-6c92fcca73c3` | `meta.id` (fallback: `sha1(raw_json)`) |
| `title` | `Marta Kostiuk` | directo |
| `user_name` | `L.zurawski` | renombrado de `user` |
| `change_type` | `edit` | renombrado de `type` |
| `bot` | `false` | directo |
| `server_name` | `pl.wikipedia.org` | directo |
| `comment` | `ranking` | directo |
| `raw_json` | (payload completo) | preservado para auditoría/reproceso |

Qué ganamos en esta capa:

- **Consultable.** PK `((event_date, wiki, event_hour), timestamp_event, source_event_id)` permite recuperar el evento con latencia de milisegundos por sus dimensiones temporales.
- **Idempotente.** Si el mismo evento se reprocesa (mismo `source_event_id`), termina en la misma fila → no se duplica.
- **Tipado.** `timestamp_event` es `TIMESTAMP` real, `bot` es `BOOLEAN`. Ya se puede agrupar/filtrar sin parseo.
- **Auditable.** El `raw_json` original se conserva: si en el futuro nos damos cuenta que necesitamos `revision.old` o `length.new`, lo podemos rescatar sin re-ingestar.

Lo que **todavía no** tenemos en esta capa:

- Volumen agregado por dimensiones (¿cuántos `edit` hubo en `plwiki` en la hora 21?).
- Contexto de negocio (¿qué idioma habla `plwiki`? ¿qué país? ¿qué tamaño de comunidad?).
- Métricas comparativas (¿cómo se compara `plwiki` con el resto?).

---

## Capa 3 — Analítica (Spark batch agregado)

`spark/jobs/recent_changes_analytics.py` lee la tabla operativa, aplica limpieza/dedupe, hace `LEFT JOIN` con el catálogo estático y agrega por dimensiones temporales + de negocio.

El evento individual `c98b245c-...` **deja de ser visible como fila** y se convierte en una **contribución** a una fila agregada:

| event_date | wiki | event_hour | change_type | language | country | project_family | total_events | bot_events |
|---|---|---|---|---|---|---|---|---|
| 2026-05-04 | plwiki | 21 | edit | pl | Poland | wikipedia | **N** | M |

Donde `N` es la cuenta de TODOS los `edit` ocurridos en `plwiki` en la hora 21 (incluyendo nuestro evento, que aporta +1). `M` es cuántos fueron por bots.

Qué ganamos:

- **Dimensión de negocio.** El JOIN con `data/static/wikis.csv` añade `language=pl`, `country=Poland`, `project_family=wikipedia`, `community_size_bucket=medium`.
- **Comparabilidad.** Ahora podemos comparar el volumen de `plwiki` contra `enwiki`, contra todas las wikis polacas, contra toda la familia `wikipedia`, etc.
- **Tamaño manejable.** 4,742 eventos raw → ~130 filas agregadas. Cabe en una hoja de cálculo, en un dashboard, en una consulta `WHERE`.

Lo que **perdemos**:

- Identidad del evento individual. Ya no podemos preguntar "¿qué editó L.zurawski exactamente?". Para eso volvemos a la capa 2.

---

## Capa 4 — Información estratégica (consultas Etapa 5)

La capa 3 produce un agregado **listo para consumo**. Las 6 consultas de Etapa 5 (`spark/jobs/etapa5_queries.py`) lo convierten en respuestas accionables.

Qué dice **nuestro evento original** (su contribución a las consultas):

- **Q1** (Top wikis): `plwiki` aparece o no aparece en el top 20. Si aparece, este evento contribuyó +1 a su `total_events` y +0 a su `bot_events`.
- **Q2** (Pareto): el ranking de `plwiki` (digamos top-7) y el % acumulado nos dicen qué porción del stream global representa.
- **Q3** (Anomalías bot): si `plwiki` tiene `bot_pct ≥ 90%`, queda flagged. Nuestro evento aporta evidencia de tráfico humano → ayuda a NO flagear.
- **Q4** (Top contribuyentes): si `L.zurawski` hizo ≥ N eventos en el período, aparece en el ranking.
- **Q5** (Project family): el evento se acumula en el bucket `wikipedia` (la familia más grande).
- **Q6** (Cobertura): como `plwiki` está en `wikis.csv`, el evento cuenta como `enriched`, no `unmapped`.

## Mapa visual de la transformación

```
Wikimedia (SSE)
    │  JSON anidado, schema-on-read, no queryable
    ▼
Kafka topic `wikimedia.recentchange`
    │  durabilidad, desacople, replay
    ▼
Cassandra `recent_changes_raw`               <-- CAPA OPERATIVA
    │  tipado, indexable por (fecha+wiki+hora),
    │  raw_json preservado para auditoría
    ▼
Spark job `recent_changes_analytics`
    │  limpieza + dedupe + LEFT JOIN catalogo estatico + agregacion
    ▼
Cassandra/CSV `changes_by_wiki_hour_enriched` <-- CAPA ANALITICA
    │  con dimensiones de negocio: language, country, project_family
    ▼
Spark job `etapa5_queries`
    │  6 consultas (Pareto, anomalias bot, top contributors, ...)
    ▼
CSV bajo `spark/output/etapa5/`              <-- INFORMACION ESTRATEGICA
    │  visualizaciones en docs/charts_etapa5/
    ▼
Decisiones (priorizar moderacion, validar bots, capacidad)
```

## Por qué cada capa es necesaria

- **Sin Kafka**: si Cassandra cae, perdemos los eventos del stream durante el outage. Kafka nos da una ventana de reproceso de horas.
- **Sin la capa operativa raw**: no podríamos auditar un evento específico ni reprocesar la analítica con lógica nueva. El `raw_json` preservado nos da reproducibilidad.
- **Sin la capa analítica agregada**: cada consulta tendría que escanear todas las filas. Con el agregado, las queries son baratas.
- **Sin el enriquecimiento estático**: tendríamos los volúmenes pero no el contexto. Una agregación sin negocio no decide nada.
