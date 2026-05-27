# Proyecto Final - Equipo 1

## Bases de Datos No Relacionales
Proyecto enfocado en el diseÃ±o e implementaciÃ³n de una arquitectura de datos no relacional y distribuida de extremo a extremo, utilizando un stream de datos real.

> **Â¿Primera vez corriendo el proyecto?** Sigue [**RUNBOOK.md**](RUNBOOK.md) paso a paso. Te guÃ­a desde clonar el repo hasta ver las grÃ¡ficas finales en ~15 minutos.

## Ãndice

- [1. DescripciÃ³n general del proyecto](#1-descripciÃ³n-general-del-proyecto)
- [2. Stream seleccionado](#2-stream-seleccionado)
- [3. Arquitectura actual del proyecto](#3-arquitectura-actual-del-proyecto)
- [4. TecnologÃ­as utilizadas](#4-tecnologÃ­as-utilizadas)
- [5. Etapa 2: Infraestructura y configuraciÃ³n](#5-etapa-2-infraestructura-y-configuraciÃ³n)
- [6. Etapa 3: Pipeline de datos en tiempo real](#6-etapa-3-pipeline-de-datos-en-tiempo-real)
- [7. Etapa 4: AnalÃ­tica batch con Spark](#7-etapa-4-analÃ­tica-batch-con-spark)
- [8. CÃ³mo levantar y validar el flujo completo](#8-cÃ³mo-levantar-y-validar-el-flujo-completo)
- [9. DescripciÃ³n del stream de datos: Wikimedia RecentChange](#9-descripciÃ³n-del-stream-de-datos-wikimedia-recentchange)
- [10. Etapa 5: AnÃ¡lisis de resultados](#10-etapa-5-anÃ¡lisis-de-resultados)
- [11. Documentos complementarios](#11-documentos-complementarios)

---

## 1. DescripciÃ³n general del proyecto
El proyecto tiene como objetivo diseÃ±ar e implementar una arquitectura de datos no relacional de extremo a extremo a partir de un stream real de Wikimedia. La soluciÃ³n actual contempla una capa de ingesta, una capa operativa de almacenamiento y una capa analÃ­tica reproducible para generar agregados a partir de los eventos capturados.

El estado real del repositorio corresponde a un entorno local y acadÃ©mico, no a una plataforma productiva de alta disponibilidad.

## 2. Stream seleccionado
Se utiliza el stream **Wikimedia EventStreams - RecentChange**, que publica eventos en tiempo real sobre cambios recientes en pÃ¡ginas de Wikimedia.

## 3. Arquitectura actual del proyecto
El flujo general del sistema es el siguiente:

Wikimedia EventStreams â†’ Kafka â†’ Cassandra recent_changes_raw â†’ Spark â†’ CSV analÃ­tico

```mermaid
flowchart LR
    A[ðŸŒ Wikimedia Foundation] -->|Server-Sent Events SSE| B[ðŸ“¡ EventStreams]
    B -->|JSON Events ~3000/min| C[Apache Kafka]
    C -->|Stream Processing| D[(Apache Cassandra)]
    D -->|Query / ETL| E[Apache Spark]
    E -->|Resultados| F[ðŸ“Š CSV analÃ­tico]

    subgraph Fuente
        A
        B
    end

    subgraph Ingesta
        C
    end

    subgraph Almacenamiento Operativo
        D
    end

    subgraph AnalÃ­tica
        E
        F
    end
```

## 4. TecnologÃ­as utilizadas

### Apache Kafka
Se utiliza como capa de mensajerÃ­a para recibir y desacoplar el flujo de eventos en tiempo real.

### Apache Cassandra
Se utiliza como base de datos NoSQL de ingesta y operaciÃ³n, optimizada para escrituras rÃ¡pidas. En el estado actual del proyecto se ejecuta en un solo nodo local.

### Apache Spark
Se utiliza como motor de procesamiento batch para limpieza, transformaciÃ³n y generaciÃ³n de resultados agregados reproducibles.

## 5. Etapa 2: Infraestructura y configuraciÃ³n

El proyecto ofrece **tres stacks de Docker Compose** segÃºn el escenario que se quiera demostrar; todos arrancan con un Ãºnico comando.

| Archivo | PropÃ³sito | CuÃ¡ndo usarlo |
|---|---|---|
| `docker-compose.yml` | Stack acadÃ©mico mononodo, sin auth | Desarrollo rÃ¡pido, smoke tests |
| `docker-compose.secure.yml` | Mononodo + control de accesos (Cassandra auth + Kafka SASL/ACL) | Demostrar requisito "ImplementaciÃ³n de Control de Accesos" |
| `docker-compose.multinode.yml` | 3 nodos Cassandra con `RF=3` y `NetworkTopologyStrategy` | Demostrar replicaciÃ³n + failover real |

### 5.1 Servicios comunes
- `zookeeper`, `kafka` â€” capa de mensajerÃ­a
- `cassandra` (1 o 3 instancias segÃºn stack) â€” capa operativa NoSQL
- `cassandra-init` â€” aplica `schema.cql` automÃ¡ticamente
- `spark-master`, `spark-worker` â€” capa analÃ­tica OLAP

CaracterÃ­sticas transversales:
- volumen persistente para cada nodo Cassandra
- `healthcheck` del servicio `cassandra`
- keyspace `wikimedia` con tablas:
  - `recent_changes_raw` (operativa, particionada por `(event_date, wiki, event_hour)`)
  - `changes_by_wiki_hour` (agregada para Spark)
  - `recent_changes` (legacy, conservada por compatibilidad)

### 5.2 Stack mononodo seguro (`docker-compose.secure.yml`)

Cumple el requisito **ImplementaciÃ³n de Control de Accesos** del brief.

- Cassandra arranca parcheando `cassandra.yaml` para activar `PasswordAuthenticator` + `CassandraAuthorizer`.
- `cassandra/secure_setup.cql` crea roles con least privilege:
  - `pipeline_writer` â€” `MODIFY` sobre `recent_changes_raw`, `SELECT` en keyspace
  - `analytics_reader` â€” solo `SELECT`
  - `analytics_writer` â€” `MODIFY` sobre `changes_by_wiki_hour`
- Kafka usa `SASL_PLAINTEXT://localhost:9093`, `AclAuthorizer` y `allow.everyone.if.no.acl.found=false`.
- `kafka-acl-init` aplica las ACLs reales (`kafka/secure_acls.sh`) automÃ¡ticamente.

Detalle completo y comandos de verificaciÃ³n: [docs/seguridad.md](docs/seguridad.md).

### 5.3 Stack multinodo (`docker-compose.multinode.yml`)

Cumple el requisito **alta disponibilidad mediante replicaciÃ³n y sharding** del brief.

- 3 nodos `cassandra-1`, `cassandra-2`, `cassandra-3` en el mismo DC (`dc1`), con vnodes (default `num_tokens=16`) que reparten particiones automÃ¡ticamente.
- Keyspace creado con `NetworkTopologyStrategy`, `replication_factor=3`. Con `CL=QUORUM` (default del driver), el sistema sobrevive a la caÃ­da de cualquier nodo individual.
- Habilita el escenario `cassandra-node-failover` documentado en [docs/resiliencia.md](docs/resiliencia.md).

### 5.4 JustificaciÃ³n CAP

La decisiÃ³n de priorizar **AP** se documenta en [docs/decisiones_cap.md](docs/decisiones_cap.md) y se materializa en el stack multinodo (con RF=3 el sistema sigue aceptando escrituras durante una particiÃ³n de 1 nodo).

## 6. Etapa 3: Pipeline de datos en tiempo real
En esta etapa quedÃ³ implementado el flujo:

Wikimedia â†’ Kafka â†’ Cassandra

### 6.1 Componentes

#### Productor: Wikimedia â†’ Kafka
Script: [`consumers/wikimedia_to_kafka.py`](consumers/wikimedia_to_kafka.py)

- consume eventos en tiempo real desde Wikimedia EventStreams (SSE)
- serializa los mensajes en JSON
- publica en el topic `wikimedia.recentchange`
- hace `flush` por lotes (`KAFKA_FLUSH_EVERY=100`)
- reconexiÃ³n SSE con backoff exponencial (1s â†’ 30s)
- soporte opcional `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT` con SASL/PLAIN

#### Consumidor: Kafka â†’ Cassandra
Script: [`consumers/kafka_to_cassandra.py`](consumers/kafka_to_cassandra.py)

- consume mensajes de Kafka con `enable_auto_commit=False`
- usa `group_id` y `auto_offset_reset` configurables por variables de entorno
- transforma cada evento al modelo de `wikimedia.recent_changes_raw`
- usa prepared statements para Cassandra
- deriva `event_date` y `event_hour` a partir de `timestamp_event`
- hace commit manual del offset solo cuando el mensaje fue procesado
- si un mensaje es invÃ¡lido, lo registra como `SKIP`, hace commit del offset y continÃºa
- si Cassandra falla al insertar, reintenta hasta `CASSANDRA_INSERT_MAX_RETRIES` y no hace commit del offset si no logra persistir
- soporte opcional `CASSANDRA_USERNAME`/`CASSANDRA_PASSWORD` (driver `PlainTextAuthProvider`)
- soporte opcional `KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT`

### 6.2 GarantÃ­a de caudal â€” pruebas de carga

ImplementaciÃ³n reproducible en [`tests/load/load_test.py`](tests/load/load_test.py): inyecta eventos sintÃ©ticos al topic, espera el drenado del consumer, y compara `enviados a Kafka` vs `vistos en Cassandra` con tolerancia configurable. Falla con exit code distinto de cero si la diferencia supera el umbral.

```bash
python3 tests/load/load_test.py --rate 200 --duration 300 --tolerance-pct 1.0
```

DiseÃ±o completo, escenarios y reporte: [tests/load/README.md](tests/load/README.md).

### 6.3 Resiliencia ante fallos

Mecanismos implementados (productor + consumer) y tres escenarios reproducibles:

- `cassandra-restart` â€” Cassandra cae 30s mientras el consumer estÃ¡ corriendo; verificar 0 pÃ©rdida.
- `kafka-restart` â€” Kafka cae 30s mientras el productor estÃ¡ corriendo; verificar reconexiÃ³n con backoff.
- `cassandra-node-failover` â€” sÃ³lo en stack multinodo; tirar 1 de 3 nodos y verificar continuidad de escrituras.

```bash
bash scripts/resilience_demo.sh cassandra-restart
bash scripts/resilience_demo.sh kafka-restart
bash scripts/resilience_demo.sh cassandra-node-failover  # requiere docker-compose.multinode.yml
```

Detalle de cada escenario, mecanismos y tabla de resultados: [docs/resiliencia.md](docs/resiliencia.md).

## 7. Etapa 4: AnalÃ­tica batch con Spark
En esta etapa se implementÃ³ un job Spark real para procesar los datos persistidos en Cassandra. El job cumple los tres requisitos del brief: **limpieza**, **enriquecimiento** y **transformaciÃ³n**.

### 7.1 Job analÃ­tico
Script principal: [`spark/jobs/recent_changes_analytics.py`](spark/jobs/recent_changes_analytics.py)

Pipeline (cada paso es una funciÃ³n pura testeable):

1. **`read_raw`** â€” lee `wikimedia.recent_changes_raw` desde Cassandra.
2. **`clean`** â€” drop de filas sin `timestamp_event`/`wiki`, normalizaciÃ³n de `wiki` y `change_type` (lower + trim), cast robusto de `bot` a boolean, validaciÃ³n de `event_hour âˆˆ [0, 23]`, fallback de `event_date` a partir del timestamp.
3. **`dedupe`** â€” `dropDuplicates(["event_date", "wiki", "source_event_id"])`, aprovechando que `source_event_id` es estable (`meta.id` o sha1 del payload).
4. **`enrich`** â€” `LEFT JOIN` con [`data/static/wikis.csv`](data/static/wikis.csv) (catÃ¡logo wiki â†’ idioma, paÃ­s, project_family, community_size_bucket). Wikis no listadas se completan con `unknown` (no se descartan).
5. **`aggregate`** â€” agrupaciÃ³n por dimensiones temporales + dimensiones de negocio enriquecidas, con mÃ©tricas: `total_events`, `bot_events`, `unique_users` (countDistinct user), `unique_pages` (countDistinct title).
6. **Escritura** â€” CSV en `spark/output/changes_by_wiki_hour` (siempre); append a `wikimedia.changes_by_wiki_hour` si `ANALYTICS_WRITE_TO_CASSANDRA=true`.

Modo de ejecuciÃ³n del job (Spark batch, sin streaming):

- **Incremental (default)**: procesa solo los eventos recientes de `recent_changes_raw` dentro de los Ãºltimos `ANALYTICS_WINDOW_MINUTES` (default: `8`).
- **Full refresh**: si `ANALYTICS_FULL_REFRESH=true`, procesa todo el histÃ³rico (comportamiento original).

El job imprime un reporte: filas leÃ­das, tras limpieza, tras dedupe, duplicados eliminados, filas agregadas finales.

### 7.2 Dataset estÃ¡tico

[`data/static/wikis.csv`](data/static/wikis.csv) cubre las wikis con mayor volumen del stream y permite cruzar el evento crudo con metadatos de negocio. Esquema y origen documentados en [data/static/README.md](data/static/README.md).

### 7.3 Tests unitarios

[`tests/spark/test_recent_changes_analytics.py`](tests/spark/test_recent_changes_analytics.py) prueba `clean`, `dedupe`, `enrich` y `aggregate` con DataFrames sintÃ©ticos, sin necesidad de Cassandra ni cluster Spark:

```bash
pip install -r tests/requirements.txt
pytest tests/spark -v
```

### 7.4 EjecuciÃ³n del job

Script de apoyo: [`scripts/run_analytics.sh`](scripts/run_analytics.sh)

- copia el job y el dataset estÃ¡tico al contenedor `spark-master-proyectoFinal`
- ejecuta `spark-submit` con el connector `spark-cassandra-connector_2.13:3.5.1`
- descarga al host el directorio completo de salida generado por Spark

## 8. CÃ³mo levantar y validar el flujo completo

### 8.1 Levantar infraestructura

```bash
docker compose up -d zookeeper kafka cassandra cassandra-init spark-master spark-worker
```

### 8.2 Correr productor y consumer

En terminales separadas:

```bash
python3 consumers/wikimedia_to_kafka.py
```

```bash
python3 consumers/kafka_to_cassandra.py
```

### 8.3 Validar datos en Cassandra

Verificar que la tabla raw ya recibe eventos:

```bash
docker exec cassandra cqlsh -e "SELECT event_date, wiki, event_hour, timestamp_event, source_event_id, title FROM wikimedia.recent_changes_raw LIMIT 10;"
```

Consulta rÃ¡pida de conteo:

```bash
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wikimedia.recent_changes_raw;"
```

### 8.4 Correr analytics

```bash
bash scripts/run_analytics.sh
```

Ejemplos de modo:

```bash
# Incremental (default, ultimos 8 minutos)
ANALYTICS_WINDOW_MINUTES=8 bash scripts/run_analytics.sh

# Historico completo (full refresh)
ANALYTICS_FULL_REFRESH=true bash scripts/run_analytics.sh
```

### 8.5 Revisar la salida generada

```bash
ls -R spark/output/changes_by_wiki_hour
```

```bash
head -n 20 spark/output/changes_by_wiki_hour/part-*.csv
```

### 8.6 Seguridad y control de accesos

El control de accesos se entrega como **stack alternativo opt-in** en `docker-compose.secure.yml`:

```bash
docker compose -f docker-compose.secure.yml up -d
docker logs cassandra-init     # debe terminar con "Cassandra inicializado en modo seguro"
docker logs kafka-acl-init     # debe terminar con el listado de ACLs

# Producer/consumer con credenciales
export KAFKA_PORT=9093
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export KAFKA_SASL_USERNAME=pipeline_writer
export KAFKA_SASL_PASSWORD=pipeline_writer_pw
export CASSANDRA_USERNAME=pipeline_writer
export CASSANDRA_PASSWORD=pipeline_writer_pw

python3 consumers/wikimedia_to_kafka.py
python3 consumers/kafka_to_cassandra.py
```

Detalle del modelo (roles, ACLs, comandos de verificaciÃ³n) en [docs/seguridad.md](docs/seguridad.md).

### 8.7 Cluster multinodo y failover

```bash
docker compose -f docker-compose.multinode.yml up -d
# espera ~2 min al bootstrap completo
docker exec cassandra-1 nodetool status
# debe mostrar 3 nodos UN (Up + Normal)

# Demostrar failover (1 nodo down)
bash scripts/resilience_demo.sh cassandra-node-failover
```

### 8.8 ValidaciÃ³n integral

Hay un script integrador que valida estÃ¡ticamente todo el repo y, si Docker estÃ¡ arriba, encadena un smoke test end-to-end:

```bash
bash scripts/verify.sh static     # py_compile + bash -n + docker compose config
bash scripts/verify.sh e2e        # levanta stack default + load test corto
```

### 8.9 Decisiones CAP
La justificaciÃ³n CAP del proyecto se documenta en [docs/decisiones_cap.md](docs/decisiones_cap.md).

En sÃ­ntesis:

- el diseÃ±o privilegia disponibilidad y tolerancia a particiones (AP)
- esa prioridad se materializa **operativamente** en el stack multinodo (`docker-compose.multinode.yml`) con `RF=3` y `NetworkTopologyStrategy`
- el stack mononodo se conserva para desarrollo local
- Spark consolida el anÃ¡lisis de manera posterior sobre datos ya persistidos

### 8.10 Limitaciones reconocidas

- Compose por defecto (`docker-compose.yml`) sigue siendo mononodo y sin auth para que el flujo acadÃ©mico arranque con un solo comando.
- `SASL_PLAINTEXT` envÃ­a credenciales sin TLS â€” apto para entorno acadÃ©mico, no producciÃ³n.
- No hay orquestaciÃ³n externa tipo Airflow; el scheduling del job analÃ­tico se hace manualmente.

## 9. DescripciÃ³n del stream de datos: Wikimedia RecentChange

### 9.0 Enlaces a APIs y documentaciÃ³n oficial

| Recurso | URL |
|---|---|
| Endpoint del stream (SSE) | https://stream.wikimedia.org/v2/stream/recentchange |
| DocumentaciÃ³n EventStreams | https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams |
| CatÃ¡logo de streams disponibles | https://stream.wikimedia.org/?doc |
| Esquema JSON `mediawiki/recentchange` | https://schema.wikimedia.org/#!/primary/jsonschema/mediawiki/recentchange |
| DocumentaciÃ³n de MediaWiki RecentChanges | https://www.mediawiki.org/wiki/Manual:RCFeed |
| PolÃ­tica de uso de la API | https://api.wikimedia.org/wiki/Documentation/Policies/User-Agent |

> El stream se consume por HTTP/SSE sin autenticaciÃ³n, pero la polÃ­tica de Wikimedia exige enviar un `User-Agent` identificable; el productor lo configura vÃ­a la variable `USER_AGENT` (`consumers/wikimedia_to_kafka.py`).

### 9.1 Resumen
El stream `recentchange` es un flujo de datos en tiempo real que transmite todos los cambios realizados en los proyectos de Wikimedia, como Wikipedia, Wikidata, Wikimedia Commons y otros. Cada evento representa una acciÃ³n que ocurre en una pÃ¡gina: por ejemplo, una ediciÃ³n, creaciÃ³n de pÃ¡gina, categorizaciÃ³n o registro de acciones administrativas.

Los eventos se publican continuamente mediante un servicio llamado **EventStreams**, que envÃ­a datos estructurados en formato **JSON** a travÃ©s del protocolo **Server-Sent Events (SSE)**.

Cada registro del stream contiene informaciÃ³n como:

- Usuario que realizÃ³ el cambio
- PÃ¡gina afectada
- Tipo de acciÃ³n (ediciÃ³n, creaciÃ³n, log, etc.)
- Comentario del cambio
- Identificadores de revisiones
- Longitud del contenido antes y despuÃ©s
- Marca de tiempo del evento
- InformaciÃ³n tÃ©cnica del servidor y del wiki

Este stream permite observar la actividad global de ediciÃ³n de Wikipedia en tiempo real, lo que resulta Ãºtil para:

- anÃ¡lisis de actividad
- monitoreo de bots
- detecciÃ³n de vandalismo
- investigaciÃ³n acadÃ©mica
- aplicaciones de procesamiento de datos en streaming

### 9.2 Origen y autorÃ­a
El stream es generado por los sistemas de **MediaWiki**, el software que gestiona Wikipedia y otros proyectos de Wikimedia.

La entidad responsable de recolectar y publicar estos datos es:

**Wikimedia Foundation (WMF)**

Esta organizaciÃ³n sin fines de lucro opera los servidores de Wikipedia y mantiene la infraestructura que genera los eventos de cambios recientes.

#### Infraestructura tÃ©cnica
El flujo de datos funciona de la siguiente manera:

1. Cuando ocurre una modificaciÃ³n en una pÃ¡gina de MediaWiki, el sistema registra el evento.
2. Ese evento se envÃ­a a la plataforma de eventos.
3. Los eventos se almacenan y distribuyen mediante **Apache Kafka**.
4. El servicio **EventStreams** publica esos eventos en tiempo real a travÃ©s de HTTP.

### 9.3 Diccionario de datos
Un evento tÃ­pico del stream contiene atributos como los siguientes:

| Atributo | Significado |
|--------|-------------|
| `$schema` | Identificador del esquema JSON que define la estructura del evento |
| `meta` | Objeto con metadatos tÃ©cnicos del evento |
| `meta.uri` | URL relacionada con el cambio |
| `meta.id` | Identificador Ãºnico del evento |
| `meta.dt` | Fecha y hora del evento |
| `meta.stream` | Nombre del stream (`mediawiki.recentchange`) |
| `meta.domain` | Dominio del sitio donde ocurriÃ³ el cambio |
| `id` | Identificador del cambio dentro del sistema |
| `type` | Tipo de cambio (`edit`, `new`, `log`, `categorize`, `external`) |
| `namespace` | Espacio de nombres de la pÃ¡gina |
| `title` | TÃ­tulo de la pÃ¡gina modificada |
| `comment` | Comentario del editor sobre el cambio |
| `timestamp` | Momento en que ocurriÃ³ la modificaciÃ³n |
| `user` | Nombre del usuario que realizÃ³ la ediciÃ³n |
| `bot` | Indica si el cambio fue realizado por un bot |
| `server_url` | URL del servidor del wiki |
| `server_name` | Nombre del servidor |
| `server_script_path` | Ruta del script de MediaWiki |
| `wiki` | Identificador interno del wiki (por ejemplo, `enwiki`) |

### 9.4 Variables cuantitativas
Los atributos numÃ©ricos del stream incluyen:

- `id`
- `namespace`
- `timestamp`
- `partition`
- `offset`
- `revision IDs` (cuando estÃ¡n presentes)
- `old_len` y `new_len` (longitud del contenido antes y despuÃ©s)

Estas variables permiten realizar anÃ¡lisis estadÃ­sticos como:

- frecuencia de ediciones
- crecimiento de pÃ¡ginas
- actividad por periodos
- volumen de cambios por wiki

### 9.5 Variables cualitativas
Las variables categÃ³ricas incluyen:

- `type` â†’ tipo de cambio (`edit`, `new`, `log`, etc.)
- `user` â†’ nombre del usuario
- `title` â†’ pÃ¡gina afectada
- `wiki` â†’ proyecto especÃ­fico
- `server_name`
- `domain`
- `stream`
- `bot` (`true` / `false`)

Estas variables describen caracterÃ­sticas o etiquetas del evento en lugar de valores numÃ©ricos.

### 9.6 Texto no estructurado
Existen campos con texto libre o semi-estructurado, principalmente:

- `comment`
- `parsedcomment`

Estos campos contienen el mensaje que el editor escribiÃ³ al realizar el cambio, por ejemplo:

- explicaciÃ³n de la modificaciÃ³n
- referencias a secciones editadas
- descripciÃ³n del cambio

Este tipo de texto puede usarse para **anÃ¡lisis de lenguaje natural (NLP)** o **detecciÃ³n automÃ¡tica de vandalismo**.

### 9.7 Series temporales
El stream incluye varios atributos temporales que permiten analizar la actividad en el tiempo:

| Atributo | DescripciÃ³n |
|--------|-------------|
| `timestamp` | instante en que ocurriÃ³ la ediciÃ³n |
| `meta.dt` | fecha y hora de emisiÃ³n del evento |
| `meta.offset` | posiciÃ³n temporal dentro del stream |

Estas variables permiten construir:

- series de actividad por minuto u hora
- patrones diarios de ediciÃ³n
- picos de actividad ante eventos noticiosos
- anÃ¡lisis de comportamiento de usuarios

### 9.8 Consideraciones Ã©ticas
El procesamiento del stream **Wikimedia RecentChange** implica ciertas consideraciones Ã©ticas relacionadas con el uso responsable de los datos.

#### Privacidad y datos sensibles
Aunque los datos del stream son **pÃºblicos**, algunos atributos pueden contener informaciÃ³n potencialmente sensible, como:

- `user`: nombre del usuario que realizÃ³ la ediciÃ³n
- `comment`: mensaje escrito por el editor
- direcciones IP en el caso de usuarios no registrados

El uso de estos datos debe respetar las polÃ­ticas de privacidad de Wikimedia y evitar la identificaciÃ³n o exposiciÃ³n indebida de usuarios individuales.

#### Riesgos de sesgo
El anÃ¡lisis de los datos puede generar **interpretaciones sesgadas** si no se considera el contexto en el que se producen las ediciones. Por ejemplo:

- algunas comunidades de editores pueden estar mÃ¡s representadas que otras
- ciertos idiomas o wikis pueden tener mayor actividad
- los bots generan grandes volÃºmenes de cambios que pueden distorsionar mÃ©tricas de participaciÃ³n humana

Por ello, es importante diferenciar entre **ediciones humanas y automatizadas** al realizar anÃ¡lisis.

#### Uso responsable de los datos
Los datos del stream pueden utilizarse para aplicaciones como:

- monitoreo de actividad
- anÃ¡lisis acadÃ©mico
- investigaciÃ³n en ciencia de datos

Sin embargo, deben evitarse usos que puedan:

- acosar o rastrear usuarios individuales
- generar perfiles personales sin consentimiento
- manipular informaciÃ³n o crear herramientas de vigilancia indebida

#### Transparencia y reproducibilidad
Dado que los datos provienen de una plataforma abierta, se recomienda mantener prÃ¡cticas de **transparencia en el anÃ¡lisis**, documentando:

- los mÃ©todos utilizados
- los filtros aplicados
- las limitaciones del dataset

Esto contribuye a un uso Ã©tico y responsable de la informaciÃ³n disponible en el stream.

## 10. Etapa 5: AnÃ¡lisis de resultados

Sobre los datos persistidos en `recent_changes_raw` y enriquecidos con `data/static/wikis.csv`, ejecutamos **6 consultas analÃ­ticas complejas** que extraen inteligencia de negocio.

### 10.1 Consultas implementadas

| # | Consulta | Tipo de complejidad | Pregunta de negocio |
|---|---|---|---|
| Q1 | Top 20 wikis enriquecidas | JOIN + agregaciÃ³n con condicionales | Â¿DÃ³nde se concentra la actividad y quÃ© comunidad es? |
| Q2 | ConcentraciÃ³n del trÃ¡fico (Pareto) | Window function (running sum) | Â¿Es vÃ¡lida la regla 80/20 en el stream? |
| Q3 | Wikis bot-dominadas | `HAVING` sobre columna calculada | Â¿Hay wikis predominantemente automatizadas? |
| Q4 | Top 20 contribuyentes | AgregaciÃ³n + COUNT DISTINCT | Â¿QuiÃ©nes son los usuarios mÃ¡s activos? |
| Q5 | DistribuciÃ³n por familia de proyecto | JOIN + agregaciÃ³n multi-dim | Â¿CÃ³mo se distribuye la actividad entre Wikipedia, Commons, Wikidata, etc.? |
| Q6 | Cobertura del enriquecimiento | Flag derivado + window | Â¿QuÃ© % del stream estÃ¡ mapeado en el catÃ¡logo estÃ¡tico? |

### 10.2 CÃ³mo correrlo

```bash
# 1. Genera los CSVs ejecutando el job Spark
bash scripts/run_etapa5_queries.sh
# -> spark/output/etapa5/q{1..6}_.../part-*.csv

# 2. Genera las grÃ¡ficas a partir de los CSVs
python3 scripts/visualize_etapa5.py
# -> docs/charts_etapa5/q{1..6}_*.png
```

Job principal: [`spark/jobs/etapa5_queries.py`](spark/jobs/etapa5_queries.py)

### 10.3 Hallazgos clave

| Hallazgo | ImplicaciÃ³n |
|---|---|
| 80% del trÃ¡fico viene de < 20% de las wikis | Concentrar moderaciÃ³n/observabilidad en el top |
| ~60% del stream es automatizado (bots) | Filtrar `bot=true` para mÃ©tricas de actividad humana |
| Algunas wikis estÃ¡n â‰¥ 90% dominadas por bots | Lista de auditorÃ­a operativa |
| Top contribuyentes son ~todos bots de mantenimiento | Exclude-list para anÃ¡lisis humano |
| `commons` y `wikipedia` dominan; el stream NO es Wikipedia-cÃ©ntrico | Cualquier producto basado solo en Wikipedia pierde gran parte del stream |
| El catÃ¡logo estÃ¡tico (39 wikis) cubre > 90% del trÃ¡fico | Enriquecimiento de alto leverage |

Detalle completo, interpretaciÃ³n e impacto de negocio: [docs/etapa5_hallazgos.md](docs/etapa5_hallazgos.md).

### 10.4 Trazabilidad y ciclo de vida del dato

Para responder al requisito "evento crudo â†’ informaciÃ³n estratÃ©gica" del brief, [docs/etapa5_lifecycle.md](docs/etapa5_lifecycle.md) sigue un evento real (`c98b245c-d39f-462e-b46f-6c92fcca73c3`, edit de `Marta Kostiuk` en `plwiki` por `L.zurawski`) a travÃ©s de las 4 capas:

1. **Capa cruda** â€” JSON anidado en Kafka, schema-on-read, no queryable.
2. **Capa operativa** â€” fila tipada en Cassandra `recent_changes_raw`, indexable por `(event_date, wiki, event_hour)`, con `raw_json` preservado para auditorÃ­a.
3. **Capa analÃ­tica** â€” el evento individual aporta `+1` a un agregado por dimensiones enriquecidas (`changes_by_wiki_hour` + JOIN con catÃ¡logo estÃ¡tico).
4. **InformaciÃ³n estratÃ©gica** â€” su contribuciÃ³n alimenta las 6 consultas que dan decisiones accionables.

## 11. Documentos complementarios

| Documento | Contenido |
|---|---|
| [docs/arquitectura.md](docs/arquitectura.md) | Diagrama detallado y descripciÃ³n de cada componente |
| [docs/decisiones_cap.md](docs/decisiones_cap.md) | JustificaciÃ³n CAP (AP) |
| [docs/seguridad.md](docs/seguridad.md) | Modelo de control de accesos (roles + ACLs) |
| [docs/resiliencia.md](docs/resiliencia.md) | Mecanismos de resiliencia y escenarios reproducibles |
| [docs/etapa5_hallazgos.md](docs/etapa5_hallazgos.md) | Hallazgos e impacto de negocio de las 6 consultas |
| [docs/etapa5_consultas_integradas.md](docs/etapa5_consultas_integradas.md) | Documento uniforme con las 6 consultas oficiales y las 7 consultas complementarias no repetidas |
| [docs/etapa5_lifecycle.md](docs/etapa5_lifecycle.md) | Trazabilidad del dato evento por evento |
| [tests/load/README.md](tests/load/README.md) | DiseÃ±o de la prueba de carga y reporte de referencia |
| [data/static/README.md](data/static/README.md) | Diccionario del dataset estÃ¡tico de enriquecimiento |

