# Runbook — Cómo correr el proyecto end-to-end

Pasos para que cualquier miembro del equipo (o el profe) levante el pipeline completo en su máquina y reproduzca las 5 etapas. Probado en macOS con Docker Desktop.

> Tiempo total estimado: **~15 minutos la primera vez**, **~3 minutos** en runs posteriores (los datos en Cassandra persisten).

---

## 0. Pre-requisitos (una sola vez en tu máquina)

| Software | Cómo instalar | Verificación |
|---|---|---|
| **Docker Desktop** | https://www.docker.com/products/docker-desktop | `docker --version` |
| **Git** | Viene en macOS o `brew install git` | `git --version` |
| **Python 3** | Viene en macOS o `brew install python` | `python3 --version` |

> **Importante**: Docker Desktop tiene que estar **abierto y corriendo** (icono de ballena en la barra de menú estable, sin animación) antes de cualquier `docker compose ...`. Si no, todos los comandos fallan con `Cannot connect to the Docker daemon`.

---

## 1. Clonar el repo (primera vez)

```bash
cd ~
git clone git@github.com:bdnr-proyecto-final/ProyectoFinal-Eqp1.git
cd ProyectoFinal-Eqp1
```

(O si ya lo tienes clonado: `cd /ruta/donde/lo/tengas && git pull origin main`)

---

## 2. Instalar dependencias de Python (primera vez)

```bash
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib
```

Verifica que quedó:

```bash
python3 -c "import cassandra, kafka, sseclient, requests, pandas, matplotlib; print('OK paquetes cargados')"
```

Debes ver `OK paquetes cargados`.

> **Si usas Anaconda y aparece `(base)` al inicio de tu prompt**: usa **siempre** `python3 -m pip` (NO `pip` suelto). Si no, los paquetes se instalan en un Python distinto al que corres.
>
> **Si tu `pip` está viejo y se queja de que no encuentra `cassandra-driver==3.30.0`**: el comando de arriba ya está sin pin de versión, así que se instala la última compatible y debería funcionar.

---

## 3. Levantar la infraestructura

```bash
docker compose up -d
```

Espera ~60 segundos a que Cassandra arranque y el schema se aplique. Verifica que cassandra-init terminó OK:

```bash
docker logs cassandra-init
# Debe terminar con: "Schema aplicado correctamente."
```

Confirma que los 5 contenedores están UP:

```bash
docker ps
```

Debes ver:
- `kafka` — UP
- `zookeeper` — UP
- `cassandra` — UP (healthy)
- `spark-master-proyectoFinal` — UP
- `spark-worker-proyectoFinal` — UP

---

## 4. Arrancar producer y consumer

**Necesitas 2 terminales abiertas en VS Code** (botón `+` en el panel de terminales, o `Cmd+Shift+ñ`).

### Terminal 1 — Consumer (Kafka → Cassandra)

```bash
cd ~/ProyectoFinal-Eqp1
python3 consumers/kafka_to_cassandra.py
```

Verás:
```
[CASSANDRA] Conectado a keyspace 'wikimedia'.
[CASSANDRA] Prepared statement listo para wikimedia.recent_changes_raw.
[KAFKA] Escuchando topic 'wikimedia.recentchange'...
[PIPELINE] Iniciando consumo de Kafka hacia Cassandra...
[KAFKA] Esperando nuevos mensajes...
```

**Déjala corriendo.** No la cierres.

### Terminal 2 — Producer (Wikimedia → Kafka)

```bash
cd ~/ProyectoFinal-Eqp1
python3 consumers/wikimedia_to_kafka.py
```

Verás:
```
[KAFKA] Productor conectado a localhost:9092 (protocol=PLAINTEXT)
[PIPELINE] Iniciando flujo Wikimedia → Kafka...
[WIKIMEDIA] Conectando a https://stream.wikimedia.org/v2/stream/recentchange ...
[SEND] 100 eventos enviados | ultimo_title='...'
[SEND] 200 eventos enviados | ...
```

**Déjala corriendo.** No la cierres.

A los pocos segundos, en la Terminal 1 vas a empezar a ver:
```
[PROGRESS] 100 eventos insertados en wikimedia.recent_changes_raw.
[PROGRESS] 200 eventos insertados en wikimedia.recent_changes_raw.
```

El pipeline está vivo.

---

## 5. Verificar que los datos están en Cassandra

Abre una **Terminal 3** (las otras dos se quedan corriendo):

```bash
cd ~/ProyectoFinal-Eqp1

# Conteo total
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wikimedia.recent_changes_raw;"

# Muestra de 5 filas
docker exec cassandra cqlsh -e "SELECT event_date, wiki, event_hour, title FROM wikimedia.recent_changes_raw LIMIT 5;"
```

Deberías ver el conteo creciendo si lo corres varias veces.

---

## 6. (Opcional) Correr las 6 consultas analíticas de Etapa 5

Espera a tener al menos ~3,000-5,000 eventos en Cassandra (3 minutos del producer corriendo suele bastar).

En Terminal 3:

```bash
bash scripts/run_etapa5_queries.sh
```

Tarda ~30-60 segundos. Verás los resultados de las 6 consultas en pantalla:

```
Q1 - Top 20 wikis por volumen (enriquecidas)
+------------+--------+...
| commonswiki|     mul|...
| zhwikisource | ...
...

Q2 - Concentracion del trafico (Pareto)
...

Q3 - Wikis bot-dominadas
...
```

Los CSVs quedan en `spark/output/etapa5/q*/part-*.csv`.

---

## 7. (Opcional) Regenerar las gráficas con los datos nuevos

```bash
python3 scripts/visualize_etapa5.py
```

Las 6 PNG quedan en `docs/charts_etapa5/`. Para verlas:

```bash
open docs/charts_etapa5/
```

(Abre Finder en esa carpeta — doble click a cualquier PNG.)

---

## 8. Apagar todo limpio

### a) Detener producer y consumer

- En Terminal 1: `Ctrl + C`
- En Terminal 2: `Ctrl + C`

### b) Detener contenedores Docker

En cualquier terminal:

```bash
cd ~/ProyectoFinal-Eqp1
docker compose down
```

(Tarda ~15 segundos.)

### c) (Opcional) Cerrar Docker Desktop

Si no vas a usar Docker hoy: icono de la ballena en la barra de menú → **Quit Docker Desktop**. Recupera ~3 GB de RAM.

---

## Lo que se queda guardado (NO se borra)

| Cosa | Dónde vive | Cuándo se borra |
|---|---|---|
| Datos en Cassandra | Volumen Docker `cassandra_data` | Solo si haces `docker compose down -v` |
| Gráficas y CSVs generados | `spark/output/`, `docs/charts_etapa5/` | Solo si los borras a mano |
| Código y commits | Git/GitHub | Nunca (a menos que reescribas historia) |

Mañana puedes hacer `docker compose up -d` y Cassandra arranca con TODOS los eventos que ya habías ingestado. No tienes que volver a producir/consumir desde cero.

Si quieres empezar limpio en algún momento:
```bash
docker compose down -v   # el -v borra los datos también
```

---

## Troubleshooting frecuente

### `Cannot connect to the Docker daemon`
Docker Desktop no está corriendo. Abre la app y espera a que el icono de la ballena esté estable.

### `ModuleNotFoundError: No module named 'cassandra'` (o `kafka`, `requests`, etc.)
Tu Python no tiene los paquetes instalados, o los instalaste en un Python distinto.
Vuelve a correr:
```bash
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib
python3 -c "import cassandra; print('OK')"
```

### `NoBrokersAvailable` en el producer/consumer
Kafka no está listo. Espera 30 segundos más después de `docker compose up -d` y reintenta. Si persiste:
```bash
docker compose restart kafka
```

### El producer dice "Stream interrumpido"
Es normal: el SSE de Wikimedia cierra conexiones periódicamente. El producer reconecta solo con backoff exponencial (1s → 30s). Si dejaste el pipeline corriendo mucho tiempo, esto va a pasar; no es error.

### `bash scripts/run_etapa5_queries.sh` falla con "package not found"
Spark está descargando el conector de Cassandra por primera vez (~50MB). Espera y reintenta. El próximo run usa caché y es instantáneo.

### Los logs de producer/consumer no aparecen en tiempo real
Si los corres con `python3` (sin `-u`), Python bufferiza stdout. Para forzar output inmediato:
```bash
python3 -u consumers/wikimedia_to_kafka.py
```

---

## Resumen visual

```
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  Terminal 1                Terminal 2         Terminal 3 │
│  ┌────────────┐           ┌────────────┐    ┌──────────┐ │
│  │ consumer   │           │ producer   │    │ queries  │ │
│  │ (~ Cassan- │ <─eventos │ (Wikimedia │    │ (Spark   │ │
│  │  dra)      │           │  → Kafka)  │    │  Etapa 5)│ │
│  └────────────┘           └────────────┘    └──────────┘ │
│        ↑                        ↑                 ↑      │
│        │                        │                 │      │
│        └────── docker compose up -d ──────────────┘      │
│                                                          │
│   Cassandra + Kafka + Zookeeper + Spark master + worker  │
└──────────────────────────────────────────────────────────┘
```

---

## Comandos de un vistazo (cheat-sheet)

```bash
# Setup (primera vez)
git clone git@github.com:bdnr-proyecto-final/ProyectoFinal-Eqp1.git
cd ProyectoFinal-Eqp1
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib

# Cada vez que quieras correrlo
docker compose up -d                              # terminal cualquiera (espera 60s)
python3 consumers/kafka_to_cassandra.py           # terminal 1
python3 consumers/wikimedia_to_kafka.py           # terminal 2
bash scripts/run_etapa5_queries.sh                # terminal 3 (tras unos minutos)
python3 scripts/visualize_etapa5.py               # terminal 3
open docs/charts_etapa5/                          # ver gráficas

# Cuando termines
# Ctrl+C en terminal 1 y 2
docker compose down
```

Si algo falla, revisa la sección de **Troubleshooting** arriba o pregunta en el grupo del equipo.
