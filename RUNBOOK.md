# Cómo correr el proyecto

Esta guía es para que cualquier integrante del equipo (o quien quiera probarlo) pueda levantar el proyecto desde cero en su computadora. La probamos en macOS con Docker Desktop.

La primera vez toma como 15 minutos. Las siguientes veces toma como 3 minutos porque los datos en Cassandra se quedan guardados entre ejecuciones.

---

## 0. Lo que necesitas tener instalado

Estas cosas se instalan una sola vez:

- **Docker Desktop**: se descarga de https://www.docker.com/products/docker-desktop. Para revisar si ya lo tienes corre `docker --version` en la terminal.
- **Git**: viene en macOS por defecto, o se instala con `brew install git`. Verifica con `git --version`.
- **Python 3**: viene en macOS, o se instala con `brew install python`. Verifica con `python3 --version`.

Antes de correr cualquier cosa con Docker, **abre Docker Desktop** y espera a que el ícono de la ballena en la barra de menú deje de animarse. Si no lo abres, todos los comandos `docker ...` fallan con un mensaje que dice "Cannot connect to the Docker daemon".

---

## 1. Clonar el repositorio

Esto también es una sola vez:

```bash
cd ~
git clone git@github.com:bdnr-proyecto-final/ProyectoFinal-Eqp1.git
cd ProyectoFinal-Eqp1
```

Si ya tienes el repo clonado de antes, en vez de hacer `clone` corre:

```bash
cd /ruta/donde/lo/tengas/ProyectoFinal-Eqp1
git pull origin main
```

---

## 2. Instalar las librerías de Python

Para que los scripts puedan correr necesitamos varias librerías. La primera vez en cada computadora hay que instalarlas:

```bash
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib
```

Para confirmar que se instalaron bien:

```bash
python3 -c "import cassandra, kafka, sseclient, requests, pandas, matplotlib; print('OK paquetes cargados')"
```

Si imprime `OK paquetes cargados`, ya quedó.

**Nota para quienes usan Anaconda:** si al iniciar tu terminal ves `(base)` al principio del prompt, estás en el ambiente base de conda. En ese caso usa siempre `python3 -m pip` y NO solo `pip`. Si usas `pip` suelto, los paquetes se pueden instalar en un Python distinto al que ejecuta los scripts y luego marca error de que no encuentra los módulos.

---

## 3. Levantar la infraestructura

Con Docker Desktop ya abierto:

```bash
docker compose up -d
```

Esto arranca todos los contenedores. Espera como un minuto a que Cassandra termine de iniciar y se cree el esquema. Para confirmar que el setup terminó bien:

```bash
docker logs cassandra-init
```

Al final del log debe aparecer la línea "Schema aplicado correctamente."

Para revisar que los 5 contenedores estén corriendo:

```bash
docker ps
```

Deberías ver:

- `kafka`
- `zookeeper`
- `cassandra` (con la palabra "healthy" entre paréntesis)
- `spark-master-proyectoFinal`
- `spark-worker-proyectoFinal`

---

## 4. Arrancar el productor y el consumidor

Necesitas **2 terminales abiertas** al mismo tiempo. En VS Code se abren con el botón de "+" en la parte de arriba del panel de terminales, o con el atajo `Cmd + Shift + ñ`.

### Terminal 1: Consumidor (Kafka → Cassandra)

```bash
cd ~/ProyectoFinal-Eqp1
python3 consumers/kafka_to_cassandra.py
```

Cuando arranca bien vas a ver mensajes así:

```
[CASSANDRA] Conectado a keyspace 'wikimedia'.
[CASSANDRA] Prepared statement listo para wikimedia.recent_changes_raw.
[KAFKA] Escuchando topic 'wikimedia.recentchange'...
[PIPELINE] Iniciando consumo de Kafka hacia Cassandra...
[KAFKA] Esperando nuevos mensajes...
```

Deja esta terminal abierta y corriendo. Si la cierras, se detiene el consumidor.

### Terminal 2: Productor (Wikimedia → Kafka)

```bash
cd ~/ProyectoFinal-Eqp1
python3 consumers/wikimedia_to_kafka.py
```

Cuando arranca bien:

```
[KAFKA] Productor conectado a localhost:9092 (protocol=PLAINTEXT)
[PIPELINE] Iniciando flujo Wikimedia → Kafka...
[WIKIMEDIA] Conectando a https://stream.wikimedia.org/v2/stream/recentchange ...
[SEND] 100 eventos enviados | ultimo_title='...'
[SEND] 200 eventos enviados | ...
```

Esta terminal también se queda abierta.

A los pocos segundos vas a ver en la Terminal 1 cómo el consumidor empieza a guardar los eventos:

```
[PROGRESS] 100 eventos insertados en wikimedia.recent_changes_raw.
[PROGRESS] 200 eventos insertados en wikimedia.recent_changes_raw.
```

Ya está corriendo el pipeline.

---

## 5. Revisar que los datos sí están llegando a Cassandra

Abre una **tercera terminal** (las otras dos siguen corriendo):

```bash
cd ~/ProyectoFinal-Eqp1

# Cuenta cuántos eventos hay guardados
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM wikimedia.recent_changes_raw;"

# Saca una muestra de 5 filas
docker exec cassandra cqlsh -e "SELECT event_date, wiki, event_hour, title FROM wikimedia.recent_changes_raw LIMIT 5;"
```

Si corres el `COUNT` varias veces, deberías ver que el número va creciendo.

---

## 6. Correr las consultas de la Etapa 5 (opcional)

Una vez que tengamos al menos unos 3,000 a 5,000 eventos guardados (usualmente con tener el productor corriendo unos 3 minutos basta), podemos correr las 6 consultas analíticas.

En la Terminal 3:

```bash
bash scripts/run_etapa5_queries.sh
```

Esto tarda entre 30 y 60 segundos. Vas a ver los resultados de las 6 consultas en pantalla:

```
Q1 - Top 20 wikis por volumen (enriquecidas)
...

Q2 - Concentracion del trafico (Pareto)
...

Q3 - Wikis bot-dominadas
...
```

Los archivos CSV con los resultados quedan guardados en `spark/output/etapa5/q*/part-*.csv`.

---

## 7. Generar las gráficas (opcional)

```bash
python3 scripts/visualize_etapa5.py
```

Las 6 gráficas en formato PNG quedan en `docs/charts_etapa5/`. Para abrir la carpeta en Finder y verlas:

```bash
open docs/charts_etapa5/
```

Doble click a cualquier PNG y se abre en Vista Previa.

---

## 8. Apagar todo cuando termines

Para apagar limpio, hacemos los pasos en orden inverso al arranque.

### 8.1 Detener el productor y el consumidor

En las terminales 1 y 2, presiona:

```
Ctrl + C
```

(O sea la tecla Control + la letra C, no Cmd.)

### 8.2 Detener los contenedores de Docker

En cualquier terminal:

```bash
cd ~/ProyectoFinal-Eqp1
docker compose down
```

Tarda como 15 segundos.

### 8.3 (Opcional) Cerrar Docker Desktop

Si ya no vas a usar Docker hoy, dale click al ícono de la ballena en la barra de menú y selecciona "Quit Docker Desktop". Esto libera como 3 GB de RAM.

---

## Qué se guarda y qué no entre ejecuciones

- **Los datos en Cassandra se quedan guardados** entre ejecuciones, gracias al volumen `cassandra_data` de Docker. La próxima vez que hagas `docker compose up -d`, los eventos que ya habías producido siguen ahí.
- **Las gráficas y CSVs también se quedan**, en sus carpetas dentro del repo.
- **El código y los commits están en Git**, así que tampoco se pierden.

Si en algún momento quieres empezar desde cero y borrar los datos:

```bash
docker compose down -v
```

El `-v` también borra los volúmenes. **No lo uses si quieres conservar los datos** que tienes.

---

## Problemas que pueden salir y cómo resolverlos

### Error: "Cannot connect to the Docker daemon"

Significa que Docker Desktop no está corriendo. Ábrelo y espera a que la ballena de la barra de menú deje de animarse.

### Error: "ModuleNotFoundError: No module named 'cassandra'" (o kafka, requests, etc.)

Falta instalar las librerías de Python, o las instalaste en otro Python distinto al que estás usando. Vuelve a correr:

```bash
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib
python3 -c "import cassandra; print('OK')"
```

### Error: "NoBrokersAvailable"

Kafka todavía no está listo. Espera 30 segundos más después de `docker compose up -d` y vuelve a intentar. Si sigue, reinicia el contenedor:

```bash
docker compose restart kafka
```

### El productor dice "Stream interrumpido"

Es normal. La conexión SSE de Wikimedia se cierra cada cierto tiempo. El productor reconecta solo y vuelve a leer eventos. No es error.

### El script de las consultas (`run_etapa5_queries.sh`) tarda mucho la primera vez

Es porque Spark está descargando por primera vez el conector de Cassandra (como 50 MB). La siguiente ejecución es casi instantánea porque queda en caché.

---

## Resumen rápido (cheat-sheet)

Si ya sabes lo que estás haciendo y solo necesitas los comandos:

```bash
# Setup (solo la primera vez)
git clone git@github.com:bdnr-proyecto-final/ProyectoFinal-Eqp1.git
cd ProyectoFinal-Eqp1
python3 -m pip install --user kafka-python cassandra-driver sseclient-py requests pandas matplotlib

# Cada vez que quieras correrlo
docker compose up -d                              # cualquier terminal (esperar 60s)
python3 consumers/kafka_to_cassandra.py           # terminal 1
python3 consumers/wikimedia_to_kafka.py           # terminal 2
bash scripts/run_etapa5_queries.sh                # terminal 3 (después de varios minutos)
python3 scripts/visualize_etapa5.py               # terminal 3
open docs/charts_etapa5/                          # ver gráficas

# Cuando termines
# Ctrl+C en terminal 1 y 2
docker compose down
```

Si algo no funciona, revisa la sección de problemas más arriba, o pregunta en el grupo.
