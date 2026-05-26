-- Schema para cluster Cassandra multinodo (Etapa 2 - replicacion + sharding).
--
-- Diferencias contra schema.cql (mononodo):
--   - keyspace usa NetworkTopologyStrategy con replication_factor=3 sobre dc1
--   - el resto del schema es identico para no romper consumers ni Spark job
--
-- Sharding: Cassandra reparte automaticamente las particiones (vnodes) entre
-- los 3 nodos. Cada particion se determina por el hash de la PK; en
-- recent_changes_raw esto es ((event_date, wiki, event_hour)).
--
-- Replicacion: cada particion vive en 3 nodos. Con CL=QUORUM, las escrituras
-- requieren 2 acks y el sistema sobrevive a la caida de 1 nodo.

CREATE KEYSPACE IF NOT EXISTS wikimedia
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

USE wikimedia;

CREATE TABLE IF NOT EXISTS recent_changes (
    id UUID PRIMARY KEY,
    title TEXT,
    user_name TEXT,
    type TEXT,
    wiki TEXT,
    bot BOOLEAN,
    server_name TEXT,
    comment TEXT,
    timestamp_event TIMESTAMP,
    parsed_date TEXT
);

CREATE TABLE IF NOT EXISTS recent_changes_raw (
    event_date DATE,
    wiki TEXT,
    event_hour INT,
    timestamp_event TIMESTAMP,
    source_event_id TEXT,
    title TEXT,
    user_name TEXT,
    change_type TEXT,
    bot BOOLEAN,
    server_name TEXT,
    comment TEXT,
    raw_json TEXT,
    PRIMARY KEY ((event_date, wiki, event_hour), timestamp_event, source_event_id)
) WITH CLUSTERING ORDER BY (timestamp_event DESC, source_event_id ASC);

CREATE TABLE IF NOT EXISTS changes_by_wiki_hour (
    event_date DATE,
    wiki TEXT,
    event_hour INT,
    change_type TEXT,
    total_events BIGINT,
    bot_events BIGINT,
    PRIMARY KEY ((event_date, wiki), event_hour, change_type)
) WITH CLUSTERING ORDER BY (event_hour ASC, change_type ASC);
