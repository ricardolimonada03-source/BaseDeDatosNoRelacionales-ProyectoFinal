-- Setup de control de accesos en Cassandra (Etapa 2).
--
-- Este archivo se ejecuta cuando Cassandra arranca con
-- authenticator: PasswordAuthenticator
-- authorizer:    CassandraAuthorizer
-- (configurado en docker-compose.secure.yml).
--
-- Cassandra crea por defecto el usuario 'cassandra' con password 'cassandra'.
-- Aqui:
-- 1. Cambiamos su password (no se deja el default).
-- 2. Creamos roles con least privilege para productor/consumer y para analista.
-- 3. Bloqueamos LOGIN del superusuario para forzar el uso de roles dedicados.

-- 1. Rotar password del superusuario.
ALTER ROLE cassandra WITH PASSWORD = 'cassandra-rotada-en-bootstrap';

-- 2. Roles operativos.
CREATE ROLE IF NOT EXISTS pipeline_writer WITH PASSWORD = 'pipeline_writer_pw' AND LOGIN = true;
CREATE ROLE IF NOT EXISTS analytics_reader WITH PASSWORD = 'analytics_reader_pw' AND LOGIN = true;
CREATE ROLE IF NOT EXISTS analytics_writer WITH PASSWORD = 'analytics_writer_pw' AND LOGIN = true;

-- 3. Permisos minimos por rol.
-- Productor/consumer del pipeline: solo INSERT/SELECT en raw.
GRANT SELECT ON KEYSPACE wikimedia TO pipeline_writer;
GRANT MODIFY ON wikimedia.recent_changes_raw TO pipeline_writer;

-- Analista (lectura): solo SELECT en todo el keyspace.
GRANT SELECT ON KEYSPACE wikimedia TO analytics_reader;

-- Spark agregando: SELECT en raw + MODIFY en agregada.
GRANT SELECT ON wikimedia.recent_changes_raw TO analytics_writer;
GRANT MODIFY ON wikimedia.changes_by_wiki_hour TO analytics_writer;

-- 4. Endurecer superusuario: opcional, dejar comentado para no perder acceso de emergencia.
-- ALTER ROLE cassandra WITH LOGIN = false;
