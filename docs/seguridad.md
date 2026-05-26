# Control de accesos (Etapa 2)

El brief exige "Implementación de Control de Accesos: se deben configurar los permisos y protocolos de seguridad para restringir el acceso a los datos." Para no romper el flujo académico mononodo, el control de accesos se entrega como **stack alternativo opt-in**: se levanta con `docker-compose.secure.yml`.

## Resumen del modelo

| Capa | Protocolo | Mecanismo | Roles |
|---|---|---|---|
| Cassandra | nativo CQL sobre TCP/9042 | `PasswordAuthenticator` + `CassandraAuthorizer` | `pipeline_writer`, `analytics_reader`, `analytics_writer` |
| Kafka | `SASL_PLAINTEXT` sobre TCP/9093 | SASL/PLAIN + `AclAuthorizer` | `admin` (super-user), `pipeline_writer`, `analytics_reader` |

Principio: **least privilege**. El productor/consumer del pipeline no puede leer ni escribir en tablas analíticas; el analista no puede insertar nada.

## Cassandra

### Configuración

`docker-compose.secure.yml` arranca Cassandra parcheando `/etc/cassandra/cassandra.yaml`:

```yaml
authenticator: PasswordAuthenticator
authorizer:    CassandraAuthorizer
```

### Roles aplicados

`cassandra/secure_setup.cql` se ejecuta automáticamente desde `cassandra-init`:

| Rol | Login | Permisos |
|---|---|---|
| `cassandra` (default) | sí | password rotada en bootstrap; opcional `LOGIN=false` para hardening |
| `pipeline_writer` | sí | `SELECT` en keyspace, `MODIFY` en `recent_changes_raw` |
| `analytics_reader` | sí | `SELECT` en todo el keyspace |
| `analytics_writer` | sí | `SELECT` en raw, `MODIFY` en `changes_by_wiki_hour` |

Las credenciales se inyectan al consumer mediante variables de entorno (`CASSANDRA_USERNAME`, `CASSANDRA_PASSWORD`) — soportado por `consumers/kafka_to_cassandra.py:cassandra_auth_provider()`.

### Verificación manual

```bash
# Como pipeline_writer puedo escribir en raw...
docker exec cassandra cqlsh -u pipeline_writer -p pipeline_writer_pw \
  -e "INSERT INTO wikimedia.recent_changes_raw (event_date, wiki, event_hour, timestamp_event, source_event_id, title) VALUES ('2026-04-12', 'enwiki', 14, toTimestamp(now()), 'test', 'Test');"

# ...pero NO en la tabla analitica
docker exec cassandra cqlsh -u pipeline_writer -p pipeline_writer_pw \
  -e "INSERT INTO wikimedia.changes_by_wiki_hour (event_date, wiki, event_hour, change_type, total_events, bot_events) VALUES ('2026-04-12', 'enwiki', 14, 'edit', 1, 0);"
# -> Unauthorized: User pipeline_writer has no MODIFY permission

# Como analytics_reader solo leo
docker exec cassandra cqlsh -u analytics_reader -p analytics_reader_pw \
  -e "SELECT COUNT(*) FROM wikimedia.recent_changes_raw;"
```

## Kafka

### Configuración

- Listener externo: `SASL_PLAINTEXT://localhost:9093`.
- JAAS server-side: [`kafka/jaas/kafka_server_jaas.conf`](../kafka/jaas/kafka_server_jaas.conf) define `admin`, `pipeline_writer` y `analytics_reader`.
- Authorizer: `kafka.security.authorizer.AclAuthorizer`. `allow.everyone.if.no.acl.found=false` (deny by default).
- Super-user: `User:admin` (única identidad sin restricción de ACL).

### ACLs aplicadas

`kafka/secure_acls.sh` se ejecuta desde el contenedor `kafka-acl-init` y otorga:

| Principal | Topic / Group | Operaciones |
|---|---|---|
| `User:pipeline_writer` | topic `wikimedia.recentchange` | `WRITE`, `READ`, `DESCRIBE`, `CREATE` |
| `User:pipeline_writer` | group `wikimedia-cassandra-consumer` | `READ`, `DESCRIBE` |
| `User:analytics_reader` | topic `wikimedia.recentchange` | `READ`, `DESCRIBE` |
| `User:analytics_reader` | group `wikimedia-analytics-reader` | `READ`, `DESCRIBE` |

### Verificación manual

```bash
# Listar ACLs activas
docker exec kafka kafka-acls \
  --bootstrap-server localhost:9093 \
  --command-config /etc/kafka/admin_client.properties \
  --list

# Probar que un usuario sin ACL es rechazado
docker exec kafka kafka-console-producer \
  --bootstrap-server localhost:9093 \
  --topic wikimedia.recentchange \
  --producer.config <(echo "
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='intruso' password='nopw';
")
# -> SaslAuthenticationException
```

## Cómo correr el stack seguro

```bash
# Levantar
docker compose -f docker-compose.secure.yml up -d

# Esperar a que cassandra-init y kafka-acl-init terminen
docker logs cassandra-init
docker logs kafka-acl-init

# Producer/consumer con credenciales (en el host)
export KAFKA_PORT=9093
export KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
export KAFKA_SASL_USERNAME=pipeline_writer
export KAFKA_SASL_PASSWORD=pipeline_writer_pw
export CASSANDRA_USERNAME=pipeline_writer
export CASSANDRA_PASSWORD=pipeline_writer_pw

python3 consumers/wikimedia_to_kafka.py
python3 consumers/kafka_to_cassandra.py
```

## Por qué se entrega como stack alternativo

El compose por defecto (`docker-compose.yml`) corre sin auth para que el flujo académico arranque en un solo `docker compose up` sin parametrizar credenciales. El compose seguro (`docker-compose.secure.yml`) demuestra el cumplimiento del requisito del brief y permite al equipo enseñar deny-by-default + roles + ACLs en la presentación.

## Limitaciones

- `SASL_PLAINTEXT` envía credenciales en claro: en producción se usaría `SASL_SSL` con certificados. El brief no exige TLS y agregar PKI completa quedaría fuera del alcance académico.
- Las contraseñas viven en el repo por reproducibilidad académica. En producción irían en un secret manager (Vault, AWS Secrets Manager, etc.).
