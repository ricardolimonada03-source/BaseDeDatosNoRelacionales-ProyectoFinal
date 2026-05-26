"""
Job analitico de la Etapa 4.

Pipeline de transformacion:
    1. Lee Cassandra wikimedia.recent_changes_raw
    2. Limpia: drop nulls, normaliza tipos y strings
    3. Dedupe por (event_date, wiki, source_event_id)
    4. Enriquece con dataset estatico data/static/wikis.csv (LEFT JOIN por wiki)
    5. Agrega por (event_date, wiki, event_hour, change_type) con metricas de negocio
    6. Escribe CSV (siempre) y opcionalmente Cassandra agregada

Variables de entorno:
    KEYSPACE                          (default: wikimedia)
    RAW_TABLE                         (default: recent_changes_raw)
    AGG_TABLE                         (default: changes_by_wiki_hour)
    CASSANDRA_HOST                    (default: cassandra)
    CASSANDRA_PORT                    (default: 9042)
    ANALYTICS_OUTPUT_PATH             (default: /tmp/spark-output/changes_by_wiki_hour)
    ANALYTICS_STATIC_WIKIS_PATH       (default: /opt/spark/static/wikis.csv)
    ANALYTICS_WRITE_TO_CASSANDRA      (default: false)
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


KEYSPACE = os.getenv("KEYSPACE", "wikimedia")
RAW_TABLE = os.getenv("RAW_TABLE", "recent_changes_raw")
AGG_TABLE = os.getenv("AGG_TABLE", "changes_by_wiki_hour")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
OUTPUT_PATH = os.getenv(
    "ANALYTICS_OUTPUT_PATH", "/tmp/spark-output/changes_by_wiki_hour"
)
STATIC_WIKIS_PATH = os.getenv(
    "ANALYTICS_STATIC_WIKIS_PATH", "/opt/spark/static/wikis.csv"
)
WRITE_TO_CASSANDRA = os.getenv("ANALYTICS_WRITE_TO_CASSANDRA", "false").lower() == "true"

WIKIS_SCHEMA = StructType(
    [
        StructField("wiki", StringType(), nullable=False),
        StructField("language", StringType(), nullable=True),
        StructField("language_name", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("project_family", StringType(), nullable=True),
        StructField("community_size_bucket", StringType(), nullable=True),
    ]
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("recent-changes-analytics")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=RAW_TABLE, keyspace=KEYSPACE)
        .load()
    )


def read_static_wikis(spark: SparkSession, path: str) -> DataFrame:
    """Lee el catalogo estatico de wikis. Si no existe, devuelve DataFrame vacio compatible."""
    try:
        return (
            spark.read.option("header", "true")
            .schema(WIKIS_SCHEMA)
            .csv(path)
        )
    except Exception as exc:
        print(
            f"[SPARK] WARN: no se pudo leer dataset estatico '{path}': {exc}. "
            "Continuando sin enriquecimiento."
        )
        return spark.createDataFrame([], WIKIS_SCHEMA)


def clean(raw_df: DataFrame) -> DataFrame:
    """
    Limpieza explicita:
    - drop filas sin timestamp_event o sin wiki (claves indispensables)
    - normaliza wiki y change_type (lower + trim)
    - cast bot a boolean robusto
    - cast event_hour a int 0-23
    - drop filas con campos requeridos restantes nulos
    """
    cleaned = (
        raw_df.dropna(subset=["timestamp_event", "wiki"])
        .withColumn("wiki", F.lower(F.trim(F.col("wiki"))))
        .withColumn(
            "change_type",
            F.lower(F.trim(F.coalesce(F.col("change_type"), F.lit("unknown")))),
        )
        .withColumn("bot", F.coalesce(F.col("bot").cast(BooleanType()), F.lit(False)))
        .withColumn(
            "event_hour",
            F.coalesce(F.col("event_hour").cast(IntegerType()), F.hour(F.col("timestamp_event"))),
        )
        .withColumn("event_date", F.coalesce(F.col("event_date"), F.to_date(F.col("timestamp_event"))))
        .filter((F.col("event_hour") >= 0) & (F.col("event_hour") <= 23))
        .filter(F.col("event_date").isNotNull())
    )
    return cleaned


def dedupe(cleaned_df: DataFrame) -> DataFrame:
    """
    Elimina duplicados a nivel de evento de origen.
    `source_event_id` es estable: viene de meta.id o de un sha1 del payload (consumer).
    """
    if "source_event_id" in cleaned_df.columns:
        return cleaned_df.dropDuplicates(["event_date", "wiki", "source_event_id"])
    return cleaned_df.dropDuplicates(
        ["event_date", "wiki", "event_hour", "timestamp_event", "title", "user_name"]
    )


def enrich(cleaned_df: DataFrame, wikis_df: DataFrame) -> DataFrame:
    """LEFT JOIN con catalogo estatico. Rellena 'unknown' donde no haya match."""
    joined = cleaned_df.join(wikis_df, on="wiki", how="left")
    return (
        joined.withColumn("language", F.coalesce(F.col("language"), F.lit("unknown")))
        .withColumn("language_name", F.coalesce(F.col("language_name"), F.lit("unknown")))
        .withColumn("country", F.coalesce(F.col("country"), F.lit("unknown")))
        .withColumn("project_family", F.coalesce(F.col("project_family"), F.lit("unknown")))
        .withColumn(
            "community_size_bucket",
            F.coalesce(F.col("community_size_bucket"), F.lit("unknown")),
        )
    )


def aggregate(enriched_df: DataFrame) -> DataFrame:
    """Agregacion por dimensiones temporales + dimensiones de negocio enriquecidas."""
    return (
        enriched_df.groupBy(
            "event_date",
            "wiki",
            "event_hour",
            "change_type",
            "language",
            "country",
            "project_family",
            "community_size_bucket",
        )
        .agg(
            F.count(F.lit(1)).cast(LongType()).alias("total_events"),
            F.sum(F.when(F.col("bot") == F.lit(True), F.lit(1)).otherwise(F.lit(0)))
            .cast(LongType())
            .alias("bot_events"),
            F.countDistinct(F.col("user_name")).cast(LongType()).alias("unique_users"),
            F.countDistinct(F.col("title")).cast(LongType()).alias("unique_pages"),
        )
        .orderBy("event_date", "wiki", "event_hour", "change_type")
    )


def main() -> int:
    spark = create_spark_session()
    try:
        raw_df = read_raw(spark)
        raw_count = raw_df.count()

        cleaned_df = clean(raw_df)
        cleaned_count = cleaned_df.count()

        deduped_df = dedupe(cleaned_df)
        deduped_count = deduped_df.count()

        wikis_df = read_static_wikis(spark, STATIC_WIKIS_PATH)
        enriched_df = enrich(deduped_df, wikis_df)

        aggregated_df = aggregate(enriched_df)
        aggregated_count = aggregated_df.count()

        (
            aggregated_df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(OUTPUT_PATH)
        )

        if WRITE_TO_CASSANDRA:
            cassandra_cols = [
                "event_date",
                "wiki",
                "event_hour",
                "change_type",
                "total_events",
                "bot_events",
            ]
            (
                aggregated_df.select(*cassandra_cols)
                .write.format("org.apache.spark.sql.cassandra")
                .options(table=AGG_TABLE, keyspace=KEYSPACE)
                .mode("append")
                .save()
            )
            print(f"[SPARK] Agregados insertados en {KEYSPACE}.{AGG_TABLE}")

        print(f"[SPARK] Filas leidas desde Cassandra:    {raw_count}")
        print(f"[SPARK] Filas tras limpieza:              {cleaned_count}")
        print(f"[SPARK] Filas tras dedupe:                {deduped_count}")
        print(f"[SPARK] Duplicados eliminados:            {cleaned_count - deduped_count}")
        print(f"[SPARK] Filas agregadas finales:          {aggregated_count}")
        print(f"[SPARK] Salida CSV en:                    {OUTPUT_PATH}")
        print(f"[SPARK] Dataset estatico:                 {STATIC_WIKIS_PATH}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
