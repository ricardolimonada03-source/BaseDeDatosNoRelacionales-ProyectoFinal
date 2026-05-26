"""
Job Spark - Etapa 5: Analisis de resultados.

Ejecuta seis consultas analiticas complejas sobre `recent_changes_raw`
cruzando con el catalogo estatico `data/static/wikis.csv`. Cada consulta
guarda su resultado en CSV bajo `spark/output/etapa5/<qN>/` y imprime un
resumen tabular a stdout.

Consultas implementadas:

    Q1 - Top 20 wikis por volumen, enriquecidas con metadata de negocio
    Q2 - Concentracion del trafico (Pareto: % acumulado vs % de wikis)
    Q3 - Wikis bot-dominadas (>= 90% bots con minimo de eventos)
    Q4 - Top 20 contribuyentes (usuarios mas activos del periodo)
    Q5 - Distribucion por familia de proyecto Wikimedia
    Q6 - Cobertura del enriquecimiento estatico

Las consultas son "complejas" segun el brief porque usan:
- JOIN entre dato operativo y dataset estatico
- window functions (running sum para Pareto)
- HAVING sobre columnas calculadas
- COUNT DISTINCT, agregaciones condicionales

Variables de entorno (opcionales):
    KEYSPACE                       (default: wikimedia)
    RAW_TABLE                      (default: recent_changes_raw)
    CASSANDRA_HOST                 (default: cassandra)
    CASSANDRA_PORT                 (default: 9042)
    ANALYTICS_STATIC_WIKIS_PATH    (default: /opt/spark/static/wikis.csv)
    ETAPA5_OUTPUT_PATH             (default: /tmp/spark-output/etapa5)
    ETAPA5_MIN_EVENTS_BOT_ANOMALY  (default: 50)
    ETAPA5_BOT_THRESHOLD_PCT       (default: 90.0)
"""

from __future__ import annotations

import os
import sys
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


KEYSPACE = os.getenv("KEYSPACE", "wikimedia")
RAW_TABLE = os.getenv("RAW_TABLE", "recent_changes_raw")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
STATIC_WIKIS_PATH = os.getenv(
    "ANALYTICS_STATIC_WIKIS_PATH", "/opt/spark/static/wikis.csv"
)
OUTPUT_BASE = os.getenv("ETAPA5_OUTPUT_PATH", "/tmp/spark-output/etapa5")
MIN_EVENTS_BOT_ANOMALY = int(os.getenv("ETAPA5_MIN_EVENTS_BOT_ANOMALY", "50"))
BOT_THRESHOLD_PCT = float(os.getenv("ETAPA5_BOT_THRESHOLD_PCT", "90.0"))


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


# -------------------------- Spark/Cassandra setup ---------------------------


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("etapa5-queries")
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


def read_static(spark: SparkSession) -> DataFrame:
    try:
        return (
            spark.read.option("header", "true")
            .schema(WIKIS_SCHEMA)
            .csv(STATIC_WIKIS_PATH)
        )
    except Exception as exc:
        print(f"[WARN] no se pudo leer {STATIC_WIKIS_PATH}: {exc}")
        return spark.createDataFrame([], WIKIS_SCHEMA)


def write_csv(df: DataFrame, name: str) -> str:
    """Escribe `df` en `OUTPUT_BASE/name/` como un solo CSV con header."""
    path = os.path.join(OUTPUT_BASE, name)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(path)
    )
    return path


def print_section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def show_summary(df: DataFrame, n: int = 10) -> None:
    df.show(n, truncate=80)


# ------------------------------- consultas ---------------------------------


def q1_top_wikis_enriquecidas(raw: DataFrame, wikis: DataFrame) -> DataFrame:
    """Q1 - Top 20 wikis por volumen, con metadata de negocio.

    JOIN: enriquece cada wiki con idioma, pais y project_family.
    AGG : volumen, bots, usuarios unicos, paginas unicas, % bots.
    """
    enriched = raw.join(wikis, on="wiki", how="left")
    return (
        enriched.groupBy(
            "wiki",
            F.coalesce(F.col("language"), F.lit("unknown")).alias("language"),
            F.coalesce(F.col("country"), F.lit("unknown")).alias("country"),
            F.coalesce(F.col("project_family"), F.lit("unknown")).alias("project_family"),
        )
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0)).alias("bot_events"),
            F.countDistinct(F.col("user_name")).alias("unique_users"),
            F.countDistinct(F.col("title")).alias("unique_pages"),
        )
        .withColumn(
            "bot_pct",
            F.round(F.col("bot_events") * 100.0 / F.col("total_events"), 1),
        )
        .orderBy(F.col("total_events").desc())
        .limit(20)
    )


def q2_pareto_concentration(raw: DataFrame) -> DataFrame:
    """Q2 - Concentracion del trafico (curva de Pareto).

    Window function: % acumulado de eventos al ordenar wikis por volumen
    descendente. Sirve para responder: "20% de las wikis concentran X% del
    trafico?".
    """
    by_wiki = (
        raw.groupBy("wiki")
        .agg(F.count(F.lit(1)).alias("total_events"))
        .filter(F.col("total_events") > 0)
    )

    total_window = Window.partitionBy()
    cum_window = Window.partitionBy().orderBy(F.col("total_events").desc()).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    with_pcts = (
        by_wiki.withColumn("total_global", F.sum("total_events").over(total_window))
        .withColumn(
            "pct_total",
            F.round(F.col("total_events") * 100.0 / F.col("total_global"), 2),
        )
        .withColumn(
            "pct_acumulado",
            F.round(F.sum("total_events").over(cum_window) * 100.0 / F.col("total_global"), 2),
        )
        .withColumn(
            "rank_wiki",
            F.row_number().over(Window.partitionBy().orderBy(F.col("total_events").desc())),
        )
        .drop("total_global")
        .orderBy("rank_wiki")
    )
    return with_pcts


def q3_bot_anomalies(raw: DataFrame, min_events: int, threshold_pct: float) -> DataFrame:
    """Q3 - Wikis bot-dominadas.

    HAVING sobre columna calculada: bot_pct >= threshold AND total >= min.
    Identifica wikis cuya actividad reciente es predominantemente automatizada,
    candidatas a auditoria de moderacion o flagging.
    """
    return (
        raw.groupBy("wiki")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0)).alias("bot_events"),
            F.countDistinct(F.col("user_name")).alias("unique_bot_users"),
        )
        .withColumn(
            "bot_pct",
            F.round(F.col("bot_events") * 100.0 / F.col("total_events"), 1),
        )
        .filter(
            (F.col("total_events") >= min_events) & (F.col("bot_pct") >= threshold_pct)
        )
        .orderBy(F.col("bot_pct").desc(), F.col("total_events").desc())
    )


def q4_top_contribuyentes(raw: DataFrame) -> DataFrame:
    """Q4 - Top 20 usuarios mas activos.

    Distingue bots de humanos. Identifica editores prolificos (humanos
    relevantes) y bots ruidosos (candidatos a filtro).
    """
    return (
        raw.filter(F.col("user_name").isNotNull())
        .groupBy("user_name")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.countDistinct(F.col("wiki")).alias("wikis_touched"),
            F.countDistinct(F.col("title")).alias("unique_pages"),
            F.max(F.col("bot").cast("int")).alias("is_bot"),
        )
        .orderBy(F.col("events").desc())
        .limit(20)
    )


def q5_project_family_dist(raw: DataFrame, wikis: DataFrame) -> DataFrame:
    """Q5 - Distribucion por familia de proyecto Wikimedia.

    Agrupa por project_family (wikipedia, commons, wikidata, wikisource,
    wikinews, ...). Permite ver donde se concentra la actividad mas alla
    de la wiki individual.
    """
    enriched = raw.join(wikis, on="wiki", how="left").withColumn(
        "project_family",
        F.coalesce(F.col("project_family"), F.lit("unknown")),
    )
    return (
        enriched.groupBy("project_family")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.countDistinct(F.col("wiki")).alias("wikis_distintas"),
            F.countDistinct(F.col("user_name")).alias("usuarios_distintos"),
            F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0)).alias("bot_events"),
        )
        .withColumn(
            "bot_pct",
            F.round(F.col("bot_events") * 100.0 / F.col("events"), 1),
        )
        .orderBy(F.col("events").desc())
    )


def q6_enrichment_coverage(raw: DataFrame, wikis: DataFrame) -> DataFrame:
    """Q6 - Cobertura del enriquecimiento.

    Calcula que porcentaje de eventos del stream tiene su wiki mapeada
    en el catalogo estatico vs cuantos quedan como 'unmapped'. Es una
    metrica de calidad del enriquecimiento.
    """
    enriched = raw.join(wikis, on="wiki", how="left")
    flagged = enriched.withColumn(
        "coverage",
        F.when(F.col("language").isNull(), F.lit("unmapped")).otherwise(F.lit("enriched")),
    )
    total_window = Window.partitionBy()
    return (
        flagged.groupBy("coverage")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.countDistinct(F.col("wiki")).alias("wikis"),
        )
        .withColumn("total_global", F.sum("events").over(total_window))
        .withColumn(
            "pct",
            F.round(F.col("events") * 100.0 / F.col("total_global"), 1),
        )
        .drop("total_global")
        .orderBy(F.col("events").desc())
    )


# ---------------------------------- main -----------------------------------


def main() -> int:
    spark = create_spark_session()
    try:
        raw = read_raw(spark).cache()
        total_raw = raw.count()
        print(f"\n[ETAPA5] Filas leidas desde {KEYSPACE}.{RAW_TABLE}: {total_raw:,}")

        wikis = read_static(spark)
        n_wikis_static = wikis.count()
        print(f"[ETAPA5] Filas del catalogo estatico: {n_wikis_static}")

        # ------------------------------- Q1 -------------------------------
        print_section("Q1 - Top 20 wikis por volumen (enriquecidas)")
        q1 = q1_top_wikis_enriquecidas(raw, wikis).cache()
        show_summary(q1, 20)
        write_csv(q1, "q1_top_wikis_enriquecidas")

        # ------------------------------- Q2 -------------------------------
        print_section("Q2 - Concentracion del trafico (Pareto)")
        q2 = q2_pareto_concentration(raw).cache()
        # imprimimos los primeros 20 + los que cruzan el 50% y el 80%
        show_summary(q2, 20)
        n_wikis = q2.count()
        cross50 = q2.filter(F.col("pct_acumulado") >= 50).orderBy("rank_wiki").first()
        cross80 = q2.filter(F.col("pct_acumulado") >= 80).orderBy("rank_wiki").first()
        if cross50:
            print(
                f"  50% del trafico al wiki #{cross50['rank_wiki']} de {n_wikis} "
                f"({cross50['rank_wiki'] * 100.0 / n_wikis:.1f}% de las wikis)"
            )
        if cross80:
            print(
                f"  80% del trafico al wiki #{cross80['rank_wiki']} de {n_wikis} "
                f"({cross80['rank_wiki'] * 100.0 / n_wikis:.1f}% de las wikis)"
            )
        write_csv(q2, "q2_pareto_concentration")

        # ------------------------------- Q3 -------------------------------
        print_section(
            f"Q3 - Wikis bot-dominadas (>= {BOT_THRESHOLD_PCT}% bots, "
            f">= {MIN_EVENTS_BOT_ANOMALY} eventos)"
        )
        q3 = q3_bot_anomalies(raw, MIN_EVENTS_BOT_ANOMALY, BOT_THRESHOLD_PCT).cache()
        show_summary(q3, 30)
        print(f"  Total wikis flagged: {q3.count()}")
        write_csv(q3, "q3_bot_anomalies")

        # ------------------------------- Q4 -------------------------------
        print_section("Q4 - Top 20 contribuyentes (usuarios)")
        q4 = q4_top_contribuyentes(raw).cache()
        show_summary(q4, 20)
        write_csv(q4, "q4_top_contribuyentes")

        # ------------------------------- Q5 -------------------------------
        print_section("Q5 - Distribucion por familia de proyecto")
        q5 = q5_project_family_dist(raw, wikis).cache()
        show_summary(q5, 20)
        write_csv(q5, "q5_project_family_dist")

        # ------------------------------- Q6 -------------------------------
        print_section("Q6 - Cobertura del enriquecimiento estatico")
        q6 = q6_enrichment_coverage(raw, wikis).cache()
        show_summary(q6, 5)
        write_csv(q6, "q6_enrichment_coverage")

        print()
        print("=" * 72)
        print(f"[ETAPA5] Listo. Resultados CSV en: {OUTPUT_BASE}")
        print("=" * 72)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
