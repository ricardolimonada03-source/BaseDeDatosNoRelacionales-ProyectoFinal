"""
Tests unitarios para spark/jobs/recent_changes_analytics.py

Cubren las funciones puras del pipeline (clean, dedupe, enrich, aggregate)
sin necesidad de Cassandra ni del cluster Spark.

Para correrlos en local:
    pip install pyspark==3.5.* pytest
    pytest tests/spark -v

En CI o en el contenedor spark-master tambien funcionan:
    docker exec spark-master-proyectoFinal bash -lc \\
        "pip install pytest && cd /opt/repo && pytest tests/spark -v"
"""

import datetime as dt
import os
import sys

import pytest


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "spark", "jobs"))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import recent_changes_analytics as job  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-recent-changes-analytics")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_rows():
    base = dt.datetime(2026, 4, 12, 14, 30, 0)
    return [
        # Cubre: dos duplicados (mismo source_event_id), uno nulo, uno con wiki UPPERCASE,
        # uno sin change_type, uno con event_hour fuera de rango, uno valido distinto.
        {
            "wiki": "enwiki",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 14,
            "timestamp_event": base,
            "source_event_id": "evt-1",
            "title": "Page A",
            "user_name": "alice",
            "change_type": "edit",
            "bot": False,
            "server_name": "en.wikipedia.org",
            "comment": "fix typo",
            "raw_json": "{}",
        },
        {  # duplicado exacto
            "wiki": "enwiki",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 14,
            "timestamp_event": base,
            "source_event_id": "evt-1",
            "title": "Page A",
            "user_name": "alice",
            "change_type": "edit",
            "bot": False,
            "server_name": "en.wikipedia.org",
            "comment": "fix typo",
            "raw_json": "{}",
        },
        {  # null timestamp -> debe caer en clean
            "wiki": "enwiki",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 14,
            "timestamp_event": None,
            "source_event_id": "evt-2",
            "title": "Page B",
            "user_name": "bob",
            "change_type": "edit",
            "bot": False,
            "server_name": "en.wikipedia.org",
            "comment": "",
            "raw_json": "{}",
        },
        {  # uppercase + change_type null + bot=True
            "wiki": "  ESWIKI ",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 14,
            "timestamp_event": base,
            "source_event_id": "evt-3",
            "title": "Pagina C",
            "user_name": "bot1",
            "change_type": None,
            "bot": True,
            "server_name": "es.wikipedia.org",
            "comment": "",
            "raw_json": "{}",
        },
        {  # event_hour invalido -> debe caer en clean
            "wiki": "frwiki",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 99,
            "timestamp_event": base,
            "source_event_id": "evt-4",
            "title": "Page D",
            "user_name": "claire",
            "change_type": "new",
            "bot": False,
            "server_name": "fr.wikipedia.org",
            "comment": "",
            "raw_json": "{}",
        },
        {  # wiki desconocida -> debe enriquecer con 'unknown'
            "wiki": "xxwiki",
            "event_date": dt.date(2026, 4, 12),
            "event_hour": 14,
            "timestamp_event": base,
            "source_event_id": "evt-5",
            "title": "Page E",
            "user_name": "dave",
            "change_type": "edit",
            "bot": False,
            "server_name": "xx.wikipedia.org",
            "comment": "",
            "raw_json": "{}",
        },
    ]


@pytest.fixture
def wikis_static_rows():
    return [
        ("enwiki", "en", "English", "Global", "wikipedia", "xlarge"),
        ("eswiki", "es", "Spanish", "Global", "wikipedia", "large"),
        ("frwiki", "fr", "French", "Global", "wikipedia", "large"),
    ]


def test_clean_drops_invalid_rows(spark, raw_rows):
    raw_df = spark.createDataFrame(raw_rows)
    cleaned = job.clean(raw_df)
    cleaned_rows = cleaned.collect()
    # 6 entrada -> 1 cae por timestamp null, 1 cae por event_hour invalido
    assert cleaned.count() == 4
    # Normalizacion: wiki en lowercase y sin espacios
    wikis = sorted(r["wiki"] for r in cleaned_rows)
    assert wikis == ["enwiki", "enwiki", "eswiki", "xxwiki"]
    # change_type null se reemplaza por 'unknown'
    es_row = [r for r in cleaned_rows if r["wiki"] == "eswiki"][0]
    assert es_row["change_type"] == "unknown"


def test_dedupe_collapses_identical_source_event_id(spark, raw_rows):
    raw_df = spark.createDataFrame(raw_rows)
    cleaned = job.clean(raw_df)
    deduped = job.dedupe(cleaned)
    # de 4 sobrevivientes, 2 son duplicados de evt-1 -> quedan 3
    assert deduped.count() == 3


def test_enrich_adds_static_columns_with_unknown_fallback(spark, raw_rows, wikis_static_rows):
    raw_df = spark.createDataFrame(raw_rows)
    wikis_df = spark.createDataFrame(wikis_static_rows, schema=job.WIKIS_SCHEMA)
    cleaned = job.clean(raw_df)
    deduped = job.dedupe(cleaned)
    enriched = job.enrich(deduped, wikis_df)

    rows_by_wiki = {r["wiki"]: r for r in enriched.collect()}
    assert rows_by_wiki["enwiki"]["language"] == "en"
    assert rows_by_wiki["enwiki"]["country"] == "Global"
    assert rows_by_wiki["eswiki"]["language"] == "es"
    # wiki no listada en catalogo estatico -> fallback 'unknown'
    assert rows_by_wiki["xxwiki"]["language"] == "unknown"
    assert rows_by_wiki["xxwiki"]["country"] == "unknown"


def test_aggregate_produces_expected_metrics(spark, raw_rows, wikis_static_rows):
    raw_df = spark.createDataFrame(raw_rows)
    wikis_df = spark.createDataFrame(wikis_static_rows, schema=job.WIKIS_SCHEMA)
    cleaned = job.clean(raw_df)
    deduped = job.dedupe(cleaned)
    enriched = job.enrich(deduped, wikis_df)
    aggregated = job.aggregate(enriched)

    rows = {(r["wiki"], r["change_type"]): r for r in aggregated.collect()}

    # enwiki/edit: tras dedupe queda 1 fila (bot=False)
    assert rows[("enwiki", "edit")]["total_events"] == 1
    assert rows[("enwiki", "edit")]["bot_events"] == 0
    assert rows[("enwiki", "edit")]["unique_users"] == 1
    assert rows[("enwiki", "edit")]["unique_pages"] == 1

    # eswiki/unknown: 1 fila, bot=True
    assert rows[("eswiki", "unknown")]["total_events"] == 1
    assert rows[("eswiki", "unknown")]["bot_events"] == 1

    # xxwiki/edit: 1 fila, no enriquecida
    assert rows[("xxwiki", "edit")]["total_events"] == 1
    assert rows[("xxwiki", "edit")]["language"] == "unknown" if "language" in rows[("xxwiki", "edit")].asDict() else True
