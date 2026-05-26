"""
Etapa 5 - Consultas analíticas básicas

Este script genera visualizaciones a partir del archivo agregado:

    data/exports/changes_by_wiki_hour.csv

Consulta implementada por ahora:
1. Top wikis por volumen de eventos
"""

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd


# Raíz del proyecto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Archivo agregado exportado localmente
LOCAL_EXPORT = os.path.join(PROJECT_ROOT, "data", "exports", "changes_by_wiki_hour.csv")

# Salida original de Spark, por si no existe el export local
SPARK_OUTPUT_GLOB = os.path.join(
    PROJECT_ROOT,
    "spark",
    "output",
    "changes_by_wiki_hour",
    "part-*.csv",
)

# Carpetas de salida
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "spark", "output", "charts")
TABLES_DIR = os.path.join(PROJECT_ROOT, "spark", "output", "analysis_tables")


def load_data() -> pd.DataFrame:
    """
    Carga los datos agregados.

    Primero intenta leer:
        data/exports/changes_by_wiki_hour.csv

    Si no existe, intenta leer:
        spark/output/changes_by_wiki_hour/part-*.csv
    """

    if os.path.exists(LOCAL_EXPORT):
        print(f"Leyendo archivo local: {LOCAL_EXPORT}")
        df = pd.read_csv(LOCAL_EXPORT)
    else:
        paths = sorted(glob.glob(SPARK_OUTPUT_GLOB))

        if not paths:
            sys.exit(
                "No se encontró el archivo de datos.\n"
                "Verifica que exista data/exports/changes_by_wiki_hour.csv "
                "o corre primero scripts/run_analytics.sh."
            )

        print("Leyendo archivos generados por Spark...")
        dfs = [pd.read_csv(path) for path in paths]
        df = pd.concat(dfs, ignore_index=True)

    required_columns = {"wiki", "total_events"}

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias: {missing_columns}")

    return df


def chart_top_wikis(df: pd.DataFrame, top_n: int = 15) -> None:
    """
    Consulta 1:
    Top wikis por volumen total de eventos.
    """

    top_wikis = (
        df.groupby("wiki", as_index=False)
        .agg(total_events=("total_events", "sum"))
        .sort_values("total_events", ascending=False)
        .head(top_n)
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "01_top_wikis.csv")
    out_png = os.path.join(OUTPUT_DIR, "01_top_wikis.png")

    top_wikis.to_csv(out_csv, index=False)

    plot_df = top_wikis.sort_values("total_events", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 7))

    ax.barh(plot_df["wiki"], plot_df["total_events"])

    ax.set_title(
        f"Top {top_n} wikis por volumen de eventos",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Total de eventos")
    ax.set_ylabel("Wiki")

    max_value = plot_df["total_events"].max()

    for i, value in enumerate(plot_df["total_events"]):
        ax.text(
            value + max_value * 0.01,
            i,
            f"{int(value):,}",
            va="center",
            fontsize=9,
        )

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")


def main() -> int:
    df = load_data()

    print(f"Filas cargadas: {len(df):,}")
    print("Columnas disponibles:")
    print(list(df.columns))

    chart_top_wikis(df)

    print("\nConsulta 1 terminada correctamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())