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
import numpy as np

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

def chart_change_types(df: pd.DataFrame) -> None:
    """
    Consulta 2:
    Distribución de eventos por tipo de cambio.
    Esta consulta permite identificar qué tipos de cambios son más frecuentes
    dentro del stream de Wikimedia, por ejemplo: edit, categorize, log, new.
    """

    required_columns = {"change_type", "total_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 2: {missing_columns}")

    change_types = (
        df.groupby("change_type", as_index=False)
        .agg(total_events=("total_events", "sum"))
        .sort_values("total_events", ascending=False)
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "02_change_types.csv")
    out_png = os.path.join(OUTPUT_DIR, "02_change_types.png")

    change_types.to_csv(out_csv, index=False)

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(change_types["change_type"], change_types["total_events"])

    ax.set_title(
        "Distribución de eventos por tipo de cambio",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Tipo de cambio")
    ax.set_ylabel("Total de eventos")

    max_value = change_types["total_events"].max()

    for i, value in enumerate(change_types["total_events"]):
        ax.text(
            i,
            value + max_value * 0.01,
            f"{int(value):,}",
            ha="center",
            fontsize=9,
        )

    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")

def chart_bot_vs_human(df: pd.DataFrame, top_n: int = 10) -> None:
    """
    Consulta 3:
    Bots vs humanos por wiki.
    Esta consulta compara cuántos eventos fueron generados por bots y cuántos
    fueron generados por usuarios humanos dentro de las wikis más activas.
    """

    required_columns = {"wiki", "total_events", "bot_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 3: {missing_columns}")

    bot_human = (
        df.groupby("wiki", as_index=False)
        .agg(
            total_events=("total_events", "sum"),
            bot_events=("bot_events", "sum"),
        )
    )

    bot_human["human_events"] = bot_human["total_events"] - bot_human["bot_events"]
    bot_human["bot_share"] = bot_human["bot_events"] / bot_human["total_events"]

    bot_human = (
        bot_human
        .sort_values("total_events", ascending=False)
        .head(top_n)
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "03_bot_vs_human.csv")
    out_png = os.path.join(OUTPUT_DIR, "03_bot_vs_human.png")

    bot_human.to_csv(out_csv, index=False)

    fig, ax = plt.subplots(figsize=(11, 6))

    ax.bar(bot_human["wiki"], bot_human["human_events"], label="Humanos")
    ax.bar(
        bot_human["wiki"],
        bot_human["bot_events"],
        bottom=bot_human["human_events"],
        label="Bots",
    )

    ax.set_title(
        f"Bots vs humanos en las {top_n} wikis más activas",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Wiki")
    ax.set_ylabel("Total de eventos")
    ax.legend()

    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")

def chart_heatmap(df: pd.DataFrame, top_n: int = 10) -> None:
    """
    Consulta 4:
    Heatmap wiki × tipo de cambio.

    Esta consulta muestra qué tipos de cambio predominan dentro de las
    wikis más activas, usando un mapa de calor.
    """

    required_columns = {"wiki", "change_type", "total_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 4: {missing_columns}")

    top_wikis = (
        df.groupby("wiki", as_index=False)
        .agg(total_events=("total_events", "sum"))
        .sort_values("total_events", ascending=False)
        .head(top_n)["wiki"]
        .tolist()
    )

    filtered = df[df["wiki"].isin(top_wikis)].copy()

    heatmap_matrix = pd.pivot_table(
        filtered,
        index="wiki",
        columns="change_type",
        values="total_events",
        aggfunc="sum",
        fill_value=0,
    )

    heatmap_matrix["__total__"] = heatmap_matrix.sum(axis=1)
    heatmap_matrix = heatmap_matrix.sort_values("__total__", ascending=False)
    heatmap_matrix = heatmap_matrix.drop(columns="__total__")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "04_heatmap_matrix.csv")
    out_png = os.path.join(OUTPUT_DIR, "04_heatmap.png")

    heatmap_matrix.to_csv(out_csv)

    fig, ax = plt.subplots(figsize=(10, 7))

    im = ax.imshow(heatmap_matrix.values, aspect="auto")

    ax.set_title(
        f"Heatmap wiki × tipo de cambio (top {top_n} wikis)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Tipo de cambio")
    ax.set_ylabel("Wiki")

    ax.set_xticks(range(len(heatmap_matrix.columns)))
    ax.set_xticklabels(heatmap_matrix.columns, rotation=30, ha="right")

    ax.set_yticks(range(len(heatmap_matrix.index)))
    ax.set_yticklabels(heatmap_matrix.index)

    for i in range(len(heatmap_matrix.index)):
        for j in range(len(heatmap_matrix.columns)):
            value = heatmap_matrix.iloc[i, j]
            ax.text(
                j,
                i,
                f"{int(value)}",
                ha="center",
                va="center",
                fontsize=8,
            )

    fig.colorbar(im, ax=ax, label="Total de eventos")
    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")


def chart_automation_index(df: pd.DataFrame, top_n: int = 15, min_events: int = 10) -> None:
    """
    Consulta 5:
    Índice de automatización por wiki y tipo de cambio.

    Esta consulta identifica qué combinaciones de wiki y tipo de cambio
    tienen mayor presencia de bots, considerando tanto la proporción de bots
    como el volumen total de eventos.
    """

    required_columns = {"wiki", "change_type", "total_events", "bot_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 5: {missing_columns}")

    automation = (
        df.groupby(["wiki", "change_type"], as_index=False)
        .agg(
            total_events=("total_events", "sum"),
            bot_events=("bot_events", "sum"),
        )
    )

    automation = automation[automation["total_events"] >= min_events].copy()

    if automation.empty:
        sys.exit(
            "No hay suficientes datos para calcular el índice de automatización. "
            f"Prueba bajando min_events, actualmente es {min_events}."
        )

    automation["human_events"] = automation["total_events"] - automation["bot_events"]
    automation["bot_share"] = automation["bot_events"] / automation["total_events"]

    automation["automation_score"] = automation["bot_share"] * np.log1p(
        automation["total_events"]
    )

    automation = automation.sort_values("automation_score", ascending=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "05_automation_index.csv")
    out_png = os.path.join(OUTPUT_DIR, "05_automation_index.png")

    automation.to_csv(out_csv, index=False)

    top = automation.head(top_n).copy()
    top["label"] = top["wiki"].astype(str) + " | " + top["change_type"].astype(str)
    top = top.sort_values("automation_score", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(top["label"], top["automation_score"])

    ax.set_title(
        f"Top {top_n} combinaciones con mayor índice de automatización",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Índice de automatización")
    ax.set_ylabel("Wiki | Tipo de cambio")

    max_score = top["automation_score"].max()

    for i, row in enumerate(top.itertuples()):
        ax.text(
            row.automation_score + max_score * 0.01,
            i,
            f"{row.bot_share:.1%} bots | {int(row.total_events):,} eventos",
            va="center",
            fontsize=8,
        )

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")

def chart_change_type_entropy(df: pd.DataFrame, top_n: int = 15, min_events: int = 10) -> None:
    """
    Consulta 6:
    Diversidad por entropía.

    Esta consulta mide qué tan diversa es la actividad de cada wiki según
    la distribución de sus tipos de cambio.

    Una entropía alta indica que la actividad está repartida entre varios
    tipos de cambio. Una entropía baja indica que la wiki está dominada por
    un solo tipo de cambio.
    """

    required_columns = {"wiki", "change_type", "total_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 6: {missing_columns}")

    # Agrupar eventos por wiki y tipo de cambio
    by_type = (
        df.groupby(["wiki", "change_type"], as_index=False)
        .agg(total_events=("total_events", "sum"))
    )

    # Total de eventos por wiki
    by_type["wiki_total_events"] = by_type.groupby("wiki")["total_events"].transform("sum")

    # Filtrar wikis con muy pocos eventos para evitar resultados poco representativos
    by_type = by_type[by_type["wiki_total_events"] >= min_events].copy()

    if by_type.empty:
        sys.exit(
            "No hay suficientes datos para calcular diversidad por entropía. "
            f"Prueba bajando min_events, actualmente es {min_events}."
        )

    # Proporción de cada tipo de cambio dentro de cada wiki
    by_type["p"] = by_type["total_events"] / by_type["wiki_total_events"]

    # Componente de entropía: -p * log(p)
    by_type["entropy_component"] = -by_type["p"] * np.log(by_type["p"])

    # Entropía por wiki
    entropy = (
        by_type.groupby("wiki", as_index=False)
        .agg(
            total_events=("wiki_total_events", "first"),
            entropy=("entropy_component", "sum"),
            num_change_types=("change_type", "nunique"),
        )
    )

    # Entropía máxima posible para el número de tipos observados
    entropy["max_entropy"] = np.log(entropy["num_change_types"])

    # Entropía normalizada entre 0 y 1
    entropy["normalized_entropy"] = np.where(
        entropy["max_entropy"] > 0,
        entropy["entropy"] / entropy["max_entropy"],
        0,
    )

    # Tipo de cambio dominante por wiki
    dominant_idx = by_type.groupby("wiki")["total_events"].idxmax()

    dominant = (
        by_type.loc[dominant_idx, ["wiki", "change_type", "total_events"]]
        .rename(
            columns={
                "change_type": "dominant_change_type",
                "total_events": "dominant_events",
            }
        )
    )

    entropy = entropy.merge(dominant, on="wiki", how="left")

    entropy["dominant_share"] = entropy["dominant_events"] / entropy["total_events"]

    entropy = entropy.sort_values("normalized_entropy", ascending=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "06_change_type_entropy.csv")
    out_png = os.path.join(OUTPUT_DIR, "06_change_type_entropy.png")

    entropy.to_csv(out_csv, index=False)

    # Top wikis más diversas
    top = entropy.head(top_n).copy()
    top = top.sort_values("normalized_entropy", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(top["wiki"], top["normalized_entropy"])

    ax.set_title(
        f"Top {top_n} wikis con mayor diversidad de tipos de cambio",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Entropía normalizada")
    ax.set_ylabel("Wiki")
    ax.set_xlim(0, 1.05)

    for i, row in enumerate(top.itertuples()):
        ax.text(
            row.normalized_entropy + 0.01,
            i,
            f"{row.normalized_entropy:.2f} | domina: {row.dominant_change_type} ({row.dominant_share:.1%})",
            va="center",
            fontsize=8,
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
    chart_change_types(df)
    chart_bot_vs_human(df)
    chart_heatmap(df)
    chart_automation_index(df)
    chart_change_type_entropy(df)
    
    print("\nConsultas 1, 2, 3, 4, 5 y 6 terminadas correctamente.")


if __name__ == "__main__":
    sys.exit(main())