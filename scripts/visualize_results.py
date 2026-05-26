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
RAW_EXPORT = os.path.join(PROJECT_ROOT, "data", "exports", "recent_changes_raw.csv")

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

def load_raw_data() -> pd.DataFrame:
    """
    Carga los datos crudos exportados desde Cassandra.

    Este archivo se usa para consultas que necesitan nivel evento,
    como concentración por usuarios, páginas o análisis de comentarios.
    """

    if not os.path.exists(RAW_EXPORT):
        sys.exit(
            "No se encontró el archivo raw:\n"
            f"{RAW_EXPORT}\n"
            "Primero exporta recent_changes_raw desde Cassandra."
        )

    print(f"Leyendo archivo raw: {RAW_EXPORT}")

    try:
        raw_df = pd.read_csv(RAW_EXPORT, encoding="utf-8", on_bad_lines="skip")
    except UnicodeDecodeError:
        raw_df = pd.read_csv(RAW_EXPORT, encoding="latin1", on_bad_lines="skip")

    # Compatibilidad por si alguna versión antigua usa 'type' en vez de 'change_type'
    if "change_type" not in raw_df.columns and "type" in raw_df.columns:
        raw_df = raw_df.rename(columns={"type": "change_type"})

    required_columns = {"wiki", "user_name", "title"}
    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias en el archivo raw: {missing_columns}")

    return raw_df

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

def chart_activity_anomalies(df: pd.DataFrame, top_n: int = 15, min_events: int = 5) -> None:
    """
    Consulta 7:
    Picos anómalos de actividad por hora.

    Esta consulta detecta horas en las que una wiki tuvo actividad
    inusualmente alta comparada contra su propio promedio horario.
    """

    required_columns = {"event_date", "event_hour", "wiki", "total_events"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 7: {missing_columns}")

    hourly = (
        df.groupby(["event_date", "event_hour", "wiki"], as_index=False)
        .agg(total_events=("total_events", "sum"))
    )

    hourly = hourly[hourly["total_events"] >= min_events].copy()

    if hourly.empty:
        sys.exit(
            "No hay suficientes datos para calcular picos anómalos. "
            f"Prueba bajando min_events, actualmente es {min_events}."
        )

    hourly["wiki_avg_hourly_events"] = hourly.groupby("wiki")["total_events"].transform("mean")
    hourly["wiki_std_hourly_events"] = hourly.groupby("wiki")["total_events"].transform("std")

    # Si una wiki solo tiene una hora registrada, la desviación estándar queda vacía.
    # En ese caso ponemos z_score = 0 para evitar errores.
    hourly["wiki_std_hourly_events"] = hourly["wiki_std_hourly_events"].fillna(0)

    hourly["z_score"] = np.where(
        hourly["wiki_std_hourly_events"] > 0,
        (hourly["total_events"] - hourly["wiki_avg_hourly_events"])
        / hourly["wiki_std_hourly_events"],
        0,
    )

    hourly = hourly.sort_values("z_score", ascending=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "07_activity_anomalies.csv")
    out_png = os.path.join(OUTPUT_DIR, "07_activity_anomalies.png")

    hourly.to_csv(out_csv, index=False)

    top = hourly.head(top_n).copy()

    top["label"] = (
        top["wiki"].astype(str)
        + " | "
        + top["event_date"].astype(str)
        + " h"
        + top["event_hour"].astype(str)
    )

    # Si no hay variación suficiente para detectar anomalías, se muestra el top por volumen.
    if top["z_score"].max() == 0:
        top = top.sort_values("total_events", ascending=True)
        x_values = top["total_events"]
        x_label = "Total de eventos"
        title = f"Top {top_n} horas con mayor actividad registrada"
    else:
        top = top.sort_values("z_score", ascending=True)
        x_values = top["z_score"]
        x_label = "Z-score de actividad"
        title = f"Top {top_n} picos anómalos de actividad por hora"

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(top["label"], x_values)

    ax.set_title(
        title,
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel(x_label)
    ax.set_ylabel("Wiki | Fecha y hora")

    max_value = x_values.max()

    for i, row in enumerate(top.itertuples()):
        value = x_values.iloc[i]
        ax.text(
            value + max_value * 0.01,
            i,
            f"{int(row.total_events):,} eventos | z={row.z_score:.2f}",
            va="center",
            fontsize=8,
        )

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")

def chart_user_page_concentration(raw_df: pd.DataFrame, top_n: int = 15, min_events: int = 10) -> None:
    """
    Consulta 8:
    Concentración de actividad por usuarios y páginas.

    Esta consulta mide si la actividad de una wiki está distribuida entre
    muchos usuarios/páginas o si está concentrada en pocos.
    """

    required_columns = {"wiki", "user_name", "title"}
    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 8: {missing_columns}")

    df = raw_df.copy()

    df["wiki"] = df["wiki"].astype(str).str.strip()
    df["user_name"] = df["user_name"].astype(str).str.strip()
    df["title"] = df["title"].astype(str).str.strip()

    df = df[df["wiki"] != ""].copy()

    raw_totals = (
        df.groupby("wiki", as_index=False)
        .size()
        .rename(columns={"size": "raw_total_events"})
    )

    raw_totals = raw_totals[raw_totals["raw_total_events"] >= min_events].copy()

    if raw_totals.empty:
        sys.exit(
            "No hay suficientes datos para calcular concentración. "
            f"Prueba bajando min_events, actualmente es {min_events}."
        )

    valid_wikis = set(raw_totals["wiki"])
    df = df[df["wiki"].isin(valid_wikis)].copy()

    def compute_concentration(data: pd.DataFrame, item_col: str, prefix: str) -> pd.DataFrame:
        tmp = data[["wiki", item_col]].dropna().copy()
        tmp[item_col] = tmp[item_col].astype(str).str.strip()
        tmp = tmp[tmp[item_col] != ""].copy()

        counts = (
            tmp.groupby(["wiki", item_col], as_index=False)
            .size()
            .rename(columns={"size": "events"})
        )

        totals = (
            counts.groupby("wiki", as_index=False)["events"]
            .sum()
            .rename(columns={"events": "metric_total_events"})
        )

        counts = counts.merge(totals, on="wiki", how="left")
        counts["share"] = counts["events"] / counts["metric_total_events"]

        hhi = (
            counts.groupby("wiki", as_index=False)["share"]
            .apply(lambda x: float((x ** 2).sum()))
            .rename(columns={"share": f"{prefix}_hhi"})
        )

        base_metrics = (
            counts.groupby("wiki", as_index=False)
            .agg(
                **{
                    f"{prefix}_unique_count": (item_col, "nunique"),
                    f"{prefix}_top1_share": ("share", "max"),
                    f"{prefix}_metric_total_events": ("metric_total_events", "first"),
                }
            )
        )

        top10 = (
            counts.sort_values(["wiki", "events"], ascending=[True, False])
            .groupby("wiki")
            .head(10)
            .groupby("wiki", as_index=False)["events"]
            .sum()
            .rename(columns={"events": f"{prefix}_top10_events"})
        )

        metrics = base_metrics.merge(top10, on="wiki", how="left")
        metrics = metrics.merge(hhi, on="wiki", how="left")

        metrics[f"{prefix}_top10_events"] = metrics[f"{prefix}_top10_events"].fillna(0)

        metrics[f"{prefix}_top10_share"] = (
            metrics[f"{prefix}_top10_events"] / metrics[f"{prefix}_metric_total_events"]
        )

        return metrics

    user_metrics = compute_concentration(df, "user_name", "user")
    page_metrics = compute_concentration(df, "title", "page")

    concentration = raw_totals.merge(user_metrics, on="wiki", how="left")
    concentration = concentration.merge(page_metrics, on="wiki", how="left")

    numeric_cols = concentration.select_dtypes(include=["number"]).columns
    concentration[numeric_cols] = concentration[numeric_cols].fillna(0)

    concentration["average_top10_share"] = (
        concentration["user_top10_share"] + concentration["page_top10_share"]
    ) / 2

    concentration = concentration.sort_values("average_top10_share", ascending=False)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "08_user_page_concentration.csv")
    out_png = os.path.join(OUTPUT_DIR, "08_user_page_concentration.png")

    concentration.to_csv(out_csv, index=False)

    top = (
        concentration.sort_values("raw_total_events", ascending=False)
        .head(top_n)
        .sort_values("average_top10_share", ascending=True)
    )

    y = np.arange(len(top))
    height = 0.38

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(
        y - height / 2,
        top["user_top10_share"],
        height,
        label="Top 10 usuarios",
    )

    ax.barh(
        y + height / 2,
        top["page_top10_share"],
        height,
        label="Top 10 páginas",
    )

    ax.set_title(
        f"Concentración de actividad por usuarios y páginas (top {top_n} wikis)",
        fontsize=14,
        fontweight="bold",
    )

    ax.set_xlabel("Proporción de eventos concentrados en el top 10")
    ax.set_ylabel("Wiki")
    ax.set_yticks(y)
    ax.set_yticklabels(top["wiki"])
    ax.set_xlim(0, 1.05)
    ax.legend()

    for i, row in enumerate(top.itertuples()):
        ax.text(
            row.user_top10_share + 0.01,
            i - height / 2,
            f"{row.user_top10_share:.1%}",
            va="center",
            fontsize=8,
        )

        ax.text(
            row.page_top10_share + 0.01,
            i + height / 2,
            f"{row.page_top10_share:.1%}",
            va="center",
            fontsize=8,
        )

    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    print(f"Gráfica generada: {out_png}")
    print(f"Tabla generada:   {out_csv}")

def chart_revert_vandalism_signals(
    raw_df: pd.DataFrame,
    top_n: int = 15,
    min_events: int = 10,
) -> None:
    """
    Consulta 9:
    Señales de reversión, corrección o posible vandalismo.

    Esta consulta usa el campo de texto libre 'comment' para buscar palabras
    asociadas con reversión, rollback, deshacer cambios, spam o vandalismo.
    """

    required_columns = {"wiki", "comment"}
    missing_columns = required_columns - set(raw_df.columns)

    if missing_columns:
        sys.exit(f"Faltan columnas necesarias para la Consulta 9: {missing_columns}")

    df = raw_df.copy()

    df["wiki"] = df["wiki"].fillna("").astype(str).str.strip()
    df["comment_text"] = df["comment"].fillna("").astype(str).str.lower()

    df = df[df["wiki"] != ""].copy()

    reversion_pattern = (
        r"\b(revert|reverted|reverting|rollback|undo|undid|rv)\b"
        r"|deshacer|revertir|revertid|reversi[oó]n"
    )

    vandalism_pattern = (
        r"\b(vandal|vandalism|spam)\b"
        r"|vandalismo|vandalis"
    )

    df["reversion_signal"] = df["comment_text"].str.contains(
        reversion_pattern,
        regex=True,
        na=False,
    )

    df["vandalism_signal"] = df["comment_text"].str.contains(
        vandalism_pattern,
        regex=True,
        na=False,
    )

    df["moderation_signal"] = df["reversion_signal"] | df["vandalism_signal"]

    signals = (
        df.groupby("wiki", as_index=False)
        .agg(
            total_events=("moderation_signal", "size"),
            signal_events=("moderation_signal", "sum"),
            reversion_events=("reversion_signal", "sum"),
            vandalism_spam_events=("vandalism_signal", "sum"),
        )
    )

    signals = signals[signals["total_events"] >= min_events].copy()

    if signals.empty:
        sys.exit(
            "No hay suficientes datos para calcular señales de reversión/vandalismo. "
            f"Prueba bajando min_events, actualmente es {min_events}."
        )

    signals["signal_rate"] = signals["signal_events"] / signals["total_events"]
    signals["reversion_rate"] = signals["reversion_events"] / signals["total_events"]
    signals["vandalism_spam_rate"] = (
        signals["vandalism_spam_events"] / signals["total_events"]
    )

    signals = signals.sort_values(
        ["signal_events", "signal_rate"],
        ascending=False,
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)

    out_csv = os.path.join(TABLES_DIR, "09_revert_vandalism_signals.csv")
    out_png = os.path.join(OUTPUT_DIR, "09_revert_vandalism_signals.png")

    signals.to_csv(out_csv, index=False)

    top = signals[signals["signal_events"] > 0].head(top_n).copy()

    if top.empty:
        top = signals.sort_values("total_events", ascending=False).head(top_n).copy()

    top = top.sort_values("signal_rate", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 7))

    ax.barh(top["wiki"], top["signal_rate"])

    ax.set_title(
        f"Top {top_n} wikis con señales de reversión o posible vandalismo",
        fontsize=14,
        fontweight="bold",
    )

    ax.set_xlabel("Proporción de eventos con señales")
    ax.set_ylabel("Wiki")

    max_value = max(float(top["signal_rate"].max()), 0.01)
    ax.set_xlim(0, min(1.05, max(0.10, max_value * 1.35)))

    for i, row in enumerate(top.itertuples()):
        ax.text(
            row.signal_rate + max_value * 0.03,
            i,
            f"{row.signal_events} señales / {row.total_events} eventos",
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
    raw_df = load_raw_data()

    print(f"Filas agregadas cargadas: {len(df):,}")
    print(f"Filas raw cargadas: {len(raw_df):,}")
    print("Columnas agregadas disponibles:")
    print(list(df.columns))
    print("Columnas raw disponibles:")
    print(list(raw_df.columns))


    chart_top_wikis(df)
    chart_change_types(df)
    chart_bot_vs_human(df)
    chart_heatmap(df)
    chart_automation_index(df)
    chart_change_type_entropy(df)
    chart_activity_anomalies(df)
    chart_user_page_concentration(raw_df)
    chart_revert_vandalism_signals(raw_df) 

    print("\nConsultas 1, 2, 3, 4, 5, 6, 7, 8 y 9 terminadas correctamente.")


if __name__ == "__main__":
    sys.exit(main())