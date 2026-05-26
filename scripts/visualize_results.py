"""
Genera graficas estaticas (PNG) a partir del CSV producido por el job Spark.

Uso:
    python3 scripts/visualize_results.py

Lee:    spark/output/changes_by_wiki_hour/part-*.csv
Escribe: spark/output/charts/*.png

Anticipa la Etapa 5 del brief (Analisis de resultados) con cuatro lecturas
distintas del mismo agregado:

  1. Top wikis por volumen total de eventos (horizontal bar)
  2. Distribucion de change_type a nivel global (bar)
  3. Bot vs human por wiki - top 10 (stacked bar)
  4. Heatmap wiki x change_type para las 10 wikis mas activas
"""

from __future__ import annotations

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")  # backend sin display, util en headless
import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT_GLOB = os.path.join(PROJECT_ROOT, "spark/output/changes_by_wiki_hour/part-*.csv")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "spark/output/charts")


def load_data() -> pd.DataFrame:
    paths = sorted(glob.glob(INPUT_GLOB))
    if not paths:
        sys.exit(
            f"No se encontro CSV en {INPUT_GLOB}. "
            "Corre primero scripts/run_analytics.sh o el job Spark."
        )
    dfs = [pd.read_csv(p) for p in paths]
    df = pd.concat(dfs, ignore_index=True)
    df["human_events"] = df["total_events"] - df["bot_events"]
    return df


def chart_top_wikis(df: pd.DataFrame, out: str, top_n: int = 15) -> None:
    by_wiki = (
        df.groupby("wiki")["total_events"].sum().sort_values(ascending=True).tail(top_n)
    )
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(by_wiki.index, by_wiki.values, color="steelblue")
    ax.set_title(f"Top {top_n} wikis por volumen de eventos", fontsize=14, fontweight="bold")
    ax.set_xlabel("Total de eventos")
    ax.set_ylabel("Wiki")
    for i, v in enumerate(by_wiki.values):
        ax.text(v + max(by_wiki.values) * 0.01, i, f"{int(v):,}", va="center", fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  guardado: {out}")


def chart_change_types(df: pd.DataFrame, out: str) -> None:
    by_type = df.groupby("change_type")["total_events"].sum().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(by_type.index, by_type.values, color=["#4c72b0", "#dd8452", "#55a868", "#c44e52", "#8172b3"])
    ax.set_title("Distribucion de eventos por tipo de cambio", fontsize=14, fontweight="bold")
    ax.set_xlabel("Tipo de cambio")
    ax.set_ylabel("Total de eventos")
    for i, v in enumerate(by_type.values):
        ax.text(i, v + max(by_type.values) * 0.01, f"{int(v):,}", ha="center", fontsize=10)
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  guardado: {out}")


def chart_bot_vs_human(df: pd.DataFrame, out: str, top_n: int = 10) -> None:
    by_wiki = df.groupby("wiki").agg(bot=("bot_events", "sum"), human=("human_events", "sum"))
    by_wiki["total"] = by_wiki["bot"] + by_wiki["human"]
    top = by_wiki.sort_values("total", ascending=False).head(top_n)
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.bar(top.index, top["human"], label="Humanos", color="#4c72b0")
    ax.bar(top.index, top["bot"], bottom=top["human"], label="Bots", color="#dd8452")
    ax.set_title(f"Bots vs humanos (top {top_n} wikis)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Wiki")
    ax.set_ylabel("Eventos")
    ax.legend()
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  guardado: {out}")


def chart_heatmap(df: pd.DataFrame, out: str, top_n: int = 10) -> None:
    top_wikis = df.groupby("wiki")["total_events"].sum().nlargest(top_n).index
    pivot = (
        df[df["wiki"].isin(top_wikis)]
        .pivot_table(
            index="wiki", columns="change_type", values="total_events", aggfunc="sum", fill_value=0
        )
        .loc[top_wikis]
    )

    fig, ax = plt.subplots(figsize=(9, 6))
    im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title(f"Heatmap: wiki x tipo de cambio (top {top_n})", fontsize=14, fontweight="bold")

    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            v = int(pivot.values[i, j])
            if v > 0:
                ax.text(j, i, f"{v}", ha="center", va="center",
                        color="white" if v > pivot.values.max() / 2 else "black",
                        fontsize=9)

    fig.colorbar(im, ax=ax, label="Total eventos")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  guardado: {out}")


def print_summary(df: pd.DataFrame) -> None:
    total = int(df["total_events"].sum())
    bots = int(df["bot_events"].sum())
    wikis = df["wiki"].nunique()
    types = df["change_type"].nunique()
    print()
    print("=" * 50)
    print(f"  Total eventos analizados   : {total:,}")
    print(f"  Eventos generados por bots : {bots:,} ({bots/total*100:.1f}%)")
    print(f"  Eventos humanos            : {total - bots:,} ({(total - bots)/total*100:.1f}%)")
    print(f"  Wikis distintas            : {wikis}")
    print(f"  Tipos de cambio            : {types}")
    print("=" * 50)


def main() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = load_data()
    print(f"Cargadas {len(df)} filas agregadas.")

    print_summary(df)
    print()

    print("Generando graficas...")
    chart_top_wikis(df, os.path.join(OUTPUT_DIR, "01_top_wikis.png"))
    chart_change_types(df, os.path.join(OUTPUT_DIR, "02_change_types.png"))
    chart_bot_vs_human(df, os.path.join(OUTPUT_DIR, "03_bot_vs_human.png"))
    chart_heatmap(df, os.path.join(OUTPUT_DIR, "04_heatmap.png"))
    print(f"\nListo. Graficas en: {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
