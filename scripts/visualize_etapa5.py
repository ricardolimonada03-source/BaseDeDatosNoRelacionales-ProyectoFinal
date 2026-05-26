"""
Genera graficas PNG para cada una de las 6 consultas de la Etapa 5.

Uso:
    python3 scripts/visualize_etapa5.py

Lee de: spark/output/etapa5/q1.../*.csv  (uno por consulta)
Escribe: docs/charts_etapa5/*.png

Cada grafica esta diseniada para responder visualmente la pregunta de
negocio de su consulta correspondiente.
"""

from __future__ import annotations

import glob
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT_BASE = os.path.join(PROJECT_ROOT, "spark/output/etapa5")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "docs/charts_etapa5")


def load_query(name: str) -> pd.DataFrame:
    paths = sorted(glob.glob(os.path.join(INPUT_BASE, name, "part-*.csv")))
    if not paths:
        sys.exit(
            f"No se encontro CSV de '{name}' en {INPUT_BASE}. "
            "Corre primero scripts/run_etapa5_queries.sh."
        )
    return pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)


def chart_q1(df: pd.DataFrame, out: str) -> None:
    df = df.copy()
    df = df.sort_values("total_events", ascending=True).tail(20)

    fig, ax = plt.subplots(figsize=(12, 8))
    colors = ["#c44e52" if p >= 80 else "#dd8452" if p >= 50 else "#4c72b0"
              for p in df["bot_pct"]]
    ax.barh(df["wiki"], df["total_events"], color=colors)
    ax.set_title("Q1 - Top 20 wikis por volumen (color = % bots)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Total de eventos")
    for i, (v, pct) in enumerate(zip(df["total_events"], df["bot_pct"])):
        ax.text(v + max(df["total_events"]) * 0.01, i,
                f"{int(v):,} ({pct:.0f}% bots)", va="center", fontsize=8)
    legend_handles = [
        plt.Rectangle((0, 0), 1, 1, color="#4c72b0", label="< 50% bots"),
        plt.Rectangle((0, 0), 1, 1, color="#dd8452", label="50-80% bots"),
        plt.Rectangle((0, 0), 1, 1, color="#c44e52", label=">= 80% bots"),
    ]
    ax.legend(handles=legend_handles, loc="lower right")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def chart_q2(df: pd.DataFrame, out: str) -> None:
    df = df.copy().sort_values("rank_wiki")
    df["pct_wikis"] = df["rank_wiki"] * 100.0 / len(df)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(df["pct_wikis"], df["pct_acumulado"], color="#4c72b0", linewidth=2)
    ax.fill_between(df["pct_wikis"], df["pct_acumulado"], alpha=0.2, color="#4c72b0")
    ax.axhline(80, color="#c44e52", linestyle="--", label="80% del trafico")
    ax.axvline(20, color="#dd8452", linestyle="--", label="20% de las wikis")
    cross_idx = df[df["pct_acumulado"] >= 80].iloc[0] if (df["pct_acumulado"] >= 80).any() else None
    if cross_idx is not None:
        ax.annotate(
            f"80% trafico\nen {cross_idx['pct_wikis']:.1f}% de wikis",
            xy=(cross_idx["pct_wikis"], 80),
            xytext=(cross_idx["pct_wikis"] + 10, 60),
            arrowprops=dict(arrowstyle="->", color="black"),
            fontsize=10,
        )
    ax.set_title("Q2 - Concentracion del trafico (curva de Pareto)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("% de wikis (ordenadas por volumen desc.)")
    ax.set_ylabel("% acumulado de eventos")
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 105)
    ax.grid(alpha=0.3)
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def chart_q3(df: pd.DataFrame, out: str) -> None:
    df = df.copy().sort_values("total_events", ascending=True)
    if df.empty:
        # genera grafica vacia con mensaje
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.text(0.5, 0.5, "No hubo wikis bot-dominadas con los umbrales actuales",
                ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.savefig(out, dpi=120)
        plt.close(fig)
        print(f"  {out} (vacio)")
        return

    fig, ax = plt.subplots(figsize=(11, max(4, 0.4 * len(df))))
    ax.barh(df["wiki"], df["total_events"], color="#c44e52")
    for i, (v, pct) in enumerate(zip(df["total_events"], df["bot_pct"])):
        ax.text(v + max(df["total_events"]) * 0.01, i,
                f"{int(v):,} ({pct:.0f}% bots)", va="center", fontsize=9)
    ax.set_title("Q3 - Wikis bot-dominadas (>= 90% bots)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Total de eventos en el periodo")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def chart_q4(df: pd.DataFrame, out: str) -> None:
    df = df.copy().sort_values("events", ascending=True).tail(20)
    colors = ["#dd8452" if b == 1 else "#4c72b0" for b in df["is_bot"]]

    fig, ax = plt.subplots(figsize=(11, 8))
    ax.barh(df["user_name"], df["events"], color=colors)
    for i, (v, w) in enumerate(zip(df["events"], df["wikis_touched"])):
        ax.text(v + max(df["events"]) * 0.01, i,
                f"{int(v):,} ({int(w)} wikis)", va="center", fontsize=8)
    ax.set_title("Q4 - Top 20 contribuyentes (bots naranja, humanos azul)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Eventos")
    legend_handles = [
        plt.Rectangle((0, 0), 1, 1, color="#4c72b0", label="Humano"),
        plt.Rectangle((0, 0), 1, 1, color="#dd8452", label="Bot"),
    ]
    ax.legend(handles=legend_handles, loc="lower right")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def chart_q5(df: pd.DataFrame, out: str) -> None:
    df = df.copy().sort_values("events", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(df["project_family"], df["events"], color="#4c72b0", label="Eventos")
    for i, (e, b, w) in enumerate(zip(df["events"], df["bot_pct"], df["wikis_distintas"])):
        ax.text(i, e + max(df["events"]) * 0.01,
                f"{int(e):,}\n{b:.0f}% bots\n{int(w)} wikis",
                ha="center", fontsize=8)
    ax.set_title("Q5 - Distribucion de eventos por familia de proyecto",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Familia de proyecto")
    ax.set_ylabel("Eventos")
    plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def chart_q6(df: pd.DataFrame, out: str) -> None:
    df = df.copy()
    colors = {"enriched": "#55a868", "unmapped": "#c44e52"}

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.pie(
        df["events"],
        labels=[f"{c}\n{e:,} eventos\n({p}%)"
                for c, e, p in zip(df["coverage"], df["events"], df["pct"])],
        colors=[colors.get(c, "#999") for c in df["coverage"]],
        autopct="",
        startangle=90,
        wedgeprops=dict(edgecolor="white", linewidth=2),
    )
    ax.set_title("Q6 - Cobertura del enriquecimiento estatico",
                 fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    print(f"  {out}")


def main() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Generando graficas en {OUTPUT_DIR}/ ...")
    chart_q1(load_query("q1_top_wikis_enriquecidas"), os.path.join(OUTPUT_DIR, "q1_top_wikis.png"))
    chart_q2(load_query("q2_pareto_concentration"), os.path.join(OUTPUT_DIR, "q2_pareto.png"))
    chart_q3(load_query("q3_bot_anomalies"), os.path.join(OUTPUT_DIR, "q3_bot_anomalies.png"))
    chart_q4(load_query("q4_top_contribuyentes"), os.path.join(OUTPUT_DIR, "q4_top_users.png"))
    chart_q5(load_query("q5_project_family_dist"), os.path.join(OUTPUT_DIR, "q5_project_family.png"))
    chart_q6(load_query("q6_enrichment_coverage"), os.path.join(OUTPUT_DIR, "q6_coverage.png"))
    print(f"\nListo. Graficas en {OUTPUT_DIR}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
