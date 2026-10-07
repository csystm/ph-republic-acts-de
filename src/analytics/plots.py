"""Visualization layer for the clustering analytics.

Reads the artifacts written by `tfidf_cluster` and `cluster_analysis`
and produces both matplotlib PNGs (portable) and Plotly HTML embeds
(interactive, self-contained except for the plotly.js CDN reference).

Outputs under `outputs/analytics/plots/`:
- elbow.png / elbow.html
- silhouette.png / silhouette.html
- scatter.png / scatter.html
- top_terms.png / top_terms.html
"""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless — no display needed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from src.analytics.cluster_analysis import (
    CLUSTER_TERMS_JSON,
    CLUSTERS_PARQUET,
    OUT_DIR,
    METRICS_PATH,
)
from src.analytics.tfidf_cluster import REPO_ROOT, load_corpus
from src.utils.io import atomic_write_bytes, atomic_write_text, get_logger

log = get_logger(__name__)

PLOTS_DIR = OUT_DIR / "plots"
TOP_TERMS_N_SHOWN = 10  # bars per cluster in the top-terms chart


# ----------------------------------------------------------------------
# IO helpers
# ----------------------------------------------------------------------
def _read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _save_png(fig: plt.Figure, dest: Path) -> None:
    fig.tight_layout()
    # Render to a BytesIO and write atomically
    import io as _io

    buf = _io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    atomic_write_bytes(buf.getvalue(), dest)
    log.info("Wrote %s", dest.relative_to(REPO_ROOT))


def _save_plotly(fig: go.Figure, dest: Path, title: str) -> None:
    """Write a self-contained HTML embed (plotly.js pulled from CDN)."""
    html = pio.to_html(
        fig,
        include_plotlyjs="cdn",
        full_html=True,
        config={"responsive": True, "displaylogo": False},
    )
    atomic_write_text(html, dest)
    log.info("Wrote %s", dest.relative_to(REPO_ROOT))


# ----------------------------------------------------------------------
# Charts
# ----------------------------------------------------------------------
def plot_elbow(metrics: dict) -> None:
    ks = metrics["k"]
    inertia = metrics["inertia"]
    k_elbow = metrics["k_elbow"]
    k_final = metrics["final_k"]

    # --- matplotlib PNG ---
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(ks, inertia, marker="o", linewidth=1.5, color="#1f77b4")
    ax.axvline(k_elbow, color="#888", linestyle="--", linewidth=1, label=f"elbow k={k_elbow}")
    ax.axvline(k_final, color="#d62728", linestyle=":", linewidth=1.5, label=f"final k={k_final}")
    ax.set_xlabel("k (number of clusters)")
    ax.set_ylabel("Inertia (within-cluster sum of squares)")
    ax.set_title("Elbow curve — KMeans inertia across k")
    ax.grid(alpha=0.3)
    ax.legend()
    _save_png(fig, PLOTS_DIR / "elbow.png")

    # --- Plotly HTML ---
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=ks, y=inertia, mode="lines+markers",
        name="inertia", line=dict(color="#1f77b4"),
        hovertemplate="k=%{x}<br>inertia=%{y:.0f}<extra></extra>",
    ))
    fig2.add_vline(x=k_elbow, line=dict(color="#888", dash="dash"),
                   annotation_text=f"elbow k={k_elbow}", annotation_position="top")
    fig2.add_vline(x=k_final, line=dict(color="#d62728", dash="dot"),
                   annotation_text=f"final k={k_final}", annotation_position="top")
    fig2.update_layout(
        title="Elbow curve — KMeans inertia across k",
        xaxis_title="k (number of clusters)",
        yaxis_title="Inertia",
        template="plotly_white",
    )
    _save_plotly(fig2, PLOTS_DIR / "elbow.html", "Elbow")


def plot_silhouette(metrics: dict) -> None:
    ks = metrics["k"]
    sil = metrics["silhouette"]
    k_elbow = metrics["k_elbow"]
    k_sil_max = metrics["k_silhouette_max"]
    k_final = metrics["final_k"]

    # --- matplotlib PNG ---
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.plot(ks, sil, marker="o", linewidth=1.5, color="#2ca02c")
    ax.axvline(k_elbow, color="#888", linestyle="--", linewidth=1, label=f"elbow k={k_elbow}")
    ax.axvline(k_sil_max, color="#9467bd", linestyle=":", linewidth=1, label=f"sil max k={k_sil_max}")
    ax.axvline(k_final, color="#d62728", linestyle=":", linewidth=1.5, label=f"final k={k_final}")
    ax.set_xlabel("k (number of clusters)")
    ax.set_ylabel("Silhouette score (sample of 2,000)")
    ax.set_title("Silhouette score across k")
    ax.grid(alpha=0.3)
    ax.legend(loc="lower right", fontsize=8)
    _save_png(fig, PLOTS_DIR / "silhouette.png")

    # --- Plotly HTML ---
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=ks, y=sil, mode="lines+markers",
        name="silhouette", line=dict(color="#2ca02c"),
        hovertemplate="k=%{x}<br>silhouette=%{y:.4f}<extra></extra>",
    ))
    fig2.add_vline(x=k_elbow, line=dict(color="#888", dash="dash"),
                   annotation_text=f"elbow k={k_elbow}", annotation_position="bottom")
    fig2.add_vline(x=k_sil_max, line=dict(color="#9467bd", dash="dot"),
                   annotation_text=f"sil max k={k_sil_max}", annotation_position="bottom")
    fig2.add_vline(x=k_final, line=dict(color="#d62728", dash="dot"),
                   annotation_text=f"final k={k_final}", annotation_position="top")
    fig2.update_layout(
        title="Silhouette score across k",
        xaxis_title="k (number of clusters)",
        yaxis_title="Silhouette",
        template="plotly_white",
    )
    _save_plotly(fig2, PLOTS_DIR / "silhouette.html", "Silhouette")


def _cluster_palette(n: int) -> list[str]:
    """Discrete palette from plotly's qualitative sets, wrapped if needed."""
    base = px.colors.qualitative.Dark24 + px.colors.qualitative.Set3
    return [base[i % len(base)] for i in range(n)]


def plot_scatter(
    clusters: pd.DataFrame,
    df: pd.DataFrame,
    summary: dict,
) -> None:
    """2D projection colored by cluster. Hover shows ra_id + truncated title."""
    joined = clusters.merge(
        df[["ra_id", "title", "ra_year"]].rename(columns={"ra_id": "ra_id"}),
        on="ra_id",
        how="left",
    )
    joined["cluster"] = joined["cluster"].astype(int)
    joined["title_short"] = (
        joined["title"].fillna("").str.slice(0, 90).str.replace("\n", " ", regex=False)
    )
    joined["year_str"] = joined["ra_year"].fillna("").astype(str)

    n_clusters = int(joined["cluster"].max()) + 1
    palette = _cluster_palette(n_clusters)

    # --- Plotly HTML (interactive, the primary view) ---
    fig = go.Figure()
    for c in range(n_clusters):
        sub = joined[joined["cluster"] == c]
        fig.add_trace(go.Scattergl(
            x=sub["x"], y=sub["y"],
            mode="markers",
            name=f"C{c}",
            marker=dict(size=5, color=palette[c], opacity=0.7),
            text=sub["ra_id"] + " · " + sub["year_str"],
            customdata=sub["title_short"],
            hovertemplate=(
                "<b>%{text}</b><br>%{customdata}<extra></extra>"
            ),
        ))
    fig.update_layout(
        title=f"t-SNE projection of 11,966 RAs (k={n_clusters} clusters)",
        xaxis_title="t-SNE 1", yaxis_title="t-SNE 2",
        template="plotly_white",
        legend_title="cluster",
        height=700,
    )
    _save_plotly(fig, PLOTS_DIR / "scatter.html", "Scatter")

    # --- matplotlib PNG (static fallback) ---
    fig, ax = plt.subplots(figsize=(9, 7))
    for c in range(n_clusters):
        sub = joined[joined["cluster"] == c]
        ax.scatter(sub["x"], sub["y"], s=6, alpha=0.6, color=palette[c], label=f"C{c}")
    ax.set_xlabel("t-SNE 1")
    ax.set_ylabel("t-SNE 2")
    ax.set_title(f"t-SNE projection (k={n_clusters})")
    ax.legend(markerscale=3, fontsize=7, ncol=2, loc="best", framealpha=0.8)
    ax.grid(alpha=0.2)
    _save_png(fig, PLOTS_DIR / "scatter.png")


def plot_top_terms(terms: dict) -> None:
    """One horizontal bar chart per cluster, arranged in a grid."""
    clusters = sorted(terms.keys(), key=lambda x: int(x))
    n = len(clusters)
    cols = 3
    rows = (n + cols - 1) // cols

    # --- matplotlib PNG: grid of horizontal bar charts ---
    fig, axes = plt.subplots(rows, cols, figsize=(13, 3 * rows))
    axes = np.atleast_1d(axes).ravel()
    for i, c in enumerate(clusters):
        ax = axes[i]
        words = terms[c][:TOP_TERMS_N_SHOWN][::-1]
        # Fake bar heights: decreasing by rank; real values not needed
        heights = np.arange(len(words), 0, -1)
        ax.barh(words, heights, color="#4c72b0")
        ax.set_title(f"Cluster {c}", fontsize=10)
        ax.tick_params(axis="y", labelsize=8)
        ax.set_xticks([])
    for j in range(len(clusters), len(axes)):
        axes[j].axis("off")
    fig.suptitle(f"Top {TOP_TERMS_N_SHOWN} terms per cluster", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    _save_png(fig, PLOTS_DIR / "top_terms.png")

    # --- Plotly HTML: single grouped bar, one trace per cluster ---
    fig2 = go.Figure()
    for c in clusters:
        words = terms[c][:TOP_TERMS_N_SHOWN][::-1]
        heights = list(range(len(words), 0, -1))
        fig2.add_trace(go.Bar(
            x=heights, y=words,
            orientation="h",
            name=f"C{c}",
            visible=(c == clusters[0]),  # only first visible by default
            hovertemplate="%{y}<extra></extra>",
        ))
    # Dropdown to toggle clusters
    buttons = []
    for i, c in enumerate(clusters):
        vis = [False] * len(clusters)
        vis[i] = True
        buttons.append(dict(
            label=f"C{c}", method="update",
            args=[{"visible": vis}, {"title": f"Top terms — cluster {c}"}],
        ))
    fig2.update_layout(
        title=f"Top terms — cluster {clusters[0]}",
        updatemenus=[dict(
            active=0, buttons=buttons,
            x=1.02, xanchor="left", y=1.0, yanchor="top",
        )],
        template="plotly_white",
        height=500,
        showlegend=False,
        margin=dict(l=120, r=120, t=60, b=40),
    )
    _save_plotly(fig2, PLOTS_DIR / "top_terms.html", "Top terms")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main() -> int:
    log.info("=== Generating plots ===")
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    for p in (METRICS_PATH, CLUSTERS_PARQUET, CLUSTER_TERMS_JSON):
        if not p.exists():
            raise FileNotFoundError(
                f"{p.relative_to(REPO_ROOT)} missing — run the earlier analytics steps first."
            )

    metrics = _read_json(METRICS_PATH)
    terms = _read_json(CLUSTER_TERMS_JSON)
    summary = _read_json(OUT_DIR / "cluster_summary.json")
    clusters = pd.read_parquet(CLUSTERS_PARQUET)
    df = load_corpus()

    log.info("Plotting elbow + silhouette...")
    plot_elbow(metrics)
    plot_silhouette(metrics)

    log.info("Plotting scatter...")
    plot_scatter(clusters, df, summary)

    log.info("Plotting top terms...")
    plot_top_terms(terms)

    log.info("=== Plots complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())