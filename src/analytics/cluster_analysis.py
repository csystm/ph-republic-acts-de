"""Final KMeans fit + dimensionality reduction + per-cluster analytics.

Companion to `tfidf_cluster.py`. That module does the k-sweep and writes
`metrics.json`. This module reads the chosen `final_k` and produces:

- `clusters.parquet`     — ra_id, cluster, x, y
- `cluster_terms.json`   — cluster -> top-N terms (from centroid ranking)
- `cluster_summary.json` — cluster -> size, year range, top-3 representatives

Determinism: random_state=42 everywhere. TSNE uses init="pca", n_jobs=-1.

Fallback: set env var ANALYTICS_TSNE_FALLBACK=1 to skip TSNE and use
SVD(n_components=2) instead. ~10x faster, less cluster separation.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.manifold import TSNE

from src.analytics.tfidf_cluster import (
    OUT_DIR,
    RANDOM_STATE,
    REPO_ROOT,
    build_tfidf,
    drop_empty_content,
    drop_mega_statutes,
    load_corpus,
)
from src.utils.io import atomic_write_json, atomic_write_parquet, get_logger

log = get_logger(__name__)

METRICS_PATH = OUT_DIR / "metrics.json"
CLUSTERS_PARQUET = OUT_DIR / "clusters.parquet"
CLUSTER_TERMS_JSON = OUT_DIR / "cluster_terms.json"
CLUSTER_SUMMARY_JSON = OUT_DIR / "cluster_summary.json"

TOP_TERMS_N = 15
TOP_REPS_N = 3
SVD_COMPONENTS = 50
TSNE_PERPLEXITY = 30


def read_final_k() -> int:
    if not METRICS_PATH.exists():
        raise FileNotFoundError(
            f"{METRICS_PATH.relative_to(REPO_ROOT)} not found. "
            "Run `python -m src.analytics.tfidf_cluster` first."
        )
    with METRICS_PATH.open(encoding="utf-8") as f:
        metrics = json.load(f)
    k = int(metrics["final_k"])
    log.info(
        "Read final_k=%d from %s (final_silhouette=%.4f)",
        k,
        METRICS_PATH.relative_to(REPO_ROOT),
        float(metrics["final_silhouette"]),
    )
    return k


def fit_final(X, k: int) -> tuple[KMeans, np.ndarray]:
    log.info("Fitting final KMeans: k=%d", k)
    km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
    labels = km.fit_predict(X)
    sizes = np.bincount(labels, minlength=k)
    log.info(
        "Cluster sizes: min=%d max=%d median=%d",
        int(sizes.min()),
        int(sizes.max()),
        int(np.median(sizes)),
    )
    return km, labels


def reduce_2d(X) -> np.ndarray:
    """SVD(50) -> t-SNE(2), or SVD(2) if fallback env var is set."""
    if os.environ.get("ANALYTICS_TSNE_FALLBACK") == "1":
        log.info("ANALYTICS_TSNE_FALLBACK=1 -> using SVD(2) for 2D projection")
        svd = TruncatedSVD(n_components=2, random_state=RANDOM_STATE)
        xy = svd.fit_transform(X)
        return np.asarray(xy)

    log.info("SVD: n_components=%d", SVD_COMPONENTS)
    svd = TruncatedSVD(n_components=SVD_COMPONENTS, random_state=RANDOM_STATE)
    X_svd = svd.fit_transform(X)
    evr = float(svd.explained_variance_ratio_.sum())
    log.info("SVD explained variance (50 comps): %.4f", evr)

    log.info(
        "t-SNE: perplexity=%d, init='pca', n_jobs=-1 (this takes a few minutes)",
        TSNE_PERPLEXITY,
    )
    tsne = TSNE(
        n_components=2,
        perplexity=TSNE_PERPLEXITY,
        init="pca",
        learning_rate="auto",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    xy = tsne.fit_transform(X_svd)
    log.info("t-SNE done.")
    return np.asarray(xy)


def top_terms_per_cluster(km: KMeans, vec, n: int = TOP_TERMS_N) -> dict:
    """Rank terms by centroid value in TF-IDF space."""
    vocab = np.asarray(vec.get_feature_names_out())
    out: dict[str, list[str]] = {}
    for c in range(km.n_clusters):
        centroid = km.cluster_centers_[c]
        top_idx = np.argsort(centroid)[::-1][:n]
        out[str(c)] = [str(vocab[i]) for i in top_idx]
    return out


def build_summary(
    df: pd.DataFrame,
    X,
    km: KMeans,
    labels: np.ndarray,
) -> dict:
    """Per-cluster: size, year range, top-3 representatives (closest to centroid)."""
    # Per-row squared distance to its assigned centroid
    # km.transform(X) -> (n, k) distances in TF-IDF space
    dists = km.transform(X)
    row_dist = dists[np.arange(X.shape[0]), labels]

    summary: dict[str, dict] = {}
    for c in range(km.n_clusters):
        mask = labels == c
        n = int(mask.sum())
        idx_in_cluster = np.where(mask)[0]
        # top-3 closest to centroid
        local = row_dist[idx_in_cluster]
        top_local = idx_in_cluster[np.argsort(local)[:TOP_REPS_N]]
        reps = df.iloc[top_local][["ra_id", "title", "ra_year"]].to_dict(
            orient="records"
        )
        years = df.loc[mask, "ra_year"].dropna()
        summary[str(c)] = {
            "size": n,
            "year_min": int(years.min()) if len(years) else None,
            "year_max": int(years.max()) if len(years) else None,
            "representatives": reps,
        }
    return summary


def main() -> int:
    log.info("=== Cluster analysis (final fit + 2D + summaries) ===")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    k = read_final_k()

    df = load_corpus()
    df, _ = drop_mega_statutes(df)
    df = drop_empty_content(df)
    df = df.reset_index(drop=True)

    X, vec = build_tfidf(df["content_normalized"])

    km, labels = fit_final(X, k)

    terms = top_terms_per_cluster(km, vec)
    log.info("Top terms for cluster 0: %s", terms["0"])

    summary = build_summary(df, X, km, labels)
    log.info("Cluster summary computed for %d clusters", len(summary))

    log.info("Reducing to 2D for scatter...")
    xy = reduce_2d(X)

    clusters_df = pd.DataFrame(
        {
            "ra_id": df["ra_id"].astype(str).values,
            "cluster": labels.astype(int),
            "x": xy[:, 0].astype(float),
            "y": xy[:, 1].astype(float),
        }
    )
    atomic_write_parquet(clusters_df, CLUSTERS_PARQUET)
    log.info(
        "Wrote %s (%d rows)",
        CLUSTERS_PARQUET.relative_to(REPO_ROOT),
        len(clusters_df),
    )

    atomic_write_json(terms, CLUSTER_TERMS_JSON)
    log.info("Wrote %s", CLUSTER_TERMS_JSON.relative_to(REPO_ROOT))

    atomic_write_json(summary, CLUSTER_SUMMARY_JSON)
    log.info("Wrote %s", CLUSTER_SUMMARY_JSON.relative_to(REPO_ROOT))

    log.info("=== Step 3a complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())