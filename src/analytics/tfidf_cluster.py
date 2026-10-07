"""TF-IDF + K-Means clustering of Philippine Republic Acts.

Bonus analytics. Reads the curated corpus and produces 
cluster assignments, metrics, and visualization
embeds under `outputs/analytics/`.

Design notes:
- Input text is `content_normalized` (already lowercased and
  boilerplate-stripped by `merge_sources.normalize_content`). Never
  re-clean here.
- Mega-statutes RA-386, RA-5050, RA-8424 are dropped from the TF-IDF
  fit only; they remain in the curated parquet. See Handoff v2 §3.4.
- Determinism: `random_state=42` wherever accepted. Reruns converge.
- Sparse matrices are never densified (`.toarray()` is forbidden:
  11,966 x 5,000 dense is ~470 MB).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score

from src.utils.io import atomic_write_json, get_logger

log = get_logger(__name__)

# --- Paths -------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]
CURATED_PARQUET = REPO_ROOT / "data" / "curated" / "ra_master.parquet"
OUT_DIR = REPO_ROOT / "outputs" / "analytics"
PLOTS_DIR = OUT_DIR / "plots"

# --- Constants ---------------------------------------------------------
MEGA_STATUTES = ("RA-386", "RA-5050", "RA-8424")
RANDOM_STATE = 42
K_MIN = 2
K_MAX = 30
K_FLOOR = 5  # if silhouette suggests k<5, cap at 5
SILHOUETTE_SAMPLE = 2000  # O(n^2) is too slow at n=11966

# --- Vectorizer config -------------------------------
TFIDF_KWARGS = dict(
    max_features=5000,
    min_df=5,
    max_df=0.3,
    ngram_range=(1, 1),
    stop_words="english",
)


# ----------------------------------------------------------------------
# Load + filter
# ----------------------------------------------------------------------
def load_corpus(path: Path = CURATED_PARQUET) -> pd.DataFrame:
    log.info("Loading %s", path.relative_to(REPO_ROOT))
    df = pd.read_parquet(path)
    log.info("Loaded %d rows x %d cols", *df.shape)
    return df


def drop_mega_statutes(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Drop the three mega-statutes from the analytics frame only."""
    ids = set(df["ra_id"])
    present = [ra for ra in MEGA_STATUTES if ra in ids]
    missing = [ra for ra in MEGA_STATUTES if ra not in ids]
    if missing:
        log.warning("Mega-statutes not present in corpus (skipped): %s", missing)
    if present:
        log.info(
            "Dropping %d mega-statutes from TF-IDF fit only: %s",
            len(present),
            present,
        )
    return df[~df["ra_id"].isin(present)].copy(), present


def drop_empty_content(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    mask = (
        df["content_normalized"].notna()
        & (df["content_normalized"].str.len() > 0)
    )
    out = df[mask].copy()
    dropped = before - len(out)
    if dropped:
        log.info("Dropped %d rows with empty content_normalized", dropped)
    return out


# ----------------------------------------------------------------------
# TF-IDF
# ----------------------------------------------------------------------
def build_tfidf(texts: pd.Series) -> tuple[sparse.csr_matrix, TfidfVectorizer]:
    log.info("Fitting TfidfVectorizer: %s", TFIDF_KWARGS)
    vec = TfidfVectorizer(**TFIDF_KWARGS)
    X = vec.fit_transform(texts)
    density = X.nnz / (X.shape[0] * X.shape[1])
    log.info(
        "TF-IDF matrix: %d x %d (nnz=%d, density=%.4f%%)",
        X.shape[0], X.shape[1], X.nnz, density * 100.0,
    )
    log.info("Vocabulary size: %d", len(vec.vocabulary_))
    return X, vec


# ----------------------------------------------------------------------
# k-sweep
# ----------------------------------------------------------------------
def k_sweep(
    X: sparse.csr_matrix,
    k_min: int = K_MIN,
    k_max: int = K_MAX,
) -> dict:
    """Fit KMeans for k=k_min..k_max; record inertia and silhouette."""
    rng = np.random.default_rng(RANDOM_STATE)
    n = X.shape[0]
    n_sample = min(SILHOUETTE_SAMPLE, n)
    sample_idx = np.sort(rng.choice(n, size=n_sample, replace=False))
    X_sample = X[sample_idx]
    log.info(
        "k-sweep: k=%d..%d, silhouette sample=%d of %d",
        k_min, k_max, n_sample, n,
    )

    ks, inertias, silhouettes = [], [], []
    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = km.fit_predict(X)
        sil = float(silhouette_score(X_sample, labels[sample_idx]))
        log.info(
            "k=%2d | inertia=%12.2f | silhouette=%.4f",
            k, km.inertia_, sil,
        )
        ks.append(k)
        inertias.append(float(km.inertia_))
        silhouettes.append(sil)

    return {
        "k": ks,
        "inertia": inertias,
        "silhouette": silhouettes,
        "n_docs_sampled_for_silhouette": n_sample,
    }


def pick_final_k(sweep: dict, flat_margin: float = 0.02) -> dict:
    """Pick k from the two curves, with a safeguard.

    - If the global silhouette max beats the best silhouette near the
      elbow by less than `flat_margin`, the curve is effectively flat
      past the elbow and the max is a noise spike. In that case, prefer
      the elbow-adjacent peak (interpretable cluster count).
    - Otherwise, prefer the global silhouette max (the handoff's default).
    - Final k is floored at K_FLOOR.
    """
    ks = np.asarray(sweep["k"], dtype=float)
    inertia = np.asarray(sweep["inertia"], dtype=float)
    sil = np.asarray(sweep["silhouette"], dtype=float)

    # --- silhouette max ---
    k_sil_max = int(ks[np.argmax(sil)])
    sil_max_val = float(sil.max())

    # --- elbow: max perpendicular distance from endpoint chord ---
    x = (ks - ks.min()) / (ks.max() - ks.min())
    y = (inertia - inertia.min()) / (inertia.max() - inertia.min())
    p1 = np.array([x[0], y[0]])
    p2 = np.array([x[-1], y[-1]])
    d = p2 - p1
    d_norm = float(np.linalg.norm(d))
    if d_norm == 0.0:
        k_elbow = int(ks[0])
    else:
        pts = np.column_stack([x, y]) - p1
        dists = np.abs(d[0] * pts[:, 1] - d[1] * pts[:, 0]) / d_norm
        k_elbow = int(ks[np.argmax(dists)])

    # --- best silhouette in [k_elbow, k_elbow + 2] ---
    lo = int(np.searchsorted(ks, k_elbow))
    hi = min(lo + 3, len(ks))
    window = sil[lo:hi]
    k_sil_near_elbow = int(ks[lo + int(np.argmax(window))])
    sil_near_elbow_val = float(window.max())

    margin = sil_max_val - sil_near_elbow_val
    flat = margin < flat_margin

    if flat:
        k_final = k_sil_near_elbow
        reason = "silhouette_flat_near_elbow"
    else:
        k_final = k_sil_max
        reason = "silhouette_max"

    k_final = max(k_final, K_FLOOR)
    return {
        "k_silhouette_max": k_sil_max,
        "k_elbow": k_elbow,
        "k_silhouette_near_elbow": k_sil_near_elbow,
        "k_final": k_final,
        "k_floor_applied": k_final == K_FLOOR and k_final != k_sil_max,
        "decision_reason": reason,
        "silhouette_margin": margin,
        "flat_margin_threshold": flat_margin,
    }


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main() -> int:
    log.info("=== TF-IDF + K-Means analytics ===")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_corpus()
    df, dropped_mega = drop_mega_statutes(df)
    df = drop_empty_content(df)

    log.info("Working corpus: %d rows", len(df))
    log.info("Year range: %s..%s", df["ra_year"].min(), df["ra_year"].max())
    log.info(
        "Null approval_date in working corpus: %d",
        int(df["approval_date"].isna().sum()),
    )

    # --- Step 2: vectorize + k-sweep ---
    X, vec = build_tfidf(df["content_normalized"])
    sweep = k_sweep(X)
    choice = pick_final_k(sweep)
    log.info(
        "Final k: %d | reason=%s | elbow=%d | sil_max=%d | sil_near_elbow=%d | margin=%.4f",
        choice["k_final"],
        choice["decision_reason"],
        choice["k_elbow"],
        choice["k_silhouette_max"],
        choice["k_silhouette_near_elbow"],
        choice["silhouette_margin"],
    )

    # Refit at final k so we can report its inertia and silhouette.
    km_final = KMeans(
        n_clusters=choice["k_final"], random_state=RANDOM_STATE, n_init=10
    )
    labels_final = km_final.fit_predict(X)

    rng = np.random.default_rng(RANDOM_STATE)
    n_sample = min(SILHOUETTE_SAMPLE, X.shape[0])
    sample_idx = np.sort(rng.choice(X.shape[0], size=n_sample, replace=False))
    final_sil = float(silhouette_score(X[sample_idx], labels_final[sample_idx]))
    log.info(
        "Final silhouette (k=%d): %.4f", choice["k_final"], final_sil
    )

    metrics = {
        "k": sweep["k"],
        "inertia": sweep["inertia"],
        "silhouette": sweep["silhouette"],
        "final_k": choice["k_final"],
        "final_inertia": float(km_final.inertia_),
        "final_silhouette": final_sil,
        "k_silhouette_max": choice["k_silhouette_max"],
        "k_elbow": choice["k_elbow"],
        "k_floor_applied": choice["k_floor_applied"],
        "vocabulary_size": int(len(vec.vocabulary_)),
        "n_docs_fit": int(X.shape[0]),
        "n_docs_sampled_for_silhouette": n_sample,
        "dropped_mega_statutes": dropped_mega,
        "random_state": RANDOM_STATE,
        "tfidf_kwargs": dict(TFIDF_KWARGS),
        "decision_reason": choice["decision_reason"],
        "k_silhouette_near_elbow": choice["k_silhouette_near_elbow"],
        "silhouette_margin": choice["silhouette_margin"],
        "flat_margin_threshold": choice["flat_margin_threshold"],
    }
    atomic_write_json(metrics, OUT_DIR / "metrics.json")
    log.info(
        "Wrote %s", (OUT_DIR / "metrics.json").relative_to(REPO_ROOT)
    )

    log.info("=== Step 2 complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())