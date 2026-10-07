"""Assemble the GitHub Pages site under docs/.

Reads every artifact written by tfidf_cluster / cluster_analysis /
plots / temporal_eda and produces a self-contained static site:

- docs/index.html                — the site
- docs/assets/style.css          — stylesheet
- docs/assets/plots/*            — copies of the Plotly HTML embeds and PNGs
- docs/data/*.json               — pre-computed JSON the site reads
- docs/.nojekyll                 — disable Jekyll processing

All writes are atomic. Re-running is idempotent.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from src.analytics.tfidf_cluster import REPO_ROOT
from src.utils.io import atomic_write_text, get_logger

log = get_logger(__name__)

# --- Source artifacts --------------------------------------------------
OUT_DIR = REPO_ROOT / "outputs" / "analytics"
SRC_PLOTS = OUT_DIR / "plots"
SRC_METRICS = OUT_DIR / "metrics.json"
SRC_TERMS = OUT_DIR / "cluster_terms.json"
SRC_SUMMARY = OUT_DIR / "cluster_summary.json"

# --- Destinations ------------------------------------------------------
DOCS = REPO_ROOT / "docs"
DOCS_ASSETS = DOCS / "assets"
DOCS_PLOTS = DOCS_ASSETS / "plots"
DOCS_DATA = DOCS / "data"
DOCS_INDEX = DOCS / "index.html"
DOCS_CSS = DOCS_ASSETS / "style.css"
DOCS_NOJEKYLL = DOCS / ".nojekyll"

GITHUB_REPO_URL = "https://github.com/csystm/ph-republic-acts-de"

# --- Human-authored cluster theme names --------------------------------
# Curated by inspecting outputs/analytics/cluster_terms.json.
# Kept in source (not in a JSON artifact) so it is version-controlled and
# reviewed, not machine-generated.
CLUSTER_THEMES = {
    0:  "Provincial government & fiscal offices",
    1:  "Telecom station permits",
    2:  "Community education & sports",
    3:  "Hospital appropriations",
    4:  "School name changes",
    5:  "Infrastructure, courts & holidays",
    6:  "City charters",
    7:  "Hospital bed expansion",
    8:  "Barrio creation",
    9:  "Agricultural & vocational appropriations",
    10: "State universities & colleges",
    11: "Electric franchises",
    12: "Numbered amendments (boilerplate)",
    13: "Broadcasting franchises",
    14: "Ice plant franchises",
}

# --- CSS (kept out of f-strings so { } don't need escaping) ------------
CSS = """
body {
  max-width: 74ch;
  margin: 2em auto;
  padding: 0 1em;
  font-family: sans-serif;
  line-height: 1.5;
  color: #222;
}
h1, h2, h3 { line-height: 1.25; }
h2 {
  margin-top: 2em;
  padding-bottom: 0.2em;
  border-bottom: 1px solid #ccc;
  font-size: 1.3em;
}
p.tagline { color: #555; }
a { color: #1f4e79; }
code {
  font-family: monospace;
  font-size: 0.95em;
  background: #f4f4f4;
  padding: 0 0.2em;
}
aside {
  border-left: 3px solid #bbb;
  margin: 1em 0;
  padding: 0 1em;
  color: #444;
}
table {
  border-collapse: collapse;
  width: 100%;
  margin: 1em 0;
}
th, td {
  border: 1px solid #ccc;
  padding: 4px 8px;
  text-align: left;
  vertical-align: top;
}
th { background: #f4f4f4; }
figure { margin: 1.5em 0; }
figure img {
  max-width: 100%;
  height: auto;
  border: 1px solid #ccc;
}
figcaption {
  font-size: 0.9em;
  color: #555;
  margin-top: 0.4em;
}
iframe { width: 100%; border: 1px solid #ccc; }
footer {
  margin-top: 3em;
  border-top: 1px solid #ccc;
  padding-top: 1em;
  color: #555;
  font-size: 0.9em;
}
"""


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _read_json(p: Path) -> dict:
    with p.open(encoding="utf-8") as f:
        return json.load(f)


def _copy(src: Path, dst: Path) -> None:
    """Atomic-ish copy via a temp file + os.replace."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    shutil.copyfile(src, tmp)
    tmp.replace(dst)
    log.info("Copied %s -> %s", src.relative_to(REPO_ROOT), dst.relative_to(REPO_ROOT))


def copy_assets() -> None:
    """Copy Plotly HTML embeds and PNGs into docs/assets/plots/."""
    DOCS_PLOTS.mkdir(parents=True, exist_ok=True)
    wanted = [
        "elbow.png", "silhouette.png",
        "scatter.png", "top_terms.png",
        "temporal_by_year.png", "temporal_by_month.png", "temporal_by_dow.png",
        "scatter.html",
        "elbow.html", "silhouette.html",
        "top_terms.html",
        "temporal_by_year.html", "temporal_by_month.html", "temporal_by_dow.html",
    ]
    for name in wanted:
        src = SRC_PLOTS / name
        if not src.exists():
            log.warning("Missing plot: %s", src.relative_to(REPO_ROOT))
            continue
        _copy(src, DOCS_PLOTS / name)


def copy_data_jsons() -> None:
    """Copy small JSONs into docs/data/ so the site is fully static."""
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    for name in ("metrics.json", "cluster_terms.json", "cluster_summary.json"):
        _copy(OUT_DIR / name, DOCS_DATA / name)


def write_css() -> None:
    atomic_write_text(CSS, DOCS_CSS)
    log.info("Wrote %s", DOCS_CSS.relative_to(REPO_ROOT))


def ensure_nojekyll() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    if not DOCS_NOJEKYLL.exists():
        atomic_write_text("", DOCS_NOJEKYLL)
        log.info("Wrote %s (empty)", DOCS_NOJEKYLL.relative_to(REPO_ROOT))
    else:
        log.info("Already present: %s", DOCS_NOJEKYLL.relative_to(REPO_ROOT))


# ----------------------------------------------------------------------
# HTML rendering
# ----------------------------------------------------------------------
def _cluster_row(cid: str, terms: dict, summary: dict) -> str:
    cid_int = int(cid)
    theme = CLUSTER_THEMES.get(cid_int, f"Cluster {cid_int}")
    info = summary.get(cid, {})
    size = info.get("size", 0)
    ymin = info.get("year_min", "")
    ymax = info.get("year_max", "")
    reps = info.get("representatives", []) or []
    top_terms = ", ".join(terms.get(cid, [])[:10])
    rep_html = "".join(
        f'<div><b>{r["ra_id"]}</b> ({r.get("ra_year","")}) — {str(r.get("title",""))[:110]}</div>'
        for r in reps
    )
    return f"""      <tr>
        <td>C{cid}</td>
        <td>{theme}</td>
        <td>{size:,}</td>
        <td>{ymin}–{ymax}</td>
        <td><small>{top_terms}</small></td>
        <td><small>{rep_html}</small></td>
      </tr>
"""


def render_index_html() -> str:
    metrics = _read_json(SRC_METRICS)
    terms = _read_json(SRC_TERMS)
    summary = _read_json(SRC_SUMMARY)

    k = metrics["final_k"]
    sil = metrics["final_silhouette"]
    k_elbow = metrics["k_elbow"]
    k_sil_max = metrics["k_silhouette_max"]
    n_docs = metrics["n_docs_fit"]
    vocab = metrics["vocabulary_size"]
    n_sample = metrics["n_docs_sampled_for_silhouette"]
    reason = metrics.get("decision_reason", "")

    rows = "".join(
        _cluster_row(cid, terms, summary)
        for cid in sorted(terms.keys(), key=lambda x: int(x))
    )

    html = INDEX_TEMPLATE
    replacements = {
        "__K__": str(k),
        "__SIL__": f"{sil:.4f}",
        "__K_ELBOW__": str(k_elbow),
        "__K_SIL_MAX__": str(k_sil_max),
        "__N_DOCS__": f"{n_docs:,}",
        "__VOCAB__": f"{vocab:,}",
        "__N_SAMPLE__": f"{n_sample:,}",
        "__REASON__": reason,
        "__CLUSTER_ROWS__": rows,
        "__REPO_URL__": GITHUB_REPO_URL,
    }
    for k_ph, v in replacements.items():
        html = html.replace(k_ph, v)
    return html


def write_index() -> None:
    html = render_index_html()
    atomic_write_text(html, DOCS_INDEX)
    log.info("Wrote %s", DOCS_INDEX.relative_to(REPO_ROOT))


# ----------------------------------------------------------------------
# HTML template (placeholders substituted above)
# ----------------------------------------------------------------------
INDEX_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Philippine Republic Acts — Similarity Analytics</title>
<meta name="description" content="TF-IDF + K-Means clustering of 11,969 Philippine Republic Acts (1946-2025).">
<link rel="stylesheet" href="./assets/style.css">
</head>
<body>

<header>
  <h1>Philippine Republic Acts — Similarity Analytics</h1>
  <p class="tagline">
    Text mining 11,969 Republic Acts enacted between 1946 and 2025 to
    surface candidate consolidation groups. TF-IDF vectorization,
    K-Means clustering, and t-SNE projection over the curated corpus.
  </p>
  <ul>
    <li><b>__N_DOCS__</b> RAs in working corpus</li>
    <li><b>1946–2025</b> year range</li>
    <li><b>__K__</b> clusters</li>
    <li><b>__SIL__</b> silhouette</li>
    <li><b>__VOCAB__</b> TF-IDF terms</li>
  </ul>
</header>

<main>

<section>
<h2>Corpus provenance</h2>
<p>
  The curated corpus combines <b>three sources in three formats</b>:
  <b>BetterGov.ph</b> (Parquet), <b>Lawphil</b> (HTML requiring DOM
  parsing), and the <b>Supreme Court E-Library</b> (JSON via a POST
  endpoint with a CSRF handshake). Each source is retrieved through a
  distinct pipeline and a distinct parsing strategy.
</p>
<p>
  The E-Library source contributes fields, not rows: a Policy B
  gap-fill recovered <b>4,159 approval dates</b>, reducing the null
  rate from <b>35.23%</b> to <b>0.48%</b> (58 nulls of 11,969).
  That recovery is what makes the date-level EDA below possible.
</p>
</section>

<section>
<h2>Method</h2>
<p>
  Documents are vectorized from the pre-normalized
  <code>content_normalized</code> column using
  <code>TfidfVectorizer(max_features=5000, min_df=5, max_df=0.3,
  stop_words="english")</code>. The <code>max_df=0.3</code> cutoff
  removes corpus-wide boilerplate (e.g. <i>section</i>,
  <i>provided</i>, <i>republic</i>) that would otherwise dominate a
  generic "legislative language" cluster.
</p>
<p>
  The three mega-statutes <b>RA-386 (Civil Code)</b>, <b>RA-5050</b>,
  and <b>RA-8424 (NIRC amendments)</b> are excluded from the TF-IDF
  fit only — they exceed 500K characters each and would otherwise pull
  every centroid toward codified-statute vocabulary. They remain in
  the curated parquet.
</p>
<p>
  K-Means is fit for <b>k = 2..30</b>; both inertia (elbow) and
  silhouette on a 2,000-point sample are recorded. The final
  <b>k = __K__</b> is chosen by an elbow/silhouette consensus rule:
  if the global silhouette maximum is a noise spike (margin &lt; 0.02
  over the elbow-adjacent peak), the elbow-adjacent peak is used.
  Here the silhouette curve is flat past k = __K_ELBOW__, so the
  elbow-adjacent peak (k = __K__) was chosen
  (<code>reason = __REASON__</code>). The global silhouette maximum
  was at k = __K_SIL_MAX__.
</p>
<p>
  For visualization, the TF-IDF matrix is reduced by
  <code>TruncatedSVD(n_components=50)</code> and then projected to 2D
  with <code>TSNE(perplexity=30, init="pca", random_state=42)</code>.
  Silhouette is computed on a sample of <b>__N_SAMPLE__</b> documents
  to keep the O(n^2) cost tractable.
</p>
<aside>
  t-SNE preserves <b>local</b> neighborhoods only. The global
  distances between clusters in the scatter plot below are not
  meaningful; only the grouping of nearby points is.
</aside>
</section>

<section>
<h2>Cluster-count selection</h2>
<figure>
  <img src="./assets/plots/elbow.png" alt="Elbow curve">
  <figcaption>
    KMeans inertia across k. The "elbow" at k = __K_ELBOW__.
    <a href="./assets/plots/elbow.html">interactive</a>
  </figcaption>
</figure>
<figure>
  <img src="./assets/plots/silhouette.png" alt="Silhouette curve">
  <figcaption>
    Silhouette on 2,000 sampled documents. Global max at
    k = __K_SIL_MAX__; final k = __K__.
    <a href="./assets/plots/silhouette.html">interactive</a>
  </figcaption>
</figure>
</section>

<section>
<h2>Corpus similarity — t-SNE projection</h2>
<p>
  Each dot is one Republic Act. Hover over any point to see its
  <code>ra_id</code>, year, and title. Colors correspond to K-Means
  cluster assignments.
</p>
<iframe src="./assets/plots/scatter.html" height="720" loading="lazy"
        title="t-SNE scatter of Republic Acts"></iframe>
</section>

<section>
<h2>Cluster themes and representative acts</h2>
<p>
  For each cluster: the assigned human-readable theme, its size, its
  year range, its top-10 TF-IDF terms, and the three Republic Acts
  closest to the cluster centroid. Cluster themes are hand-assigned
  from the term lists; the term data itself is machine-generated.
</p>
<table>
  <thead>
    <tr>
      <th>ID</th>
      <th>Theme</th>
      <th>Size</th>
      <th>Years</th>
      <th>Top terms</th>
      <th>Representative RAs</th>
    </tr>
  </thead>
  <tbody>
__CLUSTER_ROWS__  </tbody>
</table>
<p>
  <a href="./assets/plots/top_terms.html">Open interactive top-terms chart</a>
</p>
</section>

<section>
<h2>Temporal patterns</h2>
<p>
  With <code>approval_date</code> now 99.5% populated, three
  date-level views become possible.
</p>

<figure>
  <img src="./assets/plots/temporal_by_year.png" alt="RAs enacted per year">
  <figcaption>
    <b>Enactments per year.</b> The 1972–1987 gap is the Martial Law
    period: laws were issued as Presidential Decrees and Batas
    Pambansa, not Republic Acts. The post-1987 surge follows the
    restoration of the bicameral Congress.
    <a href="./assets/plots/temporal_by_year.html">interactive</a>
  </figcaption>
</figure>

<figure>
  <img src="./assets/plots/temporal_by_month.png" alt="RAs by month">
  <figcaption>
    <b>Month of approval.</b> The June spike reflects the Philippine
    Congress's rush to adjournment — the regular session ends in late
    May or early June.
    <a href="./assets/plots/temporal_by_month.html">interactive</a>
  </figcaption>
</figure>

<figure>
  <img src="./assets/plots/temporal_by_dow.png" alt="RAs by day of week">
  <figcaption>
    <b>Day of week.</b> Saturday dominates at ~33% — a pattern that
    appears independently in both source datasets (BetterGov and
    E-Library), so it is not a single-source artifact. It likely
    reflects a publication-date or recording convention rather than
    actual weekend legislative sessions.
    <a href="./assets/plots/temporal_by_dow.html">interactive</a>
  </figcaption>
</figure>
</section>

<section>
<h2>Limitations</h2>
<ul>
  <li>
    Silhouette = <b>__SIL__</b> is weak but non-trivial. Text
    clustering without embeddings rarely exceeds 0.1; values below
    0.05 would indicate no meaningful structure.
  </li>
  <li>
    Cluster <b>C12</b> captures numbered line-item amendment
    boilerplate rather than a policy theme. This is a known limitation
    of bag-of-words representations on template-heavy legal text.
  </li>
  <li>
    t-SNE distances between clusters are not interpretable; only local
    neighborhoods are preserved.
  </li>
  <li>
    The Saturday concentration in the day-of-week chart (~33% of
    enacted dates) <b>replicates across both source datasets</b> —
    33.8% in the original BetterGov dates and 30.5% in the
    E-Library-recovered dates. This rules out a single-source parsing
    artifact and suggests a systemic convention in how RA approval
    dates are recorded or published.
  </li>
</ul>
</section>

</main>

<footer>
  <p>
    <b>DSS150P Data Engineering — Group Beta.</b>
    Analytics produced by <code>src/analytics/</code>.
    Source and pipeline: <a href="__REPO_URL__">__REPO_URL__</a>.
  </p>
  <p>
    Corpus: Republic Acts of the Philippines, 1946–2025.
    Data from public government sources.
  </p>
</footer>

</body>
</html>
"""


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main() -> int:
    log.info("=== Building GitHub Pages site ===")
    DOCS.mkdir(parents=True, exist_ok=True)

    for p in (SRC_METRICS, SRC_TERMS, SRC_SUMMARY, SRC_PLOTS):
        if not p.exists():
            raise FileNotFoundError(
                f"{p.relative_to(REPO_ROOT)} missing. "
                "Run the earlier analytics steps first."
            )

    ensure_nojekyll()
    write_css()
    copy_assets()
    copy_data_jsons()
    write_index()

    log.info("=== Site build complete ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())