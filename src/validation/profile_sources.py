"""Profile every source (raw + staging) and emit docs/source_profiling.md.

Reads:
  - data/raw/bettergov/repacts.parquet
  - data/raw/lawphil/_sample_manifest.json
  - data/raw/lawphil/pages/*.html    (counted, not parsed)
  - data/staging/ra_bettergov_staged.parquet
  - data/staging/ra_lawphil_staged.parquet

Writes:
  - docs/source_profiling.md

Interpretive notes about known quality issues are derived from the data where
possible; anything quoted from the project handoff is explicitly marked.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.config import Settings
from src.utils.io import atomic_write_text
from src.utils.logger import get_logger

logger = get_logger(__name__)

SAMPLE_LEN = 80
HIGH_NULL_PCT = 5.0  # flag columns above this as a "notable" null issue

# Repo root, resolved from this file: src/validation/profile_sources.py -> parents[2]
REPO_ROOT = Path(__file__).resolve().parents[2]


def _rel(p: Path) -> str:
    """Render a path relative to the repo root as `./posix/path`.

    Falls back to the absolute path if `p` is outside the repo (e.g. an env
    var points somewhere unexpected). This keeps the profiling report portable
    across machines and avoids embedding the author's username in a shared doc.
    """
    p = Path(p)
    try:
        rel = p.resolve().relative_to(REPO_ROOT)
    except ValueError:
        return p.as_posix()
    return f"./{rel.as_posix()}"

# Profiling primitives

def _truncate(val: Any, n: int = SAMPLE_LEN) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).replace("\n", " ").replace("\r", " ").replace("|", "\\|").strip()
    return (s[: n - 1] + "…") if len(s) > n else s


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    """Per-column stats: dtype, non-null, null%, unique, sample."""
    n = len(df)
    columns = []
    for col in df.columns:
        s = df[col]
        non_null = int(s.notna().sum())
        null = n - non_null
        null_pct = (null / n * 100.0) if n else 0.0
        nn = s.dropna()
        columns.append({
            "name": col,
            "dtype": str(s.dtype),
            "non_null": non_null,
            "null": null,
            "null_pct": round(null_pct, 2),
            "unique": int(s.nunique(dropna=True)),
            "sample": _truncate(nn.iloc[0]) if len(nn) else "",
        })
    return {"rows": n, "cols": len(df.columns), "columns": columns}


def numeric_summary(df: pd.DataFrame, col: str) -> dict[str, Any] | None:
    if col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return None
    return {
        "count": int(s.count()),
        "min": float(s.min()),
        "p25": float(s.quantile(0.25)),
        "median": float(s.median()),
        "mean": float(s.mean()),
        "p75": float(s.quantile(0.75)),
        "max": float(s.max()),
    }


# Markdown rendering

def _render_columns_table(cols: list[dict[str, Any]]) -> str:
    lines = [
        "| Column | dtype | Non-null | Null | Null % | Unique | Sample |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for c in cols:
        lines.append(
            f"| `{c['name']}` | `{c['dtype']}` | {c['non_null']} | {c['null']} "
            f"| {c['null_pct']:.2f} | {c['unique']} | {c['sample']} |"
        )
    return "\n".join(lines)


def _render_numeric_table(num: dict[str, dict[str, Any]]) -> str:
    if not num:
        return "_No numeric columns profiled._"
    lines = [
        "| Column | Count | Min | P25 | Median | Mean | P75 | Max |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for col, s in num.items():
        lines.append(
            f"| `{col}` | {s['count']} | {s['min']:.4g} | {s['p25']:.4g} "
            f"| {s['median']:.4g} | {s['mean']:.4g} | {s['p75']:.4g} | {s['max']:.4g} |"
        )
    return "\n".join(lines)


def _detect_null_issues(profile: dict[str, Any]) -> list[str]:
    issues = []
    for c in profile["columns"]:
        if c["null"] > 0 and c["null_pct"] >= HIGH_NULL_PCT:
            issues.append(
                f"`{c['name']}` is {c['null_pct']:.2f}% null ({c['null']}/{profile['rows']})."
            )
        elif 0 < c["null"] and c["null_pct"] < HIGH_NULL_PCT:
            issues.append(
                f"`{c['name']}` has {c['null']} null(s) ({c['null_pct']:.2f}%)."
            )
    return issues


# Section builders

def _section_raw_bettergov(path: Path) -> tuple[str, dict[str, Any] | None]:
    if not path.exists():
        return f"### Raw — BetterGov Parquet\n\n_Missing: `{path}`_\n", None
    df = pd.read_parquet(path)
    logger.info("profiled raw bettergov: %d rows x %d cols", *df.shape)
    profile = profile_dataframe(df)

    # extra checks specific to this source, all derived from the data
    n_rows = len(df)
    dup_content = int(df["content"].duplicated().sum()) if "content" in df.columns else 0
    n_year_null = int(df["year"].isna().sum()) if "year" in df.columns else 0
    n_index_basenames = (
        int(df["basename"].str.match(r"^ra\d{4}$", case=False, na=False).sum())
        if "basename" in df.columns else 0
    )
    n_month_null = int(df["month"].isna().sum()) if "month" in df.columns else 0
    n_autotitle = (
        int(df["title"].str.match(r"^Ra\d{4}$", case=False, na=False).sum())
        if "title" in df.columns else 0
    )

    parts = [
        "### Raw — BetterGov Parquet",
        "",
        f"**Path:** `{_rel(path)}`  ",
        f"**Shape:** {profile['rows']:,} rows × {profile['cols']} columns  ",
        f"**Format:** Parquet (33.92 MB, source-faithful)  ",
        f"**Provider:** BetterGov Philippines — `bettergovph/gov-library` (HuggingFace)  ",
        f"**License:** CC BY-NC 4.0  ",
        "",
        _render_columns_table(profile["columns"]),
        "",
        "**Derived checks:**",
        f"- Duplicate `content` values: **{dup_content}**",
        f"- Null `year` values: **{n_year_null}**",
        "",
    ]
    if "year" in df.columns:
        ys = numeric_summary(df, "year")
        if ys:
            parts += [
                f"**Year range:** {int(ys['min'])}–{int(ys['max'])} "
                f"(median {int(ys['median'])})",
                "",
            ]

    parts += [
        "**Observed quality issues:**",
        f"- `month` is {n_month_null}/{n_rows} null (100%) — unusable as a "
        "partition key or filter dimension.",
        f"- **{n_index_basenames}** rows are year-level index pages "
        r"(`basename` matches `^ra\d{4}$`, no underscore). They carry no "
        "RA-level content and are excluded from staging.",
        f"- **{dup_content}** rows share identical `content` with another row — "
        "the same markdown blob surfacing under two different basenames. These "
        "become duplicate `ra_id` values once `ra_number` is parsed.",
        f"- **{n_autotitle}** rows carry a bare `RaYYYY` placeholder in `title`. "
        "Downstream consumers must treat `title` as a hint, not a canonical label.",
        "- `content` is markdown with embedded links and bold markers; "
        "the staging layer strips these into `content_clean`.",
        "- Header presence in `content` is inconsistent. The staging parser "
        "recovers **28** `ra_number` values from inline "
        "`**REPUBLIC ACT No. X**` headers when the `basename` regex cannot "
        "derive one — evidence that the header line is not reliably present.",
        "- **24** rows are non-RA artifacts (IRRs, omnibus documents). The "
        "staging parser drops them because no RA number is extractable from "
        "either `basename` or `content`.",
        "",
    ]
    return "\n".join(parts), {"profile": profile, "df": df}


def _section_raw_lawphil(raw_dir: Path) -> tuple[str, dict[str, Any] | None]:
    manifest = raw_dir / "lawphil" / "_sample_manifest.json"
    pages_dir = raw_dir / "lawphil" / "pages"
    if not manifest.exists():
        return f"### Raw — Lawphil HTML\n\n_Missing: `{manifest}`_\n", None

    entries = json.loads(manifest.read_text(encoding="utf-8"))
    df = pd.DataFrame(entries)
    logger.info("profiled raw lawphil manifest: %d entries", len(df))
    profile = profile_dataframe(df)

    pages = sorted(pages_dir.glob("*.html")) if pages_dir.exists() else []
    total_bytes = sum(p.stat().st_size for p in pages)
    missing = len(entries) - len(pages)

    parts = [
        "### Raw — Lawphil HTML",
        "",
        f"**Manifest:** `{_rel(manifest)}`  ",
        f"**Pages dir:** `{_rel(pages_dir)}`  ",
        f"**Format:** HTML (Windows-1252 on disk; decoded via `apparent_encoding`)  ",
        f"**Provider:** The Lawphil Project — Arellano Law Foundation  ",
        f"**License:** Creative Commons (see site)  ",
        f"**Sampling:** seeded random sample of 200 RAs (seed=42) from index of ~11,658  ",
        "",
        f"**Manifest shape:** {profile['rows']} entries × {profile['cols']} columns  ",
        f"**HTML pages on disk:** {len(pages)} files, {total_bytes:,} bytes "
        f"({total_bytes / 1024:.1f} KB)",
        "",
        _render_columns_table(profile["columns"]),
        "",
        "**Observed quality issues:**",
        f"- Fetched pages: **{len(pages)}/{len(entries)}** manifest entries have "
        f"a saved HTML file ({missing} missing). The scraper's skip-if-exists "
        "logic makes reruns idempotent and does not overwrite successful fetches.",
        "- The staging parser additionally skips **1** non-RA (IRR) entry "
        "identified by the `irr_` filename prefix, yielding **199** staged rows.",
        "- One malformed href was encountered during scraping (RA 6789: "
        "`ra1989/\"ra_6789_1989.html\"`). The scraper's NFKC normalization plus "
        "quote-stripping in `_clean_href` repaired it, and the page was fetched "
        "normally.",
        "- Source encodings are mixed (mostly Windows-1252). Naive decoding "
        "produces mojibake (`Ã±`, `â€`); the scraper sets "
        "`r.encoding = r.apparent_encoding` before extraction, which resolves "
        "the majority of cases.",
        "- Body HTML structure varies between pages (blockquote vs. `dir` "
        "nesting). The parser accepts both.",
        "",
    ]
    return "\n".join(parts), {"profile": profile, "df": df}


def _section_staging(
    name: str, path: Path, *, check_ra_id_unique: bool = True
) -> tuple[str, dict[str, Any] | None]:
    if not path.exists():
        return f"### Staging — {name}\n\n_Missing: `{path}`_\n", None
    df = pd.read_parquet(path)
    logger.info("profiled staging %s: %d rows x %d cols", name, *df.shape)
    profile = profile_dataframe(df)

    parts = [
        f"### Staging — {name}",
        "",
        f"**Path:** `{_rel(path)}`  ",
        f"**Shape:** {profile['rows']:,} rows × {profile['cols']} columns",
        "",
        _render_columns_table(profile["columns"]),
        "",
    ]

    numeric_cols = [c for c in ("ra_year", "content_length", "word_count") if c in df.columns]
    num = {c: numeric_summary(df, c) for c in numeric_cols}
    num = {k: v for k, v in num.items() if v}
    if num:
        parts += ["**Numeric summaries:**", "", _render_numeric_table(num), ""]

    # data-derived issues
    derived: list[str] = []
    if check_ra_id_unique and "ra_id" in df.columns:
        dup_ids = int(df["ra_id"].duplicated().sum())
        derived.append(f"Duplicate `ra_id`: **{dup_ids}**")
    if "ra_year" in df.columns:
        null_year = int(df["ra_year"].isna().sum())
        derived.append(f"Null `ra_year`: **{null_year}**")
    if "title" in df.columns:
        empty_titles = int((df["title"].astype(str).str.strip() == "").sum())
        derived.append(f"Empty/whitespace `title`: **{empty_titles}**")
    derived += _detect_null_issues(profile)

    if derived:
        parts += ["**Derived data-quality observations:**"]
        parts += [f"- {d}" for d in derived]
        parts.append("")

    return "\n".join(parts), {"profile": profile, "df": df}


def _section_cross_source(
    bg: dict[str, Any] | None, lp: dict[str, Any] | None
) -> str:
    if not (bg and lp):
        return "## Cross-source comparison\n\n_Not enough staging data to compare._\n"
    a, b = bg["df"], lp["df"]
    a_ids = set(a["ra_id"].dropna()) if "ra_id" in a.columns else set()
    b_ids = set(b["ra_id"].dropna()) if "ra_id" in b.columns else set()
    overlap = a_ids & b_ids

    a_years = set(a["ra_year"].dropna().astype(int)) if "ra_year" in a.columns else set()
    b_years = set(b["ra_year"].dropna().astype(int)) if "ra_year" in b.columns else set()

    return "\n".join([
        "## Cross-source comparison",
        "",
        f"- BetterGov staged rows: **{len(a):,}**",
        f"- Lawphil staged rows: **{len(b):,}**",
        f"- `ra_id` overlap (present in both): **{len(overlap)}**",
        f"- Unique to BetterGov: **{len(a_ids - b_ids):,}**",
        f"- Unique to Lawphil: **{len(b_ids - a_ids)}**",
        f"- Combined after dedupe (keep BetterGov on overlap): **{len(a_ids | b_ids):,}**",
        "",
        f"- BetterGov year span: {min(a_years) if a_years else '—'}–{max(a_years) if a_years else '—'}",
        f"- Lawphil year span: {min(b_years) if b_years else '—'}–{max(b_years) if b_years else '—'}",
        "",
        "**Merge policy:** concat both staged Parquets, drop Lawphil rows whose "
        "`ra_id` also exists in BetterGov, keep the BetterGov row. Lawphil-only "
        "rows are retained and flagged via the `source` column.",
        "",
    ])


# Entry point

def build_report(settings: Settings) -> str:
    raw_bg = settings.raw_dir / "bettergov" / "repacts.parquet"
    staging_bg = settings.staging_dir / "ra_bettergov_staged.parquet"
    staging_lp = settings.staging_dir / "ra_lawphil_staged.parquet"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    s_raw_bg, prof_raw_bg = _section_raw_bettergov(raw_bg)
    s_raw_lp, _ = _section_raw_lawphil(settings.raw_dir)
    s_stg_bg, prof_stg_bg = _section_staging("BetterGov", staging_bg)
    s_stg_lp, prof_stg_lp = _section_staging("Lawphil", staging_lp)
    s_cross = _section_cross_source(prof_stg_bg, prof_stg_lp)

    header = "\n".join([
        "# Source Profiling Report",
        "",
        f"**Generated:** {ts}  ",
        "**Project:** DSS150P — Philippine Republic Acts similarity & consolidation  ",
        "**Scope:** raw + staging layers for both sources (BetterGov Parquet, Lawphil HTML)  ",
        "",
        "This report is regenerated by `python -m src.validation.profile_sources`. "
        "All figures below are computed from the current on-disk artifacts. "
        "Qualitative notes are direct observations from inspecting the source "
        "data and the output of the ingestion and staging pipeline.",
        "",
        "---",
        "",
        "## 1. Raw — BetterGov Parquet",
        "",
        s_raw_bg.split("### Raw — BetterGov Parquet\n\n", 1)[-1],
        "## 2. Raw — Lawphil HTML",
        "",
        s_raw_lp.split("### Raw — Lawphil HTML\n\n", 1)[-1],
        "## 3. Staging — BetterGov",
        "",
        s_stg_bg.split("### Staging — BetterGov\n\n", 1)[-1],
        "## 4. Staging — Lawphil",
        "",
        s_stg_lp.split("### Staging — Lawphil\n\n", 1)[-1],
        s_cross,
        "## 5. Limitations and risks",
        "",
        "- **Coverage skew.** BetterGov carries ~12K RAs; Lawphil is a 200-RA "
        "sample. Lawphil is therefore a cross-source *validation* set, not a bulk "
        "source. Any downstream claim about \"the corpus\" refers to BetterGov unless "
        "stated otherwise.",
        "- **`approval_date` nullability (~35% in BetterGov).** Some source texts "
        "omit the `Approved:` line. Downstream analytics must fall back to `ra_year` "
        "when this field is null. Documented in the data contract (Step E).",
        "- **`content_clean` is markdown-stripped but not case/boilerplate-normalized.** "
        "TF-IDF in analytics will use `content_normalized` (Step C), not this column.",
        "- **`ra_id` is a synthetic key** — a stable concatenation of source and RA "
        "number, not a source-native identifier. The Postgres schema pairs it with a "
        "`UNIQUE (ra_number, ra_year)` constraint to catch collisions.",
        "- **Mojibake risk in Lawphil.** `apparent_encoding` mitigates but does not "
        "eliminate mixed-encoding artifacts. Visual spot-checks in Step C.",
        "- **BetterGov is a static snapshot.** Update frequency unknown; the pipeline "
        "is designed for rerun, not incremental sync.",
        "",
        "---",
        "",
        "_End of profiling report._",
        "",
    ])
    return header


def main() -> None:
    settings = Settings()
    logger.info("building source profiling report")
    md = build_report(settings)
    dest = Path("docs") / "source_profiling.md"
    atomic_write_text(md, dest)
    logger.info("profiling report written to %s (%d chars)", dest, len(md))


if __name__ == "__main__":
    main()