"""Profile every source (raw + staging) and emit structured outputs.

Reads:
  - data/raw/bettergov/repacts.parquet
  - data/raw/lawphil/_sample_manifest.json
  - data/raw/lawphil/pages/*.html
  - data/raw/elibrary/_manifest.json
  - data/raw/elibrary/pages/*.json
  - data/staging/ra_bettergov_staged.parquet
  - data/staging/ra_lawphil_staged.parquet
  - data/staging/ra_elibrary_staged.parquet   (optional; available later)

Writes:
  - data/profiling/sources_profile.json       (machine-readable)
  - data/profiling/sources_profile.txt        (human-readable)

Narrative documentation for the profiling deliverable lives in
``docs/source_profiling.md`` and is hand-authored, citing numbers from the
JSON this module produces. This module does not write to ``docs/``.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.config import Settings
from src.utils.io import atomic_write_json, atomic_write_text
from src.utils.logger import get_logger

logger = get_logger(__name__)

SAMPLE_LEN = 80
HIGH_NULL_PCT = 5.0

REPO_ROOT = Path(__file__).resolve().parents[2]


def _rel(p: Path) -> str:
    """Render a path relative to the repo root as `./posix/path`.

    Falls back to the absolute path if `p` is outside the repo.
    """
    p = Path(p)
    try:
        rel = p.resolve().relative_to(REPO_ROOT)
    except ValueError:
        return p.as_posix()
    return f"./{rel.as_posix()}"


def _truncate(val: Any, n: int = SAMPLE_LEN) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).replace("\n", " ").replace("\r", " ").replace("|", "\\|").strip()
    return (s[: n - 1] + "…") if len(s) > n else s


# ---------------------------------------------------------------------------
# Profiling primitives
# ---------------------------------------------------------------------------

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


def _null_issues(profile: dict[str, Any]) -> list[str]:
    issues = []
    for c in profile["columns"]:
        if c["null"] > 0 and c["null_pct"] >= HIGH_NULL_PCT:
            issues.append(
                f"`{c['name']}` is {c['null_pct']:.2f}% null "
                f"({c['null']}/{profile['rows']})."
            )
        elif 0 < c["null"] and c["null_pct"] < HIGH_NULL_PCT:
            issues.append(
                f"`{c['name']}` has {c['null']} null(s) ({c['null_pct']:.2f}%)."
            )
    return issues


def _absent(note: str) -> dict[str, Any]:
    """Uniform 'source not yet available' shape."""
    return {"present": False, "note": note}


# ---------------------------------------------------------------------------
# Raw profiles
# ---------------------------------------------------------------------------

def _profile_raw_bettergov(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _absent(f"missing: {_rel(path)}")

    df = pd.read_parquet(path)
    logger.info("profiled raw bettergov: %d rows x %d cols", *df.shape)
    profile = profile_dataframe(df)

    n_rows = len(df)
    derived: dict[str, Any] = {}
    if "content" in df.columns:
        derived["duplicate_content"] = int(df["content"].duplicated().sum())
    if "year" in df.columns:
        derived["null_year"] = int(df["year"].isna().sum())
        ys = numeric_summary(df, "year")
        if ys:
            derived["year_min"] = int(ys["min"])
            derived["year_max"] = int(ys["max"])
            derived["year_median"] = int(ys["median"])
    if "basename" in df.columns:
        derived["index_basename_rows"] = int(
            df["basename"].str.match(r"^ra\d{4}$", case=False, na=False).sum()
        )
    if "month" in df.columns:
        derived["null_month"] = int(df["month"].isna().sum())
    if "title" in df.columns:
        derived["autotitle_rows"] = int(
            df["title"].str.match(r"^Ra\d{4}$", case=False, na=False).sum()
        )

    notes = [
        "`month` is 100% null in this snapshot — unusable as a partition key.",
        "Year-level index pages (basename ~ ^raYYYY$) carry no RA-level content "
        "and are excluded from staging.",
        "Duplicate `content` values surface the same markdown blob under two "
        "different basenames; these become duplicate `ra_id` after parsing.",
        "`content` is markdown with embedded links and bold markers; staging "
        "strips these into `content_clean`.",
        "Header presence in `content` is inconsistent — the staging parser "
        "recovers some `ra_number` values from inline `**REPUBLIC ACT No. X**` "
        "headers when basename parsing fails.",
    ]

    return {
        "present": True,
        "path": _rel(path),
        "format": "parquet",
        "provider": "BetterGov Philippines — bettergovph/gov-library (HuggingFace)",
        "license": "CC BY-NC 4.0",
        **profile,
        "derived": derived,
        "notes": notes,
    }


def _profile_raw_lawphil(raw_dir: Path) -> dict[str, Any]:
    manifest = raw_dir / "lawphil" / "_sample_manifest.json"
    pages_dir = raw_dir / "lawphil" / "pages"
    if not manifest.exists():
        return _absent(f"missing: {_rel(manifest)}")

    entries = json.loads(manifest.read_text(encoding="utf-8"))
    df = pd.DataFrame(entries)
    logger.info("profiled raw lawphil manifest: %d entries", len(df))
    profile = profile_dataframe(df)

    pages = sorted(pages_dir.glob("*.html")) if pages_dir.exists() else []
    total_bytes = sum(p.stat().st_size for p in pages)

    derived = {
        "manifest_entries": len(entries),
        "html_pages_on_disk": len(pages),
        "missing_pages": len(entries) - len(pages),
        "total_bytes": total_bytes,
        "avg_page_bytes": int(total_bytes / len(pages)) if pages else 0,
    }

    notes = [
        "Mixed source encodings (mostly Windows-1252). Scraper sets "
        "`r.encoding = r.apparent_encoding` to decode correctly.",
        "Body HTML structure varies between pages (blockquote vs. dir nesting); "
        "parser accepts both.",
        "One malformed href (RA 6789) was NFKC-normalized and quote-stripped "
        "during scraping.",
    ]

    return {
        "present": True,
        "path": _rel(manifest),
        "format": "html (windows-1252 on disk)",
        "provider": "The Lawphil Project — Arellano Law Foundation",
        "license": "Creative Commons (see site)",
        **profile,
        "derived": derived,
        "notes": notes,
    }


_RA_TITLE_RE = re.compile(r"^REPUBLIC ACT NO\. (\d+)\s*$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DOC_ID_RE = re.compile(r"showdocs/\d+/(\d+)")


def _profile_raw_elibrary(raw_dir: Path) -> dict[str, Any]:
    manifest_path = raw_dir / "elibrary" / "_manifest.json"
    pages_dir = raw_dir / "elibrary" / "pages"

    if not manifest_path.exists() or not pages_dir.exists():
        return _absent(
            f"missing: {_rel(manifest_path)} or {_rel(pages_dir)}"
        )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    page_files = sorted(pages_dir.glob("page_*.json"))

    all_rows: list[list] = []
    per_page_counts: list[int] = []
    for f in page_files:
        d = json.loads(f.read_text(encoding="utf-8"))
        rows = d.get("data", [])
        per_page_counts.append(len(rows))
        all_rows.extend(rows)

    n_rows = len(all_rows)
    n_pages = len(page_files)
    logger.info("profiled raw elibrary: %d rows across %d pages", n_rows, n_pages)

    shape_counter: dict[int, int] = {}
    for r in all_rows:
        shape_counter[len(r)] = shape_counter.get(len(r), 0) + 1

    ra_rows, non_ra_rows, ra_numbers = [], [], []
    for r in all_rows:
        title = r[0] if len(r) > 0 else ""
        m = _RA_TITLE_RE.match(title or "")
        if m:
            ra_rows.append(r)
            ra_numbers.append(int(m.group(1)))
        else:
            non_ra_rows.append(r)

    dates_present = [r[1] for r in all_rows if len(r) > 1 and r[1]]
    dates_iso = [d for d in dates_present if _ISO_DATE_RE.match(d)]
    dates_bad = [d for d in dates_present if not _ISO_DATE_RE.match(d)]
    years_from_dates = sorted({int(d[:4]) for d in dates_iso}) if dates_iso else []

    anchors_present = [r[2] for r in all_rows if len(r) > 2 and r[2]]
    doc_ids = []
    for r in all_rows:
        a = r[2] if len(r) > 2 else ""
        m = _DOC_ID_RE.search(a or "")
        if m:
            doc_ids.append(m.group(1))

    num_counter: dict[int, int] = {}
    for n in ra_numbers:
        num_counter[n] = num_counter.get(n, 0) + 1
    dupes = {n: c for n, c in num_counter.items() if c > 1}

    derived: dict[str, Any] = {
        "manifest_pages": manifest.get("pages"),
        "manifest_records_total": manifest.get("records_total"),
        "manifest_records_fetched": manifest.get("records_fetched"),
        "manifest_complete": manifest.get("complete"),
        "page_files_on_disk": n_pages,
        "total_rows": n_rows,
        "row_length_distribution": {str(k): v for k, v in sorted(shape_counter.items())},
        "per_page_min": min(per_page_counts) if per_page_counts else 0,
        "per_page_max": max(per_page_counts) if per_page_counts else 0,
        "ra_rows": len(ra_rows),
        "non_ra_rows": len(non_ra_rows),
        "dates_present": len(dates_present),
        "dates_iso": len(dates_iso),
        "dates_non_iso": len(dates_bad),
        "anchors_present": len(anchors_present),
        "doc_ids_extracted": len(doc_ids),
        "duplicate_ra_numbers": len(dupes),
    }
    if ra_numbers:
        derived["ra_number_min"] = min(ra_numbers)
        derived["ra_number_max"] = max(ra_numbers)
    if years_from_dates:
        derived["approval_year_min"] = years_from_dates[0]
        derived["approval_year_max"] = years_from_dates[-1]

    sample_ra = [r[0] for r in ra_rows[:5]]
    sample_non_ra = [r[0] for r in non_ra_rows[:5] if r]

    notes = [
        "Row schema is a 3-tuple: [title, approval_date (ISO-8601), html_anchor].",
        "Non-RA rows (IRR, resolutions) are dropped at parse via "
        "`^REPUBLIC ACT NO\\. \\d+$` on the title.",
        "Index is metadata-only — no body text. `content_clean` in staging is "
        "populated from the title.",
        "Approval dates are already ISO-8601; no date normalization required.",
        "The `doc_id` extracted from the anchor's `showdocs/<n>/<doc_id>` "
        "path populates `source_path` for the E-Library source.",
    ]

    return {
        "present": True,
        "path": _rel(manifest_path),
        "format": "json (datatables-shaped, POST /republic_acts/fetch_ra)",
        "provider": "Supreme Court of the Philippines — E-Library",
        "license": "Public domain (Philippine government work)",
        "rows": n_rows,
        "cols": 3,
        "columns": [],  # JSON rows are tuples, not named columns
        "derived": derived,
        "notes": notes,
        "samples": {
            "ra_titles_first_5": sample_ra,
            "non_ra_titles_first_5": sample_non_ra,
        },
    }


# ---------------------------------------------------------------------------
# Staging profiles
# ---------------------------------------------------------------------------

def _profile_staging(
    name: str, path: Path, *, check_ra_id_unique: bool = True
) -> dict[str, Any]:
    if not path.exists():
        return _absent(f"not yet generated: {_rel(path)}")

    df = pd.read_parquet(path)
    logger.info("profiled staging %s: %d rows x %d cols", name, *df.shape)
    profile = profile_dataframe(df)

    numeric_cols = [c for c in ("ra_year", "content_length", "word_count")
                    if c in df.columns]
    numeric = {c: numeric_summary(df, c) for c in numeric_cols}
    numeric = {k: v for k, v in numeric.items() if v}

    derived: dict[str, Any] = {}
    if check_ra_id_unique and "ra_id" in df.columns:
        derived["duplicate_ra_id"] = int(df["ra_id"].duplicated().sum())
    if "ra_year" in df.columns:
        derived["null_ra_year"] = int(df["ra_year"].isna().sum())
        ys = numeric_summary(df, "ra_year")
        if ys:
            derived["ra_year_min"] = int(ys["min"])
            derived["ra_year_max"] = int(ys["max"])
    if "title" in df.columns:
        derived["empty_title"] = int(
            (df["title"].astype(str).str.strip() == "").sum()
        )
    null_notes = _null_issues(profile)

    return {
        "present": True,
        "path": _rel(path),
        "format": "parquet",
        **profile,
        "numeric": numeric,
        "derived": derived,
        "notes": null_notes,
    }


# ---------------------------------------------------------------------------
# Text rendering
# ---------------------------------------------------------------------------

def _render_section_txt(data: dict[str, Any], lines: list[str]) -> None:
    for k in ("path", "format", "provider", "license"):
        if k in data:
            lines.append(f"  {k:20s} {data[k]}")
    for k in ("rows", "cols"):
        if k in data:
            lines.append(f"  {k:20s} {data[k]:,}")

    if data.get("columns"):
        lines.append("")
        lines.append("  COLUMNS")
        lines.append(
            f"  {'name':26s} {'dtype':14s} {'non_null':>10s} {'null':>8s} "
            f"{'null%':>7s} {'uniq':>8s}  sample"
        )
        lines.append("  " + "-" * 100)
        for c in data["columns"]:
            lines.append(
                f"  {c['name'][:26]:26s} {c['dtype'][:14]:14s} "
                f"{c['non_null']:>10,} {c['null']:>8,} {c['null_pct']:>7.2f} "
                f"{c['unique']:>8,}  {c['sample'][:40]}"
            )

    if data.get("numeric"):
        lines.append("")
        lines.append("  NUMERIC")
        for col, s in data["numeric"].items():
            lines.append(
                f"    {col:22s} n={s['count']:,}  "
                f"min={s['min']:.4g}  p25={s['p25']:.4g}  "
                f"med={s['median']:.4g}  mean={s['mean']:.4g}  "
                f"p75={s['p75']:.4g}  max={s['max']:.4g}"
            )

    if data.get("derived"):
        lines.append("")
        lines.append("  DERIVED")
        for k, v in data["derived"].items():
            lines.append(f"    {k:32s} {v}")

    if data.get("samples"):
        lines.append("")
        lines.append("  SAMPLES")
        for k, vals in data["samples"].items():
            lines.append(f"    {k}:")
            for v in vals:
                lines.append(f"      - {v!r}")

    if data.get("notes"):
        lines.append("")
        lines.append("  NOTES")
        for n in data["notes"]:
            lines.append(f"    - {n}")


def _render_txt(profile: dict[str, Any]) -> str:
    lines = [
        "=" * 100,
        f"SOURCE PROFILE — generated {profile['generated_at']}",
        "=" * 100,
        "",
    ]
    for src_name, sections in profile["sources"].items():
        for layer, data in sections.items():
            lines.append("-" * 100)
            lines.append(f"{src_name}.{layer}")
            lines.append("-" * 100)
            if not data or not data.get("present", False):
                note = (data or {}).get("note", "not available")
                lines.append(f"  (skipped: {note})")
                lines.append("")
                continue
            _render_section_txt(data, lines)
            lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_profile(settings: Settings) -> dict[str, Any]:
    raw = settings.raw_dir
    stg = settings.staging_dir

    ts = datetime.now(timezone.utc).isoformat()

    sources = {
        "bettergov": {
            "raw": _profile_raw_bettergov(raw / "bettergov" / "repacts.parquet"),
            "staging": _profile_staging(
                "BetterGov", stg / "ra_bettergov_staged.parquet"
            ),
        },
        "lawphil": {
            "raw": _profile_raw_lawphil(raw),
            "staging": _profile_staging(
                "Lawphil", stg / "ra_lawphil_staged.parquet"
            ),
        },
        "elibrary": {
            "raw": _profile_raw_elibrary(raw),
            "staging": _profile_staging(
                "E-Library", stg / "ra_elibrary_staged.parquet"
            ),
        },
    }

    return {"generated_at": ts, "sources": sources}


def main() -> None:
    settings = Settings()
    logger.info("building source profile")

    profile = build_profile(settings)

    out_dir = settings.data_dir / "profiling"
    out_dir.mkdir(parents=True, exist_ok=True)

    json_dest = out_dir / "sources_profile.json"
    txt_dest = out_dir / "sources_profile.txt"

    atomic_write_json(profile, json_dest)
    atomic_write_text(_render_txt(profile), txt_dest)

    logger.info(
        "profile written: %s (%d bytes), %s (%d bytes)",
        json_dest, json_dest.stat().st_size,
        txt_dest, txt_dest.stat().st_size,
    )


if __name__ == "__main__":
    main()