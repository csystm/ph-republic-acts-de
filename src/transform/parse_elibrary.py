"""Parse Supreme Court E-Library JSON pages into a staged Parquet.

Source:  data/raw/elibrary/pages/page_*.json
         DataTables-shaped responses from POST /republic_acts/fetch_ra.
         Each row is a 3-tuple: [title, approval_date, html_anchor].
Output:  data/staging/ra_elibrary_staged.parquet

Classified as ``elibrary_json`` in docs/data_contract.yaml.

The E-Library index is **metadata-only** — it carries no body text. To keep
the staged schema aligned with the other two sources (BetterGov, Lawphil),
``content_clean`` is populated from the title. Downstream Policy B uses this
staged frame only as a lookup table for ``approval_date`` gap-fill; E-Library
rows never enter the curated set and therefore never participate in the
content-non-stub validation.

Known data-quality facts (see docs/source_profiling.md):
  - 12,467 total rows; 12,153 match ^REPUBLIC ACT NO\\. \\d+$
  - 314 non-RA rows (IRR documents) dropped here
  - Approval dates are 100% ISO-8601 already
  - doc_id extractable from every anchor (100%)
  - 14 duplicate ra_number values across pages; first-seen kept
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from src.utils.config import settings
from src.utils.io import atomic_write_parquet
from src.utils.logger import get_logger

logger = get_logger(__name__)

SOURCE = "elibrary_json"

# Title form in the JSON's first element, e.g. "REPUBLIC ACT NO. 12325".
# Anchored and trailing-whitespace tolerant; do not loosen — non-matching
# rows are IRR/resolution documents that must not be staged.
RA_TITLE_RE = re.compile(r"^REPUBLIC ACT NO\. (\d+)\s*$")

# Anchor HTML contains a link to the doc, e.g.:
#   <a href='https://elibrary.judiciary.gov.ph/thebookshelf/showdocs/2/95614'>...</a>
# We extract the provider-relative path (everything after the domain), which
# matches the source_path convention of the other two sources (relative, not
# full URL).
ANCHOR_PATH_RE = re.compile(
    r"href=['\"]https?://elibrary\.judiciary\.gov\.ph(/[^'\"]+)['\"]"
)
# Also validate the path shape so malformed anchors are caught, not silently
# stored.
DOC_PATH_SHAPE_RE = re.compile(r"^/thebookshelf/showdocs/\d+/\d+$")

ANCHOR_INNER_RE = re.compile(r"<a[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)

def parse_elibrary() -> pd.DataFrame:
    raw_dir = settings.raw_dir / "elibrary"
    pages_dir = raw_dir / "pages"
    manifest_path = raw_dir / "_manifest.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        logger.warning(
            "E-Library manifest is not marked complete (%s); parsing anyway.",
            manifest.get("complete"),
        )

    if not pages_dir.exists():
        raise FileNotFoundError(f"Pages dir not found: {pages_dir}")

    page_files = sorted(pages_dir.glob("page_*.json"))
    if not page_files:
        raise FileNotFoundError(f"No page_*.json files in {pages_dir}")
    logger.info("Found %d page files in %s", len(page_files), pages_dir)

    rows: list[dict] = []
    skipped_non_ra = 0
    skipped_no_doc_id = 0
    skipped_malformed = 0

    for f in page_files:
        payload = json.loads(f.read_text(encoding="utf-8"))
        data = payload.get("data", [])
        for entry in data:
            if not isinstance(entry, list) or len(entry) < 3:
                skipped_malformed += 1
                continue
            title, date_str, anchor = entry[0], entry[1], entry[2]

            m = RA_TITLE_RE.match(title or "")
            if not m:
                skipped_non_ra += 1
                continue
            ra_number = m.group(1)

            path_m = ANCHOR_PATH_RE.search(anchor or "")
            if not path_m:
                skipped_no_doc_id += 1
                continue
            rel_path = path_m.group(1)  # e.g. "/thebookshelf/showdocs/2/95614"
            if not DOC_PATH_SHAPE_RE.match(rel_path):
                skipped_no_doc_id += 1
                continue
            source_path = rel_path.lstrip("/")  # provider-relative

            approval_date = date_str if date_str else None
            ra_year = int(approval_date[:4]) if approval_date else None

            inner_m = ANCHOR_INNER_RE.search(anchor or "")
            descriptive_title = (inner_m.group(1) if inner_m else "").strip()
            # \u00a0 is non-breaking space; normalize to regular space so
            # word_count and downstream tokenization behave sanely.
            descriptive_title = descriptive_title.replace("\u00a0", " ")
            descriptive_title = re.sub(r"\s+", " ", descriptive_title).strip()

            # Prefer the descriptive title; fall back to the RA label if the
            # anchor's inner text is missing or suspiciously short.
            display_title = descriptive_title if len(descriptive_title) >= 15 else title
            content = display_title

            rows.append({
                "ra_id": f"RA-{ra_number}",
                "ra_number": ra_number,
                "ra_year": ra_year,
                "title": display_title,
                "approval_date": approval_date,
                "content_clean": content,
                "content_length": len(content),
                "word_count": len(content.split()),
                "source": SOURCE,
                "source_path": source_path,
            })

    logger.info("Parsed %d E-Library RA rows", len(rows))
    logger.info("Skipped %d non-RA rows (title filter)", skipped_non_ra)
    logger.info("Skipped %d rows missing doc_id", skipped_no_doc_id)
    if skipped_malformed:
        logger.warning("Skipped %d malformed rows (short tuple)", skipped_malformed)

    df = pd.DataFrame(rows)

    # Dedupe by ra_id, keep first — same convention as
    # merge_sources.dedupe_within_source. Page order is deterministic
    # (sorted glob), so "first" is reproducible across runs.
    before = len(df)
    if before:
        dup_mask = df["ra_id"].duplicated(keep="first")
        if dup_mask.any():
            for _, row in df.loc[dup_mask, ["ra_id", "ra_year", "source_path"]].iterrows():
                logger.warning(
                    "  duplicate ra_id dropped: ra_id=%s year=%s source_path=%s",
                    row["ra_id"], row["ra_year"], row["source_path"],
                )
        df = df[~dup_mask].copy()
        logger.info("Dropped %d duplicate ra_id rows", before - len(df))

        logger.info("Null title:      %d", int(df["title"].isna().sum()))
        logger.info("Null date:       %d", int(df["approval_date"].isna().sum()))
        logger.info("Null year:       %d", int(df["ra_year"].isna().sum()))
        logger.info("Year range:      %s – %s",
                    df["ra_year"].min(), df["ra_year"].max())

    return df


def write_staging(df: pd.DataFrame) -> Path:
    out = settings.staging_dir / "ra_elibrary_staged.parquet"
    return atomic_write_parquet(df, out)


if __name__ == "__main__":
    staged = parse_elibrary()
    write_staging(staged)