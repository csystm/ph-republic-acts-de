import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# RA number in <title>: "Republic Act No. 116" / "Republic Act No. 10960-A"
TITLE_TAG_PATTERN = re.compile(
    r"Republic\s+Act\s+(?:No\.?|Number)\s*(\d+[A-Za-z]?)",
    re.IGNORECASE,
)

# Meta description prefix to strip
META_PREFIX = re.compile(r"^Republic Acts\s*-\s*", re.IGNORECASE)

# Body start anchor: "Section 1." (with optional bold tag already stripped)
SECTION_1_PATTERN = re.compile(r"\bSection\s+1\.", re.IGNORECASE)

# Enactment clause: "Be it enacted by the ..."
ENACT_PATTERN = re.compile(
    r"Be\s+it\s+enacted\s+by\s+the[^:]*:",
    re.IGNORECASE,
)

# Approval date from manifest: "June 14, 1956"
DATE_TEXT_PATTERN = re.compile(r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})")


def _parse_date_text(text: str) -> str | None:
    if not text:
        return None
    m = DATE_TEXT_PATTERN.search(text)
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(1).capitalize()} {int(m.group(2))} {m.group(3)}",
            "%B %d %Y",
        ).date().isoformat()
    except (ValueError, TypeError):
        return None


def _extract_ra_number(soup: BeautifulSoup, fallback: str | None) -> str | None:
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        m = TITLE_TAG_PATTERN.search(title_tag.string)
        if m:
            return m.group(1)
    # fallback: parse from filename like "ra_6789_1989.html" or "irr_11767_2022.html"
    return fallback


def _extract_title(soup: BeautifulSoup) -> str | None:
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        t = META_PREFIX.sub("", meta["content"]).strip()
        if len(t) > 10:
            return t
    # fallback: first <b> inside blockquote after the header
    blockquote = soup.find("blockquote")
    if blockquote:
        for b in blockquote.find_all("b"):
            txt = b.get_text(" ", strip=True)
            if txt and not TITLE_TAG_PATTERN.match(txt) and len(txt) > 15:
                return txt
    return None


def _extract_body(soup: BeautifulSoup) -> str:
    blockquote = soup.find("blockquote")
    if blockquote is None:
        return ""
    # Collect just the paragraph texts
    paras = blockquote.find_all(["p", "dir"])
    texts: list[str] = []
    for p in paras:
        # Skip the header <p><b>REPUBLIC ACT No. X</b></p>
        t = p.get_text(" ", strip=True)
        if TITLE_TAG_PATTERN.match(t):
            continue
        # Skip enactment clause
        if ENACT_PATTERN.search(t) and len(t) < 300:
            continue
        texts.append(t)
    body = "\n\n".join(texts)
    # Normalize whitespace
    body = re.sub(r"[ \t]+", " ", body)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _extract_ra_number_from_filename(stem: str) -> str | None:
    """stem like 'ra_6789_1989' or 'irr_11767_2022'."""
    m = re.match(r"^ra_(\d+[a-z]?)_\d{4}$", stem, re.IGNORECASE)
    return m.group(1) if m else None


def parse_lawphil() -> pd.DataFrame:
    raw_dir = settings.raw_dir / "lawphil"
    manifest_path = raw_dir / "_sample_manifest.json"
    pages_dir = raw_dir / "pages"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    logger.info("Manifest has %d entries", len(manifest))

    rows: list[dict] = []
    skipped_non_ra = 0
    skipped_unreadable = 0

    for entry in manifest:
        source_path = entry["source_path"]
        if source_path.split("/")[-1].lower().startswith("irr_"):
            skipped_non_ra += 1
            continue

        slug = source_path.replace("/", "__")
        file_path = pages_dir / slug
        if not file_path.exists():
            logger.warning("Missing HTML: %s", file_path)
            skipped_unreadable += 1
            continue

        html = file_path.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")

        stem = slug.replace(".html", "").split("__")[-1]  # ra_6789_1989
        ra_number = _extract_ra_number(soup, _extract_ra_number_from_filename(stem))
        title = _extract_title(soup)
        body = _extract_body(soup)

        if not ra_number:
            logger.warning("Could not determine RA number for %s", slug)
            skipped_unreadable += 1
            continue

        # Year from filename or from approval date
        m_year = re.search(r"_(\d{4})$", stem)
        ra_year = int(m_year.group(1)) if m_year else None

        approval_date = _parse_date_text(entry.get("date_text", ""))
        if ra_year is None and approval_date:
            ra_year = int(approval_date[:4])

        rows.append({
            "ra_id": f"RA-{ra_number}",
            "ra_number": ra_number,
            "ra_year": ra_year,
            "title": title or entry.get("title"),
            "approval_date": approval_date,
            "content_clean": body,
            "content_length": len(body),
            "word_count": len(body.split()),
            "source": "lawphil_html",
            "source_path": source_path,
        })

    logger.info("Parsed %d Lawphil RAs", len(rows))
    logger.info("Skipped %d non-RA (IRR) entries", skipped_non_ra)
    logger.info("Skipped %d unreadable entries", skipped_unreadable)

    df = pd.DataFrame(rows)
    if len(df):
        logger.info("Null title:    %d", int(df["title"].isna().sum()))
        logger.info("Null date:     %d", int(df["approval_date"].isna().sum()))
        logger.info("Null year:     %d", int(df["ra_year"].isna().sum()))
        logger.info("Year range:    %s – %s", df["ra_year"].min(), df["ra_year"].max())

    return df


def write_staging(df: pd.DataFrame) -> Path:
    out_dir = settings.staging_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "ra_lawphil_staged.parquet"
    df.to_parquet(out, index=False)
    logger.info("Wrote %s (%d rows)", out, len(df))
    return out


if __name__ == "__main__":
    staged = parse_lawphil()
    write_staging(staged)