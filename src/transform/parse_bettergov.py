import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.utils.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Year-catalog rows (basename "ra1946", no underscore)
INDEX_BASENAME = re.compile(r"^ra\d{4}$", re.IGNORECASE)

# Real RA basename: ra_10_1946, ra_10960_2015, ra_10960a_2015
BASENAME_PATTERN = re.compile(r"^ra_?(\d+[a-z]?)_(\d{4})$", re.IGNORECASE)

# Flexible header: allows **, allows inline date, no end anchor
# Group 1 = RA number ; Group 2 = trailing text on same line (may contain date)
HEADER_LINE = re.compile(
    r"^\s*(?:\*\*)?\s*Republic\s+Act\s+(?:No\.?|Number)\s*(\d+[A-Za-z]?)"
    r"\s*(?:\*\*)?\s*(.*)$",
    re.IGNORECASE | re.MULTILINE,
)

# "Approved: September 2, 1946" or "Approved this 2nd day of September, 1946"
APPROVED_LINE = re.compile(
    r"Approved\s*:?\s*([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})",
    re.IGNORECASE,
)

# Inline date: "September 18, 1946"
INLINE_DATE = re.compile(
    r"([A-Za-z]+)\s+(\d{1,2}),?\s*(\d{4})"
)

# Lines that are NOT titles
SKIP_LINE_PREFIXES = (
    "section", "approved", "approve", "an act appropriating",
    "[", "*", "pdf/", "http", "www.",
)


def clean_markdown(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _try_parse_date(month: str, day: str, year: str) -> str | None:
    try:
        return datetime.strptime(
            f"{month.capitalize()} {int(day)} {year}", "%B %d %Y"
        ).date().isoformat()
    except (ValueError, TypeError):
        return None


def extract_header(raw: str) -> tuple[str | None, str | None, int]:
    """
    Return (ra_number, inline_date_iso, line_index_of_header).
    line_index is -1 if no header found.
    """
    if not isinstance(raw, str):
        return None, None, -1
    lines = raw.splitlines()
    for i, ln in enumerate(lines):
        m = HEADER_LINE.match(ln)
        if m:
            ra_number = m.group(1)
            tail = (m.group(2) or "").strip()
            date_iso = None
            dm = INLINE_DATE.search(tail)
            if dm:
                date_iso = _try_parse_date(*dm.groups())
            return ra_number, date_iso, i
    return None, None, -1


def extract_approval_date(raw: str) -> str | None:
    if not isinstance(raw, str):
        return None
    m = APPROVED_LINE.search(raw)
    if not m:
        return None
    return _try_parse_date(*m.groups())


def extract_title(raw: str, header_line_idx: int) -> str | None:
    """
    Title = first meaningful line after the header line.
    Falls back to first meaningful line if no header was found.
    """
    if not isinstance(raw, str):
        return None
    lines = [ln.strip() for ln in raw.splitlines()]
    start = header_line_idx + 1 if header_line_idx >= 0 else 0
    for ln in lines[start:]:
        cleaned = clean_markdown(ln).strip()
        if not cleaned or len(cleaned) < 15:
            continue
        low = cleaned.lower()
        if any(low.startswith(p) for p in SKIP_LINE_PREFIXES):
            continue
        # Skip a bare date line
        if INLINE_DATE.fullmatch(cleaned):
            continue
        return cleaned
    return None


def extract_body(raw: str, header_line_idx: int) -> str:
    if not isinstance(raw, str):
        return ""
    lines = raw.splitlines()
    start = header_line_idx + 1 if header_line_idx >= 0 else 0
    body = "\n".join(lines[start:]).strip()
    return clean_markdown(body)


def parse_bettergov() -> pd.DataFrame:
    src = settings.raw_dir / "bettergov" / "repacts.parquet"
    logger.info("Reading %s", src)
    df = pd.read_parquet(src)
    logger.info("Loaded %d raw rows", len(df))

    # 1. Drop year index rows
    is_index = df["basename"].str.match(INDEX_BASENAME, na=False)
    logger.info("Dropping %d index rows", int(is_index.sum()))
    df = df[~is_index].copy()

    # 2. Extract RA number + year from basename
    parsed = df["basename"].str.extract(BASENAME_PATTERN)
    df["ra_number"] = parsed[0]
    df["ra_year"] = parsed[1].astype("Int64")

    # 3. Parse content: header, title, dates, body
    logger.info("Parsing headers and titles ...")
    headers = df["content"].apply(extract_header)
    df["_hdr_num"] = [h[0] for h in headers]
    df["_hdr_date"] = [h[1] for h in headers]
    df["_hdr_idx"] = [h[2] for h in headers]

    # Fill missing ra_number from header
    missing = df["ra_number"].isna()
    df.loc[missing, "ra_number"] = df.loc[missing, "_hdr_num"]
    logger.info("Filled %d RA numbers from content headers", int(missing.sum()))

    # Drop rows that still have no RA number (IRRs, omnibus, etc.)
    before = len(df)
    df = df[df["ra_number"].notna()].copy()
    logger.info("Dropped %d non-RA rows (no RA number)", before - len(df))

    df["ra_id"] = "RA-" + df["ra_number"].astype(str)

    # Fill missing ra_year from approval_date, else drop
    needs_year = df["ra_year"].isna() & df["_hdr_date"].notna()
    df.loc[needs_year, "ra_year"] = (
        df.loc[needs_year, "_hdr_date"].str[:4].astype("Int64")
    )
    before = len(df)
    df = df[df["ra_year"].notna()].copy()
    logger.info("Dropped %d rows with no year", before - len(df))

    # Title
    df["title"] = [
        extract_title(c, idx)
        for c, idx in zip(df["content"], df["_hdr_idx"])
    ]
    fallback_title = "Republic Act No. " + df["ra_number"].astype(str)
    df["title"] = df["title"].fillna(fallback_title)

    # Approval date: prefer APPROVED_LINE, else inline header date
    df["approval_date"] = df["content"].apply(extract_approval_date)
    df["approval_date"] = df["approval_date"].fillna(df["_hdr_date"])

    # Body
    df["content_clean"] = [
        extract_body(c, idx)
        for c, idx in zip(df["content"], df["_hdr_idx"])
    ]

    # Metrics
    df["content_length"] = df["content_clean"].str.len().astype("Int64")
    df["word_count"] = df["content_clean"].str.split().str.len().astype("Int64")

    # Provenance
    df["source"] = "bettergov_parquet"
    df["source_path"] = df["path"]

    out_cols = [
        "ra_id", "ra_number", "ra_year", "title", "approval_date",
        "content_clean", "content_length", "word_count",
        "source", "source_path",
    ]
    out = df[out_cols].copy()

    logger.info("Staged %d rows", len(out))
    logger.info("Null ra_number: %d", int(out["ra_number"].isna().sum()))
    logger.info("Null ra_year:   %d", int(out["ra_year"].isna().sum()))
    logger.info("Null title:     %d", int(out["title"].isna().sum()))
    logger.info("Null approval:  %d", int(out["approval_date"].isna().sum()))
    logger.info("Year range:     %s – %s",
                out["ra_year"].min(), out["ra_year"].max())

    return out


def write_staging(df: pd.DataFrame) -> Path:
    out_dir = settings.staging_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "ra_bettergov_staged.parquet"
    df.to_parquet(out, index=False)
    logger.info("Wrote %s (%d rows)", out, len(df))
    return out


if __name__ == "__main__":
    staged = parse_bettergov()
    write_staging(staged)