"""Merge BetterGov and Lawphil staged Parquets into the curated ra_master.

Policy (see docs/source_profiling.md §5):
  - Union of BetterGov and Lawphil on ``ra_id``.
  - On overlap, keep the BetterGov row; log Lawphil rows dropped.
  - Drop stub rows (``word_count < MIN_WORD_COUNT``).
  - Policy B: gap-fill null ``approval_date`` values from the E-Library
    staged index. Match on ``ra_number``; nulls only; no new rows; no
    ``source`` retagging. E-Library rows never enter the curated set.
  - Add ``content_normalized`` for downstream analytics.
  - Write a single ``data/curated/ra_master.parquet`` atomically.

Diagnostics (duplicates, oversized rows, empty content) are logged before
any drop rule is applied so the choices are auditable.
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd

from src.utils.config import settings
from src.utils.io import atomic_write_parquet
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Tunable policy

MIN_WORD_COUNT = 5             # rows below this are stubs, dropped
OVERSIZED_ABSOLUTE = 500_000   # chars — log any row above this regardless of pctl

OUTPUT_COLUMNS = [
    "ra_id",
    "ra_number",
    "ra_year",
    "title",
    "approval_date",
    "content_clean",
    "content_normalized",
    "content_length",
    "word_count",
    "source",
    "source_path",
]


# Normalization patterns

_WS = re.compile(r"\s+")

_RA_HEADER = re.compile(
    r"\brepublic act (?:no|number)\.?\s*\d+[a-z]?\b",
    re.IGNORECASE,
)

_ENACT_CLAUSE = re.compile(
    r"\bbe it enacted by the senate and house of representatives[^:]*:",
    re.IGNORECASE,
)

# Section markers: "Section 1.", "Sec. 12.", "SECTION 3." — remove marker, keep text
_SECTION_MARK = re.compile(
    r"\b(?:sec(?:tion)?\.?)\s*\d+[a-z]?\.\s*",
    re.IGNORECASE,
)

# Line that *starts* with "Approved" — legal tail. Do not use DOTALL.
_APPROVED_LINE = re.compile(
    r"^\s*approved\s*:.*$",
    re.IGNORECASE | re.MULTILINE,
)


def normalize_content(text: str) -> str:
    """Produce ``content_normalized``: lowercase, boilerplate-stripped,
    whitespace-collapsed. Tokenization is deliberately *not* performed here."""
    if not isinstance(text, str) or not text:
        return ""
    s = unicodedata.normalize("NFKC", text)
    # Fancy quotes / dashes → ASCII equivalents
    s = (
        s.replace("\u2018", "'").replace("\u2019", "'")
        .replace("\u201c", '"').replace("\u201d", '"')
        .replace("\u2013", "-").replace("\u2014", "-")
        .replace("\u00a0", " ")
    )
    s = s.lower()
    s = _RA_HEADER.sub(" ", s)
    s = _ENACT_CLAUSE.sub(" ", s)
    s = _SECTION_MARK.sub(" ", s)
    s = _APPROVED_LINE.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


# Diagnostics

def _log_bettergov_dupes(df: pd.DataFrame) -> None:
    dups = df[df["ra_id"].duplicated(keep=False)].sort_values("ra_id")
    if dups.empty:
        logger.info("BetterGov: no duplicate ra_id")
        return
    logger.warning(
        "BetterGov: %d rows share %d ra_id value(s)",
        len(dups), dups["ra_id"].nunique(),
    )
    for _, row in dups.iterrows():
        logger.warning(
            "  ra_id=%s year=%s len=%s words=%s source_path=%s",
            row["ra_id"], row["ra_year"],
            row["content_length"], row["word_count"], row["source_path"],
        )


def _log_oversized(df: pd.DataFrame) -> None:
    """Log rows larger than OVERSIZED_ABSOLUTE so the operator can inspect
    them. Not a failure condition — the corpus legitimately contains codified
    mega-statutes with extensive annexes. Any change to the source snapshot
    will show up here as a new entry; nothing is allowlisted by ra_id.
    """
    big = (
        df[df["content_length"] > OVERSIZED_ABSOLUTE]
        .sort_values("content_length", ascending=False)
    )
    if big.empty:
        logger.info(
            "No rows exceed %d chars", OVERSIZED_ABSOLUTE
        )
        return
    logger.info(
        "%d row(s) exceed %d chars — review before relying on them downstream:",
        len(big), OVERSIZED_ABSOLUTE,
    )
    for _, row in big.iterrows():
        logger.info(
            "  ra_id=%s year=%s len=%d words=%d title=%r",
            row["ra_id"], row["ra_year"], row["content_length"],
            row["word_count"], (row["title"] or "")[:100],
        )


def _log_stubs(df: pd.DataFrame) -> None:
    stubs = df[df["word_count"] < MIN_WORD_COUNT]
    if stubs.empty:
        logger.info("BetterGov: no stub rows (word_count < %d)", MIN_WORD_COUNT)
        return
    logger.warning(
        "BetterGov: %d stub row(s) (word_count < %d)",
        len(stubs), MIN_WORD_COUNT,
    )
    for _, row in stubs.iterrows():
        logger.warning(
            "  ra_id=%s year=%s words=%s title=%r source_path=%s",
            row["ra_id"], row["ra_year"],
            row["word_count"], (row["title"] or "")[:80], row["source_path"],
        )


# Steps

def load_staging() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    bg_path = settings.staging_dir / "ra_bettergov_staged.parquet"
    lp_path = settings.staging_dir / "ra_lawphil_staged.parquet"
    el_path = settings.staging_dir / "ra_elibrary_staged.parquet"

    logger.info("Reading %s", bg_path)
    bg = pd.read_parquet(bg_path)
    logger.info("  -> %d rows", len(bg))

    logger.info("Reading %s", lp_path)
    lp = pd.read_parquet(lp_path)
    logger.info("  -> %d rows", len(lp))

    if el_path.exists():
        logger.info("Reading %s", el_path)
        el = pd.read_parquet(el_path)
        logger.info("  -> %d rows", len(el))
    else:
        logger.warning(
            "E-Library staging not found at %s; Policy B gap-fill disabled",
            el_path,
        )
        el = pd.DataFrame(columns=["ra_number", "approval_date"])

    return bg, lp, el


def diagnose(bg: pd.DataFrame) -> None:
    logger.info("--- Pre-merge diagnostics (BetterGov) ---")
    _log_bettergov_dupes(bg)
    _log_oversized(bg)
    _log_stubs(bg)
    logger.info("--- end diagnostics ---")


def drop_stubs(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    out = df[df["word_count"] >= MIN_WORD_COUNT].copy()
    logger.info("Dropped %d stub rows (word_count < %d)", before - len(out), MIN_WORD_COUNT)
    return out


def dedupe_within_source(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    out = df.drop_duplicates(subset="ra_id", keep="first").copy()
    logger.info("Dropped %d intra-source duplicate rows", before - len(out))
    return out


def merge_sources(bg: pd.DataFrame, lp: pd.DataFrame) -> pd.DataFrame:
    bg_ids = set(bg["ra_id"])
    lp_overlap = lp["ra_id"].isin(bg_ids)
    n_overlap = int(lp_overlap.sum())
    n_lp_only = len(lp) - n_overlap

    logger.info(
        "Lawphil overlap with BetterGov: %d/%d (dropping overlaps; "
        "%d Lawphil-only rows retained)",
        n_overlap, len(lp), n_lp_only,
    )

    lp_kept = lp[~lp_overlap]
    merged = pd.concat([bg, lp_kept], ignore_index=True)
    logger.info(
        "Merged: %d BetterGov + %d Lawphil-only = %d rows",
        len(bg), len(lp_kept), len(merged),
    )
    return merged

def gap_fill_approval_date(
    df: pd.DataFrame, el: pd.DataFrame
) -> pd.DataFrame:
    """Policy B — fill null ``approval_date`` values from the E-Library index.

    Contract:
      * match on ``ra_number`` (string, may carry a trailing letter)
      * fill nulls only — never overwrite a non-null date
      * do not add rows
      * do not touch any other column
      * do not retag ``source`` — E-Library is a lookup, not a provenance source

    Returns a copy with the same shape as ``df``.
    """
    before_nulls = int(df["approval_date"].isna().sum())
    if before_nulls == 0:
        logger.info("Policy B: no null approval_date values; skipping gap-fill")
        return df

    if el is None or el.empty:
        logger.warning(
            "Policy B: %d null approval_date(s) present but E-Library "
            "staging is empty; nothing to fill",
            before_nulls,
        )
        return df

    # Build lookup from E-Library rows that actually carry a date.
    el_dates = (
        el.loc[el["approval_date"].notna(), ["ra_number", "approval_date"]]
        .drop_duplicates(subset="ra_number", keep="first")
    )
    lookup = dict(zip(el_dates["ra_number"], el_dates["approval_date"]))
    logger.info(
        "Policy B: E-Library lookup has %d distinct ra_number -> date",
        len(lookup),
    )

    df = df.copy()
    null_mask = df["approval_date"].isna()
    fill_values = df.loc[null_mask, "ra_number"].map(lookup)
    recovered = int(fill_values.notna().sum())

    df.loc[null_mask, "approval_date"] = fill_values

    after_nulls = int(df["approval_date"].isna().sum())
    n = len(df)
    reduction_pct = (100.0 * recovered / before_nulls) if before_nulls else 0.0
    logger.info(
        "Policy B: Recovered %d approval_date values from elibrary "
        "(nulls %d -> %d, %.1f%% of gaps filled; overall %.2f%% -> %.2f%%)",
        recovered,
        before_nulls,
        after_nulls,
        reduction_pct,
        (100.0 * before_nulls / n) if n else 0.0,
        (100.0 * after_nulls / n) if n else 0.0,
    )
    return df

def add_normalized(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing content_normalized for %d rows", len(df))
    df = df.copy()
    df["content_normalized"] = df["content_clean"].fillna("").map(normalize_content)
    empty = int((df["content_normalized"].str.len() == 0).sum())
    if empty:
        logger.warning(
            "content_normalized is empty for %d row(s) — "
            "these rows should have been dropped as stubs", empty,
        )
    return df


def project_columns(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns: {missing}")
    return df[OUTPUT_COLUMNS].copy()


def write_curated(df: pd.DataFrame) -> None:
    out = settings.curated_dir / "ra_master.parquet"
    atomic_write_parquet(df, out)


# Entry point

def main() -> None:
    bg, lp, el = load_staging()
    diagnose(bg)

    bg = drop_stubs(bg)
    bg = dedupe_within_source(bg)

    merged = merge_sources(bg, lp)
    merged = gap_fill_approval_date(merged, el)
    merged = add_normalized(merged)
    curated = project_columns(merged)

    logger.info("--- Curated ra_master ---")
    logger.info("Rows:            %d", len(curated))
    logger.info("Distinct ra_id:  %d", curated["ra_id"].nunique())
    logger.info("Distinct years:  %d", curated["ra_year"].nunique())
    logger.info("Year range:      %s – %s",
                curated["ra_year"].min(), curated["ra_year"].max())
    logger.info("Source counts:   %s",
                curated["source"].value_counts().to_dict())
    logger.info("Null title:      %d", int(curated["title"].isna().sum()))
    logger.info("Null approval:   %d", int(curated["approval_date"].isna().sum()))

    write_curated(curated)
    logger.info("Merge complete.")


if __name__ == "__main__":
    main()