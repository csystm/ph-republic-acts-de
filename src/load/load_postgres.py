"""Load curated ra_master.parquet into PostgreSQL with upsert semantics.

Idempotent: reruns converge to the same table state.
Conflict target: ra_id (primary key).
Conflict action: DO UPDATE on every non-PK column.
"""
from __future__ import annotations

import sys

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.utils.config import settings
from src.utils.logger import get_logger

from sqlalchemy import MetaData, Table, create_engine, text, literal_column

logger = get_logger(__name__)

CURATED_PATH = settings.curated_dir / "ra_master.parquet"
TABLE_NAME = "ra_master"
CHUNK_SIZE = 500

# Non-PK columns, in contract order. Must match sql/schema.sql exactly.
UPDATE_COLS = [
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
ALL_COLS = ["ra_id", *UPDATE_COLS]


# --------------------------------------------------------------------------- #
# Type preparation
# --------------------------------------------------------------------------- #
def _to_date_or_none(value):
    """ISO date string / NaT / None -> datetime.date or None (psycopg2-safe)."""
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date()


def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "approval_date" in df.columns:
        df["approval_date"] = df["approval_date"].apply(_to_date_or_none)

    # pandas nullable Int64 -> object of python ints / None
    for col in ("ra_year", "content_length", "word_count"):
        if col in df.columns:
            df[col] = df[col].astype("Int64").astype("object")

    missing = [c for c in ALL_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Curated parquet is missing columns: {missing}. "
            f"Contract drift? Update docs/data_contract.yaml and re-run merge."
        )
    return df


# --------------------------------------------------------------------------- #
# Loader
# --------------------------------------------------------------------------- #
def load() -> None:
    logger.info("Step E — Postgres load starting")
    logger.info("Curated input: %s", CURATED_PATH)

    if not CURATED_PATH.exists():
        raise FileNotFoundError(
            f"Curated artifact not found: {CURATED_PATH}. "
            f"Run `python -m src.transform.merge_sources` first."
        )

    df = pd.read_parquet(CURATED_PATH)
    logger.info("Read %d rows x %d cols from parquet", len(df), df.shape[1])

    df = _prepare_dataframe(df)
    records = df[ALL_COLS].to_dict(orient="records")

    engine = create_engine(settings.postgres_uri, future=True)
    metadata = MetaData()

    with engine.begin() as conn:
        tbl = Table(TABLE_NAME, metadata, autoload_with=conn)

        before = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar_one()
        logger.info("Rows in %s before load: %d", TABLE_NAME, before)

        inserted = 0
        updated = 0

        for start in range(0, len(records), CHUNK_SIZE):
            chunk = records[start : start + CHUNK_SIZE]

            stmt = pg_insert(tbl).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=[tbl.c.ra_id],
                set_={c: stmt.excluded[c] for c in UPDATE_COLS},
            ).returning(literal_column("(xmax = 0)").label("inserted"))

            results = conn.execute(stmt).scalars().all()
            chunk_inserted = sum(1 for r in results if r)
            inserted += chunk_inserted
            updated += len(results) - chunk_inserted

            logger.info(
                "Chunk %d-%d: %d inserted, %d updated",
                start, start + len(chunk),
                chunk_inserted, len(results) - chunk_inserted,
            )

        after = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar_one()

    logger.info("Inserted: %d | Updated: %d", inserted, updated)
    logger.info("Rows in %s after load: %d (delta %+d)", TABLE_NAME, after, after - before)

    if inserted + updated != len(records):
        raise RuntimeError(
            f"Row-count mismatch: processed {inserted + updated}, expected {len(records)}"
        )


def main() -> int:
    try:
        load()
    except Exception:
        logger.exception("Step E load failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())