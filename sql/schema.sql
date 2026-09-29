-- sql/schema.sql
-- DDL for the curated ra_master table.
-- Source of truth: docs/data_contract.yaml (contract_version 1.0)
-- 11 fields, transcribed 1:1.

CREATE TABLE IF NOT EXISTS ra_master (
    ra_id               TEXT        PRIMARY KEY,
    ra_number           TEXT        NOT NULL,
    ra_year             INTEGER     NOT NULL,
    title               TEXT        NOT NULL,
    approval_date       DATE        NULL,
    content_clean       TEXT        NOT NULL,
    content_normalized  TEXT        NOT NULL,
    content_length      INTEGER     NOT NULL,
    word_count          INTEGER     NOT NULL,
    source              TEXT        NOT NULL,
    source_path         TEXT        NOT NULL,

    CONSTRAINT ra_number_year_unique UNIQUE (ra_number, ra_year),

    -- Hard ceilings mirroring validation_rules in the contract
    CONSTRAINT ra_year_range         CHECK (ra_year BETWEEN 1946 AND 2100),
    CONSTRAINT ra_id_shape           CHECK (ra_id ~ '^RA-[0-9]+[A-Z]?$'),
    CONSTRAINT ra_number_shape       CHECK (ra_number ~ '^[0-9]+[a-z]?$'),
    CONSTRAINT source_allowed        CHECK (source IN ('bettergov_parquet', 'lawphil_html')),
    CONSTRAINT word_count_nonneg     CHECK (word_count >= 0),
    CONSTRAINT content_length_nonneg CHECK (content_length >= 0),
    CONSTRAINT source_path_nonempty  CHECK (length(source_path) > 0)
);

-- Partition-key index. ra_year is the declared partition key and is used
-- by every range filter downstream.
CREATE INDEX IF NOT EXISTS idx_ra_master_year
    ON ra_master (ra_year);

-- Full-text search index. NOTE: PostgreSQL caps tsvector at 1 MB.
-- The three mega-statutes (>500K chars) may exceed this during index build
-- or insert. If CREATE INDEX or the loader fails on those rows, replace with
-- a partial index and add `AND content_length <= 500000` to demo queries:
--     ... USING GIN (to_tsvector('english', content_normalized))
--     WHERE content_length <= 500000;
CREATE INDEX IF NOT EXISTS idx_ra_master_content_fts
    ON ra_master
    USING GIN (to_tsvector('english', content_normalized));