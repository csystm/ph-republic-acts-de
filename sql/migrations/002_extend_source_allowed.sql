-- 002_extend_source_allowed.sql
-- Extend the ra_master.source CHECK constraint to include 'elibrary_json'.
--
-- We currently do not retag any curated row with this source value, and no
-- E-Library rows enter ra_master. The constraint is extended so the schema
-- matches the data contract (docs/data_contract.yaml v1.1) and so any future
-- re-run that does introduce such rows would not fail the CHECK.
--
-- Idempotent: DROP IF EXISTS makes this safe to re-apply.

ALTER TABLE ra_master DROP CONSTRAINT IF EXISTS source_allowed;

ALTER TABLE ra_master ADD CONSTRAINT source_allowed
    CHECK (source IN ('bettergov_parquet', 'lawphil_html', 'elibrary_json'));