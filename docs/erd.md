# ERD — `ra_master`

The curated layer is a **single denormalized table**. One row per Republic Act with all text in 
one place, no foreign keys needed.

## Entity-Relationship Diagram

```mermaid
erDiagram
    ra_master {
        TEXT    ra_id               PK "surrogate: 'RA-' + ra_number, uppercased"
        TEXT    ra_number           "UK, pattern ^[0-9]+[a-z]?$"
        INTEGER ra_year             UK "1946..2100 (indexed)"
        TEXT    title
        DATE    approval_date       "nullable (35.2% null)"
        TEXT    content_clean       "human-readable"
        TEXT    content_normalized  "lowercase, boilerplate-stripped (FTS)"
        INTEGER content_length      "chars of content_clean"
        INTEGER word_count          "words of content_clean"
        TEXT    source              "bettergov_parquet | lawphil_html"
        TEXT    source_path         "provider-relative path to raw artifact"
    }