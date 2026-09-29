# Data Dictionary — `ra_master`

Reference for every field in the curated `ra_master.parquet` dataset. This is the
canonical field list; the [data contract](data_contract.yaml) cites it for schema.

| Field | Type | Nullable | Description | Example |
|---|---|---|---|---|
| `ra_id` | string | no | Synthetic primary key. `"RA-" + ra_number`, uppercased. Stable across pipeline runs and across sources. | `RA-10960` |
| `ra_number` | string | no | RA number as it appears in the source. Digits with an optional trailing lowercase letter for amended/lettered acts. | `10960`, `10960a` |
| `ra_year` | int | no | Year of enactment. Derived from the basename, the approval date, or the inline header date — in that order of preference. | `2015` |
| `title` | string | no | Official short title. Falls back to `"Republic Act No. {ra_number}"` when the source has no usable title line. | `"AN ACT GRANTING..."` |
| `approval_date` | string (ISO 8601) | **yes** | Date of approval in `YYYY-MM-DD` form. Null when the source text omits an `Approved:` line and no inline date is present (~35% of rows). | `2015-07-16` |
| `content_clean` | string | no | Markdown-stripped body text. Human-readable. Boilerplate, casing, and whitespace are preserved as in the source. | `"Section 1. Any person..."` |
| `content_normalized` | string | no | Lowercased, boilerplate-stripped, whitespace-collapsed form. Purpose-built for tokenization in the analytics layer. | `"any person who..."` |
| `content_length` | int | no | Character count of `content_clean`. | `445` |
| `word_count` | int | no | Whitespace-delimited word count of `content_clean`. | `73` |
| `source` | string | no | Provenance tag. One of `bettergov_parquet` or `lawphil_html`. | `bettergov_parquet` |
| `source_path` | string | no | Path to the source artifact relative to the provider's root. Traceability from curated row back to raw file. | `datasets/markdown/repacts/1946/ra_10_1946.md` |

## Notes

- **`ra_id` is synthetic.** It is not a source-native identifier. Postgres
  enforces an additional `UNIQUE (ra_number, ra_year)` constraint to catch
  hypothetical collisions where the same number could exist in two years.
- **`approval_date` nullability is a contract, not a bug.** See §Known
  Limitations in the data contract. Downstream analytics must fall back to
  `ra_year` when this field is null.
- **`content_clean` vs `content_normalized`.** Two forms on purpose. The first
  is for human inspection; the second is for tokenization. Tokenization itself
  is deliberately *not* persisted — different analyses need different
  tokenizers, and the vectorizer does it internally.