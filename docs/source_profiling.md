# Source Profiling Report

**Last refreshed:** 2026-10-06 (from `data/profiling/sources_profile.json`)
**Project:** DSS150P — Philippine Republic Acts similarity & consolidation
**Scope:** raw + staging layers for all three sources — BetterGov Parquet, Lawphil HTML, Supreme Court E-Library JSON

This is a **hand-authored narrative** on top of machine-generated profiling. The raw figures
come from `python -m src.validation.profile_sources`, which writes
`data/profiling/sources_profile.{txt,json}`. Regenerate that output before treating any
number below as current. Qualitative notes are observations from inspecting the source data
and the ingestion/staging pipeline.

Three sources, three formats, three retrieval pipelines:

| Source              | Format                                       | Retrieval                          | Rows (raw) | Rows (staged) |
|---------------------|----------------------------------------------|------------------------------------|-----------:|--------------:|
| BetterGov (HF)      | Parquet (markdown blob in `content`)         | File download                      | 12,071     | 11,977        |
| Lawphil             | HTML (Windows-1252)                          | HTTP GET + DOM parse               | 200 pages  | 199           |
| SC E-Library        | JSON (datatables-shaped, POST response)      | HTTP POST + CSRF handshake         | 12,467     | 12,139        |

---

## 1. BetterGov — Parquet

**Provider:** BetterGov Philippines — `bettergovph/gov-library` (HuggingFace)
**License:** CC BY-NC 4.0
**Path (raw):** `./data/raw/bettergov/repacts.parquet`
**Path (staged):** `./data/staging/ra_bettergov_staged.parquet`

### 1.1 Raw

**Shape:** 12,071 rows × 8 columns
**Format:** Parquet — a source-faithful snapshot of a HuggingFace dataset with a markdown `content` column.

| Column     | dtype    | Non-null | Null   | Null %  | Unique | Sample |
|------------|----------|---------:|-------:|--------:|-------:|--------|
| `id`       | object   | 12,071   | 0      | 0.00    | 12,071 | `repacts:datasets/markdown/repacts/1946/…` |
| `source`   | object   | 12,071   | 0      | 0.00    | 1      | `repacts` |
| `year`     | int64    | 12,071   | 0      | 0.00    | 67     | `1946` |
| `month`    | object   | 0        | 12,071 | **100.00** | 0   | *(always null)* |
| `path`     | object   | 12,071   | 0      | 0.00    | 12,071 | `datasets/markdown/repacts/1946/ra1946.md` |
| `basename` | object   | 12,071   | 0      | 0.00    | 12,071 | `ra1946` |
| `title`    | object   | 12,071   | 0      | 0.00    | 11,420 | `Ra1946` |
| `content`  | object   | 12,071   | 0      | 0.00    | 12,068 | `NUMBER TITLE [Republic Act No. 89](ra_89_1946.html) October 30, 1946 An …` |

**Year range:** 1946–2025 (median 1971)

**Observed data-quality issues:**

- **`month` is 100% null.** Unusable as a partition key or filter dimension. All partitioning downstream is by `ra_year`.
- **66 year-level index pages.** `basename` matches `^ra\d{4}$` (no underscore) — these are directory-level index rows with no RA-level content and are excluded at staging.
- **3 duplicate `content` values.** The same markdown blob surfaces under two different basenames; these become duplicate `ra_id` after parsing.
- **41 bare `RaYYYY` titles.** Downstream consumers must treat raw `title` as a hint, not a canonical label.
- **Markdown wrapper.** `content` is markdown with embedded links and bold markers; staging strips these into `content_clean`.
- **Inconsistent header presence.** Some `content` blobs omit the `**REPUBLIC ACT No. X**` header. The staging parser falls back to `basename` parsing when the header is absent.
- **94 rows dropped raw → staging.** Combination of the 66 index pages and non-RA artifacts (IRRs, omnibus documents) that yield no extractable RA number.

### 1.2 Staging

**Shape:** 11,977 rows × 10 columns

| Column           | dtype | Non-null | Null   | Null %  | Unique |
|------------------|-------|---------:|-------:|--------:|-------:|
| `ra_id`          | object| 11,977   | 0      | 0.00    | 11,974 |
| `ra_number`      | object| 11,977   | 0      | 0.00    | 11,974 |
| `ra_year`        | Int64 | 11,977   | 0      | 0.00    | 67     |
| `title`          | object| 11,977   | 0      | 0.00    | 10,105 |
| `approval_date`  | object| 7,757    | 4,220  | **35.23** | 1,012 |
| `content_clean`  | object| 11,977   | 0      | 0.00    | 11,958 |
| `content_length` | Int64 | 11,977   | 0      | 0.00    | 5,735  |
| `word_count`     | Int64 | 11,977   | 0      | 0.00    | 2,702  |
| `source`         | object| 11,977   | 0      | 0.00    | 1      |
| `source_path`    | object| 11,977   | 0      | 0.00    | 11,977 |

**Numeric summaries:**

| Column | Count | Min | P25 | Median | Mean | P75 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ra_year`        | 11,977 | 1946 | 1961 | 1971 | 1982 | 2004 | 2025 |
| `content_length` | 11,977 | 0    | 1100 | 1737 | 6977 | 4529 | 2.682e+06 |
| `word_count`     | 11,977 | 0    | 176  | 273  | 1113 | 720  | 4.252e+05 |

**Staging-layer observations:**

- **`approval_date` is 35.23% null** (4,220 / 11,977) — the primary driver for the third-source integration (see §3).
- **3 duplicate `ra_id`** survive staging. Handled at merge time by dedup.
- **Long-tail of codified mega-statutes.** Three RAs exceed 500K characters: RA-386 (Civil Code, 1949), RA-5050 (SEC reorg with full salary annex, 1967), RA-8424 (NIRC amendments, 1997). Legitimate, kept in full; flagged so downstream TF-IDF/clustering can decide whether to weight or cap them.

---

## 2. Lawphil — HTML

**Provider:** The Lawphil Project — Arellano Law Foundation
**License:** Creative Commons (see site)
**Path (raw manifest):** `./data/raw/lawphil/_sample_manifest.json`
**Path (raw pages):** `./data/raw/lawphil/pages/`
**Path (staged):** `./data/staging/ra_lawphil_staged.parquet`

### 2.1 Raw

**Sampling:** seeded random sample of 200 RAs (seed = 42) from an index of ~11,658.
**Format:** HTML on disk, mostly Windows-1252. The scraper sets `r.encoding = r.apparent_encoding` before extraction to avoid mojibake.

**Manifest shape:** 200 entries × 5 columns
**HTML pages on disk:** 200 files, 2,082,254 bytes total (~10.4 KB average per page)

| Column        | dtype | Non-null | Null | Null % | Unique |
|---------------|-------|---------:|-----:|-------:|-------:|
| `ra_label`    | object| 200      | 0    | 0.00   | 200    |
| `url`         | object| 200      | 0    | 0.00   | 200    |
| `date_text`   | object| 200      | 0    | 0.00   | 134    |
| `title`       | object| 200      | 0    | 0.00   | 200    |
| `source_path` | object| 200      | 0    | 0.00   | 200    |

**Observed data-quality issues:**

- **Fetch coverage is complete.** 200 / 200 manifest entries have a saved HTML page (0 missing). The scraper's skip-if-exists logic makes reruns idempotent.
- **One malformed href** (RA 6789) encountered during scraping; the scraper's NFKC normalization and quote-stripping in `_clean_href` repaired it and the page fetched normally.
- **Mixed source encodings.** Naive decoding produces mojibake (`Ã±`, `â€`). `apparent_encoding` resolves the majority; visual spot-checks are still recommended.
- **Structural variance across pages.** Body HTML uses both `<blockquote>` and `<dir>` nesting; the parser accepts both.

### 2.2 Staging

**Shape:** 199 rows × 10 columns (1 non-RA entry with `irr_` filename prefix dropped).

| Column           | dtype | Non-null | Null | Null % | Unique |
|------------------|-------|---------:|-----:|-------:|-------:|
| `ra_id`          | object| 199      | 0    | 0.00   | 199    |
| `ra_number`      | object| 199      | 0    | 0.00   | 199    |
| `ra_year`        | int64 | 199      | 0    | 0.00   | 52     |
| `title`          | object| 199      | 0    | 0.00   | 199    |
| `approval_date`  | object| 198      | 1    | 0.50   | 131    |
| `content_clean`  | object| 199      | 0    | 0.00   | 199    |
| `content_length` | int64 | 199      | 0    | 0.00   | 195    |
| `word_count`     | int64 | 199      | 0    | 0.00   | 181    |
| `source`         | object| 199      | 0    | 0.00   | 1      |
| `source_path`    | object| 199      | 0    | 0.00   | 199    |

**Numeric summaries:**

| Column | Count | Min | P25 | Median | Mean | P75 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ra_year`        | 199 | 1947 | 1960 | 1971 | 1983 | 2007 | 2024 |
| `content_length` | 199 | 319  | 1078 | 2150 | 8215 | 6906 | 1.456e+05 |
| `word_count`     | 199 | 53   | 173  | 335  | 1304 | 1094 | 2.433e+04 |

**Staging-layer observations:**

- **`approval_date` is 0.50% null** (1 / 199) — effectively complete.
- **No duplicate `ra_id`.** Lawphil is a cross-source *validation* set, not a bulk source.

---

## 3. Supreme Court E-Library — JSON

**Provider:** Supreme Court of the Philippines — E-Library
**License:** Public domain (Philippine government work)
**Path (raw manifest):** `./data/raw/elibrary/_manifest.json`
**Path (raw pages):** `./data/raw/elibrary/pages/`
**Path (staged):** `./data/staging/ra_elibrary_staged.parquet`

### 3.1 Format classification

**The E-Library source is JSON via a REST-style POST — distinct from both BetterGov's Parquet and Lawphil's HTML.** Retrieval is a `POST` to `/republic_acts/fetch_ra` that returns a datatables-shaped JSON response; no markup parsing is performed. That gives the project **three wire formats, three retrieval pipelines, and three parsing strategies**: Parquet + file download + columnar read; HTML + GET + DOM parse; JSON + POST + CSRF handshake + structured field read.

### 3.2 Retrieval method

- **Endpoint:** `POST /republic_acts/fetch_ra` on the Supreme Court E-Library host. Page size configured via `ELIBRARY_PAGE_SIZE`.
- **CSRF handshake:** an initial `GET` primes a session cookie and scrapes the CSRF token from the page; the token is then supplied on every `POST`. This is required — the endpoint rejects requests without it.
- **TLS:** the E-Library server sends an **incomplete certificate chain**. Windows AIA-fetching silently repairs this; OpenSSL (Python `requests`, Docker containers) does not. The scraper therefore uses `certifi` as the root store **plus a shipped intermediate** (`certs/gsgccr3evtlsca2025.pem`) via `verify=<bundle>`. This supersedes the earlier `truststore` approach (see `docs/architecture.md` for the trade-off discussion).
- **Rerun control:** `scrape_elibrary --force` bypasses skip-if-exists idempotency and re-fetches all pages. Default behavior is idempotent — a re-run with all pages present is a no-op.

### 3.3 Raw

**Shape (as a tuple):** 12,467 rows × 3 fields — `[title, approval_date (ISO-8601), html_anchor]`
**On-disk layout:** 25 page files (`page_files_on_disk: 25`), ~500 records each.
**Manifest:** `manifest_complete: True` — 12,467 records fetched vs. 12,467 records total.

**Raw observations:**

- **Row schema is a 3-tuple.** `title` is the RA-number label (`"REPUBLIC ACT NO. 12325"`); `approval_date` is already ISO-8601; `html_anchor` carries the descriptive title and a `showdocs/<n>/<doc_id>` href used to populate `source_path`.
- **314 non-RA rows.** IRR, resolutions, and similar artifacts. Dropped at parse via the `^REPUBLIC ACT NO\. \d+$` filter on `title`.
- **14 duplicate RA numbers** in the raw set.
- **`approval_date` present on 100% of rows** (12,467 / 12,467), all ISO-8601, **0 non-ISO.**
- **Metadata only — no body text.** The E-Library index endpoint returns titles and dates, not the RA body. Staging's `content_clean` is therefore populated from the descriptive title, and downstream `content_length` / `word_count` are title-scale, not body-scale.
- **Year range:** 1946–2026 (`approval_year_max: 2026` includes recent filings not yet in BetterGov's snapshot).

### 3.4 Staging

**Shape:** 12,139 rows × 10 columns (314 non-RA rows dropped; 14 duplicate `ra_id` removed at parse).

| Column           | dtype | Non-null | Null | Null % | Unique |
|------------------|-------|---------:|-----:|-------:|-------:|
| `ra_id`          | object| 12,139   | 0    | 0.00   | 12,139 |
| `ra_number`      | object| 12,139   | 0    | 0.00   | 12,139 |
| `ra_year`        | int64 | 12,139   | 0    | 0.00   | 67     |
| `title`          | object| 12,139   | 0    | 0.00   | 12,085 |
| `approval_date`  | object| 12,139   | 0    | **0.00** | 1,740 |
| `content_clean`  | object| 12,139   | 0    | 0.00   | 12,085 |
| `content_length` | int64 | 12,139   | 0    | 0.00   | 519    |
| `word_count`     | int64 | 12,139   | 0    | 0.00   | 104    |
| `source`         | object| 12,139   | 0    | 0.00   | 1      |
| `source_path`    | object| 12,139   | 0    | 0.00   | 12,139 |

**Numeric summaries:**

| Column | Count | Min | P25 | Median | Mean | P75 | Max |
|---|---:|---:|---:|---:|---:|---:|---:|
| `ra_year`        | 12,139 | 1946 | 1961 | 1971 | 1982 | 2004 | 2026 |
| `content_length` | 12,139 | 21   | 136  | 180  | 192.6| 233  | 1,032 |
| `word_count`     | 12,139 | 2    | 21   | 28   | 29.83| 36   | 165   |

**Staging-layer observations:**

- **`approval_date` is 0.00% null** across 12,139 rows — this is the field the third source exists to contribute.
- **No duplicate `ra_id`.**
- **`content_length` is title-scale** (median 180 chars). The staging content column is *not* comparable to BetterGov's or Lawphil's `content_clean`, which carry RA body text. Downstream text analytics must not blend them.

---

## 4. Cross-source comparison

| Source      | Rows (staged) | Year span   | `approval_date` null % | Duplicate `ra_id` |
|-------------|--------------:|-------------|-----------------------:|------------------:|
| BetterGov   | 11,977        | 1946–2025   | 35.23%                 | 3                 |
| Lawphil     | 199           | 1947–2024   | 0.50%                  | 0                 |
| E-Library   | 12,139        | 1946–2026   | 0.00%                  | 0                 |

**Overlap and role:**

- **Lawphil → BetterGov overlap: 199 / 199.** Lawphil contributes no unique RAs. It serves as a **cross-source validation set**: the same acts profiled from an independent HTML pipeline, used to corroborate BetterGov's titles and dates.
- **E-Library → BetterGov overlap: substantial.** E-Library is the broadest-coverage source by RA-number range (`ra_number_max: 12325`) and extends one year past BetterGov's snapshot (2026 vs 2025).
- **Merge policy: Policy B — gap-fill `approval_date` only.** E-Library rows are used exclusively to fill null `approval_date` values in the BetterGov staged rows. They are **not** appended to the curated dataset. The curated `source` column remains `{bettergov_parquet: 11,969}` — no `elibrary_json` rows appear in the final product. This keeps the corpus definition stable (BetterGov as the base) while using E-Library as an enrichment lookup.

**Effect of Policy B on the curated product:**

- Starting BetterGov nulls (staged, post-dedup): 4,217
- Recovered from E-Library: **4,159**
- Remaining nulls in curated: **58** (0.48% of 11,969)
- Null rate: **35.23% → 0.48%** — far below the target of <20%.

---

## 5. Limitations and risks

- **BetterGov is the corpus; the other two are not.** Any statement about "the corpus" refers to the 11,969-row BetterGov-derived curated set unless explicitly stated otherwise.
- **Lawphil is a 200-RA sample, not a bulk source.** It exists to cross-check, not to extend coverage.
- **E-Library is metadata-only.** It carries no RA body text. Any text-analytic downstream (TF-IDF, clustering, summarization) must draw `content_clean` from BetterGov or Lawphil, never E-Library.
- **`ra_id` is a synthetic key.** Stable concatenation of source prefix and RA number — not a source-native identifier. The Postgres schema pairs it with a `UNIQUE (ra_number, ra_year)` constraint to catch collisions.
- **BetterGov `month` column is unusable (100% null).** Partitioning by month is not possible; year-level partitioning is the strategy (see `docs/data_flow.md`).
- **Mojibake risk persists in Lawphil.** `apparent_encoding` mitigates but does not eliminate mixed-encoding artifacts.
- **BetterGov is a static snapshot.** Update frequency is unknown; the pipeline is designed for rerun, not incremental sync. Refresh requires a fresh HuggingFace pull.
- **Codified mega-statutes (three, >500K chars).** RA-386, RA-5050, RA-8424. Kept in full; flagged for downstream weighting decisions.
- **TLS is non-default.** Running in a fresh environment requires the shipped intermediate at `certs/gsgccr3evtlsca2025.pem` (mounted via `docker-compose.yml`). See `certs/README.md` for provenance and refresh procedure.

---

_End of profiling report._