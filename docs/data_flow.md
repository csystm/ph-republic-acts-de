# Data Flow / Lineage — Philippine Republic Acts Pipeline

**Companion to:** `docs/architecture.md`
**Purpose:** Show what happens to a record as it moves from external
source to final storage, and reconcile row counts at every boundary.

---

## 1. End-to-End Lineage (linear view)

```
[EXTERNAL]
  BetterGov Parquet (12,071 rows) ──┐
  Lawphil HTML     (200 pages)    ──┤
  E-Library JSON   (12,467 rows)  ──┤
                                     │
                                     ▼
[EXTRACT]
  download_parquet  →  data/raw/bettergov/repacts.parquet
                       (GET via hf_hub_download)
  scrape_lawphil    →  data/raw/lawphil/_index.html
                       data/raw/lawphil/_sample_manifest.json
                       data/raw/lawphil/pages/*.html
                       (HTTP GET + DOM parse)
  scrape_elibrary   →  data/raw/elibrary/_manifest.json
                       data/raw/elibrary/pages/*.json
                       (HTTP POST /republic_acts/fetch_ra
                        with CSRF handshake; verify=<cert bundle>)
                                     │
                                     ▼
[RAW]         source-faithful, no transformation, gitignored
                                     │
                                     ▼
[STAGING]
  parse_bettergov  →  data/staging/ra_bettergov_staged.parquet   (11,977)
  parse_lawphil    →  data/staging/ra_lawphil_staged.parquet     (199)
  parse_elibrary   →  data/staging/ra_elibrary_staged.parquet    (12,139)
                                     │
                                     ▼
[CURATED]
  merge_sources    →  data/curated/ra_master.parquet             (11,969)
                      Policy B: BetterGov base rows kept;
                      elibrary_json used ONLY for gap-fill of
                      approval_date on BetterGov nulls.
                                     │
                                     ▼
[VALIDATE]
  checks           →  outputs/validation_report.json             (9P/1W/0F)
                                     │
                                     ▼
[LOAD]
  load_postgres    →  PostgreSQL table ra_master                 (11,969)
  write_partitions →  data/curated/ra_master_partitioned/        (66 years)
                      outputs/ra_master.csv                      (167 MB)
                      outputs/ra_master.jsonl                    (170 MB)
                      outputs/format_comparison.json
                                     │
                                     ▼
[VERIFY]
  verify_outputs   →  asserts presence of graded artifacts
```

---

## 2. Row-Count Reconciliation

Every number below was observed, not assumed.

| Stage | Rows | Delta | Reason |
|---|---|---|---|
| BetterGov raw | 12,071 | — | source as retrieved |
| Lawphil raw | 200 | — | seeded random sample of ~11,658 listed RAs |
| E-Library raw | 12,467 | — | full index of SC E-Library Republic Acts |
| BetterGov staged | 11,977 | **−94** | 66 year-index rows + non-RA artifacts + stubs dropped during parse |
| Lawphil staged | 199 | **−1** | 1 IRR entry (non-RA) skipped by parser |
| E-Library staged | 12,139 | **−328** | 314 non-RA rows (IRRs, resolutions) + 14 duplicate `ra_number` dropped |
| Curated (post-merge) | 11,969 | **−8 from BetterGov staged** | 5 stubs + 3 intra-source dedupe |
| Lawphil rows retained | 0 | **−199** | 199/199 overlap with BetterGov; Lawphil acts as cross-source validation set |
| E-Library rows retained | 0 | **−12,139** | Policy B: gap-fill only — no E-Library rows enter the curated layer |
| **Final curated** | **11,969** | — | |

**Reconciliation check:** 11,977 − 5 (stubs) − 3 (dedupe) = **11,969** ✓

Lawphil contributed 0 net rows in this snapshot but is retained in the
pipeline as a *validation source*: if Lawphil ever surfaces an RA that
BetterGov lacks, the merge policy retains it (`source="lawphil_html"`).

E-Library contributes 0 rows by design (Policy B). It contributes **fields**:
`approval_date` values gap-filled into BetterGov rows. See §2.1.

### 2.1 Policy B — `approval_date` gap-fill

Policy B is the merge rule that governs the third source. E-Library rows
are not appended to the curated dataset; they serve as a lookup for
`approval_date` values missing on the BetterGov base rows.

| Step | `approval_date` nulls | Rate |
|---|---:|---:|
| BetterGov staged (pre-dedup) | 4,220 | 35.23% |
| BetterGov staged (post-dedup — the curated base) | 4,217 | — |
| After Policy B gap-fill from E-Library | **58** | **0.48%** |
| **Recovered** | **4,159** | — |

**Effect:** the curated `approval_date` column is now 99.52% populated,
far below the project's "<20% null" target. The 58 residual nulls are RAs
whose approval dates are absent from both BetterGov and E-Library.

The `source` column of the curated dataset remains
`{bettergov_parquet: 11,969}` — no `elibrary_json` value appears in the
final product. This is by design: the corpus definition stays stable
(BetterGov-derived), while E-Library enriches a single field.

---

## 3. Column Lineage

Where every curated column comes from.

| Curated column | BetterGov source | Lawphil source | E-Library source | Transformation |
|---|---|---|---|---|
| `ra_id` | derived | derived | derived | `"RA-" + ra_number.upper()` |
| `ra_number` | `basename` or inline header | URL slug | parsed from title (`^REPUBLIC ACT NO\. (\d+)$`) | regex extract; NFKC |
| `ra_year` | `basename` year or approval date | URL slug year | year of `approval_date` | int cast |
| `title` | `title` column | `<title>` or heading | descriptive title from `<a>` inner text | strip; fallback `"Republic Act No. N"` |
| `approval_date` | `approval_date` column | `Approved:` line in body | native ISO-8601 field | parse to ISO; **E-Library used as gap-fill** |
| `content_clean` | `content` (markdown) | HTML body | title (metadata-only source) | markdown/HTML strip → plain text |
| `content_normalized` | `content_clean` | `content_clean` | (not applicable) | NFKC → lowercase → boilerplate strip → whitespace collapse |
| `content_length` | derived | derived | (not applicable) | `len(content_clean)` |
| `word_count` | derived | derived | (not applicable) | `len(content_clean.split())` |
| `source` | literal | literal | literal | `"bettergov_parquet"` / `"lawphil_html"` / `"elibrary_json"` |
| `source_path` | `basename` | manifest entry | `showdocs/<n>/<doc_id>` doc_id | provider-relative path |

**Note on `content_clean` from E-Library.** The E-Library endpoint is
metadata-only — it returns titles and dates, not RA body text. Any
`content_clean` derived from an E-Library row is title-scale (median
~180 chars) and is *not* comparable to the body-scale `content_clean`
produced from BetterGov or Lawphil. Downstream text analytics must draw
`content_clean` from the base sources, never from E-Library.

---

## 4. Worked Example — Tracing RA-49

RA-49 is chosen because it exercises the E-Library integration: its
BetterGov staged row has a null `approval_date`, and it is filled
from the E-Library staged layer. §4.7 gives a companion case (RA-10)
where no gap-fill is needed. Every value below is reproduced from the
on-disk artifacts.

### 4.1 Source — BetterGov Parquet (raw)

Raw row from `data/raw/bettergov/repacts.parquet`:

```
basename:    "ra_49_1946"
title:       "[ REPUBLIC ACT NO. 49, October 03, 1946 ]"
year:        1946
month:       NaN                  ← known 100%-null column
path:        "datasets/markdown/repacts/1946/ra_49_1946.md"
content:     "**[ REPUBLIC ACT NO. 49, October 03, 1946 ]**\n\n
              **AN ACT TO AMEND SECTION FOURTEEN HUNDRED AND
              FOURTEEN OF THE REVISED ADMINISTRATIVE CODE, AS
              AMENDED.**\n\n
              **Be it enacted by the Senate and House of
              Representatives of the Philippines in Congress
              assembled:**\n\n
              **Section 1.** Section fourteen hundred ..."
content_len: 5,946
```

### 4.2 After `parse_bettergov` (BetterGov staging)

```python
{
  "ra_id":          "RA-49",
  "ra_number":      "49",
  "ra_year":        1946,
  "title":          "AN ACT TO AMEND SECTION FOURTEEN HUNDRED AND "
                    "FOURTEEN OF THE REVISED ADMINISTRATIVE CODE, AS AMENDED.",
  "approval_date":  None,          # ← to be filled by Policy B
  "content_clean":  "[ REPUBLIC ACT NO. 49, October 03, 1946 ]\n\n "
                    "AN ACT TO AMEND ... (markdown bold/link markers stripped)",
  "content_length": 5922,
  "word_count":     1100,
  "source":         "bettergov_parquet",
  "source_path":    "datasets/markdown/repacts/1946/ra_49_1946.md",
}
```

`approval_date` is `None` — one of the 4,220 BetterGov staged nulls
(35.23% of the pre-dedup set). RA-49 is **not** in the Lawphil 200-RA
sample, so the Lawphil pipeline contributes 0 rows here.

### 4.3 Staging — E-Library (the enrichment lookup)

Row from `data/staging/ra_elibrary_staged.parquet`:

```python
{
  "ra_id":          "RA-49",
  "ra_number":      "49",
  "ra_year":        1946,
  "title":          "AN ACT TO AMEND SECTION FOURTEEN HUNDRED AND "
                    "FOURTEEN OF THE REVISED ADMINISTRATIVE CODE, AS AMENDED.",
  "approval_date":  "1946-10-03",  # ← the value the gap fill will use
  "content_length": 101,
  "word_count":     16,
  "source":         "elibrary_json",
  "source_path":    "thebookshelf/showdocs/2/22091",
}
```

The E-Library row is metadata-only (title + date). Its `content_length`
(101 chars) is title-scale, **not** body-scale — it is not comparable to
the BetterGov row's 5,922-char body. No text-analytic downstream may
blend them.

### 4.4 After `merge_sources` (curated, post-Policy-B)

`merge_sources` sees the null `approval_date` on the BetterGov row,
looks up the same `ra_id` in the E-Library staged parquet, and applies
the found date. The E-Library row itself is discarded — it does not
appear in `ra_master`.

```python
{
  # ... all BetterGov staging fields unchanged ...
  "approval_date":      "1946-10-03",   # ← gap-filled from elibrary_json
  "content_normalized": "[ , october 03, 1946 ] an act to amend section "
                        "fourteen hundred and fourteen of the revised "
                        "administrative code, as amended. ...",
}
```

**Note on `content_normalized`.** The boilerplate strip removes the
`REPUBLIC ACT NO. 49` token but leaves the surrounding `[ ... , <date> ]`
brackets and date clause in place. This is a known limitation of the
current strip logic for the bracket-header variant of the RA header (the
plain `**REPUBLIC ACT No. 10**` form used by other rows is stripped
cleanly). Not a data-correctness issue — `ra_id` still uniquely
identifies the row — but a text-normalization gap for downstream FTS
tokenization.

Atomic write to `data/curated/ra_master.parquet`.

### 4.5 After `checks` (validation)

RA-49 passes all applicable rules:

- `schema_conformance` — 11 fields present ✓
- `ra_id_unique` — no other row has `RA-49` ✓
- `required_non_null` — all non-nullable fields present ✓
- `year_range` — 1946 ∈ [1946, 2027] ✓
- `content_non_stub` — 1,100 words ≥ 5 ✓
- `content_normalized_non_empty` ✓
- `ra_number_shape` — `"49"` matches `^\d+[a-z]?$` ✓
- `approval_date_valid` — `"1946-10-03"` ISO-parseable ✓
- `source_distribution` (warn-only) — the row's `source` is
  `bettergov_parquet`, which is within the allowed set ✓

### 4.6 Storage — Postgres and partitioned Parquet

```sql
SELECT ra_id, ra_year, approval_date, word_count, source
FROM ra_master WHERE ra_id = 'RA-49';

 ra_id | ra_year | approval_date | word_count |       source
-------+---------+---------------+------------+-------------------
 RA-49 |    1946 | 1946-10-03    |       1100 | bettergov_parquet
```

Partitioned write lands in
`data/curated/ra_master_partitioned/year=1946/ra_master.parquet`.
Reading just that partition avoids scanning the other 65 year folders:

```python
import pandas as pd
df_1946 = pd.read_parquet("data/curated/ra_master_partitioned/year=1946")
# year=1946 partition only — the other 65 year folders are not touched
```

### 4.7 Companion case — RA-10 (no gap-fill needed)

RA-10 is also a 1946 RA, but its BetterGov staged `approval_date` was
already populated. Policy B is a no-op:

| Stage | `approval_date` | Notes |
|---|---|---|
| BetterGov staged | `"1946-09-02"` | present in source |
| E-Library staged | `"1946-09-02"` | same value |
| Curated | `"1946-09-02"` | unchanged by merge |

RA-10's `content_length` is 445, `word_count` is 73 — much smaller than
RA-49's 5,922 / 1,100. Policy B does not care about body size; it only
looks at whether `approval_date` is null. RA-10 takes the no-op path;
RA-49 takes the gap-fill path. Both end up in the same curated
`ra_master` with the same schema and the same `source` value
(`bettergov_parquet`).

---

## 5. Transformation Rules by Stage

### Raw → Staging

| Source | Rule | Rationale |
|---|---|---|
| BetterGov | Drop rows where `basename ~ ^ra\d{4}$` | Year-level index rows, not statutes |
| BetterGov | Drop rows with no extractable RA number | IRRs, omnibus documents |
| BetterGov | Drop `month` column | 100% null in source |
| BetterGov | Strip markdown | `content_clean` must be plain text |
| BetterGov | Recover `ra_number` from inline header when absent | 28 rows recovered |
| Lawphil | Skip IRR entries | Not Republic Acts |
| Lawphil | Decode with `apparent_encoding` | Windows-1252 mojibake mitigation |
| Lawphil | NFKC normalize + strip quotes from hrefs | Handles malformed RA 6789 href |
| E-Library | Drop rows where `title !~ ^REPUBLIC ACT NO\. \d+$` | IRRs, resolutions (314 rows) |
| E-Library | Dedupe on `ra_number` | 14 duplicate RA-number rows in raw |
| E-Library | Parse `doc_id` from anchor href | Populates `source_path` |
| E-Library | Populate `content_clean` from descriptive title | Source is metadata-only |
| Both | Emit `ra_id = "RA-" + ra_number.upper()` | Stable synthetic PK |

### Staging → Curated

| Rule | Rationale |
|---|---|
| Drop `word_count < 5` | Stub rows (empty or near-empty content) |
| Dedupe on `ra_id`, `keep="first"` | 3 known collisions (RA-6426, RA-7663, RA-7688) |
| Drop Lawphil rows whose `ra_id` exists in BetterGov | Cross-source dedupe; Lawphil is a validation set |
| **Policy B — gap-fill `approval_date` from E-Library** | For each row with null `approval_date`, look up the same `ra_number` in the E-Library staged parquet and copy its `approval_date` if present. E-Library rows never enter the curated output. |
| Derive `content_normalized` | NFKC → lowercase → strip boilerplate → collapse whitespace |
| Derive `content_length` | `len(content_clean)` |
| Preserve Lawphil-only rows if present | Defensive: if Lawphil ever surfaces a unique RA |

### Curated → Storage

| Rule | Rationale |
|---|---|
| Coerce `Int64 → int`, ISO `str → datetime.date` | psycopg2 doesn't handle pandas nullable types |
| Upsert on `ra_id`, update all non-PK columns | Idempotent load |
| Partition by `ra_year` | Analytical filter axis |

---

## 6. What Is *Not* Transformed

- **Raw HTML is preserved** in `data/raw/lawphil/pages/`. If the parser
  is ever wrong, the source-faithful artifact is available for re-parsing.
- **Raw Parquet is preserved** in `data/raw/bettergov/`. Same rationale.
- **Raw E-Library JSON pages are preserved** in `data/raw/elibrary/pages/`.
  Same rationale. The `_manifest.json` records the full row count so a
  later verification can confirm the on-disk pages are complete.
- **Tokenization is not persisted.** TF-IDF tokenization happens only in
  the analytics layer (`src/analytics/tfidf_cluster.py`) and never appears
  in `ra_master`.
- **E-Library rows are not persisted as curated rows.** Policy B treats
  E-Library as an enrichment lookup, not a source of new records.

---

## 7. Failure Modes and Their Visibility

| Failure | Visible where |
|---|---|
| Source download fails | Task log; retries 3× on ingestion |
| Index page structure changes | `RuntimeError` in `parse_index` with diagnostic sample of what was found |
| Single Lawphil page 404s | Logged as warning; task continues; final count < 200 |
| E-Library CSRF handshake fails | `RuntimeError` from `scrape_elibrary`; task fails red on the `ingest.scrape_elibrary` step |
| E-Library certificate verify fails | `SSLError` chain from `requests`; the shipped intermediate at `certs/gsgccr3evtlsca2025.pem` is required — see `certs/README.md` |
| E-Library page count mismatch | `_manifest.json` `manifest_complete` flag flips to `False`; verify before trusting staging counts |
| Staging produces wrong row count | Diagnostic logs from `merge_sources` (pre-drop counts) |
| Contract violation | `validation_report.json` with per-check `fail` + samples; exit code 1 |
| Postgres constraint violation | SQLAlchemy exception with constraint name; task fails red |
| Partition write fails mid-run | `.part` file cleaned up; original partition preserved |
| Output missing after run | `verify_outputs` task fails; DAG ends red |

---

## 8. Idempotency Demonstration

The most compelling evidence for rerun safety is a two-run comparison:

```
Run 1 (host, Step E):        Inserted: 11969 | Updated: 0
Run 2 (host, Step E):        Inserted: 0     | Updated: 11969
Run 3 (container, Step G):   Inserted: 0     | Updated: 11969
Run 4 (container, force):    Inserted: 0     | Updated: 11969
```

Every subsequent run converges to the same state. The `ra_master` table
is bit-identical after any number of reruns.

The E-Library ingestion path is idempotent by a parallel mechanism: the
default `scrape_elibrary` skips pages already on disk (`skip-if-exists`).
The `--force` flag bypasses this and re-fetches every page — used by the
DAG when `force_ingest=true`. Both paths converge to the same raw
`data/raw/elibrary/pages/` state.
