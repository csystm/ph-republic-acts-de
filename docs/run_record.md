# End-to-End Run Record

**Date:** 2026-10-13
**Environment:** Windows 11 + Docker 29.8.0 (Compose v5.5.1)
**Commit:** `16ec6c0` — `docs: reconcile null-rate and source-value figures across dictionary and ERD`
**Branch:** `fixes`
**Pipeline:** `ra_pipeline` — 13 tasks, curated output 11,969 rows × 11 cols

## Scope note

This record captures a full pipeline run on 2026-10-13 against a **resumed**
Docker stack. Postgres was brought back up after an unclean host shutdown
(the compose `airflow-*` services were then force-recreated to rejoin the
container network). A `docker compose down -v` fresh-volume setup was
**not** performed, because a live Postgres volume already held the 11,969
curated rows from the 2026-10-09 live demo; wiping it would have destroyed
that evidence without adding information.

The pipeline's idempotency guarantees are proven separately in
`docs/idempotency.md` (truncated table → two-run transcript from an empty
baseline). This document adds the wider end-to-end capture: a **fresh DAG
run against the live stack**, a **fresh validation pass**, and a **fresh
idempotent rerun**.

---

## 1. Stack state

```
$ docker compose ps
NAME                                      IMAGE                                   COMMAND                  SERVICE             CREATED      STATUS                   PORTS
ph-republic-acts-de-airflow-scheduler-1   ph-republic-acts-de-airflow-scheduler   "/usr/bin/dumb-init …"   airflow-scheduler   7 days ago   Up 10 seconds            8080/tcp
ph-republic-acts-de-airflow-webserver-1   ph-republic-acts-de-airflow-webserver   "/usr/bin/dumb-init …"   airflow-webserver   7 days ago   Up 11 seconds            0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
ph-republic-acts-de-postgres-1            postgres:15                             "docker-entrypoint.s…"   postgres            7 days ago   Up 2 minutes (healthy)   5432/tcp

$ docker compose ps -a
NAME                                      IMAGE                                   COMMAND                  SERVICE             CREATED      STATUS                   PORTS
airflow-init                              ph-republic-acts-de-airflow-init        "/bin/bash -c 'airfl…"   airflow-init        7 days ago   Exited (0) 5 days ago
ph-republic-acts-de-airflow-scheduler-1   ph-republic-acts-de-airflow-scheduler   "/usr/bin/dumb-init …"   airflow-scheduler   7 days ago   Up 14 seconds            8080/tcp
ph-republic-acts-de-airflow-webserver-1   ph-republic-acts-de-airflow-webserver   "/usr/bin/dumb-init …"   airflow-webserver   7 days ago   Up 15 seconds            0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
ph-republic-acts-de-postgres-1            postgres:15                             "docker-entrypoint.s…"   postgres            7 days ago   Up 2 minutes (healthy)   5432/tcp
```

Note on `airflow-init` state: `airflow-init` is a one-shot container that
runs schema migrations and admin-user creation, then exits. `Exited (0)` is
its expected terminal state, not a failure. The three long-running services
(`airflow-scheduler`, `airflow-webserver`, `postgres`) are up; Postgres
reports `(healthy)`.

---

## 2. Airflow DAG run — `ra_pipeline`

Triggered from the CLI (not the UI), 2026-10-13:

```
$ docker compose exec airflow-scheduler airflow dags trigger ra_pipeline
...
{} | ra_pipeline | manual__2026-10-13T12:47:58+00:00 | 2026-09-01 02:00:00+00:00 | 2026-10-01 02:00:00+00:00 | None | True | None | 2026-10-13 12:47:58+00:00 | manual | None | queued
```

Run history (most recent first):

```
$ docker compose exec airflow-scheduler airflow dags list-runs -d ra_pipeline --output plain
dag_id       run_id                             state    execution_date             start_date                        end_date
ra_pipeline  manual__2026-10-13T12:47:58+00:00  success  2026-10-13T12:47:58+00:00  2026-10-13T12:47:58.740384+00:00  2026-10-13T12:48:54.406381+00:00
ra_pipeline  manual__2026-10-07T14:32:48+00:00  success  2026-10-07T14:32:48+00:00  2026-10-07T14:32:55.925691+00:00  2026-10-07T14:34:48.386580+00:00
ra_pipeline  manual__2026-10-07T14:18:15+00:00  success  2026-10-07T14:18:15+00:00  2026-10-07T14:18:18.690468+00:00  2026-10-07T14:19:24.552878+00:00
ra_pipeline  manual__2026-10-07T01:03:16+00:00  success  2026-10-07T01:03:16+00:00  2026-10-07T01:03:33.803625+00:00  2026-10-07T01:05:05.736867+00:00
ra_pipeline  manual__2026-10-05T15:03:07+00:00  success  2026-10-05T15:03:07+00:00  2026-10-05T15:03:10.006880+00:00  2026-10-05T15:04:01.345185+00:00
...
```

New run: `manual__2026-10-13T12:47:58+00:00`, state `success`, wall time
~56 s (`12:47:58` → `12:48:54`). The DAG-run `success` state means all 13
tasks completed successfully.

---

## 3. Postgres row count

```
$ docker compose exec postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM ra_master;"'
 count
-------
 11969
(1 row)
```

Matches the curated Parquet row count exactly. Post-Policy-B, post-third-source
integration.

---

## 4. Validation

```
$ python -m src.validation.checks
2026-10-13 20:50:54 | INFO    | __main__ | Loading ...\data\curated\ra_master.parquet
2026-10-13 20:50:54 | INFO    | __main__ | Loaded 11969 rows x 11 cols
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] schema_conformance — All expected columns present with expected dtypes
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] ra_id_unique — ra_id is unique
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] required_non_null — All required fields non-null
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] year_range — All ra_year values in [1946, 2027]
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] content_non_stub — All rows have non-empty content
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] content_normalized_non_empty — All content_normalized values non-empty
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] ra_number_shape — All ra_number values match '^\\d+[a-z]?$'
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] approval_date_valid — All 11911 non-null approval_date values are valid ISO dates
2026-10-13 20:50:54 | INFO    | __main__ | [PASS] source_distribution — source distribution: {'bettergov_parquet': 11969}
2026-10-13 20:50:54 | WARNING | __main__ | [WARN] oversized_content — 3 row(s) exceed 500,000 chars — expected for codified mega-statutes; informational only
2026-10-13 20:50:54 | WARNING | __main__ |     RA-5050 (2,682,141 chars)
2026-10-13 20:50:54 | WARNING | __main__ |     RA-386 (785,382 chars)
2026-10-13 20:50:54 | WARNING | __main__ |     RA-8424 (524,922 chars)
2026-10-13 20:50:54 | INFO    | src.utils.io | wrote outputs\validation_report.json (2378 bytes)
2026-10-13 20:50:54 | INFO    | __main__ | Validation: 9 passed, 1 warned, 0 failed — report at outputs\validation_report.json
```

Full JSON report (also committed at `outputs/validation_report.json`):

```json
{
  "contract_version": "1.2",
  "generated_at": "2026-10-13T12:50:54.691174+00:00",
  "total_checks": 10,
  "passed": 9,
  "warned": 1,
  "failed": 0,
  "overall_status": "warn",
  "checks": [
    { "name": "schema_conformance",           "severity": "error", "status": "pass" },
    { "name": "ra_id_unique",                 "severity": "error", "status": "pass" },
    { "name": "required_non_null",            "severity": "error", "status": "pass" },
    { "name": "year_range",                   "severity": "error", "status": "pass" },
    { "name": "content_non_stub",             "severity": "error", "status": "pass" },
    { "name": "content_normalized_non_empty", "severity": "error", "status": "pass" },
    { "name": "ra_number_shape",              "severity": "error", "status": "pass" },
    { "name": "approval_date_valid",          "severity": "error", "status": "pass" },
    { "name": "source_distribution",          "severity": "warn",  "status": "pass" },
    { "name": "oversized_content",            "severity": "warn",  "status": "warn",
      "violations": 3,
      "samples": ["RA-5050 (2,682,141 chars)", "RA-386 (785,382 chars)", "RA-8424 (524,922 chars)"] }
  ]
}
```

(Abbreviated for readability; the full JSON with per-check `message` and
`violations` fields is in `outputs/validation_report.json`.)

Two cross-checks worth noting:

- `approval_date_valid` reports **11,911 non-null values** = 11,969 − 58,
  independently confirming the 0.48% null rate at validation time.
- `source_distribution` shows a single value, `bettergov_parquet`, confirming
  the curated-only source invariant (Policy B contributes fields, not rows).

---

## 5. Rerun — idempotency

Post-load rerun, no fresh truncation (table already populated from the
run in §2):

```
$ python -m src.load.load_postgres
2026-10-13 20:51:21 | INFO    | __main__ | Step E — Postgres load starting
2026-10-13 20:51:21 | INFO    | __main__ | Curated input: ...\data\curated\ra_master.parquet
2026-10-13 20:51:21 | INFO    | __main__ | Read 11969 rows x 11 cols from parquet
C:\...\src\load\load_postgres.py:95: SAWarning: Skipped unsupported reflection of expression-based index idx_ra_master_content_fts
  tbl = Table(TABLE_NAME, metadata, autoload_with=conn)
2026-10-13 20:51:21 | INFO    | __main__ | Rows in ra_master before load: 11969
2026-10-13 20:51:22 | INFO    | __main__ | Chunk 0-500: 0 inserted, 500 updated
2026-10-13 20:51:23 | INFO    | __main__ | Chunk 500-1000: 0 inserted, 500 updated
...
2026-10-13 20:51:41 | INFO    | __main__ | Chunk 11500-11969: 0 inserted, 469 updated
2026-10-13 20:51:41 | INFO    | __main__ | Inserted: 0 | Updated: 11969
2026-10-13 20:51:41 | INFO    | __main__ | Rows in ra_master after load: 11969 (delta +0)
```

Post-rerun count:

```
$ docker compose exec postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT COUNT(*) FROM ra_master;"'
 count
-------
 11969
(1 row)
```

The rerun asserts `inserted + updated == len(records)` internally
(`0 + 11969 == 11969`) and the row count is unchanged. This is the same
upsert-on-`ra_id` behavior documented in `docs/idempotency.md` §3, exercised
here on the live stack.

### On the `SAWarning`

The `SAWarning: Skipped unsupported reflection of expression-based index
idx_ra_master_content_fts` is cosmetic. SQLAlchemy cannot model the GIN
full-text index (`USING GIN (to_tsvector(...))`) in its Python metadata
object; the index itself remains active in Postgres and does not affect
the UPSERT or row counts. Documented in `docs/idempotency.md` §3.5.

---

## Cross-references

- Pipeline architecture: `docs/architecture.md`
- Data flow and reconciliation: `docs/data_flow.md`
- Load idempotency mechanics (design + truncated-table two-run proof):
  `docs/idempotency.md`
- Data contract: `docs/data_contract.yaml` (v1.2)
- Validation report: `outputs/validation_report.json`
- Committed curated data: `data/curated/ra_master.parquet` (11,969 × 11)

_End of run record._