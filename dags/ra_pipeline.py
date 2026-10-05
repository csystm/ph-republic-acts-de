"""ra_pipeline — end-to-end orchestration for the Philippine Republic Acts project.

Stages
------
1. ingest     — programmatic retrieval (BetterGov parquet, Lawphil HTML,
                Supreme Court E-Library JSON)
2. transform  — Raw -> Staging -> Curated merge
3. validate   — contract-driven data-quality checks
4. load       — Postgres upsert + partitioned Parquet + CSV/JSON exports

Design notes
------------
- No imports from `src` at parse time. Each stage is invoked as
  `python -m src.<...>` via BashOperator, so a broken module can never
  crash the scheduler on DAG parse.
- Idempotent: reruns converge. Postgres uses ON CONFLICT DO UPDATE;
  file outputs use atomic writes (temp + os.replace).
- Ingestion is gated by `params.force_ingest`. Default False = use cached
  raw data (fast for demos). Set True for a clean end-to-end run.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

REPO_ROOT_IN_CONTAINER = "/opt/airflow"
PYTHON = "python"


# --------------------------------------------------------------------- #
# Command builders
# --------------------------------------------------------------------- #
def _run(module: str) -> str:
    """Always-run bash command: execute module with correct cwd."""
    return f"cd {REPO_ROOT_IN_CONTAINER} && {PYTHON} -m {module}"


def _ingest_cmd(module: str, force_arg: str = "") -> str:
    """Conditionally-run bash command based on {{ params.force_ingest }}.

    ``force_arg`` (e.g. ``"--force"``) is appended to the module invocation
    only when force_ingest=true. Use it for modules whose own idempotency
    check would otherwise short-circuit the re-fetch.
    """
    suffix = f" {force_arg}" if force_arg else ""
    return (
        f"cd {REPO_ROOT_IN_CONTAINER}\n"
        'if [ "{{ params.force_ingest | lower }}" = "true" ]; then\n'
        f"  {PYTHON} -m {module}{suffix}\n"
        "else\n"
        '  echo "[skip] force_ingest=False; reusing cached raw data"\n'
        "fi\n"
    )


DAG_DOC = """
### ra_pipeline

End-to-end DAG for the Philippine Republic Acts similarity/consolidation project.

**Schedule:** `0 2 1 * *` — monthly, 1st of the month at 02:00 (server time).

*Why monthly, not daily?* Republic Acts are enacted at a rate of roughly
3-4 per week with irregular gaps, and the upstream sources (BetterGov on
HuggingFace, Lawphil) do not publish on a fixed cadence. A daily schedule
would produce ~30 redundant runs per month for at most a few new rows.
Monthly gives a defensible freshness bound (~30 days) without hammering
external sources.

**Stages**

1. `ingest` — programmatic retrieval (BetterGov parquet, Lawphil HTML,
   Supreme Court E-Library JSON). Gated by `params.force_ingest`
   (default `False` = use cached raw data).
2. `transform` — raw -> staging -> curated merge.
3. `validate` — contract-driven data-quality checks
   (errors gate the pipeline; warnings report only).
4. `load` — Postgres upsert, partitioned Parquet by `ra_year`, CSV/JSON exports.

**Rerun safety** — every stage is idempotent. Files use atomic writes
(temp + `os.replace`); Postgres uses `ON CONFLICT (ra_id) DO UPDATE`.

**Trigger with forced ingestion (for demos)**

```bash
airflow dags trigger ra_pipeline --conf '{"force_ingest": true}'
```
"""


with DAG(
    dag_id="ra_pipeline",
    description="Philippine Republic Acts — end-to-end pipeline",
    schedule="0 2 1 * *",
    start_date=datetime(2026, 9, 30),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "dss150p",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "force_ingest": Param(
            default=False,
            type="boolean",
            description=(
                "If True, re-download the BetterGov parquet, re-scrape "
                "Lawphil HTML, and re-fetch the Supreme Court E-Library "
                "JSON index. If False (default), reuse cached raw data."
            ),
        ),
    },
    tags=["dss150p", "philippines", "republic-acts"],
    doc_md=DAG_DOC,
) as dag:

    # -------- ingest --------
    with TaskGroup("ingest", tooltip="Programmatic retrieval") as ingest_group:
        download = BashOperator(
            task_id="download_parquet",
            bash_command=_ingest_cmd("src.extract.download_parquet"),
            retries=3,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=20),
        )
        scrape = BashOperator(
            task_id="scrape_lawphil",
            bash_command=_ingest_cmd("src.extract.scrape_lawphil"),
            retries=3,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=30),
        )
        scrape_el = BashOperator(
            task_id="scrape_elibrary",
            bash_command=_ingest_cmd("src.extract.scrape_elibrary", "--force"),
            retries=3,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=15),
        )

    # -------- transform --------
    with TaskGroup("transform", tooltip="Raw -> Staging -> Curated") as transform_group:
        parse_bg = BashOperator(
            task_id="parse_bettergov",
            bash_command=_run("src.transform.parse_bettergov"),
            execution_timeout=timedelta(minutes=10),
        )
        parse_lp = BashOperator(
            task_id="parse_lawphil",
            bash_command=_run("src.transform.parse_lawphil"),
            execution_timeout=timedelta(minutes=10),
        )
        parse_el = BashOperator(
            task_id="parse_elibrary",
            bash_command=_run("src.transform.parse_elibrary"),
            execution_timeout=timedelta(minutes=10),
        )
        merge = BashOperator(
            task_id="merge_sources",
            bash_command=_run("src.transform.merge_sources"),
            execution_timeout=timedelta(minutes=15),
        )
        [parse_bg, parse_lp, parse_el] >> merge

    # -------- validate --------
    validate = BashOperator(
        task_id="run_checks",
        bash_command=_run("src.validation.checks"),
        retries=0,   # a validation failure is a data problem; retries won't fix it
        execution_timeout=timedelta(minutes=5),
    )

    # -------- load --------
    with TaskGroup("load", tooltip="Postgres + partitioned Parquet + exports") as load_group:
        to_pg = BashOperator(
            task_id="load_postgres",
            bash_command=_run("src.load.load_postgres"),
            retries=1,
            retry_delay=timedelta(seconds=30),
            execution_timeout=timedelta(minutes=10),
        )
        to_parts = BashOperator(
            task_id="write_partitions",
            bash_command=_run("src.load.write_partitions"),
            retries=1,
            retry_delay=timedelta(seconds=30),
            execution_timeout=timedelta(minutes=15),
        )

    # -------- verify --------
    verify = BashOperator(
        task_id="verify_outputs",
        bash_command=(
            "set -e\n"
            f"cd {REPO_ROOT_IN_CONTAINER}\n"
            "test -f data/curated/ra_master.parquet\n"
            "test -f outputs/validation_report.json\n"
            "test -f outputs/format_comparison.json\n"
            "echo '[ok] curated artifacts present'"
        ),
        retries=0,
    )

    # -------- wiring --------
    ingest_group >> transform_group
    transform_group >> validate
    validate >> load_group
    load_group >> verify