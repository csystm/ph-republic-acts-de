"""Automated data-quality checks for the curated ra_master.

Contract constants (expected columns, required-non-null fields, thresholds,
patterns) are read from docs/data_contract.yaml via src.validation.contract.
Editing the YAML changes pipeline behavior; there is no second source of truth.

Each check returns a CheckResult with status of "pass" | "warn" | "fail".
Errors gate the pipeline (exit code 1); warnings are reported, not gated.

Outputs:
  - outputs/validation_report.json
"""

from __future__ import annotations

import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.utils.config import settings
from src.utils.io import atomic_write_json
from src.utils.logger import get_logger
from src.validation.contract import Contract

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Contract-derived constants — no hardcoded schema facts below this line
# --------------------------------------------------------------------------- #

_CONTRACT = Contract()

EXPECTED_COLUMNS: dict[str, str] = _CONTRACT.expected_columns
REQUIRED_NON_NULL: list[str] = _CONTRACT.required_non_null

_year_params = _CONTRACT.rule_params("year_range")
MIN_YEAR: int = _year_params["min"]
MAX_YEAR: int = date.today().year + _year_params["max_offset_years"]

_stub_params = _CONTRACT.rule_params("content_non_stub")
MIN_WORD_COUNT: int = _stub_params["min_word_count"]
MIN_CONTENT_LENGTH: int = _stub_params["min_content_length"]

RA_NUMBER_PATTERN = re.compile(
    _CONTRACT.rule_params("ra_number_shape")["pattern"]
)

OVERSIZED_ABSOLUTE: int = _CONTRACT.rule_params("oversized_content")["absolute_chars"]

ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SAMPLE_N = 5


# --------------------------------------------------------------------------- #
# Result type
# --------------------------------------------------------------------------- #

@dataclass
class CheckResult:
    name: str
    severity: str          # "error" | "warn"
    status: str            # "pass" | "fail" | "warn"
    message: str
    violations: int = 0
    samples: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #

def check_schema(df: pd.DataFrame) -> CheckResult:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    wrong_dtype = [
        f"{c}: expected {EXPECTED_COLUMNS[c]}, got {df[c].dtype}"
        for c in EXPECTED_COLUMNS
        if c in df.columns and str(df[c].dtype) != EXPECTED_COLUMNS[c]
    ]
    problems = (
        [f"missing column: {c}" for c in missing]
        + [f"unexpected column: {c}" for c in extra]
        + wrong_dtype
    )
    return CheckResult(
        name="schema_conformance",
        severity="error",
        status="pass" if not problems else "fail",
        message=(
            "All expected columns present with expected dtypes"
            if not problems
            else f"{len(problems)} schema problem(s)"
        ),
        violations=len(problems),
        samples=problems[:SAMPLE_N],
    )


def check_ra_id_unique(df: pd.DataFrame) -> CheckResult:
    dup_mask = df["ra_id"].duplicated(keep=False)
    n = int(dup_mask.sum())
    samples = (
        df.loc[dup_mask, "ra_id"].drop_duplicates().head(SAMPLE_N).tolist()
        if n else []
    )
    return CheckResult(
        name="ra_id_unique",
        severity="error",
        status="pass" if n == 0 else "fail",
        message="ra_id is unique" if n == 0 else f"{n} duplicate ra_id row(s)",
        violations=n,
        samples=samples,
    )


def check_required_non_null(df: pd.DataFrame) -> CheckResult:
    problems: list[str] = []
    for col in REQUIRED_NON_NULL:
        if col not in df.columns:
            problems.append(f"{col}: column missing")
            continue
        n = int(df[col].isna().sum())
        if n:
            problems.append(f"{col}: {n} null(s)")
    return CheckResult(
        name="required_non_null",
        severity="error",
        status="pass" if not problems else "fail",
        message=(
            "All required fields non-null"
            if not problems
            else f"{len(problems)} column(s) have nulls"
        ),
        violations=len(problems),
        samples=problems[:SAMPLE_N],
    )


def check_year_range(df: pd.DataFrame) -> CheckResult:
    s = df["ra_year"]
    bad = s[(s < MIN_YEAR) | (s > MAX_YEAR)]
    n = int(bad.shape[0])
    return CheckResult(
        name="year_range",
        severity="error",
        status="pass" if n == 0 else "fail",
        message=(
            f"All ra_year values in [{MIN_YEAR}, {MAX_YEAR}]"
            if n == 0
            else f"{n} ra_year value(s) out of range"
        ),
        violations=n,
        samples=sorted(bad.astype(int).unique().tolist())[:SAMPLE_N],
    )


def check_content_non_stub(df: pd.DataFrame) -> CheckResult:
    short_len = df[df["content_length"] < MIN_CONTENT_LENGTH]
    short_words = df[df["word_count"] < MIN_WORD_COUNT]
    bad_ids = (
        pd.concat([short_len["ra_id"], short_words["ra_id"]])
        .drop_duplicates().tolist()
    )
    n = len(bad_ids)
    return CheckResult(
        name="content_non_stub",
        severity="error",
        status="pass" if n == 0 else "fail",
        message=(
            "All rows have non-empty content"
            if n == 0
            else f"{n} stub row(s) "
                 f"(content_length<{MIN_CONTENT_LENGTH} or word_count<{MIN_WORD_COUNT})"
        ),
        violations=n,
        samples=bad_ids[:SAMPLE_N],
    )


def check_normalized_non_empty(df: pd.DataFrame) -> CheckResult:
    empty = df["content_normalized"].fillna("").str.len() == 0
    n = int(empty.sum())
    samples = df.loc[empty, "ra_id"].head(SAMPLE_N).tolist() if n else []
    return CheckResult(
        name="content_normalized_non_empty",
        severity="error",
        status="pass" if n == 0 else "fail",
        message=(
            "All content_normalized values non-empty"
            if n == 0
            else f"{n} row(s) have empty content_normalized"
        ),
        violations=n,
        samples=samples,
    )


def check_ra_number_shape(df: pd.DataFrame) -> CheckResult:
    bad_mask = ~df["ra_number"].astype(str).str.match(RA_NUMBER_PATTERN)
    n = int(bad_mask.sum())
    samples = df.loc[bad_mask, "ra_number"].head(SAMPLE_N).tolist() if n else []
    return CheckResult(
        name="ra_number_shape",
        severity="error",
        status="pass" if n == 0 else "fail",
        message=(
            f"All ra_number values match {RA_NUMBER_PATTERN.pattern!r}"
            if n == 0
            else f"{n} ra_number value(s) do not match expected shape"
        ),
        violations=n,
        samples=samples,
    )


def check_approval_date(df: pd.DataFrame) -> CheckResult:
    s = df["approval_date"]
    non_null = s.dropna()
    wrong_shape = non_null[~non_null.str.match(ISO_DATE_PATTERN)]
    bad_dates: list[str] = []
    for v in non_null:
        try:
            datetime.strptime(v, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            bad_dates.append(str(v))
    problems = (
        [f"non-ISO: {v}" for v in wrong_shape.head(SAMPLE_N)]
        + [f"unparseable: {v}" for v in bad_dates[:SAMPLE_N]]
    )
    n = len(wrong_shape) + len(bad_dates)
    return CheckResult(
        name="approval_date_valid",
        severity="error",
        status="pass" if n == 0 else "fail",
        message=(
            f"All {len(non_null)} non-null approval_date values are valid ISO dates"
            if n == 0
            else f"{n} approval_date value(s) invalid"
        ),
        violations=n,
        samples=problems[:SAMPLE_N],
    )


def check_source_distribution(df: pd.DataFrame) -> CheckResult:
    dist = df["source"].value_counts().to_dict()
    # Informational: multiple sources are permitted by contract, but the
    # current snapshot is expected to be single-source.
    return CheckResult(
        name="source_distribution",
        severity="warn",
        status="warn" if len(dist) > 1 else "pass",
        message=f"source distribution: {dist}",
        violations=0,
        samples=[],
    )


def check_oversized_content(df: pd.DataFrame) -> CheckResult:
    big = df[df["content_length"] > OVERSIZED_ABSOLUTE].sort_values(
        "content_length", ascending=False
    )
    n = len(big)
    samples = [
        f"{row['ra_id']} ({row['content_length']:,} chars)"
        for _, row in big.head(SAMPLE_N).iterrows()
    ]
    return CheckResult(
        name="oversized_content",
        severity="warn",
        status="warn" if n else "pass",
        message=(
            f"{n} row(s) exceed {OVERSIZED_ABSOLUTE:,} chars — "
            "expected for codified mega-statutes; informational only"
            if n
            else f"No rows exceed {OVERSIZED_ABSOLUTE:,} chars"
        ),
        violations=n,
        samples=samples,
    )


CHECKS: list[Callable[[pd.DataFrame], CheckResult]] = [
    check_schema,
    check_ra_id_unique,
    check_required_non_null,
    check_year_range,
    check_content_non_stub,
    check_normalized_non_empty,
    check_ra_number_shape,
    check_approval_date,
    check_source_distribution,
    check_oversized_content,
]


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #

def run_all(df: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []
    for fn in CHECKS:
        try:
            r = fn(df)
        except Exception as e:
            r = CheckResult(
                name=fn.__name__,
                severity="error",
                status="fail",
                message=f"check crashed: {type(e).__name__}: {e}",
                violations=0,
                samples=[],
            )
        results.append(r)
        level = {"pass": logger.info, "warn": logger.warning, "fail": logger.error}[r.status]
        level("[%s] %s — %s", r.status.upper(), r.name, r.message)
        for s in r.samples:
            level("    %s", s)
    return results


def summarize(results: list[CheckResult]) -> dict[str, Any]:
    errors = [r for r in results if r.status == "fail"]
    warns = [r for r in results if r.status == "warn"]
    return {
        "contract_version": _CONTRACT.version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed": sum(1 for r in results if r.status == "pass"),
        "warned": len(warns),
        "failed": len(errors),
        "overall_status": "fail" if errors else ("warn" if warns else "pass"),
        "checks": [asdict(r) for r in results],
    }


def main() -> int:
    src = settings.curated_dir / "ra_master.parquet"
    logger.info("Loading %s", src)
    df = pd.read_parquet(src)
    logger.info("Loaded %d rows x %d cols", *df.shape)

    results = run_all(df)
    report = summarize(results)

    dest = Path("outputs") / "validation_report.json"
    atomic_write_json(report, dest)
    logger.info(
        "Validation: %d passed, %d warned, %d failed — report at %s",
        report["passed"], report["warned"], report["failed"], dest,
    )
    return 0 if report["overall_status"] != "fail" else 1


if __name__ == "__main__":
    sys.exit(main())