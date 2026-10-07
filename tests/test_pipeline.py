"""
Fast, deterministic tests for the RA pipeline.

No network. No Postgres. No external services.
Runs in < 2 seconds against committed artifacts + pure functions.

    pytest tests/ -v
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from src.transform.parse_elibrary import RA_TITLE_RE


REPO_ROOT = Path(__file__).resolve().parent.parent
CURATED   = REPO_ROOT / "data" / "curated" / "ra_master.parquet"
CONTRACT  = REPO_ROOT / "docs" / "data_contract.yaml"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def curated() -> pd.DataFrame:
    """The committed curated artifact. Skipped if missing (fresh clone)."""
    if not CURATED.exists():
        pytest.skip(f"{CURATED} not present — commit or regenerate first")
    return pd.read_parquet(CURATED)


@pytest.fixture(scope="module")
def contract() -> dict:
    if not CONTRACT.exists():
        pytest.skip(f"{CONTRACT} not present")
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# 1. Curated artifact matches the data contract
# ---------------------------------------------------------------------------

def test_curated_columns_match_contract(curated, contract):
    expected = {f["name"] for f in contract["schema"]["fields"]}
    actual   = set(curated.columns)
    assert actual == expected, (
        f"curated columns drift from contract\n"
        f"  missing: {expected - actual}\n"
        f"  extra:   {actual - expected}"
    )


def test_curated_row_count_is_within_expected_range(curated):
    # Sanity band — not a frozen number. If we add a source, bump the bound.
    assert 11_000 <= len(curated) <= 13_000, f"unexpected row count: {len(curated)}"


def test_contract_declares_expected_shape(contract):
    """Lock in the headline numbers the rest of the docs cite."""
    assert contract["contract_version"] == "1.2"
    assert len(contract["schema"]["fields"]) == 11
    assert len(contract["validation_rules"]) == 10
    assert contract["schema"]["primary_key"] == "ra_id"


# ---------------------------------------------------------------------------
# 2. E-Library RA-title filter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("title", [
    "REPUBLIC ACT NO. 1",
    "REPUBLIC ACT NO. 49",
    "REPUBLIC ACT NO. 12325",
    "REPUBLIC ACT NO. 12325   ",   # trailing whitespace tolerated
])
def test_elibrary_filter_accepts_ra_titles(title):
    assert RA_TITLE_RE.match(title) is not None


@pytest.mark.parametrize("title", [
    "IRR of REPUBLIC ACT NO. 12254",   # prefix
    "REPUBLIC ACT NO. 12A",            # trailing letter
    "REPUBLIC ACT NO.12",              # missing space
    "REPUBLIC ACT NO. ",               # missing number
    "Resolution No. 12",               # wrong document type
    "republic act no. 5",              # lowercase (regex is strict)
])
def test_elibrary_filter_rejects_non_ra(title):
    assert RA_TITLE_RE.match(title) is None


def test_elibrary_filter_captures_ra_number():
    m = RA_TITLE_RE.match("REPUBLIC ACT NO. 12325")
    assert m is not None
    assert m.group(1) == "12325"


# ---------------------------------------------------------------------------
# 3. ra_id uniqueness — mirrors checks.py check #2
# ---------------------------------------------------------------------------

def test_ra_id_is_unique(curated):
    dupes = int(curated["ra_id"].duplicated().sum())
    assert dupes == 0, f"{dupes} duplicate ra_id values"


def test_ra_number_year_pair_is_unique(curated):
    dupes = int(curated.duplicated(["ra_number", "ra_year"]).sum())
    assert dupes == 0, f"{dupes} duplicate (ra_number, ra_year) pairs"


# ---------------------------------------------------------------------------
# 4. Atomic write — no temp file left behind
# ---------------------------------------------------------------------------

def test_atomic_write_leaves_no_part_file(tmp_path):
    from src.utils.io import atomic_write_bytes
    dest = tmp_path / "out.bin"
    atomic_write_bytes(b"hello", dest)
    assert dest.read_bytes() == b"hello"
    leftover = sorted(p.name for p in tmp_path.iterdir() if p.name != "out.bin")
    assert not leftover, f"atomic write left files behind: {leftover}"


# ---------------------------------------------------------------------------
# 5. Third-source headline: approval_date null rate <= 0.5%
# ---------------------------------------------------------------------------

def test_approval_date_null_rate_within_target(curated):
    rate = float(curated["approval_date"].isna().mean())
    assert rate <= 0.005, (
        f"approval_date null rate {rate:.4%} exceeds the 0.50% target; "
        f"Policy B gap-fill may have regressed"
    )


def test_source_column_contains_only_bettergov(curated):
    """Policy B is gap-fill only — no elibrary_json rows should appear."""
    sources = set(curated["source"].unique())
    assert sources == {"bettergov_parquet"}, f"unexpected source values: {sources}"