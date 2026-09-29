"""Load the YAML data contract and expose typed accessors.

The contract at docs/data_contract.yaml is the single source of truth for
the shape and validation rules of ra_master. ``checks.py`` imports its
constants from here, so the code and the contract cannot drift.

Design: a small ``Contract`` class rather than a bag of module-level
functions, so tests can instantiate it against a fixture YAML without
touching the real one.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# Repo-root-relative. This project's convention is to run modules from the
# repo root.
_DEFAULT_CONTRACT_PATH = Path("docs") / "data_contract.yaml"

# Contract `type` (plus optional `format`) -> pandas dtype string.
# Add entries here only when the contract actually uses a new type.
_PANDAS_TYPE_MAP: dict[tuple[str, str | None], str] = {
    ("string", None): "object",
    ("string", "date"): "object",   # ISO 8601 stored as Python str in Parquet
    ("integer", None): "Int64",
}


@lru_cache(maxsize=4)
def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class Contract:
    """Typed view over docs/data_contract.yaml."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else _DEFAULT_CONTRACT_PATH
        self._data = _read_yaml(self.path)

    # ---- top-level metadata -------------------------------------------------

    @property
    def version(self) -> str:
        return self._data["contract_version"]

    @property
    def dataset(self) -> str:
        return self._data["dataset"]

    # ---- schema -------------------------------------------------------------

    @property
    def schema_fields(self) -> list[dict[str, Any]]:
        return self._data["schema"]["fields"]

    @property
    def expected_columns(self) -> dict[str, str]:
        """Field name -> pandas dtype string, for schema conformance checks."""
        out: dict[str, str] = {}
        for f in self.schema_fields:
            key = (f["type"], f.get("format"))
            if key not in _PANDAS_TYPE_MAP:
                raise KeyError(
                    f"Contract field {f['name']!r} has type {key!r}, "
                    f"which is not in _PANDAS_TYPE_MAP"
                )
            out[f["name"]] = _PANDAS_TYPE_MAP[key]
        return out

    @property
    def required_non_null(self) -> list[str]:
        return [f["name"] for f in self.schema_fields if not f["nullable"]]

    def field(self, name: str) -> dict[str, Any]:
        for f in self.schema_fields:
            if f["name"] == name:
                return f
        raise KeyError(f"Unknown field in contract: {name}")

    # ---- validation rules ---------------------------------------------------

    @property
    def rules(self) -> dict[str, dict[str, Any]]:
        """Map rule name -> full rule dict (id, check, severity, params, ...)."""
        return {r["name"]: r for r in self._data["validation_rules"]}

    def rule_params(self, rule_name: str) -> dict[str, Any]:
        """Return the ``params`` block for a rule, or {} if it has none."""
        rule = self.rules.get(rule_name)
        if rule is None:
            raise KeyError(f"No such rule in contract: {rule_name}")
        return rule.get("params", {}) or {}