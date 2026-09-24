#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow PostgreSQL connector.

Field names mirror the legacy ``connector_postgres_v2`` command signatures
(``database_connection_str``/``table_name``/``schema``) because M8Flow builds
connector calls from the declared parameter names: a renamed field is silently
never sent. ``schema`` carries the per-command payload, as in the legacy
package.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _parse_payload(value: Any) -> Dict[str, Any]:
    """Accept a JSON object, or a JSON string holding one."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        raise ValueError("schema must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("schema must be a JSON object")
    return parsed


class _PostgresBase(BaseModel):
    # ``schema`` is the wire name the legacy commands declare. It shadows
    # pydantic's deprecated BaseModel.schema(), so the attribute is sql_schema
    # and the alias carries the name callers actually send.
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    database_connection_str: str = Field(
        ..., description="libpq connection string, e.g. dbname=... user=... host=..."
    )
    sql_schema: Dict[str, Any] = Field(
        default_factory=dict, alias="schema", description="Command payload."
    )

    @field_validator("database_connection_str")
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("database_connection_str must not be empty")
        return str(value).strip()

    @field_validator("sql_schema", mode="before")
    @classmethod
    def parse_schema(cls, value: Any) -> Dict[str, Any]:
        return _parse_payload(value)


class _TableBase(_PostgresBase):
    table_name: str = Field(..., description="Table name, optionally schema-qualified.")

    @field_validator("table_name")
    @classmethod
    def reject_blank_table(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("table_name must not be empty")
        return str(value).strip()


class CreateTableInput(_TableBase):
    """schema: {"column_definitions": [{"name": ..., "type": ...}, ...]}"""

    action: Literal["create_table"] = "create_table"


class DropTableInput(_TableBase):
    action: Literal["drop_table"] = "drop_table"


class InsertValuesInput(_TableBase):
    """schema: {"columns": [...], "values": [[...], ...]}"""

    action: Literal["insert_values"] = "insert_values"


class SelectValuesInput(_TableBase):
    """schema: {"columns": [...], "where": [[column, op, value], ...]}"""

    action: Literal["select_values"] = "select_values"


class UpdateValuesInput(_TableBase):
    """schema: {"set": {column: value}, "where": [[column, op, value], ...]}"""

    action: Literal["update_values"] = "update_values"


class DeleteValuesInput(_TableBase):
    """schema: {"where": [[column, op, value], ...]}"""

    action: Literal["delete_values"] = "delete_values"


class DoSqlInput(_PostgresBase):
    """schema: {"sql": ..., "values": [...], "fetch_results": bool}"""

    action: Literal["do_sql"] = "do_sql"


class PostgresResponseOutput(BaseModel):
    """What the legacy connector placed in ``command_response.body``.

    A list of row lists for fetches, otherwise ``{"result": "<n> rows affected"}``.
    """

    body: Any = None
