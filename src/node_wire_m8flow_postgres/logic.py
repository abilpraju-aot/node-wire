#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow PostgreSQL connector.

Behaviour is ported from the legacy ``connector_postgres_v2`` package so
existing process models keep working: same commands (CreateTableV2,
DropTableV2, InsertValuesV2, SelectValuesV2, UpdateValuesV2, DeleteValuesV2,
DoSQL), same ``schema`` payloads, same result body.

Unlike the legacy package, table and column names are validated before they
are placed in a statement, and values are always bound, never interpolated.

The driver is imported lazily, inside the action, so the connector package can
be installed and registered on a host that has no PostgreSQL client library --
a missing driver then surfaces as a clear configuration error rather than an
import failure that takes the whole runtime down at startup.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    CreateTableInput,
    DeleteValuesInput,
    DoSqlInput,
    DropTableInput,
    InsertValuesInput,
    PostgresResponseOutput,
    SelectValuesInput,
    UpdateValuesInput,
)

logger = logging.getLogger("connectors.m8flow_postgres")

# Unquoted, like the legacy connector, so Postgres case-folding is unchanged.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$")
# Column types are a raw SQL fragment by necessity (varchar(255), numeric(10, 2), int[]).
_COLUMN_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_ ,()\[\]]*$")
_OPERATORS = {"=", "!=", "<", ">"}


class PostgresDriverMissingError(Exception):
    """No PostgreSQL driver is installed in this runtime."""


class PostgresConfigError(Exception):
    """The request did not carry a usable command payload."""


class PostgresQueryError(Exception):
    """The database rejected the statement."""


class PostgresConnectionError(Exception):
    """The database could not be reached."""


def _timeout() -> int:
    return int(float(os.getenv("NW_TIMEOUT", "30.0")))


def _load_driver() -> Any:
    try:
        import psycopg  # type: ignore

        return psycopg
    except ImportError as exc:
        raise PostgresDriverMissingError(
            "No PostgreSQL driver installed. Add 'psycopg[binary]' to the "
            "m8flow_postgres connector package."
        ) from exc


# --- statement builders ------------------------------------------------------


def _ident(name: Any) -> str:
    text = str(name).strip()
    if not _IDENT.match(text):
        raise PostgresConfigError(f"Invalid identifier: {name!r}")
    return text


def _require(schema: Dict[str, Any], key: str) -> Any:
    if not schema.get(key):
        raise PostgresConfigError(f'schema must contain "{key}".')
    return schema[key]


def _bindable(value: Any) -> Any:
    # Legacy registered a dict -> Json adapter; json.dumps does the same job
    # without tying the connector to one driver's adapter API.
    return json.dumps(value) if isinstance(value, dict) else value


def _where(schema: Dict[str, Any]) -> Tuple[str, List[Any]]:
    parts: List[str] = []
    values: List[Any] = []
    for clause in schema.get("where") or []:
        if not isinstance(clause, (list, tuple)) or len(clause) != 3:
            raise PostgresConfigError("where entries must be [column, operator, value].")
        column, operator, value = clause
        if operator not in _OPERATORS:
            raise PostgresConfigError(f"Unsupported operator '{operator}' in where clause")
        parts.append(f"{_ident(column)} {operator} %s")
        values.append(_bindable(value))
    return (f" WHERE {' AND '.join(parts)}" if parts else ""), values


def build_create_table(table: str, schema: Dict[str, Any]) -> str:
    definitions = []
    for column in _require(schema, "column_definitions"):
        column_type = str(column.get("type", "")).strip()
        if not _COLUMN_TYPE.match(column_type):
            raise PostgresConfigError(f"Invalid column type: {column_type!r}")
        definitions.append(f"{_ident(column.get('name', ''))} {column_type}")
    return f"CREATE TABLE IF NOT EXISTS {_ident(table)} ({','.join(definitions)});"


def build_insert(table: str, schema: Dict[str, Any]) -> Tuple[str, List[List[Any]]]:
    columns = [_ident(c) for c in _require(schema, "columns")]
    rows = [[_bindable(v) for v in row] for row in _require(schema, "values")]
    placeholders = ",".join(["%s"] * len(columns))
    return f"INSERT INTO {_ident(table)} ({','.join(columns)}) VALUES ({placeholders});", rows  # noqa: S608


def build_select(table: str, schema: Dict[str, Any]) -> Tuple[str, List[Any]]:
    columns = ",".join(_ident(c) for c in _require(schema, "columns"))
    where, values = _where(schema)
    return f"SELECT {columns} FROM {_ident(table)}{where};", values  # noqa: S608


def build_update(table: str, schema: Dict[str, Any]) -> Tuple[str, List[Any]]:
    assignments = _require(schema, "set")
    if not isinstance(assignments, dict):
        raise PostgresConfigError('schema "set" must be an object of column: value.')
    set_clause = ", ".join(f"{_ident(c)} = %s" for c in assignments)
    where, where_values = _where(schema)
    values = [_bindable(v) for v in assignments.values()] + where_values
    return f"UPDATE {_ident(table)} SET {set_clause}{where};", values  # noqa: S608


def build_delete(table: str, schema: Dict[str, Any]) -> Tuple[str, List[Any]]:
    where, values = _where(schema)
    return f"DELETE FROM {_ident(table)}{where};", values  # noqa: S608


# --- execution ---------------------------------------------------------------


def _run(
    connection_string: str,
    sql: str,
    values: Optional[List[Any]],
    *,
    fetch: bool = False,
    many: bool = False,
) -> Any:
    """Execute one statement; return the legacy body (rows or a rowcount message)."""
    driver = _load_driver()
    try:
        connection = driver.connect(connection_string, connect_timeout=_timeout())
    except Exception as exc:  # noqa: BLE001 - driver-specific connection errors
        raise PostgresConnectionError(f"Could not connect to PostgreSQL: {exc}") from exc

    try:
        # ``with connection`` commits on success and rolls back on error.
        with connection:
            with connection.cursor() as cursor:
                try:
                    if many:
                        cursor.executemany(sql, values or [])
                    else:
                        cursor.execute(sql, values or None)
                except Exception as exc:  # noqa: BLE001 - driver-specific SQL errors
                    raise PostgresQueryError(str(exc)) from exc

                if fetch:
                    return [list(row) for row in cursor.fetchall()]
                if cursor.rowcount is not None and cursor.rowcount >= 0:
                    return {"result": f"{cursor.rowcount} rows affected"}
                return {"result": "ok"}
    finally:
        try:
            connection.close()
        except Exception:  # noqa: BLE001 - closing must never mask the real error
            logger.debug("Ignoring error while closing the PostgreSQL connection")


class M8FlowPostgresConnector(BaseConnector):
    """PostgreSQL table operations and raw SQL with the legacy M8Flow contract."""

    connector_id = "m8flow_postgres"
    output_model = PostgresResponseOutput

    # Scoped to m8flow_postgres only.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        PostgresDriverMissingError: (ErrorCategory.FATAL, "POSTGRES_DRIVER_MISSING"),
        PostgresConfigError: (ErrorCategory.BUSINESS, "POSTGRES_CONFIG_ERROR"),
        PostgresQueryError: (ErrorCategory.BUSINESS, "POSTGRES_QUERY_ERROR"),
        PostgresConnectionError: (ErrorCategory.RETRYABLE, "POSTGRES_CONNECT_ERROR"),
    }

    async def _execute(
        self,
        connection_string: str,
        sql: str,
        values: Optional[List[Any]],
        *,
        trace_id: str,
        action: str,
        fetch: bool = False,
        many: bool = False,
    ) -> PostgresResponseOutput:
        # The statement and values may carry literals; log only their shape.
        log_extra = {"trace_id": trace_id, "connector_id": self.connector_id, "action": action}
        logger.info("Running PostgreSQL statement", extra={**log_extra, "statement_length": len(sql)})

        # The driver is blocking; keep the event loop free.
        body = await asyncio.to_thread(_run, connection_string, sql, values, fetch=fetch, many=many)

        logger.info("PostgreSQL statement completed", extra=log_extra)
        return PostgresResponseOutput(body=body)

    @nw_action("create_table")
    async def create_table(self, params: CreateTableInput, *, trace_id: str) -> PostgresResponseOutput:
        """Create a table from column name/type definitions if it does not exist."""
        sql = build_create_table(params.table_name, params.sql_schema)
        return await self._execute(
            params.database_connection_str, sql, None, trace_id=trace_id, action="create_table"
        )

    @nw_action("drop_table")
    async def drop_table(self, params: DropTableInput, *, trace_id: str) -> PostgresResponseOutput:
        """Drop a table if it exists."""
        sql = f"DROP TABLE IF EXISTS {_ident(params.table_name)};"
        return await self._execute(
            params.database_connection_str, sql, None, trace_id=trace_id, action="drop_table"
        )

    @nw_action("insert_values")
    async def insert_values(self, params: InsertValuesInput, *, trace_id: str) -> PostgresResponseOutput:
        """Insert one or more rows."""
        sql, rows = build_insert(params.table_name, params.sql_schema)
        return await self._execute(
            params.database_connection_str, sql, rows, trace_id=trace_id, action="insert_values", many=True
        )

    @nw_action("select_values")
    async def select_values(self, params: SelectValuesInput, *, trace_id: str) -> PostgresResponseOutput:
        """Select columns, optionally filtered by a where clause."""
        sql, values = build_select(params.table_name, params.sql_schema)
        return await self._execute(
            params.database_connection_str, sql, values, trace_id=trace_id, action="select_values", fetch=True
        )

    @nw_action("update_values")
    async def update_values(self, params: UpdateValuesInput, *, trace_id: str) -> PostgresResponseOutput:
        """Set column values, optionally filtered by a where clause."""
        sql, values = build_update(params.table_name, params.sql_schema)
        return await self._execute(
            params.database_connection_str, sql, values, trace_id=trace_id, action="update_values"
        )

    @nw_action("delete_values")
    async def delete_values(self, params: DeleteValuesInput, *, trace_id: str) -> PostgresResponseOutput:
        """Delete rows matching the where clause (no where deletes all rows, as legacy did)."""
        sql, values = build_delete(params.table_name, params.sql_schema)
        return await self._execute(
            params.database_connection_str, sql, values, trace_id=trace_id, action="delete_values"
        )

    @nw_action("do_sql")
    async def do_sql(self, params: DoSqlInput, *, trace_id: str) -> PostgresResponseOutput:
        """Run an arbitrary statement with %s-bound values."""
        schema = params.sql_schema
        sql = str(_require(schema, "sql"))
        values = [_bindable(v) for v in schema.get("values") or []]
        return await self._execute(
            params.database_connection_str,
            sql,
            values,
            trace_id=trace_id,
            action="do_sql",
            fetch=bool(schema.get("fetch_results", False)),
        )
