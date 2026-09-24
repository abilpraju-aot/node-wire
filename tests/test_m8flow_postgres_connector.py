#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow PostgreSQL connector.

Fully offline: the driver loader is patched with a fake, so no database is
contacted and no driver needs to be installed to run these.
"""

from __future__ import annotations

import json
import types
from typing import Any
from unittest.mock import patch

import pytest

from node_wire_m8flow_postgres.logic import (
    M8FlowPostgresConnector,
    PostgresConnectionError,
    PostgresDriverMissingError,
)
from node_wire_runtime import ErrorCategory

_CONNECTION = {"database_connection_str": "dbname=app user=svc host=db.internal"}

# (method, sql, values) for every statement the fake cursor receives.
_EXECUTED: list[tuple[str, str, Any]] = []


class _FakeCursor:
    rowcount = 2

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        _EXECUTED.append(("execute", sql, params))

    def executemany(self, sql: str, params: Any) -> None:
        _EXECUTED.append(("executemany", sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return [("USA", "Georgia"), ("USA", "Virginia")]


class _FakeConnection:
    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def close(self) -> None:
        return None


def _fake_driver() -> Any:
    driver = types.ModuleType("psycopg")
    driver.connect = lambda _dsn, connect_timeout=None: _FakeConnection()
    return driver


@pytest.fixture(autouse=True)
def _reset_executed() -> None:
    _EXECUTED.clear()


async def _run(action: str, **payload: Any) -> Any:
    with patch("node_wire_m8flow_postgres.logic._load_driver", _fake_driver):
        return await M8FlowPostgresConnector().run({"action": action, **_CONNECTION, **payload})


def test_connector_id_and_actions_match_the_legacy_commands() -> None:
    assert M8FlowPostgresConnector.connector_id == "m8flow_postgres"
    assert set(M8FlowPostgresConnector._action_registry) == {
        "create_table",
        "drop_table",
        "insert_values",
        "select_values",
        "update_values",
        "delete_values",
        "do_sql",
    }


async def test_create_table_builds_column_definitions() -> None:
    response = await _run(
        "create_table",
        table_name="states",
        schema={
            "column_definitions": [
                {"name": "country", "type": "varchar(255)"},
                {"name": "somenum", "type": "int"},
            ]
        },
    )

    assert response.success
    assert _EXECUTED == [
        ("execute", "CREATE TABLE IF NOT EXISTS states (country varchar(255),somenum int);", None)
    ]
    assert response.data["body"] == {"result": "2 rows affected"}


async def test_drop_table() -> None:
    response = await _run("drop_table", table_name="public.states")

    assert response.success
    assert _EXECUTED[0][1] == "DROP TABLE IF EXISTS public.states;"


async def test_insert_values_runs_one_batch_with_every_row() -> None:
    rows = [["USA", "Georgia", {"a": 1}], ["USA", "Virginia", None]]
    response = await _run(
        "insert_values",
        table_name="states",
        schema=json.dumps({"columns": ["country", "state", "meta"], "values": rows}),
    )

    assert response.success
    method, sql, values = _EXECUTED[0]
    assert method == "executemany"
    assert sql == "INSERT INTO states (country,state,meta) VALUES (%s,%s,%s);"
    # dicts are JSON-encoded, as the legacy Json adapter did.
    assert values == [["USA", "Georgia", '{"a": 1}'], ["USA", "Virginia", None]]


async def test_select_values_binds_where_values_and_returns_row_lists() -> None:
    response = await _run(
        "select_values",
        table_name="states",
        schema={"columns": ["country", "state"], "where": [["somenum", "=", 33], ["abbrev", "!=", "VA"]]},
    )

    assert response.success
    assert _EXECUTED[0] == (
        "execute",
        "SELECT country,state FROM states WHERE somenum = %s AND abbrev != %s;",
        [33, "VA"],
    )
    assert response.data["body"] == [["USA", "Georgia"], ["USA", "Virginia"]]


async def test_update_values_binds_set_values_before_where_values() -> None:
    await _run(
        "update_values",
        table_name="states",
        schema={"set": {"abbrev": "ZZ"}, "where": [["abbrev", "=", "VA"]]},
    )

    assert _EXECUTED[0] == ("execute", "UPDATE states SET abbrev = %s WHERE abbrev = %s;", ["ZZ", "VA"])


async def test_delete_values_without_where_matches_legacy() -> None:
    await _run("delete_values", table_name="states", schema={})

    assert _EXECUTED[0] == ("execute", "DELETE FROM states;", None)


async def test_do_sql_fetches_only_when_asked() -> None:
    fetched = await _run(
        "do_sql", schema={"sql": "SELECT * FROM states WHERE id = %s", "values": [5], "fetch_results": True}
    )
    written = await _run("do_sql", schema={"sql": "UPDATE states SET x = 1"})

    assert _EXECUTED[0] == ("execute", "SELECT * FROM states WHERE id = %s", [5])
    assert fetched.data["body"] == [["USA", "Georgia"], ["USA", "Virginia"]]
    assert written.data["body"] == {"result": "2 rows affected"}


@pytest.mark.parametrize(
    ("action", "payload"),
    [
        ("drop_table", {"table_name": "states; DROP TABLE users"}),
        ("select_values", {"table_name": "states", "schema": {"columns": ["1; --"]}}),
        ("select_values", {"table_name": "states", "schema": {"columns": ["a"], "where": [["a", "LIKE", "x"]]}}),
        ("create_table", {"table_name": "t", "schema": {"column_definitions": [{"name": "a", "type": "int; DROP"}]}}),
        ("insert_values", {"table_name": "t", "schema": {"columns": ["a"]}}),
        ("do_sql", {"schema": {}}),
    ],
)
async def test_unsafe_or_incomplete_payloads_are_refused(action: str, payload: dict) -> None:
    response = await _run(action, **payload)

    assert response.error_code == "POSTGRES_CONFIG_ERROR"
    assert _EXECUTED == []


async def test_a_missing_driver_is_reported_as_a_configuration_fault() -> None:
    def _raise() -> Any:
        raise PostgresDriverMissingError("No PostgreSQL driver installed.")

    with patch("node_wire_m8flow_postgres.logic._load_driver", _raise):
        response = await M8FlowPostgresConnector().run(
            {"action": "do_sql", **_CONNECTION, "schema": {"sql": "SELECT 1"}}
        )

    assert response.error_code == "POSTGRES_DRIVER_MISSING"


def test_a_connection_failure_is_classified_retryable() -> None:
    """Asserted on the mapping: the runtime retries, then trips the breaker."""
    assert M8FlowPostgresConnector.error_map[PostgresConnectionError] == (
        ErrorCategory.RETRYABLE,
        "POSTGRES_CONNECT_ERROR",
    )


async def test_the_connection_string_never_reaches_the_response() -> None:
    """It carries the password; a failure must not echo it back."""

    def _failing_driver() -> Any:
        driver = types.ModuleType("psycopg")

        def _connect(_dsn: str, connect_timeout: int | None = None) -> Any:
            raise OSError("connection refused")

        driver.connect = _connect
        return driver

    with patch("node_wire_m8flow_postgres.logic._load_driver", _failing_driver):
        response = await M8FlowPostgresConnector().run(
            {"action": "do_sql", **_CONNECTION, "schema": {"sql": "SELECT 1"}}
        )

    assert "dbname=app" not in json.dumps(response.model_dump(mode="json"))
