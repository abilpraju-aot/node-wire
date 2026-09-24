#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow n8n connector.

Fully offline: httpx inside logic.py is patched.

The load-bearing case here is that an n8n failure comes back as *data* with a
successful envelope. A top-level connector error suspends the M8Flow process
instance and the UI shows an endless spinner, so a workflow must be able to
branch on an n8n failure instead of hanging.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from node_wire_m8flow_n8n.logic import M8FlowN8nConnector


class _FakeClient:
    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def request(
        self,
        method: str,
        url: str,
        headers: Any = None,
        params: Any = None,
        json: Any = None,
        auth: Any = None,
    ) -> httpx.Response:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
                "auth": auth,
            }
        )
        return self._response


def _json_response(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode(),
    )


def _patch(client: _FakeClient) -> Any:
    return patch("node_wire_m8flow_n8n.logic.httpx.AsyncClient", lambda **_kw: client)


def test_connector_id_and_actions() -> None:
    assert M8FlowN8nConnector.connector_id == "m8flow_n8n"
    assert set(M8FlowN8nConnector._action_registry) == {
        "trigger_workflow",
        "list_workflows",
        "get_workflow",
        "list_executions",
        "get_execution",
    }


@pytest.mark.parametrize(
    ("status", "payload", "expected_code"),
    [
        (500, {"message": "boom"}, "N8nRequestFailed"),
        (401, {"message": "nope"}, "N8nAuthError"),
        (404, {"message": "gone"}, "N8nNotFoundError"),
    ],
)
async def test_failures_are_returned_as_data_not_as_connector_errors(
    status: int, payload: Any, expected_code: str
) -> None:
    """A failed call must still produce a successful envelope.

    Otherwise the engine raises, the instance suspends, and the UI spins.
    """
    client = _FakeClient(_json_response(status, payload))
    connector = M8FlowN8nConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "trigger_workflow",
                "webhook_url": "https://n8n.example.com/webhook/a",
            }
        )

    assert response.success is True
    assert response.error_code is None
    assert response.data["error_code"] == expected_code
    assert response.data["status_code"] == status


async def test_get_sends_the_payload_as_query_params() -> None:
    client = _FakeClient(_json_response(200, {"ok": 1}))
    connector = M8FlowN8nConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "trigger_workflow",
                "webhook_url": "https://n8n.example.com/webhook/a",
                "method": "GET",
                "payload": {"q": "1"},
            }
        )

    assert client.calls[0]["params"] == {"q": "1"}
    assert client.calls[0]["json"] is None


async def test_non_get_sends_the_payload_as_a_json_body() -> None:
    client = _FakeClient(_json_response(200, {"ok": 1}))
    connector = M8FlowN8nConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "trigger_workflow",
                "webhook_url": "https://n8n.example.com/webhook/a",
                "payload": {"q": "1"},
            }
        )

    assert client.calls[0]["json"] == {"q": "1"}
    assert client.calls[0]["params"] is None


async def test_a_non_json_success_body_is_returned_as_text() -> None:
    """A Respond-to-Webhook node may answer with text or binary."""
    client = _FakeClient(
        httpx.Response(200, headers={"Content-Type": "text/plain"}, content=b"hello")
    )
    connector = M8FlowN8nConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "trigger_workflow", "webhook_url": "https://n8n.example.com/w/a"}
        )

    assert response.data["body"] == "hello"


async def test_basic_and_header_webhook_auth_modes() -> None:
    connector = M8FlowN8nConnector()

    client = _FakeClient(_json_response(200, {}))
    with _patch(client):
        await connector.run(
            {
                "action": "trigger_workflow",
                "webhook_url": "https://n8n.example.com/w/a",
                "auth_type": "basic",
                "username": "u",
                "password": "p",
            }
        )
    assert client.calls[0]["auth"] == ("u", "p")

    client = _FakeClient(_json_response(200, {}))
    with _patch(client):
        await connector.run(
            {
                "action": "trigger_workflow",
                "webhook_url": "https://n8n.example.com/w/a",
                "auth_type": "header",
                "auth_header_name": "X-Token",
                "auth_header_value": "v",
            }
        )
    assert client.calls[0]["headers"]["X-Token"] == "v"


async def test_public_api_uses_the_api_key_header() -> None:
    client = _FakeClient(_json_response(200, {"data": []}))
    connector = M8FlowN8nConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "list_workflows",
                "base_url": "https://n8n.example.com/",
                "api_key": "k",
            }
        )

    assert client.calls[0]["url"] == "https://n8n.example.com/api/v1/workflows"
    assert client.calls[0]["headers"]["X-N8N-API-KEY"] == "k"


async def test_a_non_http_webhook_url_is_rejected_before_any_call() -> None:
    connector = M8FlowN8nConnector()

    response = await connector.run(
        {"action": "trigger_workflow", "webhook_url": "ftp://example.com/x"}
    )

    assert response.error_code == "VALIDATION_ERROR"
