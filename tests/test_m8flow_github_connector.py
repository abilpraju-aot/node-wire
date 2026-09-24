#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow GitHub connector.

Fully offline: httpx inside logic.py is patched, so no real GitHub API is
contacted.

Coverage
--------
- connector_id and the three action names
- connect_repository / list_branches / list_pull_requests request shape
- ``protected`` is forwarded only when it is exactly "true" or "false"
- GitHub API version and Accept headers
- 401/403 -> AUTH, 404 -> BUSINESS, non-JSON body -> BUSINESS
- The token never reaches the response envelope (security boundary)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from node_wire_m8flow_github.logic import M8FlowGithubConnector


class _FakeClient:
    """Async context manager returning one canned response per GET."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def get(
        self, url: str, headers: dict[str, str] | None = None, params: Any = None
    ) -> httpx.Response:
        self.calls.append({"url": url, "headers": headers, "params": params})
        return self._response


def _json_response(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode(),
    )


def _patch(client: _FakeClient) -> Any:
    return patch(
        "node_wire_m8flow_github.logic.httpx.AsyncClient", lambda **_kw: client
    )


def test_connector_id_and_actions() -> None:
    assert M8FlowGithubConnector.connector_id == "m8flow_github"
    assert set(M8FlowGithubConnector._action_registry) == {
        "connect_repository",
        "list_branches",
        "list_pull_requests",
    }


@pytest.mark.asyncio
async def test_connect_repository_calls_the_repo_endpoint() -> None:
    client = _FakeClient(_json_response(200, {"full_name": "octocat/hello-world"}))
    connector = M8FlowGithubConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "connect_repository",
                "token": "ghp_x",
                "owner": "octocat",
                "repo": "hello-world",
            }
        )

    assert response.success
    assert response.data["status_code"] == 200
    assert client.calls[0]["url"] == "https://api.github.com/repos/octocat/hello-world"
    headers = client.calls[0]["headers"]
    assert headers["Authorization"] == "Bearer ghp_x"
    assert headers["Accept"] == "application/vnd.github+json"
    assert headers["X-GitHub-Api-Version"] == "2022-11-28"


@pytest.mark.asyncio
async def test_list_branches_forwards_protected_only_when_set() -> None:
    """An empty ``protected`` means "no filter"; GitHub rejects it as a value."""
    client = _FakeClient(_json_response(200, [{"name": "main"}]))
    connector = M8FlowGithubConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "list_branches",
                "token": "t",
                "owner": "o",
                "repo": "r",
                "protected": "true",
                "per_page": 50,
            }
        )
    assert client.calls[0]["params"] == {"per_page": 50, "page": 1, "protected": "true"}

    client = _FakeClient(_json_response(200, []))
    with _patch(client):
        await connector.run(
            {"action": "list_branches", "token": "t", "owner": "o", "repo": "r"}
        )
    assert "protected" not in client.calls[0]["params"]


@pytest.mark.asyncio
async def test_list_pull_requests_rejects_an_unknown_state() -> None:
    connector = M8FlowGithubConnector()

    response = await connector.run(
        {
            "action": "list_pull_requests",
            "token": "t",
            "owner": "o",
            "repo": "r",
            "state": "sideways",
        }
    )

    assert response.error_code == "VALIDATION_ERROR"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "payload", "expected_code", "expected_category"),
    [
        (401, {"message": "Bad credentials"}, "GITHUB_AUTH_ERROR", "AUTH"),
        (403, {"message": "Forbidden"}, "GITHUB_AUTH_ERROR", "AUTH"),
        (404, {"message": "Not Found"}, "GITHUB_NOT_FOUND", "BUSINESS"),
        (422, {"message": "Unprocessable"}, "GITHUB_REQUEST_FAILED", "BUSINESS"),
    ],
)
async def test_error_status_maps_to_the_legacy_taxonomy(
    status: int, payload: Any, expected_code: str, expected_category: str
) -> None:
    client = _FakeClient(_json_response(status, payload))
    connector = M8FlowGithubConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "connect_repository", "token": "t", "owner": "o", "repo": "r"}
        )

    assert response.error_code == expected_code
    assert response.error_category.value == expected_category


@pytest.mark.asyncio
async def test_non_json_body_is_reported_rather_than_parsed() -> None:
    client = _FakeClient(
        httpx.Response(502, headers={"Content-Type": "text/html"}, content=b"<html>")
    )
    connector = M8FlowGithubConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "connect_repository", "token": "t", "owner": "o", "repo": "r"}
        )

    assert response.error_code == "GITHUB_REQUEST_FAILED"


@pytest.mark.asyncio
async def test_token_never_appears_in_the_response() -> None:
    """Security boundary: a failure must not echo the credential back."""
    client = _FakeClient(_json_response(401, {"message": "Bad credentials"}))
    connector = M8FlowGithubConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "connect_repository",
                "token": "ghp_super_secret",
                "owner": "o",
                "repo": "r",
            }
        )

    assert "ghp_super_secret" not in json.dumps(response.model_dump(mode="json"))
