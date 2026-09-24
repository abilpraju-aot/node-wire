#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow Slack connector.

Fully offline: httpx inside logic.py is patched.

Two legacy behaviours are asserted explicitly because they differ from the
other Slack connector in this repo: the token is supplied by value, and a
direct message addresses the user through ``user_id``.
"""

from __future__ import annotations

import base64
import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from node_wire_m8flow_slack.logic import (
    M8FlowSlackConnector,
    SlackRateLimitError,
    _parse_slack_json,
)
from node_wire_runtime import ErrorCategory


class _SequencedClient:
    """Returns queued responses in order and records every POST."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_SequencedClient":
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def post(
        self,
        url: str,
        headers: Any = None,
        json: Any = None,
        data: Any = None,
        content: Any = None,
    ) -> httpx.Response:
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "data": data,
                "content": content,
            }
        )
        return self._responses.pop(0)


def _json_response(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode(),
    )


def _patch(client: _SequencedClient) -> Any:
    return patch("node_wire_m8flow_slack.logic.httpx.AsyncClient", lambda **_kw: client)


def test_connector_id_and_actions() -> None:
    assert M8FlowSlackConnector.connector_id == "m8flow_slack"
    assert set(M8FlowSlackConnector._action_registry) == {
        "post_message",
        "send_direct_message",
        "upload_file",
    }


async def test_post_message_sends_the_token_by_value() -> None:
    client = _SequencedClient(
        [_json_response(200, {"ok": True, "channel": "C1", "ts": "1.2"})]
    )
    connector = M8FlowSlackConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "post_message",
                "token": "xoxb-token",
                "channel": "#general",
                "message": "hi",
            }
        )

    assert response.success
    assert response.data["ts"] == "1.2"
    assert client.calls[0]["headers"]["Authorization"] == "Bearer xoxb-token"
    assert client.calls[0]["json"]["channel"] == "#general"


async def test_direct_message_addresses_the_user_by_user_id() -> None:
    """Legacy process models pass ``user_id``, not ``channel``."""
    client = _SequencedClient(
        [_json_response(200, {"ok": True, "channel": "D1", "ts": "2.0"})]
    )
    connector = M8FlowSlackConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "send_direct_message",
                "token": "t",
                "user_id": "U123",
                "message": "yo",
            }
        )

    assert response.success
    assert client.calls[0]["json"]["channel"] == "U123"


async def test_blocks_accept_a_json_string() -> None:
    client = _SequencedClient([_json_response(200, {"ok": True})])
    connector = M8FlowSlackConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "post_message",
                "token": "t",
                "channel": "C1",
                "message": "m",
                "blocks": '[{"type": "divider"}]',
            }
        )

    assert client.calls[0]["json"]["blocks"] == [{"type": "divider"}]


@pytest.mark.parametrize(
    ("slack_error", "expected_code", "expected_category"),
    [
        ("invalid_auth", "SLACK_AUTH_ERROR", "AUTH"),
        ("missing_scope", "SLACK_PERMISSION_ERROR", "AUTH"),
        ("channel_not_found", "SLACK_MESSAGE_FAILED", "BUSINESS"),
    ],
)
async def test_slack_errors_map_to_the_legacy_taxonomy(
    slack_error: str, expected_code: str, expected_category: str
) -> None:
    """Slack reports logical failures with HTTP 200 and ``ok: false``."""
    client = _SequencedClient(
        [_json_response(200, {"ok": False, "error": slack_error})]
    )
    connector = M8FlowSlackConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "post_message", "token": "t", "channel": "C1", "message": "m"}
        )

    assert response.error_code == expected_code
    assert response.error_category.value == expected_category


def test_rate_limiting_is_classified_retryable() -> None:
    """Asserted directly: the runtime retries this one, then trips the breaker."""
    with pytest.raises(SlackRateLimitError):
        _parse_slack_json(_json_response(200, {"ok": False, "error": "ratelimited"}))

    assert M8FlowSlackConnector.error_map[SlackRateLimitError] == (
        ErrorCategory.RETRYABLE,
        "SLACK_RATE_LIMIT",
    )


async def test_upload_runs_the_three_step_flow_and_accepts_a_channel_name() -> None:
    client = _SequencedClient(
        [
            _json_response(
                200,
                {"ok": True, "upload_url": "https://files.slack/x", "file_id": "F1"},
            ),
            httpx.Response(200, content=b""),
            _json_response(200, {"ok": True, "files": [{"id": "F1"}]}),
        ]
    )
    connector = M8FlowSlackConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "upload_file",
                "token": "t",
                "channel": "#general",
                "filename": "a.txt",
                "content_base64": base64.b64encode(b"hello").decode(),
            }
        )

    assert response.success
    assert response.data["file_id"] == "F1"
    assert client.calls[0]["data"] == {"filename": "a.txt", "length": 5}
    assert client.calls[1]["content"] == b"hello"
    # The pre-signed URL is already authorised; sending the bot token there
    # would leak it to storage.
    assert "Authorization" not in (client.calls[1]["headers"] or {})
    # A channel *name* must survive: legacy process models pass "#general".
    assert client.calls[2]["data"]["channel_id"] == "#general"


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "upload_file", "token": "t", "channel": "C1"},
        {
            "action": "upload_file",
            "token": "t",
            "channel": "C1",
            "filepath": "/x",
            "content_base64": "aGk=",
        },
    ],
)
async def test_upload_requires_exactly_one_content_source(
    payload: dict[str, Any],
) -> None:
    connector = M8FlowSlackConnector()

    response = await connector.run(payload)

    assert response.error_code == "SLACK_UPLOAD_FAILED"


async def test_upload_path_outside_the_attachments_directory_is_refused() -> None:
    connector = M8FlowSlackConnector()

    response = await connector.run(
        {
            "action": "upload_file",
            "token": "t",
            "channel": "C1",
            "filepath": "/etc/passwd",
        }
    )

    assert response.error_code == "SLACK_UPLOAD_FAILED"


async def test_a_non_json_slack_response_is_reported() -> None:
    client = _SequencedClient(
        [httpx.Response(502, headers={"Content-Type": "text/html"}, content=b"<html>")]
    )
    connector = M8FlowSlackConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "post_message", "token": "t", "channel": "C1", "message": "m"}
        )

    assert response.error_code == "SLACK_MESSAGE_FAILED"
