#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow Salesforce connector.

Fully offline: httpx inside logic.py is patched.

The cases that matter most are the ones that keep bad data out of Salesforce:
an unknown field must fail before any API call, a blank value must be dropped
rather than sent (Salesforce reads an empty string as "clear this field"), and
an expired token must be refreshed and retried once.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import httpx

from node_wire_m8flow_salesforce.logic import M8FlowSalesforceConnector

_CREDENTIALS = {
    "access_token": "token",
    "instance_url": "https://na1.salesforce.com",
}
_REFRESHABLE = {
    **_CREDENTIALS,
    "refresh_token": "refresh",
    "client_id": "client",
    "client_secret": "secret",
}


class _SequencedClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_SequencedClient":
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def request(
        self, method: str, url: str, headers: Any = None, json: Any = None
    ) -> httpx.Response:
        self.calls.append(
            {"method": method, "url": url, "headers": headers, "json": json}
        )
        return self._responses.pop(0)

    async def post(self, url: str, data: Any = None, headers: Any = None) -> httpx.Response:
        self.calls.append({"method": "POST", "url": url, "data": data, "headers": headers})
        return self._responses.pop(0)


def _json_response(status: int, payload: Any = None) -> httpx.Response:
    if payload is None:
        return httpx.Response(status)
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode(),
    )


def _patch(client: _SequencedClient) -> Any:
    return patch(
        "node_wire_m8flow_salesforce.logic.httpx.AsyncClient", lambda **_kw: client
    )


def test_connector_id_and_actions() -> None:
    assert M8FlowSalesforceConnector.connector_id == "m8flow_salesforce"
    assert set(M8FlowSalesforceConnector._action_registry) == {
        "create_lead",
        "read_lead",
        "update_lead",
        "delete_lead",
        "create_contact",
        "read_contact",
        "update_contact",
        "delete_contact",
    }


async def test_create_lead_accepts_a_json_string_and_coerces_numbers() -> None:
    client = _SequencedClient([_json_response(201, {"id": "00Q1", "success": True})])
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_lead",
                **_CREDENTIALS,
                "fields": json.dumps(
                    {"LastName": "Doe", "Company": "Acme", "NumberOfEmployees": "50"}
                ),
            }
        )

    assert response.success
    assert response.data["record_id"] == "00Q1"
    assert client.calls[0]["json"]["NumberOfEmployees"] == 50
    assert client.calls[0]["url"].endswith("/services/data/v58.0/sobjects/Lead")


async def test_an_unknown_field_fails_before_any_api_call() -> None:
    """A wrong field name should cost nothing, not a round trip."""
    client = _SequencedClient([])
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_lead",
                **_CREDENTIALS,
                "fields": {"LastName": "Doe", "Company": "Acme", "Bogus__c": 1},
            }
        )

    assert response.error_code == "SALESFORCE_VALIDATION_ERROR"
    assert "Bogus__c" in response.message
    assert client.calls == []


async def test_a_missing_required_field_is_reported_by_name() -> None:
    connector = M8FlowSalesforceConnector()

    response = await connector.run(
        {"action": "create_lead", **_CREDENTIALS, "fields": {"LastName": "Doe"}}
    )

    assert response.error_code == "SALESFORCE_VALIDATION_ERROR"
    assert "Company" in response.message


async def test_a_blank_value_is_dropped_rather_than_clearing_the_field() -> None:
    """Salesforce treats an empty string as "clear this field"."""
    client = _SequencedClient([_json_response(204)])
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "update_lead",
                **_CREDENTIALS,
                "record_id": "00Q1",
                "fields": {"Email": "", "City": "Springfield"},
            }
        )

    assert response.success
    assert client.calls[0]["json"] == {"City": "Springfield"}


async def test_the_allowlist_applies_to_updates_as_well_as_creates() -> None:
    connector = M8FlowSalesforceConnector()

    response = await connector.run(
        {
            "action": "update_lead",
            **_CREDENTIALS,
            "record_id": "00Q1",
            "fields": {"Bogus__c": "x"},
        }
    )

    assert response.error_code == "SALESFORCE_VALIDATION_ERROR"


async def test_an_update_with_nothing_left_to_send_is_refused() -> None:
    connector = M8FlowSalesforceConnector()

    response = await connector.run(
        {
            "action": "update_lead",
            **_CREDENTIALS,
            "record_id": "00Q1",
            "fields": {"Email": "   "},
        }
    )

    assert response.error_code == "SALESFORCE_VALIDATION_ERROR"


async def test_an_expired_token_is_refreshed_and_the_call_retried_once() -> None:
    client = _SequencedClient(
        [
            _json_response(401, [{"errorCode": "INVALID_SESSION_ID", "message": "expired"}]),
            _json_response(
                200,
                {"access_token": "fresh", "instance_url": "https://na9.salesforce.com"},
            ),
            _json_response(200, {"Id": "00Q1", "LastName": "Doe"}),
        ]
    )
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "read_lead", **_REFRESHABLE, "record_id": "00Q1"}
        )

    assert response.success
    assert client.calls[1]["url"] == "https://login.salesforce.com/services/oauth2/token"
    assert client.calls[2]["headers"]["Authorization"] == "Bearer fresh"
    # The org may have moved; the retry must follow the rotated instance URL.
    assert client.calls[2]["url"].startswith("https://na9.salesforce.com")


async def test_without_the_refresh_triple_a_401_is_surfaced_immediately() -> None:
    client = _SequencedClient(
        [_json_response(401, [{"errorCode": "INVALID_SESSION_ID", "message": "expired"}])]
    )
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "read_lead", **_CREDENTIALS, "record_id": "00Q1"}
        )

    assert response.error_code == "SALESFORCE_AUTH_ERROR"
    assert response.error_category.value == "AUTH"
    assert len(client.calls) == 1


async def test_a_sandbox_instance_refreshes_against_the_sandbox_endpoint() -> None:
    """Refreshing a sandbox token against login.salesforce.com fails opaquely."""
    client = _SequencedClient(
        [
            _json_response(401, {}),
            _json_response(200, {"access_token": "fresh"}),
            _json_response(200, {"Id": "x"}),
        ]
    )
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "read_lead",
                "access_token": "t",
                "instance_url": "https://acme--dev.test.salesforce.com",
                "record_id": "00Q1",
                "refresh_token": "r",
                "client_id": "c",
                "client_secret": "s",
            }
        )

    assert client.calls[1]["url"] == "https://test.salesforce.com/services/oauth2/token"


async def test_salesforce_error_arrays_are_flattened_into_one_message() -> None:
    client = _SequencedClient(
        [_json_response(403, [{"errorCode": "INSUFFICIENT_ACCESS", "message": "no perms"}])]
    )
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "read_lead", **_CREDENTIALS, "record_id": "00Q1"}
        )

    assert response.error_code == "SALESFORCE_PERMISSION_ERROR"
    assert "INSUFFICIENT_ACCESS: no perms" in response.message


async def test_a_delete_returns_no_content() -> None:
    client = _SequencedClient([_json_response(204)])
    connector = M8FlowSalesforceConnector()

    with _patch(client):
        response = await connector.run(
            {"action": "delete_contact", **_CREDENTIALS, "record_id": "003x"}
        )

    assert response.success
    assert response.data["status_code"] == 204
    assert response.data["body"] is None
