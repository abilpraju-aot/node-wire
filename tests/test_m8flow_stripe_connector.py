#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow Stripe connector.

Fully offline: httpx inside logic.py is patched, so no Stripe call is made.

The identifier and enum checks are the point of most of these: sending a
malformed id to a payments API costs a live round trip and returns a worse
error than failing locally would.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from node_wire_m8flow_stripe.logic import M8FlowStripeConnector

_KEY = {"api_key": "sk_test_123"}


class _SequencedClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_SequencedClient":
        return self

    async def __aexit__(self, *_exc: Any) -> bool:
        return False

    async def request(
        self, method: str, url: str, headers: Any = None, data: Any = None
    ) -> httpx.Response:
        self.calls.append({"method": method, "url": url, "headers": headers, "data": data})
        return self._responses.pop(0)


def _json_response(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"Content-Type": "application/json"},
        content=json.dumps(payload).encode(),
    )


def _patch(client: _SequencedClient) -> Any:
    return patch("node_wire_m8flow_stripe.logic.httpx.AsyncClient", lambda **_kw: client)


def test_connector_id_and_actions() -> None:
    assert M8FlowStripeConnector.connector_id == "m8flow_stripe"
    assert set(M8FlowStripeConnector._action_registry) == {
        "create_payment_intent",
        "create_charge",
        "create_subscription",
        "cancel_subscription",
        "issue_refund",
    }


async def test_amounts_and_metadata_arrive_form_encoded_with_an_idempotency_key() -> None:
    client = _SequencedClient([_json_response(200, {"id": "pi_1"})])
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_payment_intent",
                **_KEY,
                "amount": "2000",
                "currency": "USD",
                "metadata": '{"order": "A1"}',
            }
        )

    assert response.success
    data = client.calls[0]["data"]
    assert data["amount"] == 2000
    assert data["currency"] == "usd"
    assert data["metadata[order]"] == "A1"
    headers = client.calls[0]["headers"]
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"
    # Generated when the caller pins none, so a retry cannot double charge.
    assert headers["Idempotency-Key"]


async def test_an_explicit_idempotency_key_is_used_verbatim() -> None:
    client = _SequencedClient([_json_response(200, {"id": "pi_1"})])
    connector = M8FlowStripeConnector()

    with _patch(client):
        await connector.run(
            {
                "action": "create_payment_intent",
                **_KEY,
                "amount": "1",
                "currency": "usd",
                "idempotency_key": "caller-key",
            }
        )

    assert client.calls[0]["headers"]["Idempotency-Key"] == "caller-key"


async def test_the_whole_stripe_object_is_returned() -> None:
    """Callers branch on fields the connector cannot anticipate."""
    client = _SequencedClient(
        [_json_response(200, {"id": "ch_1", "paid": True, "outcome": {"network_status": "approved"}})]
    )
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_charge",
                **_KEY,
                "amount": "100",
                "currency": "usd",
                "source": "tok_visa",
            }
        )

    assert response.data["body"]["outcome"]["network_status"] == "approved"


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "create_subscription", "customer_id": "sub_wrong", "price_id": "price_1"},
        {"action": "create_subscription", "customer_id": "cus_1", "price_id": "prod_1"},
        {"action": "cancel_subscription", "subscription_id": "cus_1"},
        {"action": "issue_refund", "charge_id": "pi_1"},
    ],
)
async def test_malformed_identifiers_fail_before_any_api_call(
    payload: dict[str, Any],
) -> None:
    client = _SequencedClient([])
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run({**payload, **_KEY})

    assert response.error_code == "VALIDATION_ERROR"
    assert client.calls == []


async def test_a_refund_needs_a_charge_or_a_payment_intent() -> None:
    connector = M8FlowStripeConnector()

    response = await connector.run({"action": "issue_refund", **_KEY})

    assert response.error_code == "VALIDATION_ERROR"


async def test_the_refund_reason_is_checked_and_normalised() -> None:
    connector = M8FlowStripeConnector()

    rejected = await connector.run(
        {"action": "issue_refund", **_KEY, "charge_id": "ch_1", "reason": "because"}
    )
    assert rejected.error_code == "VALIDATION_ERROR"

    client = _SequencedClient([_json_response(200, {"id": "re_1"})])
    with _patch(client):
        await connector.run(
            {"action": "issue_refund", **_KEY, "charge_id": "ch_1", "reason": "DUPLICATE"}
        )
    assert client.calls[0]["data"]["reason"] == "duplicate"


async def test_an_unknown_payment_behaviour_is_refused() -> None:
    connector = M8FlowStripeConnector()

    response = await connector.run(
        {
            "action": "create_subscription",
            **_KEY,
            "customer_id": "cus_1",
            "price_id": "price_1",
            "payment_behavior": "nonsense",
        }
    )

    assert response.error_code == "VALIDATION_ERROR"


async def test_a_declined_card_reports_its_decline_code() -> None:
    """Without the decline code the caller cannot tell the customer anything."""
    client = _SequencedClient(
        [
            _json_response(
                402,
                {
                    "error": {
                        "type": "card_error",
                        "message": "Your card was declined.",
                        "decline_code": "insufficient_funds",
                    }
                },
            )
        ]
    )
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_charge",
                **_KEY,
                "amount": "100",
                "currency": "usd",
                "source": "tok_chargeDeclined",
            }
        )

    assert response.error_code == "STRIPE_CARD_ERROR"
    assert "insufficient_funds" in response.message


async def test_an_authentication_failure_is_classified_as_auth() -> None:
    client = _SequencedClient(
        [_json_response(401, {"error": {"type": "authentication_error", "message": "bad key"}})]
    )
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_charge",
                **_KEY,
                "amount": "1",
                "currency": "usd",
                "source": "tok_visa",
            }
        )

    assert response.error_code == "STRIPE_AUTH_ERROR"
    assert response.error_category.value == "AUTH"


async def test_a_card_token_is_turned_into_an_attached_payment_method() -> None:
    client = _SequencedClient(
        [
            _json_response(200, {"id": "pm_1"}),
            _json_response(200, {"id": "pm_1"}),
            _json_response(200, {"id": "sub_1"}),
        ]
    )
    connector = M8FlowStripeConnector()

    with _patch(client):
        response = await connector.run(
            {
                "action": "create_subscription",
                **_KEY,
                "customer_id": "cus_1",
                "price_id": "price_1",
                "card_token": "tok_visa",
            }
        )

    assert response.success
    assert response.data["resource_id"] == "sub_1"
    assert client.calls[0]["url"].endswith("/payment_methods")
    assert client.calls[0]["data"]["card[token]"] == "tok_visa"
    assert client.calls[1]["url"].endswith("/payment_methods/pm_1/attach")
    assert client.calls[2]["data"]["default_payment_method"] == "pm_1"
    assert client.calls[2]["data"]["items[0][price]"] == "price_1"


async def test_cancelling_now_deletes_while_period_end_updates() -> None:
    connector = M8FlowStripeConnector()

    client = _SequencedClient([_json_response(200, {"id": "sub_1"})])
    with _patch(client):
        await connector.run(
            {
                "action": "cancel_subscription",
                **_KEY,
                "subscription_id": "sub_1",
                "cancel_at_period_end": "true",
            }
        )
    assert client.calls[0]["method"] == "POST"
    assert client.calls[0]["data"]["cancel_at_period_end"] == "true"

    client = _SequencedClient([_json_response(200, {"id": "sub_1"})])
    with _patch(client):
        await connector.run(
            {"action": "cancel_subscription", **_KEY, "subscription_id": "sub_1"}
        )
    assert client.calls[0]["method"] == "DELETE"
