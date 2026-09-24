#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow Stripe connector: charges, payment intents, subscriptions, refunds.

Ported from the legacy ``connector_stripe`` package: the API key arrives per
call, requests are form-encoded against the REST API, every write carries an
``Idempotency-Key``, and the full Stripe object is returned so callers can read
``decline_code`` and friends.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any, ClassVar, Dict, Optional, Tuple, Type

import httpx

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    STRIPE_API_BASE,
    CancelSubscriptionInput,
    CreateChargeInput,
    CreatePaymentIntentInput,
    CreateSubscriptionInput,
    IssueRefundInput,
    StripeResponseOutput,
)

logger = logging.getLogger("connectors.m8flow_stripe")


class StripeAuthError(Exception):
    """Stripe rejected the API key."""


class StripeCardError(Exception):
    """The card was declined. The message carries Stripe's decline_code."""


class StripeValidationError(Exception):
    """Stripe refused the request parameters."""


class StripeRateLimitError(Exception):
    """Stripe is rate limiting this key."""


class StripeApiError(Exception):
    """Any other Stripe API failure."""


def _timeout() -> float:
    return float(os.getenv("NW_TIMEOUT", "30.0"))


def _flatten(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Flatten nested dicts into Stripe's ``metadata[key]`` form encoding."""
    flat: Dict[str, Any] = {}
    for key, value in data.items():
        composed = f"{prefix}[{key}]" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten(value, composed))
        elif value is not None and value != "":
            flat[composed] = value
    return flat


def _raise_for_stripe_error(payload: Any, status_code: int) -> None:
    """Map Stripe's error object onto the legacy taxonomy.

    ``decline_code`` and ``code`` are appended to the message: for a payments
    integration they are the whole reason a caller wants the error.
    """
    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    error_type = str(error.get("type", "") or "")
    message = str(error.get("message", "") or "Unknown Stripe error")

    if error_type == "authentication_error" or status_code == 401:
        raise StripeAuthError("Stripe authentication failed. Check your API key.")
    if error_type == "card_error":
        decline_code = error.get("decline_code")
        if decline_code:
            message = f"{message} (decline_code: {decline_code})"
        raise StripeCardError(message)
    if error_type == "invalid_request_error":
        code = error.get("code")
        if code:
            message = f"{message} (code: {code})"
        raise StripeValidationError(message)
    if error_type == "rate_limit_error" or status_code == 429:
        raise StripeRateLimitError(message)
    raise StripeApiError(message)


class M8FlowStripeConnector(BaseConnector):
    """Stripe payment operations with the legacy M8Flow contract."""

    connector_id = "m8flow_stripe"
    output_model = StripeResponseOutput

    # Scoped to m8flow_stripe only.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        StripeAuthError: (ErrorCategory.AUTH, "STRIPE_AUTH_ERROR"),
        StripeCardError: (ErrorCategory.BUSINESS, "STRIPE_CARD_ERROR"),
        StripeValidationError: (ErrorCategory.BUSINESS, "STRIPE_VALIDATION_ERROR"),
        StripeApiError: (ErrorCategory.BUSINESS, "STRIPE_API_ERROR"),
        StripeRateLimitError: (ErrorCategory.RETRYABLE, "STRIPE_RATE_LIMIT"),
        httpx.TimeoutException: (ErrorCategory.RETRYABLE, "STRIPE_TIMEOUT"),
        httpx.ConnectError: (ErrorCategory.RETRYABLE, "STRIPE_CONNECT_ERROR"),
        httpx.RequestError: (ErrorCategory.RETRYABLE, "STRIPE_REQUEST_ERROR"),
    }

    async def _call(
        self,
        method: str,
        path: str,
        api_key: str,
        body: Optional[Dict[str, Any]] = None,
        *,
        idempotency_key: str = "",
        trace_id: str,
        action: str,
    ) -> StripeResponseOutput:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        # Stripe dedupes writes by this key, so a retried call cannot double
        # charge. Generated when the caller does not pin one.
        if method.upper() == "POST":
            headers["Idempotency-Key"] = idempotency_key or str(uuid.uuid4())

        logger.info(
            "Calling Stripe",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
                "method": method,
                "path": path,
            },
        )

        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            response = await client.request(
                method,
                f"{STRIPE_API_BASE}/{path}",
                headers=headers,
                data=_flatten(body or {}) or None,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise StripeApiError("Unreadable (non JSON) response from Stripe") from exc

        if response.status_code >= 400:
            _raise_for_stripe_error(payload, response.status_code)

        return StripeResponseOutput(
            status_code=response.status_code,
            body=payload,
            resource_id=payload.get("id") if isinstance(payload, dict) else None,
        )

    @nw_action("create_payment_intent")
    async def create_payment_intent(
        self, params: CreatePaymentIntentInput, *, trace_id: str
    ) -> StripeResponseOutput:
        """Create a PaymentIntent."""
        body: Dict[str, Any] = {"amount": params.amount, "currency": params.currency}
        if params.customer_id:
            body["customer"] = params.customer_id
        if params.payment_method:
            body["payment_method"] = params.payment_method
        if params.confirm:
            body["confirm"] = "true"
        if params.description:
            body["description"] = params.description
        if params.metadata:
            body["metadata"] = params.metadata
        return await self._call(
            "POST", "payment_intents", params.api_key, body,
            idempotency_key=params.idempotency_key,
            trace_id=trace_id, action="create_payment_intent",
        )

    @nw_action("create_charge")
    async def create_charge(
        self, params: CreateChargeInput, *, trace_id: str
    ) -> StripeResponseOutput:
        """Create a Charge."""
        body: Dict[str, Any] = {
            "amount": params.amount,
            "currency": params.currency,
            "source": params.source,
        }
        if params.customer_id:
            body["customer"] = params.customer_id
        if params.description:
            body["description"] = params.description
        if params.metadata:
            body["metadata"] = params.metadata
        return await self._call(
            "POST", "charges", params.api_key, body,
            idempotency_key=params.idempotency_key,
            trace_id=trace_id, action="create_charge",
        )

    @nw_action("create_subscription")
    async def create_subscription(
        self, params: CreateSubscriptionInput, *, trace_id: str
    ) -> StripeResponseOutput:
        """Create a subscription, optionally attaching a card token first."""
        payment_method = params.default_payment_method

        if params.card_token:
            created = await self._call(
                "POST", "payment_methods", params.api_key,
                {"type": "card", "card": {"token": params.card_token}},
                trace_id=trace_id, action="create_subscription",
            )
            payment_method_id = (
                created.body.get("id") if isinstance(created.body, dict) else None
            )
            if not payment_method_id:
                raise StripeApiError("PaymentMethod created but no id returned")
            await self._call(
                "POST", f"payment_methods/{payment_method_id}/attach", params.api_key,
                {"customer": params.customer_id},
                trace_id=trace_id, action="create_subscription",
            )
            payment_method = payment_method_id

        body: Dict[str, Any] = {
            "customer": params.customer_id,
            "items": {"0": {"price": params.price_id}},
            "payment_behavior": params.payment_behavior,
        }
        if payment_method:
            body["default_payment_method"] = payment_method
        if params.metadata:
            body["metadata"] = params.metadata
        return await self._call(
            "POST", "subscriptions", params.api_key, body,
            idempotency_key=params.idempotency_key,
            trace_id=trace_id, action="create_subscription",
        )

    @nw_action("cancel_subscription")
    async def cancel_subscription(
        self, params: CancelSubscriptionInput, *, trace_id: str
    ) -> StripeResponseOutput:
        """Cancel now, or schedule cancellation at the period end."""
        if params.cancel_at_period_end:
            return await self._call(
                "POST", f"subscriptions/{params.subscription_id}", params.api_key,
                {"cancel_at_period_end": "true"},
                idempotency_key=params.idempotency_key,
                trace_id=trace_id, action="cancel_subscription",
            )
        return await self._call(
            "DELETE", f"subscriptions/{params.subscription_id}", params.api_key,
            trace_id=trace_id, action="cancel_subscription",
        )

    @nw_action("issue_refund")
    async def issue_refund(
        self, params: IssueRefundInput, *, trace_id: str
    ) -> StripeResponseOutput:
        """Refund a charge or payment intent, fully or partially."""
        body: Dict[str, Any] = {}
        if params.charge_id:
            body["charge"] = params.charge_id
        if params.payment_intent_id:
            body["payment_intent"] = params.payment_intent_id
        if params.amount is not None:
            body["amount"] = params.amount
        if params.reason:
            body["reason"] = params.reason
        if params.metadata:
            body["metadata"] = params.metadata
        return await self._call(
            "POST", "refunds", params.api_key, body,
            idempotency_key=params.idempotency_key,
            trace_id=trace_id, action="issue_refund",
        )
