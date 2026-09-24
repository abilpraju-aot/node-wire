#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow Stripe connector.

Identifier prefixes and enum values are checked here rather than left to
Stripe. A wrong id shape is a caller mistake that costs a live API round trip
and comes back as an opaque ``invalid_request_error``; failing locally names
the offending field instead.

Numbers and booleans arrive as strings because a BPMN task datum is text; they
are coerced, not rejected.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

STRIPE_API_BASE = "https://api.stripe.com/v1"

VALID_REFUND_REASONS = ("duplicate", "fraudulent", "requested_by_customer")
VALID_PAYMENT_BEHAVIORS = (
    "default_incomplete",
    "error_if_incomplete",
    "allow_incomplete",
    "pending_if_incomplete",
)

_TRUE_VALUES = ("true", "1", "yes")
_FALSE_VALUES = ("false", "0", "no", "")


def coerce_bool(value: Any, *, default: bool = False) -> bool:
    """Parse a boolean that may arrive as a string, falling back to *default*."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return default


def coerce_amount(value: Any) -> int:
    """Amounts are in the currency's smallest unit and must be positive."""
    try:
        amount = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("amount must be a positive integer") from exc
    if amount <= 0:
        raise ValueError("amount must be a positive integer")
    return amount


def coerce_metadata(value: Any) -> Optional[Dict[str, Any]]:
    """Accept a JSON object, or a JSON string holding one."""
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("metadata must be a JSON object") from exc
    if not isinstance(parsed, dict):
        raise ValueError("metadata must be a JSON object")
    return parsed


def _check_prefix(value: str, prefix: str, field_name: str) -> str:
    cleaned = str(value).strip()
    if not cleaned.startswith(prefix):
        raise ValueError(f"{field_name} must start with '{prefix}'")
    return cleaned


class _StripeBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_key: str = Field(..., description="Stripe secret key (sk_test_... / sk_live_...).")
    metadata: Optional[Dict[str, Any]] = Field(default=None)
    idempotency_key: str = Field(
        default="", description="Reused across retries; generated when omitted."
    )

    @field_validator("api_key")
    @classmethod
    def reject_blank_key(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("api_key must not be empty")
        return str(value).strip()

    @field_validator("metadata", mode="before")
    @classmethod
    def parse_metadata(cls, value: Any) -> Optional[Dict[str, Any]]:
        return coerce_metadata(value)


class _MoneyMixin(BaseModel):
    amount: int = Field(...)
    currency: str = Field(...)

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, value: Any) -> int:
        return coerce_amount(value)

    @field_validator("currency", mode="before")
    @classmethod
    def parse_currency(cls, value: Any) -> str:
        text = str(value or "").strip()
        if len(text) != 3 or not text.isalpha():
            raise ValueError("currency must be a 3-letter ISO code")
        return text.lower()


class CreatePaymentIntentInput(_StripeBase, _MoneyMixin):
    action: Literal["create_payment_intent"] = "create_payment_intent"
    customer_id: str = Field(default="")
    payment_method: str = Field(default="")
    confirm: bool = Field(default=False)
    description: str = Field(default="")

    @field_validator("customer_id")
    @classmethod
    def check_customer(cls, value: str) -> str:
        return _check_prefix(value, "cus_", "customer_id") if str(value).strip() else ""

    @field_validator("payment_method")
    @classmethod
    def check_payment_method(cls, value: str) -> str:
        return _check_prefix(value, "pm_", "payment_method") if str(value).strip() else ""

    @field_validator("confirm", mode="before")
    @classmethod
    def parse_confirm(cls, value: Any) -> bool:
        return coerce_bool(value)


class CreateChargeInput(_StripeBase, _MoneyMixin):
    action: Literal["create_charge"] = "create_charge"
    source: str = Field(..., description="Token or card id to charge.")
    customer_id: str = Field(default="")
    description: str = Field(default="")

    @field_validator("source")
    @classmethod
    def require_source(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("source is required")
        return str(value).strip()

    @field_validator("customer_id")
    @classmethod
    def check_customer(cls, value: str) -> str:
        return _check_prefix(value, "cus_", "customer_id") if str(value).strip() else ""


class CreateSubscriptionInput(_StripeBase):
    action: Literal["create_subscription"] = "create_subscription"
    customer_id: str = Field(...)
    price_id: str = Field(...)
    payment_behavior: str = Field(default="default_incomplete")
    default_payment_method: str = Field(default="")
    card_token: str = Field(
        default="", description="Creates and attaches a PaymentMethod before subscribing."
    )

    @field_validator("customer_id")
    @classmethod
    def check_customer(cls, value: str) -> str:
        return _check_prefix(value, "cus_", "customer_id")

    @field_validator("price_id")
    @classmethod
    def check_price(cls, value: str) -> str:
        return _check_prefix(value, "price_", "price_id")

    @field_validator("payment_behavior")
    @classmethod
    def check_behavior(cls, value: str) -> str:
        cleaned = str(value).strip() or "default_incomplete"
        if cleaned not in VALID_PAYMENT_BEHAVIORS:
            raise ValueError(
                f"payment_behavior must be one of {', '.join(VALID_PAYMENT_BEHAVIORS)}"
            )
        return cleaned


class CancelSubscriptionInput(_StripeBase):
    action: Literal["cancel_subscription"] = "cancel_subscription"
    subscription_id: str = Field(...)
    cancel_at_period_end: bool = Field(default=False)

    @field_validator("subscription_id")
    @classmethod
    def check_subscription(cls, value: str) -> str:
        return _check_prefix(value, "sub_", "subscription_id")

    @field_validator("cancel_at_period_end", mode="before")
    @classmethod
    def parse_flag(cls, value: Any) -> bool:
        return coerce_bool(value)


class IssueRefundInput(_StripeBase):
    action: Literal["issue_refund"] = "issue_refund"
    charge_id: str = Field(default="")
    payment_intent_id: str = Field(default="")
    amount: Optional[int] = Field(default=None, description="Omit for a full refund.")
    reason: str = Field(default="")

    @field_validator("charge_id")
    @classmethod
    def check_charge(cls, value: str) -> str:
        return _check_prefix(value, "ch_", "charge_id") if str(value).strip() else ""

    @field_validator("payment_intent_id")
    @classmethod
    def check_intent(cls, value: str) -> str:
        return _check_prefix(value, "pi_", "payment_intent_id") if str(value).strip() else ""

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount(cls, value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        return coerce_amount(value)

    @field_validator("reason")
    @classmethod
    def check_reason(cls, value: str) -> str:
        cleaned = str(value).strip().lower()
        if not cleaned:
            return ""
        if cleaned not in VALID_REFUND_REASONS:
            raise ValueError(f"reason must be one of {', '.join(VALID_REFUND_REASONS)}")
        return cleaned

    @model_validator(mode="after")
    def require_one_target(self) -> "IssueRefundInput":
        if not self.charge_id and not self.payment_intent_id:
            raise ValueError("Either charge_id or payment_intent_id is required")
        return self


class StripeResponseOutput(BaseModel):
    """Stripe's object, unabridged, plus the status it arrived with.

    The whole object is returned because callers branch on fields the
    connector cannot anticipate -- ``decline_code``, ``outcome``, invoice
    lines. ``resource_id`` is lifted out for convenience.
    """

    status_code: int
    body: Optional[Any] = None
    resource_id: Optional[str] = None
