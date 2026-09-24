#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow Salesforce connector.

Credentials arrive per call (``access_token``, ``instance_url`` and the
optional refresh triple) and record fields arrive as ``fields`` -- a JSON
string or an object -- because the M8Flow Salesforce profile stores exactly
those names.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

API_VERSION = "v58.0"


class _SalesforceBase(BaseModel):
    """Credentials shared by every action."""

    model_config = ConfigDict(extra="forbid")

    access_token: str = Field(..., description="OAuth access token.")
    instance_url: str = Field(..., description="Org instance URL.")

    # Supplying all three enables a one-shot refresh when the token has expired.
    refresh_token: str = Field(default="")
    client_id: str = Field(default="")
    client_secret: str = Field(default="")

    @field_validator("access_token", "instance_url")
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("value must not be empty")
        return str(value).strip().rstrip("/")


def can_refresh(params: "_SalesforceBase") -> bool:
    """Whether the OAuth2 refresh triple is present.

    A module-level function rather than a method: Cython compiles a plain
    method into a ``cyfunction``, which pydantic then mistakes for an
    un-annotated field and refuses to build the model.
    """
    return bool(params.refresh_token and params.client_id and params.client_secret)


class _RecordIdMixin(BaseModel):
    record_id: str = Field(..., description="The record's Salesforce id.")

    @field_validator("record_id")
    @classmethod
    def validate_record_id(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("record_id must not be empty")
        return cleaned


def _parse_fields(value: Any) -> Dict[str, Any]:
    """Accept a JSON string or a mapping; reject anything else."""
    if value is None:
        raise ValueError("fields is required")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("fields must not be empty")
        try:
            parsed = json.loads(text)
        except ValueError as exc:
            raise ValueError("fields must be valid JSON") from exc
    else:
        parsed = value
    if not isinstance(parsed, dict):
        raise ValueError("fields must be a JSON object")
    return parsed


class _FieldsMixin(BaseModel):
    fields: Dict[str, Any] = Field(..., description="Record fields as JSON or an object.")

    @field_validator("fields", mode="before")
    @classmethod
    def coerce_fields(cls, value: Any) -> Dict[str, Any]:
        return _parse_fields(value)


class CreateLeadInput(_SalesforceBase, _FieldsMixin):
    action: Literal["create_lead"] = "create_lead"


class CreateContactInput(_SalesforceBase, _FieldsMixin):
    action: Literal["create_contact"] = "create_contact"


class ReadLeadInput(_SalesforceBase, _RecordIdMixin):
    action: Literal["read_lead"] = "read_lead"


class ReadContactInput(_SalesforceBase, _RecordIdMixin):
    action: Literal["read_contact"] = "read_contact"


class UpdateLeadInput(_SalesforceBase, _RecordIdMixin, _FieldsMixin):
    action: Literal["update_lead"] = "update_lead"


class UpdateContactInput(_SalesforceBase, _RecordIdMixin, _FieldsMixin):
    action: Literal["update_contact"] = "update_contact"


class DeleteLeadInput(_SalesforceBase, _RecordIdMixin):
    action: Literal["delete_lead"] = "delete_lead"


class DeleteContactInput(_SalesforceBase, _RecordIdMixin):
    action: Literal["delete_contact"] = "delete_contact"


class SalesforceResponseOutput(BaseModel):
    """Salesforce's payload plus the status it arrived with.

    A delete returns 204 with no body, so ``body`` is optional.
    """

    status_code: int
    body: Optional[Any] = None
    record_id: Optional[str] = None
