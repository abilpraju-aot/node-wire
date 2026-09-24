#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow n8n connector.

Two integration styles with disjoint credentials, matching the legacy
connector and the M8Flow n8n profile: ``trigger_workflow`` calls a webhook URL
and needs no API key, while the Public API actions need ``base_url`` +
``api_key``.
"""
from __future__ import annotations

from typing import Any, Literal, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator

AUTH_NONE = "none"
AUTH_HEADER = "header"
AUTH_BASIC = "basic"
_AUTH_TYPES = (AUTH_NONE, AUTH_HEADER, AUTH_BASIC)

_WEBHOOK_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")


class _ApiBase(BaseModel):
    """Public API credentials shared by the non-webhook actions."""

    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(..., description="n8n instance URL, e.g. https://n8n.example.com")
    api_key: str = Field(..., description="n8n Public API key (Settings > n8n API).")

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        cleaned = str(value).strip().rstrip("/")
        parsed = urlparse(cleaned)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("base_url must be a valid http(s) URL")
        return cleaned

    @field_validator("api_key")
    @classmethod
    def reject_blank_key(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("api_key must not be empty")
        return str(value).strip()


class TriggerWorkflowInput(BaseModel):
    """Invoke a workflow through its Webhook node."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["trigger_workflow"] = "trigger_workflow"
    webhook_url: str = Field(..., description="Full n8n webhook URL.")
    method: str = Field(default="POST")
    payload: Optional[Any] = Field(
        default=None,
        description="Sent as query params for GET, otherwise as a JSON body.",
    )
    auth_type: str = Field(default=AUTH_NONE, description='"none", "header" or "basic".')
    auth_header_name: str = Field(default="")
    auth_header_value: str = Field(default="")
    username: str = Field(default="")
    password: str = Field(default="")

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("webhook_url is required.")
        parsed = urlparse(cleaned)
        if parsed.scheme not in ("http", "https"):
            raise ValueError("webhook_url must use the http or https scheme.")
        if not parsed.netloc:
            raise ValueError("webhook_url is not a valid URL.")
        return cleaned

    @field_validator("method", mode="before")
    @classmethod
    def normalize_method(cls, value: Any) -> str:
        normalized = str(value or "POST").strip().upper()
        if normalized not in _WEBHOOK_METHODS:
            raise ValueError(f"method must be one of: {', '.join(_WEBHOOK_METHODS)}")
        return normalized

    @field_validator("auth_type", mode="before")
    @classmethod
    def normalize_auth_type(cls, value: Any) -> str:
        normalized = str(value or AUTH_NONE).strip().lower()
        if normalized not in _AUTH_TYPES:
            raise ValueError(f"auth_type must be one of: {', '.join(_AUTH_TYPES)}")
        return normalized


class ListWorkflowsInput(_ApiBase):
    action: Literal["list_workflows"] = "list_workflows"
    active: str = Field(default="", description='"true", "false", or "" for all.')
    limit: int = Field(default=100, ge=1, le=250)
    cursor: str = Field(default="")

    @field_validator("active", mode="before")
    @classmethod
    def normalize_active(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in ("true", "false", ""):
            raise ValueError('active must be "true", "false", or ""')
        return normalized


class GetWorkflowInput(_ApiBase):
    action: Literal["get_workflow"] = "get_workflow"
    workflow_id: str = Field(..., description="The workflow's id.")

    @field_validator("workflow_id")
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("workflow_id must not be empty")
        return str(value).strip()


class ListExecutionsInput(_ApiBase):
    action: Literal["list_executions"] = "list_executions"
    workflow_id: str = Field(default="")
    status: str = Field(default="", description='"success", "error", "waiting", or "".')
    limit: int = Field(default=100, ge=1, le=250)
    cursor: str = Field(default="")

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in ("success", "error", "waiting", ""):
            raise ValueError('status must be "success", "error", "waiting", or ""')
        return normalized


class GetExecutionInput(_ApiBase):
    action: Literal["get_execution"] = "get_execution"
    execution_id: str = Field(..., description="The execution's id.")
    include_data: bool = Field(default=False)

    @field_validator("execution_id")
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("execution_id must not be empty")
        return str(value).strip()


class N8nResponseOutput(BaseModel):
    """n8n's response, or a failure rendered as data.

    A failed call still returns ``success`` at the envelope level with
    ``error_code``/``message`` populated here. See the module docstring of
    ``logic.py`` for why failures must not surface as connector errors.
    """

    status_code: int
    body: Optional[Any] = None
    error_code: Optional[str] = None
    message: Optional[str] = None
