#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow SMTP connector.

Field names mirror the legacy ``connector_smtp.SendHTMLEmail`` signature
(``smtp_host``, ``email_to``, ``email_body_html`` ...) because the M8Flow SMTP
profile stores exactly those names: a renamed field is silently never sent.

Recipients accept a single string with ``,`` or ``;`` separators, matching the
legacy split, and are de-duplicated while preserving order.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Control characters in a header value allow header injection (CRLF splitting).
_HEADER_UNSAFE = set(range(0, 32)) | {127}


def split_recipients(value: str | None) -> List[str]:
    """Split a recipient string on ``,`` or ``;``, dropping blanks."""
    if not value:
        return []
    return [part.strip() for part in str(value).replace(";", ",").split(",") if part.strip()]


def dedupe_keep_order(items: List[str]) -> List[str]:
    """De-duplicate while preserving first-seen order."""
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


class Attachment(BaseModel):
    """One attachment, supplied either by sandboxed path or inline base64."""

    model_config = ConfigDict(extra="forbid")

    filename: str = Field(..., description="Display name; also drives MIME detection.")
    path: Optional[str] = Field(default=None, description="Absolute path inside the attachments dir.")
    content_base64: Optional[str] = Field(default=None, description="Base64-encoded content.")
    content_type: Optional[str] = Field(default=None, description='Override, e.g. "application/pdf".')

    @field_validator("filename")
    @classmethod
    def require_filename(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("attachment requires a non-empty 'filename'")
        return str(value).strip()


class SmtpSendInput(BaseModel):
    """The legacy SendHTMLEmail parameter set."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["send_email"] = "send_email"

    # Relay. Supplied per call so one tenant can drive several relays, which is
    # what the M8Flow SMTP profile models.
    smtp_host: str = Field(..., description="SMTP server hostname.")
    smtp_port: int = Field(..., ge=1, le=65535, description="SMTP server port.")
    smtp_user: Optional[str] = Field(default=None)
    smtp_password: Optional[str] = Field(default=None)
    smtp_starttls: Optional[bool] = Field(
        default=False, description="Upgrade the connection with STARTTLS."
    )

    # Message.
    email_from: str = Field(..., description="Envelope and header sender.")
    email_to: str = Field(..., description='Recipients, "," or ";" separated.')
    email_subject: str = Field(...)
    email_body: str = Field(..., description="Plain-text body.")
    email_body_html: Optional[str] = Field(
        default=None, description="HTML alternative part; sent alongside the plain text."
    )
    email_cc: Optional[str] = Field(default=None)
    email_bcc: Optional[str] = Field(default=None, description="Envelope-only; never a header.")
    email_reply_to: Optional[str] = Field(default=None)

    attachments: Optional[List[Attachment]] = Field(default=None)

    @field_validator("smtp_host")
    @classmethod
    def require_host(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("smtp_host must not be empty")
        return str(value).strip()

    @field_validator("email_subject", "email_from")
    @classmethod
    def reject_header_injection(cls, value: str) -> str:
        if any(ord(ch) in _HEADER_UNSAFE for ch in str(value)):
            raise ValueError("header value must not contain control characters")
        return value

    @field_validator("attachments", mode="before")
    @classmethod
    def normalize_attachments(cls, value: Any) -> Any:
        # Legacy accepted None and treated it as an empty list.
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError("attachments must be a list")
        return value


def envelope_recipients(params: "SmtpSendInput") -> List[str]:
    """To + Cc + Bcc, split, merged and de-duplicated.

    A module-level function rather than a method: Cython compiles a plain
    method into a ``cyfunction``, which pydantic then mistakes for an
    un-annotated field and refuses to build the model.
    """
    return dedupe_keep_order(
        split_recipients(params.email_to)
        + split_recipients(params.email_cc)
        + split_recipients(params.email_bcc)
    )


class SmtpSendOutput(BaseModel):
    """Delivery outcome.

    ``recipient_count`` rather than the addresses themselves: the list is PII
    and the caller already knows what it sent.
    """

    sent: bool
    recipient_count: int
    attachment_count: int = 0
    logs: List[str] = Field(default_factory=list)


class SmtpRelayNotAllowedError(ValueError):
    """The requested relay host is not on the configured egress allowlist."""


def build_attachment_meta(attachment: Attachment) -> Dict[str, Any]:
    """Non-sensitive attachment descriptor for logging."""
    return {"filename": attachment.filename, "inline": attachment.content_base64 is not None}
