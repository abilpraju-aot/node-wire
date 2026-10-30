#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow SMTP connector: plain-text or HTML mail with attachments.

Ported from the legacy ``connector_smtp.SendHTMLEmail`` so existing M8Flow
process models keep working: the relay and credentials arrive per call (the
M8Flow SMTP profile stores them), CC/BCC/Reply-To and attachments are
supported, and ``smtp_starttls`` still defaults to off.

Because the relay host is caller-supplied, ``NW_M8FLOW_SMTP_ALLOWED_HOSTS`` can
be set to pin which relays credentials may be sent to. It is unset by default,
which preserves the legacy behaviour.
"""
from __future__ import annotations

import base64
import binascii
import logging
import mimetypes
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import ClassVar, Dict, List, Tuple, Type

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    Attachment,
    SmtpRelayNotAllowedError,
    SmtpSendInput,
    SmtpSendOutput,
    envelope_recipients,
    split_recipients,
)

logger = logging.getLogger("connectors.m8flow_smtp")

_ATTACHMENTS_DIR_ENV = "M8FLOW_CONNECTOR_SMTP_ATTACHMENTS_USER_ACCESS_DIR"
_ATTACHMENTS_LIMIT_ENV = "M8FLOW_CONNECTOR_SMTP_ATTACHMENTS_LIMIT_IN_MB"
_TIMEOUT_ENV = "M8FLOW_CONNECTOR_SMTP_TIMEOUT_SECONDS"
_ALLOWED_HOSTS_ENV = "NW_M8FLOW_SMTP_ALLOWED_HOSTS"

_HARD_ATTACHMENTS_LIMIT_MB = 100
_DEFAULT_ATTACHMENTS_LIMIT_MB = 100
_DEFAULT_TIMEOUT_SECONDS = 30


class SmtpAttachmentError(ValueError):
    """An attachment could not be read, was too large, or escaped the sandbox."""


class SmtpNoRecipientsError(ValueError):
    """To, Cc and Bcc were all empty."""


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _attachments_dir() -> str:
    return os.environ.get(_ATTACHMENTS_DIR_ENV, "/attachments")


def _attachments_limit_bytes() -> int:
    """Effective cap in bytes, clamped to the hard maximum.

    A non-positive configured value falls back to the default rather than to
    zero, matching legacy -- a 0 there would reject every attachment.
    """
    requested = _env_int(_ATTACHMENTS_LIMIT_ENV, _DEFAULT_ATTACHMENTS_LIMIT_MB)
    if requested <= 0:
        requested = _DEFAULT_ATTACHMENTS_LIMIT_MB
    return min(requested, _HARD_ATTACHMENTS_LIMIT_MB) * 1024 * 1024


def _timeout_seconds() -> int:
    timeout = _env_int(_TIMEOUT_ENV, _DEFAULT_TIMEOUT_SECONDS)
    return timeout if timeout > 0 else _DEFAULT_TIMEOUT_SECONDS


def _assert_relay_allowed(host: str) -> None:
    """Reject relays outside the allowlist, when one is configured."""
    raw = os.environ.get(_ALLOWED_HOSTS_ENV, "")
    if not raw.strip():
        return
    allowed = {h.strip().lower().rstrip(".") for h in raw.replace(",", " ").split() if h.strip()}
    if host.strip().lower().rstrip(".") not in allowed:
        raise SmtpRelayNotAllowedError("smtp_host is not on the configured relay allowlist")


def _guess_content_type(filename: str, explicit: str | None) -> Tuple[str, str]:
    if explicit and "/" in explicit:
        maintype, subtype = explicit.split("/", 1)
        return maintype, subtype
    guessed, _ = mimetypes.guess_type(filename)
    if guessed and "/" in guessed:
        maintype, subtype = guessed.split("/", 1)
        return maintype, subtype
    return "application", "octet-stream"


def _resolve_attachment_path(path_value: str) -> str:
    """Resolve an attachment path, refusing anything outside the sandbox.

    On POSIX -- where the connector actually runs in production -- Windows
    drive letters and UNC paths are rejected outright, because ``os.path`` does
    not treat them as absolute there and they would otherwise slip past the
    containment check as relative paths. On Windows they are ordinary paths, so
    the ``commonpath`` containment check below is what guards them.
    """
    allowed_root = os.path.realpath(_attachments_dir())

    if os.name != "nt" and (":" in path_value[:3] or path_value.startswith("\\\\")):
        raise SmtpAttachmentError(
            f"Attachment path must be under '{allowed_root}'. "
            f"Windows/UNC paths are not allowed: {path_value!r}"
        )
    if not os.path.isabs(path_value):
        raise SmtpAttachmentError(
            f"Attachment path must be an absolute path under '{allowed_root}'. Got: {path_value!r}"
        )

    candidate = os.path.realpath(path_value)
    try:
        common = os.path.commonpath([allowed_root, candidate])
    except ValueError as exc:  # different drives on Windows
        raise SmtpAttachmentError(
            f"Attachment path must be under '{allowed_root}'. Got: {path_value!r}"
        ) from exc
    if common != allowed_root:
        raise SmtpAttachmentError(
            f"Attachment path must be under '{allowed_root}'. Got: {path_value!r}"
        )
    return candidate


def _estimated_base64_size(encoded: str) -> int:
    """Decoded size of a base64 string, without decoding it.

    Checked before ``b64decode`` so an oversized payload is rejected instead of
    being materialised in memory.
    """
    cleaned = "".join(encoded.split())
    if not cleaned:
        return 0
    padding = cleaned.count("=", -2)
    return (len(cleaned) * 3) // 4 - padding


def _enforce_limit(filename: str, size: int, limit: int) -> None:
    if size > limit:
        raise SmtpAttachmentError(
            f"Attachment {filename!r} is {size} bytes, over the "
            f"{limit // (1024 * 1024)} MB limit."
        )


def _read_attachment(attachment: Attachment, index: int, limit: int) -> Tuple[bytes, str, str]:
    """Return (content, maintype, subtype) for one attachment."""
    maintype, subtype = _guess_content_type(attachment.filename, attachment.content_type)

    if attachment.path:
        resolved = _resolve_attachment_path(attachment.path)
        if not os.path.isfile(resolved):
            raise SmtpAttachmentError(f"Attachment #{index} not found: {attachment.filename!r}")
        _enforce_limit(attachment.filename, os.path.getsize(resolved), limit)
        with open(resolved, "rb") as handle:
            content = handle.read()
        # Re-check: the file may have grown between the stat and the read.
        _enforce_limit(attachment.filename, len(content), limit)
        return content, maintype, subtype

    if attachment.content_base64:
        _enforce_limit(
            attachment.filename, _estimated_base64_size(attachment.content_base64), limit
        )
        try:
            content = base64.b64decode(attachment.content_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise SmtpAttachmentError(
                f"Attachment #{index} ({attachment.filename!r}) is not valid base64."
            ) from exc
        _enforce_limit(attachment.filename, len(content), limit)
        return content, maintype, subtype

    raise SmtpAttachmentError(
        f"Attachment #{index} ({attachment.filename!r}) needs either 'path' or 'content_base64'."
    )


def _build_message(params: SmtpSendInput, logs: List[str]) -> EmailMessage:
    message = EmailMessage()
    message["From"] = params.email_from
    message["To"] = ", ".join(split_recipients(params.email_to))
    if params.email_cc:
        message["Cc"] = ", ".join(split_recipients(params.email_cc))
    if params.email_reply_to:
        message["Reply-To"] = params.email_reply_to
    message["Subject"] = params.email_subject

    message.set_content(params.email_body or "")
    if params.email_body_html:
        # multipart/alternative: clients pick HTML, text remains the fallback.
        message.add_alternative(params.email_body_html, subtype="html")
        logs.append("added html alternative part")

    if params.attachments:
        limit = _attachments_limit_bytes()
        logs.append(f"attachment limit {limit // (1024 * 1024)} MB")
        for index, attachment in enumerate(params.attachments, start=1):
            content, maintype, subtype = _read_attachment(attachment, index, limit)
            message.add_attachment(
                content, maintype=maintype, subtype=subtype, filename=attachment.filename
            )
            logs.append(
                f"attached {attachment.filename!r} as {maintype}/{subtype} ({len(content)} bytes)"
            )
    return message


class M8FlowSmtpConnector(BaseConnector):
    """SMTP delivery with the legacy M8Flow contract."""

    connector_id = "m8flow_smtp"
    output_model = SmtpSendOutput

    # Scoped to m8flow_smtp only. The original exception types are raised
    # unwrapped so this map actually applies -- re-raising as a builtin would
    # make the runtime's MRO lookup miss every entry here.
    #
    # Nothing here is RETRYABLE on purpose: the runtime would resend up to 3x.
    # A send is not idempotent (a drop after DATA may already have delivered,
    # so a retry duplicates the mail), and the retries outlive the caller's
    # HTTP timeout. The workflow task is the retry unit.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        SmtpRelayNotAllowedError: (ErrorCategory.FATAL, "SMTP_RELAY_NOT_ALLOWED"),
        SmtpAttachmentError: (ErrorCategory.BUSINESS, "SMTP_ATTACHMENT_ERROR"),
        SmtpNoRecipientsError: (ErrorCategory.BUSINESS, "SMTP_NO_RECIPIENTS"),
        smtplib.SMTPAuthenticationError: (ErrorCategory.AUTH, "SMTP_AUTH_ERROR"),
        smtplib.SMTPConnectError: (ErrorCategory.BUSINESS, "SMTP_CONNECT_ERROR"),
        smtplib.SMTPServerDisconnected: (ErrorCategory.BUSINESS, "SMTP_DISCONNECTED"),
        TimeoutError: (ErrorCategory.BUSINESS, "SMTP_TIMEOUT"),
        OSError: (ErrorCategory.BUSINESS, "SMTP_CONNECT_ERROR"),
        smtplib.SMTPException: (ErrorCategory.BUSINESS, "SMTP_ERROR"),
    }

    @nw_action("send_email")
    async def send_email(self, params: SmtpSendInput, *, trace_id: str) -> SmtpSendOutput:
        """Send one message, optionally HTML and/or with attachments."""
        import asyncio

        _assert_relay_allowed(params.smtp_host)

        recipients = envelope_recipients(params)
        if not recipients:
            raise SmtpNoRecipientsError("No recipients provided (To/Cc/Bcc all empty).")

        logs: List[str] = [f"relay {params.smtp_host}:{params.smtp_port}"]
        message = _build_message(params, logs)

        sender_domain = params.email_from.rsplit("@", 1)[-1] if "@" in params.email_from else ""
        logger.info(
            "Sending SMTP message",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": "send_email",
                "host": params.smtp_host,
                "port": params.smtp_port,
                # Domain only: the full address and the recipient list are PII.
                "sender_domain": sender_domain,
                "recipient_count": len(recipients),
                "attachment_count": len(params.attachments or []),
            },
        )

        # smtplib is blocking; keep the event loop free.
        await asyncio.to_thread(self._deliver, params, message, recipients, logs)

        logger.info(
            "SMTP message sent",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": "send_email",
                "recipient_count": len(recipients),
            },
        )
        return SmtpSendOutput(
            sent=True,
            recipient_count=len(recipients),
            attachment_count=len(params.attachments or []),
            logs=logs,
        )

    @staticmethod
    def _deliver(
        params: SmtpSendInput,
        message: EmailMessage,
        recipients: List[str],
        logs: List[str],
    ) -> None:
        with smtplib.SMTP(
            params.smtp_host, params.smtp_port, timeout=_timeout_seconds()
        ) as server:
            if params.smtp_starttls:
                server.starttls(context=ssl.create_default_context())
                logs.append("started tls")
            # Only authenticate when both halves are present; an anonymous relay
            # rejects a login attempt outright.
            if params.smtp_user and params.smtp_password:
                server.login(params.smtp_user, params.smtp_password)
                logs.append("authenticated")
            server.send_message(message, to_addrs=recipients)
            logs.append(f"sent to {len(recipients)} recipient(s)")

