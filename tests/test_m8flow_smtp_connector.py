#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""
Unit tests for the M8Flow SMTP connector.

Fully offline: ``smtplib.SMTP`` inside logic.py is replaced with a recorder, so
no mail is ever sent.

Most of these assert capabilities the legacy connector had -- HTML bodies, CC,
BCC, Reply-To and attachments -- plus the containment rules that keep an
attachment path inside its sandbox.
"""

from __future__ import annotations

import base64
import os
import tempfile
from email.message import EmailMessage
from typing import Any
from unittest.mock import patch

import pytest

from node_wire_m8flow_smtp.logic import M8FlowSmtpConnector

_BASE_REQUEST = {
    "action": "send_email",
    "smtp_host": "smtp.example.com",
    "smtp_port": 587,
    "email_from": "sender@example.com",
    "email_subject": "Subject",
    "email_body": "plain text",
}


class _RecordingSmtp:
    """Stands in for ``smtplib.SMTP`` and records what delivery did."""

    sent: list[dict[str, Any]] = []

    def __init__(self, host: str, port: int, timeout: int | None = None) -> None:
        self.host = host
        self.port = port
        self.started_tls = False
        self.username: str | None = None

    def __enter__(self) -> "_RecordingSmtp":
        return self

    def __exit__(self, *_exc: Any) -> bool:
        return False

    def starttls(self, context: Any = None) -> None:
        self.started_tls = True

    def login(self, username: str, password: str) -> None:
        self.username = username

    def send_message(self, message: EmailMessage, to_addrs: list[str] | None = None) -> None:
        _RecordingSmtp.sent.append(
            {
                "message": message,
                "to_addrs": to_addrs,
                "started_tls": self.started_tls,
                "username": self.username,
                "host": self.host,
                "port": self.port,
            }
        )


@pytest.fixture(autouse=True)
def _reset_recorder() -> None:
    _RecordingSmtp.sent = []


def _patch_smtp() -> Any:
    return patch("node_wire_m8flow_smtp.logic.smtplib.SMTP", _RecordingSmtp)


def test_connector_id_and_action() -> None:
    assert M8FlowSmtpConnector.connector_id == "m8flow_smtp"
    assert set(M8FlowSmtpConnector._action_registry) == {"send_email"}


async def test_html_cc_bcc_and_reply_to_are_all_supported() -> None:
    """The four message features the plain NodeWire SMTP connector lacks."""
    connector = M8FlowSmtpConnector()

    with _patch_smtp():
        response = await connector.run(
            {
                **_BASE_REQUEST,
                "email_to": "to1@example.com, to2@example.com",
                "email_cc": "cc@example.com",
                "email_bcc": "bcc@example.com",
                "email_reply_to": "reply@example.com",
                "email_body_html": "<b>hello</b>",
            }
        )

    assert response.success
    delivery = _RecordingSmtp.sent[0]
    message = delivery["message"]

    assert sorted(delivery["to_addrs"]) == [
        "bcc@example.com",
        "cc@example.com",
        "to1@example.com",
        "to2@example.com",
    ]
    assert message["Cc"] == "cc@example.com"
    assert message["Reply-To"] == "reply@example.com"
    # Bcc belongs in the envelope only; a header would disclose the recipient.
    assert message["Bcc"] is None
    assert any(part.get_content_type() == "text/html" for part in message.walk())
    assert response.data["recipient_count"] == 4


async def test_recipients_split_on_semicolons_and_are_deduplicated() -> None:
    connector = M8FlowSmtpConnector()

    with _patch_smtp():
        await connector.run(
            {**_BASE_REQUEST, "email_to": "a@example.com;b@example.com,a@example.com"}
        )

    assert _RecordingSmtp.sent[0]["to_addrs"] == ["a@example.com", "b@example.com"]


async def test_starttls_is_off_unless_asked_for() -> None:
    """Legacy defaulted to a plain connection; that default is preserved."""
    connector = M8FlowSmtpConnector()

    with _patch_smtp():
        await connector.run({**_BASE_REQUEST, "email_to": "a@example.com"})
        assert _RecordingSmtp.sent[0]["started_tls"] is False

        await connector.run(
            {**_BASE_REQUEST, "email_to": "a@example.com", "smtp_starttls": True}
        )
        assert _RecordingSmtp.sent[1]["started_tls"] is True


async def test_login_only_happens_when_both_credentials_are_present() -> None:
    """An anonymous relay refuses a half-supplied login outright."""
    connector = M8FlowSmtpConnector()

    with _patch_smtp():
        await connector.run(
            {**_BASE_REQUEST, "email_to": "a@example.com", "smtp_user": "u"}
        )
        assert _RecordingSmtp.sent[0]["username"] is None

        await connector.run(
            {
                **_BASE_REQUEST,
                "email_to": "a@example.com",
                "smtp_user": "u",
                "smtp_password": "p",
            }
        )
        assert _RecordingSmtp.sent[1]["username"] == "u"


async def test_a_base64_attachment_gets_its_content_type_guessed() -> None:
    connector = M8FlowSmtpConnector()

    with _patch_smtp():
        response = await connector.run(
            {
                **_BASE_REQUEST,
                "email_to": "a@example.com",
                "attachments": [
                    {
                        "filename": "report.pdf",
                        "content_base64": base64.b64encode(b"%PDF-1.4").decode(),
                    }
                ],
            }
        )

    assert response.success
    assert response.data["attachment_count"] == 1
    types = [part.get_content_type() for part in _RecordingSmtp.sent[0]["message"].walk()]
    assert "application/pdf" in types


async def test_an_attachment_inside_the_sandbox_is_read() -> None:
    connector = M8FlowSmtpConnector()

    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "note.txt")
        with open(path, "wb") as handle:
            handle.write(b"hello")

        with patch.dict(
            os.environ,
            {"M8FLOW_CONNECTOR_SMTP_ATTACHMENTS_USER_ACCESS_DIR": directory},
        ), _patch_smtp():
            response = await connector.run(
                {
                    **_BASE_REQUEST,
                    "email_to": "a@example.com",
                    "attachments": [{"filename": "note.txt", "path": path}],
                }
            )

    assert response.success


async def test_an_attachment_outside_the_sandbox_is_refused() -> None:
    connector = M8FlowSmtpConnector()

    response = await connector.run(
        {
            **_BASE_REQUEST,
            "email_to": "a@example.com",
            "attachments": [{"filename": "passwd", "path": "/etc/passwd"}],
        }
    )

    assert response.error_code == "SMTP_ATTACHMENT_ERROR"


async def test_an_empty_recipient_set_is_refused() -> None:
    connector = M8FlowSmtpConnector()

    response = await connector.run({**_BASE_REQUEST, "email_to": "   "})

    assert response.error_code == "SMTP_NO_RECIPIENTS"


async def test_a_relay_off_the_allowlist_is_refused_when_one_is_configured() -> None:
    """The allowlist is opt-in, so the legacy per-call relay still works."""
    connector = M8FlowSmtpConnector()

    with patch.dict(os.environ, {"NW_M8FLOW_SMTP_ALLOWED_HOSTS": "other.example.com"}):
        response = await connector.run({**_BASE_REQUEST, "email_to": "a@example.com"})

    assert response.error_code == "SMTP_RELAY_NOT_ALLOWED"


async def test_control_characters_in_the_subject_are_rejected() -> None:
    """A bare newline in a header would let a caller inject extra headers."""
    connector = M8FlowSmtpConnector()

    response = await connector.run(
        {
            **_BASE_REQUEST,
            "email_to": "a@example.com",
            "email_subject": "Hello\r\nBcc: attacker@example.com",
        }
    )

    assert response.error_code == "VALIDATION_ERROR"


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (TimeoutError("timed out"), "SMTP_TIMEOUT"),
        (OSError(101, "Network is unreachable"), "SMTP_CONNECT_ERROR"),
    ],
)
async def test_an_unreachable_relay_fails_once_without_retrying(exc: BaseException, code: str) -> None:
    """A send is not idempotent: the runtime must not resend on a transport error."""
    attempts: list[int] = []

    def _unreachable(*_args: Any, **_kwargs: Any) -> Any:
        attempts.append(1)
        raise exc

    connector = M8FlowSmtpConnector()
    with patch("node_wire_m8flow_smtp.logic.smtplib.SMTP", _unreachable):
        response = await connector.run({**_BASE_REQUEST, "email_to": "a@example.com"})

    assert response.success is False
    assert response.error_code == code
    assert len(attempts) == 1
