#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow Slack connector.

Field names mirror the legacy ``connector_slack`` command signatures -- the
token arrives by value as ``token``, and a direct message addresses the user
through ``user_id`` -- because the M8Flow Slack profile stores those names.
"""
from __future__ import annotations

from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

SLACK_API_BASE = "https://slack.com/api"


class _SlackBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str = Field(..., description="Slack bot OAuth token (xoxb-...).")

    @field_validator("token")
    @classmethod
    def reject_blank_token(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("token must not be empty")
        return str(value).strip()


class PostMessageInput(_SlackBase):
    """Post a message to a channel."""

    action: Literal["post_message"] = "post_message"
    channel: str = Field(..., description='Channel id (C123...) or name (#general).')
    message: str = Field(..., description="Plain-text body; also the notification fallback.")
    blocks: Optional[Union[str, List[Any]]] = Field(
        default="", description="Block Kit payload: a JSON array, or a JSON string of one."
    )

    @field_validator("channel")
    @classmethod
    def reject_blank_channel(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("channel must not be empty")
        return str(value).strip()


class SendDirectMessageInput(_SlackBase):
    """Send a direct message to a user."""

    action: Literal["send_direct_message"] = "send_direct_message"
    user_id: str = Field(..., description="Slack user id (U123...).")
    message: str = Field(...)
    blocks: Optional[Union[str, List[Any]]] = Field(default="")

    @field_validator("user_id")
    @classmethod
    def reject_blank_user(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("user_id must not be empty")
        return str(value).strip()


class UploadFileInput(_SlackBase):
    """Upload a file and share it to a channel.

    Exactly one of ``filepath`` or ``content_base64`` must be supplied.
    """

    action: Literal["upload_file"] = "upload_file"
    channel: str = Field(..., description="Channel id or name to share into.")
    filename: str = Field(default="")
    initial_comment: str = Field(default="")
    filepath: str = Field(default="", description="Path inside the attachments directory.")
    content_base64: str = Field(default="", description="Base64-encoded file content.")

    @field_validator("filename", "initial_comment", "filepath", "content_base64", mode="before")
    @classmethod
    def none_to_empty(cls, value: Any) -> str:
        # Legacy normalised None to "" rather than rejecting it.
        return "" if value is None else str(value)


class SlackResponseOutput(BaseModel):
    """Slack's acknowledgement plus the fields callers most often need."""

    ok: bool
    status_code: int
    channel: Optional[str] = None
    ts: Optional[str] = None
    file_id: Optional[str] = None
    body: Optional[Any] = None
    logs: List[str] = Field(default_factory=list)
