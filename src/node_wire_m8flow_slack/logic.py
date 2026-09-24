#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow Slack connector: messages, direct messages and file uploads.

Ported from the legacy ``connector_slack`` package: the token is supplied per
call, a direct message addresses the user by ``user_id``, and ``upload_file``
accepts a channel **name** as well as an id (Slack resolves it), which keeps
existing M8Flow process models working.

Slack signals logical failures with HTTP 200 and ``ok: false``, so the response
body -- not the status code -- decides success.
"""
from __future__ import annotations

import base64
import binascii
import json
import logging
import os
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type

import httpx

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    SLACK_API_BASE,
    PostMessageInput,
    SendDirectMessageInput,
    SlackResponseOutput,
    UploadFileInput,
)

logger = logging.getLogger("connectors.m8flow_slack")

_ATTACHMENTS_DIR_ENV = "M8FLOW_CONNECTOR_SLACK_ATTACHMENTS_USER_ACCESS_DIR"
_UPLOAD_LIMIT_ENV = "M8FLOW_CONNECTOR_SLACK_UPLOAD_FILE_LIMIT_MB"

_DEFAULT_UPLOAD_LIMIT_MB = 50
_HARD_UPLOAD_LIMIT_MB = 100

_AUTH_ERRORS = {"invalid_auth", "token_revoked", "account_inactive", "not_authed"}
_PERMISSION_ERRORS = {"missing_scope", "invalid_scopes"}
_RATE_ERRORS = {"ratelimited", "rate_limited"}


class SlackAuthError(Exception):
    """Slack rejected the token."""


class SlackPermissionError(Exception):
    """The token lacks a required OAuth scope."""


class SlackRateLimitError(Exception):
    """Slack is rate limiting this token."""


class SlackMessageError(Exception):
    """Slack refused the message (bad channel, malformed blocks, ...)."""


class SlackUploadError(Exception):
    """The upload could not be prepared, transferred or completed."""


def _timeout() -> float:
    return float(os.getenv("NW_TIMEOUT", "30.0"))


def _auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _attachments_dir() -> str:
    return os.environ.get(_ATTACHMENTS_DIR_ENV, "/slack_attachments")


def _upload_limit_bytes() -> int:
    """Effective upload cap, clamped to the hard maximum.

    A non-positive configured value falls back to the default rather than to
    zero, matching legacy -- a 0 there would reject every upload.
    """
    raw = os.environ.get(_UPLOAD_LIMIT_ENV)
    requested = _DEFAULT_UPLOAD_LIMIT_MB
    if raw is not None and str(raw).strip():
        try:
            requested = int(str(raw).strip())
        except (TypeError, ValueError):
            requested = _DEFAULT_UPLOAD_LIMIT_MB
    if requested <= 0:
        requested = _DEFAULT_UPLOAD_LIMIT_MB
    return min(requested, _HARD_UPLOAD_LIMIT_MB) * 1024 * 1024


def _raise_for_slack_error(payload: Dict[str, Any], status_code: int) -> None:
    """Map a Slack ``ok: false`` body onto the legacy error taxonomy."""
    error = str(payload.get("error", "") or "")
    detail = error or "unknown_error"
    if error in _AUTH_ERRORS:
        raise SlackAuthError(f"Slack authentication failed: {detail}")
    if error in _PERMISSION_ERRORS:
        raise SlackPermissionError(f"Slack permission denied: {detail}")
    if error in _RATE_ERRORS or status_code == 429:
        raise SlackRateLimitError(f"Slack rate limit: {detail}")
    raise SlackMessageError(f"Slack request failed: {detail}")


def _parse_slack_json(response: httpx.Response) -> Dict[str, Any]:
    """Parse a Slack API response, treating an unreadable body as a failure."""
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise SlackMessageError("Unreadable (non JSON) response from Slack")
    try:
        payload = response.json()
    except ValueError as exc:
        raise SlackMessageError("Unreadable (non JSON) response from Slack") from exc
    if not isinstance(payload, dict):
        raise SlackMessageError("Unexpected (non object) response from Slack")
    if payload.get("ok") is not True:
        _raise_for_slack_error(payload, response.status_code)
    return payload


def _normalize_blocks(blocks: Any) -> Optional[List[Any]]:
    """Accept a Block Kit array, or a JSON string holding one."""
    if blocks is None or blocks == "":
        return None
    if isinstance(blocks, list):
        return blocks
    try:
        parsed = json.loads(blocks)
    except (TypeError, ValueError) as exc:
        raise SlackMessageError("blocks must be valid JSON.") from exc
    if not isinstance(parsed, list):
        raise SlackMessageError("blocks must be a JSON array.")
    return parsed


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


def _resolve_upload_path(filepath: str) -> str:
    """Resolve an upload path, refusing anything outside the sandbox."""
    allowed_root = os.path.realpath(_attachments_dir())
    candidate = os.path.realpath(filepath)
    if candidate != allowed_root and not candidate.startswith(allowed_root + os.sep):
        raise SlackUploadError(f"Upload path must be under '{allowed_root}'. Got: {filepath!r}")
    return candidate


class M8FlowSlackConnector(BaseConnector):
    """Slack messaging and uploads with the legacy M8Flow contract."""

    connector_id = "m8flow_slack"
    output_model = SlackResponseOutput

    # Scoped to m8flow_slack only. Transport faults are RETRYABLE so the
    # runtime retries them, which the legacy connector could not do.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        SlackAuthError: (ErrorCategory.AUTH, "SLACK_AUTH_ERROR"),
        SlackPermissionError: (ErrorCategory.AUTH, "SLACK_PERMISSION_ERROR"),
        SlackRateLimitError: (ErrorCategory.RETRYABLE, "SLACK_RATE_LIMIT"),
        SlackMessageError: (ErrorCategory.BUSINESS, "SLACK_MESSAGE_FAILED"),
        SlackUploadError: (ErrorCategory.BUSINESS, "SLACK_UPLOAD_FAILED"),
        httpx.TimeoutException: (ErrorCategory.RETRYABLE, "SLACK_TIMEOUT"),
        httpx.ConnectError: (ErrorCategory.RETRYABLE, "SLACK_CONNECT_ERROR"),
        httpx.RequestError: (ErrorCategory.RETRYABLE, "SLACK_REQUEST_ERROR"),
    }

    async def _post_message(
        self, token: str, channel: str, message: str, blocks: Any, *, trace_id: str, action: str
    ) -> SlackResponseOutput:
        body: Dict[str, Any] = {"channel": channel, "text": message}
        normalized = _normalize_blocks(blocks)
        if normalized is not None:
            body["blocks"] = normalized

        logger.info(
            "Posting Slack message",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
            },
        )
        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            response = await client.post(
                f"{SLACK_API_BASE}/chat.postMessage",
                headers=_auth_headers(token),
                json=body,
            )
        payload = _parse_slack_json(response)
        return SlackResponseOutput(
            ok=True,
            status_code=response.status_code,
            channel=payload.get("channel"),
            ts=payload.get("ts"),
            body=payload,
        )

    @nw_action("post_message")
    async def post_message(
        self, params: PostMessageInput, *, trace_id: str
    ) -> SlackResponseOutput:
        """Post a message to a channel."""
        return await self._post_message(
            params.token,
            params.channel,
            params.message,
            params.blocks,
            trace_id=trace_id,
            action="post_message",
        )

    @nw_action("send_direct_message")
    async def send_direct_message(
        self, params: SendDirectMessageInput, *, trace_id: str
    ) -> SlackResponseOutput:
        """Send a direct message.

        Slack accepts a user id as the ``channel`` of ``chat.postMessage`` and
        opens the IM itself, which is what the legacy connector relied on.
        """
        return await self._post_message(
            params.token,
            params.user_id,
            params.message,
            params.blocks,
            trace_id=trace_id,
            action="send_direct_message",
        )

    @nw_action("upload_file")
    async def upload_file(
        self, params: UploadFileInput, *, trace_id: str
    ) -> SlackResponseOutput:
        """Upload a file through Slack's external-upload flow.

        Three steps: reserve a pre-signed URL, PUT the bytes to it, then
        finalise and share. ``files.upload`` was retired by Slack.
        """
        logs: List[str] = []
        limit = _upload_limit_bytes()
        logs.append(f"upload limit {limit // (1024 * 1024)} MB")

        content = self._read_upload_content(params, limit, logs)
        filename = params.filename or os.path.basename(params.filepath) or "upload"

        logger.info(
            "Uploading file to Slack",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": "upload_file",
                "size_bytes": len(content),
            },
        )

        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            # Step 1 - reserve an upload slot.
            reserve = await client.post(
                f"{SLACK_API_BASE}/files.getUploadURLExternal",
                headers=_auth_headers(params.token),
                data={"filename": filename, "length": len(content)},
            )
            reserved = _parse_slack_json(reserve)
            upload_url = reserved.get("upload_url")
            file_id = reserved.get("file_id")
            if not upload_url or not file_id:
                raise SlackUploadError("Slack did not return an upload URL and file id.")
            logs.append("reserved upload url")

            # Step 2 - transfer the bytes to the pre-signed URL (no auth header).
            transfer = await client.post(
                upload_url,
                content=content,
                headers={"Content-Type": "application/octet-stream"},
            )
            if transfer.status_code != 200:
                raise SlackUploadError(
                    f"Upload to pre-signed URL failed with HTTP {transfer.status_code}"
                )
            logs.append(f"transferred {len(content)} bytes")

            # Step 3 - finalise and share.
            complete_data: Dict[str, Any] = {
                "files": json.dumps([{"id": file_id, "title": filename}]),
                # A channel name is accepted here; Slack resolves it.
                "channel_id": params.channel,
            }
            if params.initial_comment:
                complete_data["initial_comment"] = params.initial_comment
            complete = await client.post(
                f"{SLACK_API_BASE}/files.completeUploadExternal",
                headers=_auth_headers(params.token),
                data=complete_data,
            )
            payload = _parse_slack_json(complete)
        logs.append("completed upload")

        return SlackResponseOutput(
            ok=True,
            status_code=complete.status_code,
            channel=params.channel,
            file_id=file_id,
            body=payload,
            logs=logs,
        )

    @staticmethod
    def _read_upload_content(params: UploadFileInput, limit: int, logs: List[str]) -> bytes:
        if params.filepath and params.content_base64:
            raise SlackUploadError("Provide either 'filepath' or 'content_base64', not both.")

        if params.filepath:
            resolved = _resolve_upload_path(params.filepath)
            if not os.path.isfile(resolved):
                raise SlackUploadError(f"No such file in upload directory: {params.filepath!r}")
            size = os.path.getsize(resolved)
            if size > limit:
                raise SlackUploadError(
                    f"File is {size} bytes, over the {limit // (1024 * 1024)} MB limit."
                )
            with open(resolved, "rb") as handle:
                content = handle.read()
            # Re-check: the file may have grown between the stat and the read.
            if len(content) > limit:
                raise SlackUploadError(
                    f"File is {len(content)} bytes, over the "
                    f"{limit // (1024 * 1024)} MB limit."
                )
            logs.append("read file from upload directory")
            return content

        if params.content_base64:
            if _estimated_base64_size(params.content_base64) > limit:
                raise SlackUploadError(
                    f"File exceeds the {limit // (1024 * 1024)} MB limit."
                )
            try:
                content = base64.b64decode(params.content_base64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise SlackUploadError("content_base64 is not valid base64.") from exc
            if len(content) > limit:
                raise SlackUploadError(
                    f"File is {len(content)} bytes, over the "
                    f"{limit // (1024 * 1024)} MB limit."
                )
            logs.append("decoded inline content")
            return content

        raise SlackUploadError("Either 'filepath' or 'content_base64' must be provided.")
