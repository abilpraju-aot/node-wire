#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow n8n connector: trigger workflows and query the Public API.

**Failures are returned as data, not as connector errors.** This is deliberate
and is carried over from the legacy connector. The M8Flow engine raises an
uncaught error whenever a connector reports a failure that no BPMN error
boundary catches; the process instance is then suspended and the UI shows an
endless spinner. Returning the n8n status and message inside the output model
lets a workflow display or branch on the failure instead of hanging.

Only transport-level faults (connect/timeout) are raised, so the runtime can
retry them -- those never carry an n8n status to report.
"""
from __future__ import annotations

import logging
import os
from typing import Any, ClassVar, Dict, Optional, Tuple, Type

import httpx

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    AUTH_BASIC,
    AUTH_HEADER,
    GetExecutionInput,
    GetWorkflowInput,
    ListExecutionsInput,
    ListWorkflowsInput,
    N8nResponseOutput,
    TriggerWorkflowInput,
)

logger = logging.getLogger("connectors.m8flow_n8n")


def _timeout() -> float:
    return float(os.getenv("NW_TIMEOUT", "30.0"))


def _api_headers(api_key: str) -> Dict[str, str]:
    return {"X-N8N-API-KEY": api_key, "Accept": "application/json"}


def _classify(status_code: int, message: str) -> tuple[int, str, str]:
    """Map an n8n failure to the legacy (status, error_code, message) triple."""
    if status_code in (401, 403):
        return (status_code, "N8nAuthError", "n8n authentication failed. Check your API key.")
    if status_code == 404:
        return (404, "N8nNotFoundError", f"n8n resource not found: {message}")
    return (
        status_code if status_code >= 400 else 400,
        "N8nRequestFailed",
        message or "Unknown n8n error",
    )


def _extract_message(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("message", "error"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
    return "Unknown n8n error"


def _api_result(response: httpx.Response) -> N8nResponseOutput:
    """Parse a Public API response, rendering failures as data."""
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        return N8nResponseOutput(
            status_code=response.status_code,
            error_code="N8nRequestFailed",
            message="Unreadable (non JSON) response from n8n",
        )
    try:
        payload = response.json()
    except ValueError:
        return N8nResponseOutput(
            status_code=response.status_code,
            error_code="N8nRequestFailed",
            message="Unreadable (non JSON) response from n8n",
        )

    if response.status_code < 400:
        return N8nResponseOutput(status_code=response.status_code, body=payload)

    status, code, message = _classify(response.status_code, _extract_message(payload))
    logger.warning("n8n API error: %s", code)
    return N8nResponseOutput(status_code=status, error_code=code, message=message)


class M8FlowN8nConnector(BaseConnector):
    """n8n workflow triggering and Public API queries."""

    connector_id = "m8flow_n8n"
    output_model = N8nResponseOutput

    # Only transport faults are raised; n8n's own errors travel as data.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        httpx.TimeoutException: (ErrorCategory.RETRYABLE, "N8N_TIMEOUT"),
        httpx.ConnectError: (ErrorCategory.RETRYABLE, "N8N_CONNECT_ERROR"),
        httpx.RequestError: (ErrorCategory.RETRYABLE, "N8N_REQUEST_ERROR"),
    }

    async def _api_call(
        self,
        method: str,
        base_url: str,
        path: str,
        api_key: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        trace_id: str,
        action: str,
    ) -> N8nResponseOutput:
        url = f"{base_url}/api/v1/{path}"
        logger.info(
            "Calling n8n Public API",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
                "path": path,
            },
        )
        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            response = await client.request(
                method, url, headers=_api_headers(api_key), params=params
            )
        return _api_result(response)

    @nw_action("trigger_workflow")
    async def trigger_workflow(
        self, params: TriggerWorkflowInput, *, trace_id: str
    ) -> N8nResponseOutput:
        """Invoke a workflow via its webhook URL."""
        headers: Dict[str, str] = {}
        auth: Optional[tuple[str, str]] = None
        if params.auth_type == AUTH_HEADER and params.auth_header_name:
            headers[params.auth_header_name] = params.auth_header_value
        elif params.auth_type == AUTH_BASIC:
            auth = (params.username, params.password)

        is_get = params.method == "GET"
        logger.info(
            "Triggering n8n webhook",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": "trigger_workflow",
                "method": params.method,
                "auth_type": params.auth_type,
            },
        )
        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            response = await client.request(
                params.method,
                params.webhook_url,
                headers=headers or None,
                params=params.payload if is_get else None,
                json=None if is_get else params.payload,
                auth=auth,
            )

        if response.status_code >= 400:
            text = response.text or f"n8n webhook returned HTTP {response.status_code}"
            status, code, message = _classify(response.status_code, text)
            logger.warning("n8n webhook error: %s", code)
            return N8nResponseOutput(status_code=status, error_code=code, message=message)

        # A "Respond to Webhook" node may return text or binary; legacy handed
        # the raw text back rather than failing on a non-JSON success body.
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            try:
                return N8nResponseOutput(status_code=response.status_code, body=response.json())
            except ValueError:
                pass
        return N8nResponseOutput(status_code=response.status_code, body=response.text)

    @nw_action("list_workflows")
    async def list_workflows(
        self, params: ListWorkflowsInput, *, trace_id: str
    ) -> N8nResponseOutput:
        """List workflows on the instance."""
        query: Dict[str, Any] = {"limit": params.limit}
        if params.active:
            query["active"] = params.active
        if params.cursor:
            query["cursor"] = params.cursor
        return await self._api_call(
            "GET",
            params.base_url,
            "workflows",
            params.api_key,
            params=query,
            trace_id=trace_id,
            action="list_workflows",
        )

    @nw_action("get_workflow")
    async def get_workflow(
        self, params: GetWorkflowInput, *, trace_id: str
    ) -> N8nResponseOutput:
        """Fetch one workflow by id."""
        return await self._api_call(
            "GET",
            params.base_url,
            f"workflows/{params.workflow_id}",
            params.api_key,
            trace_id=trace_id,
            action="get_workflow",
        )

    @nw_action("list_executions")
    async def list_executions(
        self, params: ListExecutionsInput, *, trace_id: str
    ) -> N8nResponseOutput:
        """List workflow executions."""
        query: Dict[str, Any] = {"limit": params.limit}
        if params.workflow_id:
            query["workflowId"] = params.workflow_id
        if params.status:
            query["status"] = params.status
        if params.cursor:
            query["cursor"] = params.cursor
        return await self._api_call(
            "GET",
            params.base_url,
            "executions",
            params.api_key,
            params=query,
            trace_id=trace_id,
            action="list_executions",
        )

    @nw_action("get_execution")
    async def get_execution(
        self, params: GetExecutionInput, *, trace_id: str
    ) -> N8nResponseOutput:
        """Fetch one execution's status and, optionally, its data."""
        return await self._api_call(
            "GET",
            params.base_url,
            f"executions/{params.execution_id}",
            params.api_key,
            params={"includeData": "true"} if params.include_data else None,
            trace_id=trace_id,
            action="get_execution",
        )
