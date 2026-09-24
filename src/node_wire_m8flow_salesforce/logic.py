#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow Salesforce connector: Lead and Contact CRUD.

Ported from the legacy ``connector_salesforce`` package. Three behaviours are
load-bearing for existing M8Flow process models and are preserved here:

* Credentials arrive per call, so one tenant can address several orgs.
* ``fields`` is validated against an allowlist and coerced before it is sent,
  on update as well as create -- see :mod:`field_mapping`.
* A 401 triggers one refresh-and-retry when the refresh triple was supplied,
  picking up a rotated ``instance_url`` from the token response.
"""
from __future__ import annotations

import logging
import os
from typing import Any, ClassVar, Dict, Optional, Tuple, Type

import httpx

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .field_mapping import FieldMappingError, prepare_contact_fields, prepare_lead_fields
from .schema import (
    API_VERSION,
    CreateContactInput,
    CreateLeadInput,
    DeleteContactInput,
    DeleteLeadInput,
    ReadContactInput,
    ReadLeadInput,
    SalesforceResponseOutput,
    UpdateContactInput,
    UpdateLeadInput,
    can_refresh,
)

logger = logging.getLogger("connectors.m8flow_salesforce")

_PRODUCTION_TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
_SANDBOX_TOKEN_URL = "https://test.salesforce.com/services/oauth2/token"


class SalesforceAuthError(Exception):
    """Salesforce rejected the credentials."""


class SalesforcePermissionError(Exception):
    """The user lacks permission for this object or operation."""


class SalesforceValidationError(Exception):
    """Salesforce refused the payload, or local field validation failed."""


class SalesforceApiError(Exception):
    """Any other Salesforce API failure."""


class SalesforceTransientError(Exception):
    """Salesforce returned a 5xx; worth retrying."""


def _timeout() -> float:
    return float(os.getenv("NW_TIMEOUT", "30.0"))


def _token_url(instance_url: str) -> str:
    """Pick the sandbox or production token endpoint from the instance URL.

    Refreshing a sandbox token against login.salesforce.com fails with an
    opaque invalid_grant, so this is inferred rather than configured.
    """
    return _SANDBOX_TOKEN_URL if "test.salesforce.com" in instance_url else _PRODUCTION_TOKEN_URL


def _parse_error_message(payload: Any) -> str:
    """Flatten Salesforce's error array into one readable message."""
    if isinstance(payload, list):
        parts = []
        for item in payload:
            if isinstance(item, dict):
                code = item.get("errorCode", "")
                message = item.get("message", "")
                parts.append(f"{code}: {message}" if code else str(message))
        if parts:
            return ". ".join(parts)
    if isinstance(payload, dict):
        for key in ("message", "error_description", "error"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
    return "Unknown Salesforce error"


def _raise_for_status(response: httpx.Response) -> None:
    """Map a Salesforce error response onto the legacy taxonomy."""
    if response.status_code < 400:
        return
    try:
        payload = response.json()
    except ValueError:
        payload = None
    message = _parse_error_message(payload)

    if response.status_code == 401:
        raise SalesforceAuthError("Authentication failed or token expired.")
    if response.status_code == 403:
        raise SalesforcePermissionError(message)
    if response.status_code in (400, 422):
        raise SalesforceValidationError(message)
    if response.status_code >= 500:
        raise SalesforceTransientError(message)
    raise SalesforceApiError(message)


class M8FlowSalesforceConnector(BaseConnector):
    """Salesforce Lead and Contact operations with the legacy M8Flow contract."""

    connector_id = "m8flow_salesforce"
    output_model = SalesforceResponseOutput

    # Scoped to m8flow_salesforce only.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        SalesforceAuthError: (ErrorCategory.AUTH, "SALESFORCE_AUTH_ERROR"),
        SalesforcePermissionError: (ErrorCategory.AUTH, "SALESFORCE_PERMISSION_ERROR"),
        SalesforceValidationError: (ErrorCategory.BUSINESS, "SALESFORCE_VALIDATION_ERROR"),
        FieldMappingError: (ErrorCategory.BUSINESS, "SALESFORCE_VALIDATION_ERROR"),
        SalesforceApiError: (ErrorCategory.BUSINESS, "SALESFORCE_API_ERROR"),
        SalesforceTransientError: (ErrorCategory.RETRYABLE, "SALESFORCE_TRANSIENT_ERROR"),
        httpx.TimeoutException: (ErrorCategory.RETRYABLE, "SALESFORCE_TIMEOUT"),
        httpx.ConnectError: (ErrorCategory.RETRYABLE, "SALESFORCE_CONNECT_ERROR"),
        httpx.RequestError: (ErrorCategory.RETRYABLE, "SALESFORCE_REQUEST_ERROR"),
    }

    async def _refresh(self, params: Any) -> Optional[Tuple[str, str]]:
        """Exchange the refresh token. Returns (access_token, instance_url)."""
        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            response = await client.post(
                _token_url(params.instance_url),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": params.refresh_token,
                    "client_id": params.client_id,
                    "client_secret": params.client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        if response.status_code >= 400:
            return None
        try:
            payload = response.json()
        except ValueError:
            return None
        token = payload.get("access_token")
        if not token:
            return None
        # The org may have moved; the token response carries the current host.
        return token, (payload.get("instance_url") or params.instance_url).rstrip("/")

    async def _request(
        self,
        method: str,
        path: str,
        params: Any,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        trace_id: str,
        action: str,
    ) -> SalesforceResponseOutput:
        token = params.access_token
        instance = params.instance_url

        logger.info(
            "Calling Salesforce",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
                "method": method,
                "path": path,
            },
        )

        response = await self._send(method, instance, path, token, json_body)

        # One refresh-and-retry on an expired token, when we hold the triple.
        if response.status_code == 401 and can_refresh(params):
            refreshed = await self._refresh(params)
            if refreshed is not None:
                token, instance = refreshed
                logger.info(
                    "Refreshed Salesforce token; retrying once",
                    extra={
                        "trace_id": trace_id,
                        "connector_id": self.connector_id,
                        "action": action,
                    },
                )
                response = await self._send(method, instance, path, token, json_body)

        _raise_for_status(response)

        body: Any = None
        if response.status_code != 204 and response.content:
            try:
                body = response.json()
            except ValueError:
                body = {"raw": response.text}

        record_id = None
        if isinstance(body, dict):
            record_id = body.get("id") or body.get("Id")

        return SalesforceResponseOutput(
            status_code=response.status_code, body=body, record_id=record_id
        )

    @staticmethod
    async def _send(
        method: str,
        instance_url: str,
        path: str,
        token: str,
        json_body: Optional[Dict[str, Any]],
    ) -> httpx.Response:
        url = f"{instance_url}/services/data/{API_VERSION}/sobjects/{path}"
        async with httpx.AsyncClient(timeout=_timeout(), trust_env=False) as client:
            return await client.request(
                method,
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json=json_body,
            )

    # ---- Lead ------------------------------------------------------------

    @nw_action("create_lead")
    async def create_lead(
        self, params: CreateLeadInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Create a Lead."""
        body = prepare_lead_fields(params.fields)
        return await self._request(
            "POST", "Lead", params, json_body=body, trace_id=trace_id, action="create_lead"
        )

    @nw_action("read_lead")
    async def read_lead(self, params: ReadLeadInput, *, trace_id: str) -> SalesforceResponseOutput:
        """Read one Lead by id."""
        return await self._request(
            "GET", f"Lead/{params.record_id}", params, trace_id=trace_id, action="read_lead"
        )

    @nw_action("update_lead")
    async def update_lead(
        self, params: UpdateLeadInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Partially update a Lead."""
        body = prepare_lead_fields(params.fields, for_update=True)
        if not body:
            raise SalesforceValidationError("At least one field is required for update.")
        return await self._request(
            "PATCH",
            f"Lead/{params.record_id}",
            params,
            json_body=body,
            trace_id=trace_id,
            action="update_lead",
        )

    @nw_action("delete_lead")
    async def delete_lead(
        self, params: DeleteLeadInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Delete a Lead."""
        return await self._request(
            "DELETE", f"Lead/{params.record_id}", params, trace_id=trace_id, action="delete_lead"
        )

    # ---- Contact ---------------------------------------------------------

    @nw_action("create_contact")
    async def create_contact(
        self, params: CreateContactInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Create a Contact."""
        body = prepare_contact_fields(params.fields)
        return await self._request(
            "POST", "Contact", params, json_body=body, trace_id=trace_id, action="create_contact"
        )

    @nw_action("read_contact")
    async def read_contact(
        self, params: ReadContactInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Read one Contact by id."""
        return await self._request(
            "GET",
            f"Contact/{params.record_id}",
            params,
            trace_id=trace_id,
            action="read_contact",
        )

    @nw_action("update_contact")
    async def update_contact(
        self, params: UpdateContactInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Partially update a Contact."""
        body = prepare_contact_fields(params.fields, for_update=True)
        if not body:
            raise SalesforceValidationError("At least one field is required for update.")
        return await self._request(
            "PATCH",
            f"Contact/{params.record_id}",
            params,
            json_body=body,
            trace_id=trace_id,
            action="update_contact",
        )

    @nw_action("delete_contact")
    async def delete_contact(
        self, params: DeleteContactInput, *, trace_id: str
    ) -> SalesforceResponseOutput:
        """Delete a Contact."""
        return await self._request(
            "DELETE",
            f"Contact/{params.record_id}",
            params,
            trace_id=trace_id,
            action="delete_contact",
        )
