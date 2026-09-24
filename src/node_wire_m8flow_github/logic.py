#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""M8Flow GitHub connector: repository metadata, branches and pull requests.

Behaviour is ported from the legacy ``connector_github`` package so existing
M8Flow process models keep working: same parameter names, same GitHub
endpoints, same error taxonomy. The token is supplied per call (legacy
contract) and is never logged or echoed in an error message.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Tuple, Type

import httpx

from node_wire_runtime import BaseConnector, ErrorCategory, nw_action

from .schema import (
    GITHUB_API_BASE,
    ConnectRepositoryInput,
    GithubResponseOutput,
    ListBranchesInput,
    ListPullRequestsInput,
)

logger = logging.getLogger("connectors.m8flow_github")

_API_VERSION = "2022-11-28"

# Legacy mapped these GitHub messages to an auth failure even on a non-401/403.
_AUTH_MESSAGES = ("Bad credentials", "Requires authentication")


class GithubAuthError(Exception):
    """GitHub rejected the token (401/403, or an auth-flavoured message)."""


class GithubNotFoundError(Exception):
    """The requested repository, branch or pull request does not exist."""


class GithubRequestError(Exception):
    """GitHub returned an error that is neither auth nor not-found."""


def _auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": _API_VERSION,
    }


def _raise_for_github_error(response: httpx.Response) -> Any:
    """Return the parsed body, or raise the mapped error.

    Mirrors the legacy ``_github_error_to_connector_error`` mapping, including
    its treatment of a non-JSON body as an unreadable-response failure.
    """
    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise GithubRequestError("Unreadable (non JSON) response from GitHub")
    try:
        payload = response.json()
    except ValueError as exc:
        raise GithubRequestError("Unreadable (non JSON) response from GitHub") from exc

    if response.status_code < 400:
        return payload

    message = ""
    if isinstance(payload, dict):
        message = str(payload.get("message", "") or "")

    if response.status_code in (401, 403) or any(m in message for m in _AUTH_MESSAGES):
        raise GithubAuthError("GitHub authentication failed. Check your token.")
    if response.status_code == 404 or "Not Found" in message:
        raise GithubNotFoundError(f"GitHub resource not found: {message or 'Not Found'}")
    raise GithubRequestError(message or "Unknown GitHub error")


class M8FlowGithubConnector(BaseConnector):
    """GitHub repository operations with the legacy M8Flow contract."""

    connector_id = "m8flow_github"
    output_model = GithubResponseOutput

    # Scoped to m8flow_github only.
    error_map: ClassVar[Dict[Type[BaseException], Tuple[ErrorCategory, str]]] = {
        GithubAuthError: (ErrorCategory.AUTH, "GITHUB_AUTH_ERROR"),
        GithubNotFoundError: (ErrorCategory.BUSINESS, "GITHUB_NOT_FOUND"),
        GithubRequestError: (ErrorCategory.BUSINESS, "GITHUB_REQUEST_FAILED"),
        httpx.TimeoutException: (ErrorCategory.RETRYABLE, "GITHUB_TIMEOUT"),
        httpx.ConnectError: (ErrorCategory.RETRYABLE, "GITHUB_CONNECT_ERROR"),
        httpx.RequestError: (ErrorCategory.RETRYABLE, "GITHUB_REQUEST_ERROR"),
    }

    async def _get(
        self,
        path: str,
        *,
        token: str,
        params: Dict[str, Any] | None,
        trace_id: str,
        action: str,
    ) -> GithubResponseOutput:
        url = f"{GITHUB_API_BASE}{path}"
        logger.info(
            "Calling GitHub API",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
                # Path only: it carries no credentials, unlike the headers.
                "path": path,
            },
        )
        async with httpx.AsyncClient(timeout=self._timeout(), trust_env=False) as client:
            response = await client.get(url, headers=_auth_headers(token), params=params)

        body = _raise_for_github_error(response)
        logger.info(
            "GitHub API call completed",
            extra={
                "trace_id": trace_id,
                "connector_id": self.connector_id,
                "action": action,
                "path": path,
                "status_code": response.status_code,
            },
        )
        return GithubResponseOutput(status_code=response.status_code, body=body)

    @staticmethod
    def _timeout() -> float:
        import os

        return float(os.getenv("NW_TIMEOUT", "30.0"))

    @nw_action("connect_repository")
    async def connect_repository(
        self, params: ConnectRepositoryInput, *, trace_id: str
    ) -> GithubResponseOutput:
        """Verify access to a repository and return its metadata."""
        return await self._get(
            f"/repos/{params.owner}/{params.repo}",
            token=params.token,
            params=None,
            trace_id=trace_id,
            action="connect_repository",
        )

    @nw_action("list_branches")
    async def list_branches(
        self, params: ListBranchesInput, *, trace_id: str
    ) -> GithubResponseOutput:
        """List repository branches, optionally filtered by protection status."""
        query: Dict[str, Any] = {"per_page": params.per_page, "page": params.page}
        # Legacy only forwarded `protected` when explicitly true/false; "" means
        # "no filter" and GitHub would reject it as an invalid value.
        if params.protected in ("true", "false"):
            query["protected"] = params.protected
        return await self._get(
            f"/repos/{params.owner}/{params.repo}/branches",
            token=params.token,
            params=query,
            trace_id=trace_id,
            action="list_branches",
        )

    @nw_action("list_pull_requests")
    async def list_pull_requests(
        self, params: ListPullRequestsInput, *, trace_id: str
    ) -> GithubResponseOutput:
        """List pull requests filtered by state."""
        return await self._get(
            f"/repos/{params.owner}/{params.repo}/pulls",
            token=params.token,
            params={
                "state": params.state,
                "per_page": params.per_page,
                "page": params.page,
            },
            trace_id=trace_id,
            action="list_pull_requests",
        )
