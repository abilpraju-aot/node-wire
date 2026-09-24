#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Input/output models for the M8Flow GitHub connector.

Field names mirror the legacy ``connector_github`` command signatures
(``token``/``owner``/``repo``) because M8Flow builds connector calls from the
declared parameter names: a renamed field is silently never sent.
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

GITHUB_API_BASE = "https://api.github.com"

# Legacy accepted "true"/"false" to filter branches and "" to mean "no filter".
_PROTECTED_VALUES = ("true", "false", "")
_PR_STATES = ("open", "closed", "all")


class _GithubBase(BaseModel):
    """Shared repository coordinates and credentials."""

    model_config = ConfigDict(extra="forbid")

    token: str = Field(..., description="GitHub personal access token or OAuth token.")
    owner: str = Field(..., description="Repository owner (user or organisation).")
    repo: str = Field(..., description="Repository name.")

    @field_validator("token", "owner", "repo")
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not str(value).strip():
            raise ValueError("value must not be empty")
        return str(value).strip()


class _GithubPaged(_GithubBase):
    """Adds the pagination window shared by the list actions."""

    per_page: int = Field(default=30, ge=1, le=100)
    page: int = Field(default=1, ge=1)


class ConnectRepositoryInput(_GithubBase):
    action: Literal["connect_repository"] = "connect_repository"


class ListBranchesInput(_GithubPaged):
    action: Literal["list_branches"] = "list_branches"
    protected: str = Field(
        default="",
        description='Filter: "true" for protected only, "false" for unprotected only, "" for all.',
    )

    @field_validator("protected", mode="before")
    @classmethod
    def normalize_protected(cls, value: Any) -> str:
        if value is None:
            return ""
        normalized = str(value).strip().lower()
        if normalized not in _PROTECTED_VALUES:
            raise ValueError('protected must be "true", "false", or "" (all)')
        return normalized


class ListPullRequestsInput(_GithubPaged):
    action: Literal["list_pull_requests"] = "list_pull_requests"
    state: str = Field(default="open", description='One of "open", "closed", "all".')

    @field_validator("state", mode="before")
    @classmethod
    def normalize_state(cls, value: Any) -> str:
        if value is None:
            return "open"
        normalized = str(value).strip().lower() or "open"
        if normalized not in _PR_STATES:
            raise ValueError(f"state must be one of: {', '.join(_PR_STATES)}")
        return normalized


class GithubResponseOutput(BaseModel):
    """GitHub's JSON payload plus the status it arrived with.

    ``body`` is a dict for single-resource reads and a list for the paged
    listings, matching what the legacy connector placed in ``parsed_body``.
    """

    status_code: int
    body: Optional[Any] = None
