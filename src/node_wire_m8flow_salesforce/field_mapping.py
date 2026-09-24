#
# SPDX-FileCopyrightText: 2026 AOT Technologies
# SPDX-License-Identifier: Apache-2.0
#
"""Field allowlists and value coercion for Salesforce Lead and Contact.

Two reasons this exists rather than relying on pydantic field declarations:

* An unknown field name must fail *here*, with a clear message naming the
  field, instead of being forwarded to Salesforce and coming back as an opaque
  API error -- and it must fail on the update path too, not only on create.
* Blank values are dropped rather than sent. Salesforce treats an empty string
  as "clear this field", so forwarding one would silently erase data the caller
  never intended to touch.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, FrozenSet, Mapping

LEAD_REQUIRED: FrozenSet[str] = frozenset({"LastName", "Company"})
LEAD_OPTIONAL: FrozenSet[str] = frozenset(
    {
        "FirstName", "Title", "Email", "Phone", "MobilePhone", "Street", "City",
        "State", "PostalCode", "Country", "Description", "LeadSource", "Status",
        "Rating", "Website", "NumberOfEmployees", "Industry", "AnnualRevenue",
    }
)
CONTACT_REQUIRED: FrozenSet[str] = frozenset({"LastName"})
CONTACT_OPTIONAL: FrozenSet[str] = frozenset(
    {
        "AccountId", "FirstName", "Title", "Email", "Phone", "MobilePhone",
        "MailingStreet", "MailingCity", "MailingState", "MailingPostalCode",
        "MailingCountry", "Description", "LeadSource", "Department",
    }
)

# Anything not listed is treated as a string.
LEAD_FIELD_TYPES: Dict[str, str] = {
    "NumberOfEmployees": "number",
    "AnnualRevenue": "number",
}
CONTACT_FIELD_TYPES: Dict[str, str] = {}


class FieldMappingError(ValueError):
    """A field was unknown, required-but-empty, or the wrong type.

    The message is safe to surface in workflow logs: it names the field, never
    the value.
    """


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _coerce(field_name: str, value: Any, expected: str) -> Any:
    """Coerce one value to the declared type, or raise ``FieldMappingError``."""
    if expected == "number":
        if isinstance(value, bool):
            raise FieldMappingError(f"Field '{field_name}' must be a number.")
        if isinstance(value, (int, float)):
            return value
        text = str(value).strip()
        try:
            # Keep floats as floats; Salesforce accepts both for numeric fields.
            return float(text) if ("." in text or "e" in text.lower()) else int(text)
        except (TypeError, ValueError) as exc:
            raise FieldMappingError(f"Field '{field_name}' must be a number.") from exc

    if expected == "boolean":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("true", "1", "yes"):
            return True
        if text in ("false", "0", "no"):
            return False
        raise FieldMappingError(f"Field '{field_name}' must be a boolean (true/false).")

    if expected == "date":
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        text = str(value).strip()
        # Accept a full ISO timestamp (with or without a Z suffix) and keep the
        # date part, which is what a Salesforce date field stores.
        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            return text[:10]
        except ValueError:
            pass
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise FieldMappingError(f"Field '{field_name}' must be a date (YYYY-MM-DD).") from exc

    return str(value).strip() if isinstance(value, str) else str(value)


def _prepare(
    fields: Mapping[str, Any],
    *,
    required: FrozenSet[str],
    optional: FrozenSet[str],
    field_types: Mapping[str, str],
    object_name: str,
    enforce_required: bool,
) -> Dict[str, Any]:
    allowed = required | optional
    for key in fields:
        if key not in allowed:
            raise FieldMappingError(f"Invalid field for {object_name}: '{key}'.")

    if enforce_required:
        for name in sorted(required):
            if _is_blank(fields.get(name)):
                raise FieldMappingError(
                    f"Required field for {object_name} is missing or empty: '{name}'."
                )

    prepared: Dict[str, Any] = {}
    for key, value in fields.items():
        if _is_blank(value):
            if enforce_required and key in required:
                raise FieldMappingError(
                    f"Required field for {object_name} is missing or empty: '{key}'."
                )
            # Blank means "not supplied"; sending it would clear the field.
            continue
        prepared[key] = _coerce(key, value, field_types.get(key, "string"))
    return prepared


def prepare_lead_fields(fields: Mapping[str, Any], *, for_update: bool = False) -> Dict[str, Any]:
    """Validate and coerce Lead fields. ``for_update`` makes the set partial."""
    return _prepare(
        fields,
        required=LEAD_REQUIRED,
        optional=LEAD_OPTIONAL,
        field_types=LEAD_FIELD_TYPES,
        object_name="Lead",
        enforce_required=not for_update,
    )


def prepare_contact_fields(
    fields: Mapping[str, Any], *, for_update: bool = False
) -> Dict[str, Any]:
    """Validate and coerce Contact fields. ``for_update`` makes the set partial."""
    return _prepare(
        fields,
        required=CONTACT_REQUIRED,
        optional=CONTACT_OPTIONAL,
        field_types=CONTACT_FIELD_TYPES,
        object_name="Contact",
        enforce_required=not for_update,
    )
