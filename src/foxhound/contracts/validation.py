"""Small, transport-neutral validation predicates shared by contracts."""

from __future__ import annotations

import re
from urllib.parse import urlsplit


_OPAQUE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
_SHA256_DIGEST = re.compile(r"^[0-9a-f]{64}$")


def is_bounded_text(value: object, *, maximum: int) -> bool:
    """Whether ``value`` is nonempty, trimmed printable text within a bound."""
    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and len(value) <= maximum
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
    )


def is_opaque_identifier(value: object) -> bool:
    """Whether ``value`` is a bounded identifier with no embedded content."""
    return is_bounded_text(value, maximum=200) and bool(
        _OPAQUE_IDENTIFIER.fullmatch(value)
    )


def is_sha256_digest(value: object) -> bool:
    """Whether ``value`` is a canonical lowercase SHA-256 hex digest."""
    return isinstance(value, str) and bool(_SHA256_DIGEST.fullmatch(value))


def is_positive_row_id(value: object) -> bool:
    """Whether ``value`` is a positive database identity, excluding ``bool``."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def is_external_receipt_reference(value: object) -> bool:
    """Whether a bounded adapter receipt is an opaque id or canonical HTTPS URL."""
    if not is_bounded_text(value, maximum=1000):
        return False
    if "://" not in value:
        return is_opaque_identifier(value)
    parsed = urlsplit(value)
    return (
        parsed.scheme == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
    )
