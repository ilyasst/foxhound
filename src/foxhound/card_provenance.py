"""Shared, bounded source-evidence projection for reader-facing cards."""

from __future__ import annotations

import html
import json
from dataclasses import dataclass
from typing import Sequence

from .contracts.task_candidate import ContractError, parse_task_candidate
from .source_policy import source_kinds_accepting


_LINKABLE_HOSTS = ("github.com",)
_ORIGIN_PATHS = {"issue": "issues", "review_request": "pull"}
ADDRESSABLE_ORIGINS = source_kinds_accepting("addressable_origin")


@dataclass(frozen=True)
class CardSourceEvidence:
    """One validated source basename and its bounded verbatim extract."""

    name: str
    role: str
    extract: str


def stored_origin_sources(value: object) -> tuple[CardSourceEvidence, ...]:
    """Decode only evidence already validated by the candidate contract."""
    if not isinstance(value, str) or not value:
        return ()
    try:
        candidate = parse_task_candidate(json.loads(value))
    except (json.JSONDecodeError, TypeError, ContractError):
        return ()
    return tuple(
        CardSourceEvidence(source.name, source.role, source.extract)
        for source in candidate.evidence.sources
    )


def origin_lines(
    *,
    kind: str,
    record: str,
    item: str,
    sources: Sequence[CardSourceEvidence],
    html_output: bool,
) -> list[str]:
    """Render the same origin and literal evidence on every task surface."""
    if sources:
        shown_kind = kind.replace("_", " ").title() or "Source"
        lines = [
            f"<b>From:</b> {_escape(shown_kind)}"
            if html_output else f"From: {shown_kind}",
            "<b>Source files and evidence:</b>"
            if html_output else "Source files and evidence:",
        ]
        for source in sources:
            role = source.role.replace("_", " ").title()
            if html_output:
                lines.extend((
                    f"• <code>{_escape(source.name)}</code> — {_escape(role)}",
                    f"<blockquote>{_escape(source.extract)}</blockquote>",
                ))
            else:
                lines.extend((
                    f"- {source.name} — {role}",
                    *(f"> {line}" for line in source.extract.splitlines()),
                ))
        return lines

    lines = _origin_identity_lines(
        kind=kind, record=record, item=item, html_output=html_output
    )
    warning = "Source extract not provided."
    lines.append(
        f"⚠️ <i>{_escape(warning)}</i>" if html_output else f"⚠️ {warning}"
    )
    return lines


def _origin_identity_lines(
    *, kind: str, record: str, item: str, html_output: bool
) -> list[str]:
    if not (record and item):
        return ["<b>From:</b> Unknown source" if html_output
                else "From: Unknown source"]
    if kind not in ADDRESSABLE_ORIGINS or "/" not in record:
        return [f"<b>From:</b> {_escape(record)}" if html_output
                else f"From: {record}"]
    name = record.rsplit("/", 1)[-1]
    number = item.split("/", 1)[0]
    shown = f"{name} #{number}"
    if not html_output or not record.startswith(_LINKABLE_HOSTS):
        return [f"<b>From:</b> {_escape(shown)}" if html_output
                else f"From: {shown}"]
    url = f"https://{record}/{_ORIGIN_PATHS.get(kind, 'issues')}/{number}"
    return [f'<b>From:</b> <a href="{_escape(url)}">{_escape(shown)}</a>']


def _escape(value: str) -> str:
    return html.escape(value, quote=False)
