"""Shared, bounded source-evidence projection for reader-facing cards."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from typing import Sequence

from .contracts.task_candidate import ContractError, parse_task_candidate
from .source_policy import source_kinds_accepting


_LINKABLE_HOSTS = ("github.com",)
_ORIGIN_PATHS = {"issue": "issues", "review_request": "pull"}
ADDRESSABLE_ORIGINS = source_kinds_accepting("addressable_origin")

#: A record identifier that says nothing to the person reading the card.
#: Some producers key a record by digest rather than by name, and a digest
#: is not a source: it cannot be searched for, opened, or recognised. When
#: one of those arrives without evidence, the two lines this module would
#: otherwise emit -- the digest, and a warning that no extract came with it
#: -- spend two of a card's few readable lines telling the reader nothing
#: they can act on. The warning is worth keeping where the record IS
#: nameable, because there it says "this source exists and you were not
#: shown it"; against a digest it only says the pipeline is a pipeline.
_OPAQUE_RECORD_RE = re.compile(r"[0-9a-f]{8,}")

#: How much of one source extract a card may quote.
#:
#: The candidate contract lets an extract run to 1,200 characters, and a
#: card may carry three of them, so the evidence block alone could reach
#: 3,600 -- ahead of the summary, and on an execution card ahead of the
#: plan the reader is there to approve. One real card spent 2,400 on a raw
#: handoff JSON blob and a transcript with one word repeated fifty times.
#:
#: The extract exists to let the reader recognise where the task came
#: from, and recognition happens in the first sentence or two. The whole
#: extract stays in the candidate payload and in the KB task file.
MAX_CARD_EXTRACT_CHARS = 400


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
    """Render the same origin and literal evidence on every task surface.

    Empty when there is nothing a reader can use: no evidence, and a
    record identified only by digest. Callers must not assume a block.
    """
    if sources:
        lines = [
            *_source_identity_lines(
                kind=kind,
                record=record,
                item=item,
                html_output=html_output,
            ),
            "<b>Source files and evidence:</b>"
            if html_output else "Source files and evidence:",
        ]
        for source in sources:
            role = source.role.replace("_", " ").title()
            extract = quotable(source.extract)
            if html_output:
                lines.extend((
                    f"• <code>{_escape(source.name)}</code> — {_escape(role)}",
                    f"<blockquote>{_escape(extract)}</blockquote>",
                ))
            else:
                lines.extend((
                    f"- {source.name} — {role}",
                    *(f"> {line}" for line in extract.splitlines()),
                ))
        return lines

    if _OPAQUE_RECORD_RE.fullmatch(record or ""):
        return []

    lines = _origin_identity_lines(
        kind=kind, record=record, item=item, html_output=html_output
    )
    warning = "Source extract not provided."
    lines.append(
        f"⚠️ <i>{_escape(warning)}</i>" if html_output else f"⚠️ {warning}"
    )
    return lines


def origin_url(*, kind: str, record: str, item: str) -> str | None:
    """Return the verified public URL for an addressable task origin."""
    host = record.split("/", 1)[0]
    if (
        kind not in ADDRESSABLE_ORIGINS
        or "/" not in record
        or host not in _LINKABLE_HOSTS
        or not item
    ):
        return None
    number = item.split("/", 1)[0]
    return f"https://{record}/{_ORIGIN_PATHS.get(kind, 'issues')}/{number}"


def _source_identity_lines(
    *, kind: str, record: str, item: str, html_output: bool
) -> list[str]:
    """Prefer a useful forge link, otherwise name the source kind.

    Opaque record ids are storage keys, not reader context. Once literal
    evidence is available its filenames and extracts identify an
    unaddressable source more usefully than repeating such a key.
    """
    if origin_url(kind=kind, record=record, item=item):
        return _origin_identity_lines(
            kind=kind, record=record, item=item, html_output=html_output
        )
    shown_kind = kind.replace("_", " ").title() or "Source"
    return [
        f"<b>From:</b> {_escape(shown_kind)}"
        if html_output else f"From: {shown_kind}"
    ]


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
    url = origin_url(kind=kind, record=record, item=item)
    if url is None:
        return [f"<b>From:</b> {_escape(shown)}"]
    return [f'<b>From:</b> <a href="{_escape(url)}">{_escape(shown)}</a>']


def quotable(value: str) -> str:
    """As much of one extract as recognising a source takes.

    Cut on a word boundary where there is one near the end, so the quote
    stops mid-sentence rather than mid-word: a reader can tell a sentence
    was interrupted, and cannot tell a mangled word from the source's own.
    """
    if len(value) <= MAX_CARD_EXTRACT_CHARS:
        return value
    head = value[:MAX_CARD_EXTRACT_CHARS]
    space = head.rfind(" ")
    if space >= MAX_CARD_EXTRACT_CHARS - 80:
        head = head[:space]
    return head.rstrip() + " …"


def _escape(value: str) -> str:
    return html.escape(value, quote=False)
