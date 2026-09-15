"""Bounded, read-only client for the GW knowledge search service."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from .contracts.task_owner_equivalence import (
    OwnerEquivalenceContractError,
    OwnerEquivalenceResolutionError,
    TaskOwnerEquivalence,
    owner_equivalence_request,
    owner_equivalence_request_document,
    parse_owner_equivalence_response,
)


SEARCH_SCHEMA = "gw.search"
SEARCH_SCHEMA_VERSION = 1
EXECUTION_CONTEXT_REQUEST_SCHEMA = "gw.execution-context-request"
EXECUTION_CONTEXT_RESPONSE_SCHEMA = "gw.execution-context"
EXECUTION_CONTEXT_SCHEMA_VERSION = 1
OWNER_MEETING_REQUEST_SCHEMA = "gw.owner-upcoming-meeting-request"
OWNER_MEETING_RESPONSE_SCHEMA = "gw.owner-upcoming-meeting"
OWNER_MEETING_SCHEMA_VERSION = 1
LAYER_ORDER = ("kb", "secondary", "emails")

_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_DOCUMENT_ID_RE = re.compile(r"^(kb|secondary|emails):(.+)$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_DOMAIN_RE = re.compile(
    r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)


class KnowledgeClientError(OwnerEquivalenceResolutionError):
    """Base class for content-free GW knowledge client failures."""


class KnowledgeConfigError(KnowledgeClientError):
    pass


class KnowledgeRequestError(KnowledgeClientError):
    pass


class KnowledgeTransportError(KnowledgeClientError):
    pass


class KnowledgeResponseError(KnowledgeClientError):
    pass


@dataclass(frozen=True)
class KnowledgeClientConfig:
    endpoint: str
    alias: str
    token: str = field(repr=False)
    timeout_seconds: float = 5.0
    max_response_bytes: int = 512 * 1024

    def __post_init__(self) -> None:
        endpoint = _endpoint(self.endpoint)
        if endpoint != self.endpoint:
            raise KnowledgeConfigError("knowledge endpoint is not canonical")
        if not _ALIAS_RE.fullmatch(self.alias):
            raise KnowledgeConfigError("knowledge alias is invalid")
        if (not 32 <= len(self.token) <= 4_096
                or any(char.isspace() for char in self.token)):
            raise KnowledgeConfigError("knowledge token is invalid")
        if (isinstance(self.timeout_seconds, bool)
                or not isinstance(self.timeout_seconds, (int, float))
                or not math.isfinite(self.timeout_seconds)
                or not 0 < self.timeout_seconds <= 30):
            raise KnowledgeConfigError("knowledge timeout is invalid")
        if (isinstance(self.max_response_bytes, bool)
                or not isinstance(self.max_response_bytes, int)
                or not 1_024 <= self.max_response_bytes <= 1024 * 1024):
            raise KnowledgeConfigError("knowledge response limit is invalid")


@dataclass(frozen=True)
class KnowledgeDocument:
    id: str
    path: str
    excerpt: str
    kb_path: str | None = None
    section: str | None = None
    ranking_score: float | None = None


@dataclass(frozen=True)
class KnowledgeLayer:
    name: str
    total_results: int
    truncated: bool
    documents: tuple[KnowledgeDocument, ...]


@dataclass(frozen=True)
class KnowledgeSearchResult:
    layers: tuple[KnowledgeLayer, ...]

    @property
    def document_count(self) -> int:
        return sum(len(layer.documents) for layer in self.layers)


@dataclass(frozen=True)
class ExecutionContext:
    alias: str
    revision: str
    display_name: str = field(repr=False)
    operator_context: str = field(repr=False)
    self_aliases: tuple[str, ...] = field(default=(), repr=False)
    institution_domains: tuple[str, ...] = field(default=(), repr=False)


@dataclass(frozen=True)
class OwnerUpcomingMeeting:
    match: bool
    checked_at: str
    evidence_revision: str


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GwKnowledgeClient:
    """A fixed-route client with no producer discovery or write methods."""

    def __init__(self, config: KnowledgeClientConfig) -> None:
        if not isinstance(config, KnowledgeClientConfig):
            raise KnowledgeConfigError("knowledge client configuration is invalid")
        self._config = config
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), _NoRedirect()
        )

    def search(
        self,
        query: str,
        *,
        layers: Sequence[str] = ("kb",),
        context_lines: int = 0,
        max_matches_per_document: int | None = None,
        max_results_per_layer: int = 10,
    ) -> KnowledgeSearchResult:
        request = _search_request(
            alias=self._config.alias,
            query=query,
            layers=layers,
            context_lines=context_lines,
            max_matches_per_document=max_matches_per_document,
            max_results_per_layer=max_results_per_layer,
        )
        document = self._request_json("/v1/search", request)
        return _parse_search_response(document, request)

    def resolve_task_owner(
        self,
        *,
        candidate_id: str,
        source_revision: str,
        legacy_task_id: int,
        legacy_digest: str,
    ) -> TaskOwnerEquivalence:
        """Request one identity-bound, read-only owner equivalence."""
        try:
            request = owner_equivalence_request(
                alias=self._config.alias,
                candidate_id=candidate_id,
                source_revision=source_revision,
                legacy_task_id=legacy_task_id,
                legacy_digest=legacy_digest,
            )
            payload = owner_equivalence_request_document(request)
        except OwnerEquivalenceContractError:
            raise KnowledgeRequestError(
                "owner equivalence request is invalid"
            ) from None
        document = self._request_json(
            "/v1/task-owner-equivalence", payload
        )
        try:
            return parse_owner_equivalence_response(document, request)
        except OwnerEquivalenceContractError:
            raise KnowledgeResponseError(
                "GW owner equivalence response is invalid"
            ) from None

    def execution_context(self) -> ExecutionContext:
        """Read one bounded allowlisted persona-variable snapshot."""
        request = {
            "schema": EXECUTION_CONTEXT_REQUEST_SCHEMA,
            "schema_version": EXECUTION_CONTEXT_SCHEMA_VERSION,
            "alias": self._config.alias,
        }
        document = self._request_json("/v1/execution-context", request)
        return _parse_execution_context(document, self._config.alias)

    def owner_upcoming_meeting(
        self, *, owner: str, owner_ref: Mapping[str, object]
    ) -> OwnerUpcomingMeeting:
        """Check one exact owner identity without receiving calendar content."""
        request = _owner_meeting_request(
            alias=self._config.alias,
            owner=owner,
            owner_ref=owner_ref,
        )
        document = self._request_json(
            "/v1/owner-upcoming-meeting", request
        )
        return _parse_owner_meeting_response(document)

    def _request_json(
        self, route: str, request: Mapping[str, Any]
    ) -> object:
        payload = (json.dumps(
            request,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ) + "\n").encode("utf-8")
        message = urllib.request.Request(
            self._config.endpoint + route,
            data=payload,
            method="POST",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._config.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with self._opener.open(
                message, timeout=self._config.timeout_seconds
            ) as response:
                content_type = response.headers.get_content_type()
                if content_type != "application/json":
                    raise KnowledgeResponseError(
                        "GW knowledge response has an unsupported media type"
                    )
                declared = response.headers.get("Content-Length")
                if declared is not None:
                    try:
                        declared_size = int(declared)
                    except ValueError:
                        raise KnowledgeResponseError(
                            "GW knowledge response length is invalid"
                        ) from None
                    if (declared_size < 0
                            or declared_size > self._config.max_response_bytes):
                        raise KnowledgeResponseError(
                            "GW knowledge response exceeds its size limit"
                        )
                raw = response.read(self._config.max_response_bytes + 1)
        except KnowledgeResponseError:
            raise
        except (
            OSError,
            TimeoutError,
            socket.timeout,
            urllib.error.HTTPError,
            urllib.error.URLError,
        ):
            raise KnowledgeTransportError("GW knowledge request failed") from None
        if len(raw) > self._config.max_response_bytes:
            raise KnowledgeResponseError(
                "GW knowledge response exceeds its size limit"
            )
        try:
            return json.loads(raw, object_pairs_hook=_strict_object)
        except (UnicodeDecodeError, ValueError, TypeError):
            raise KnowledgeResponseError("GW knowledge response is invalid") from None


def _endpoint(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise KnowledgeConfigError("knowledge endpoint is invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise KnowledgeConfigError("knowledge endpoint is invalid") from None
    if (parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path not in {"", "/"}):
        raise KnowledgeConfigError("knowledge endpoint is invalid")
    if parsed.scheme == "http":
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            raise KnowledgeConfigError(
                "unencrypted knowledge endpoint must be loopback"
            ) from None
        if not address.is_loopback:
            raise KnowledgeConfigError(
                "unencrypted knowledge endpoint must be loopback"
            )
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    return f"{parsed.scheme}://{host}" + (f":{port}" if port is not None else "")


def _search_request(
    *,
    alias: str,
    query: object,
    layers: Sequence[str],
    context_lines: object,
    max_matches_per_document: object,
    max_results_per_layer: object,
) -> dict[str, Any]:
    if (not isinstance(query, str) or query != query.strip() or not query
            or len(query) > 2_048 or query.startswith("-")
            or any(ord(char) < 32 or ord(char) == 127 for char in query)):
        raise KnowledgeRequestError("knowledge query is invalid")
    if isinstance(layers, (str, bytes)):
        raise KnowledgeRequestError("knowledge layers are invalid")
    requested = tuple(layers)
    if (not requested or len(requested) > len(LAYER_ORDER)
            or len(requested) != len(set(requested))
            or any(layer not in LAYER_ORDER for layer in requested)):
        raise KnowledgeRequestError("knowledge layers are invalid")
    ordered = [layer for layer in LAYER_ORDER if layer in requested]
    context = _bounded_int(context_lines, "context lines", 0, 5)
    maximum = (
        None
        if max_matches_per_document is None
        else _bounded_int(max_matches_per_document, "match limit", 0, 50)
    )
    result_limit = _bounded_int(
        max_results_per_layer, "result limit", 0, 20
    )
    return {
        "alias": alias,
        "query": query,
        "layers": ordered,
        "context_lines": context,
        "max_matches_per_document": maximum,
        "max_results_per_layer": result_limit,
    }


def _parse_search_response(
    value: object, request: Mapping[str, Any]
) -> KnowledgeSearchResult:
    root = _object(value, "search response")
    allowed_fields = {
        "schema", "schema_version", "ok", "query", "parameters", "layers",
    }
    if "excluded" in root:
        allowed_fields.add("excluded")
    _exact_fields(
        root,
        "search response",
        allowed_fields,
    )
    version = root["schema_version"]
    if (root["schema"] != SEARCH_SCHEMA
            or isinstance(version, bool)
            or version != SEARCH_SCHEMA_VERSION
            or root["ok"] is not True
            or root["query"] != request["query"]):
        raise KnowledgeResponseError("GW knowledge response identity is invalid")
    parameters = _object(root["parameters"], "search parameters")
    allowed_parameters = {
        "layers", "context_lines", "max_matches_per_document",
        "max_results_per_layer",
    }
    if "ranking" in parameters:
        allowed_parameters.add("ranking")
    _exact_fields(parameters, "search parameters", allowed_parameters)
    for name in (
        "layers", "context_lines", "max_matches_per_document",
        "max_results_per_layer",
    ):
        if parameters[name] != request[name]:
            raise KnowledgeResponseError(
                "GW knowledge response parameters do not match the request"
            )
    if "ranking" in parameters and parameters["ranking"] != "hybrid_rrf":
        raise KnowledgeResponseError("GW knowledge ranking is unsupported")
    if "excluded" in root:
        excluded = _object(root["excluded"], "search exclusions")
        _exact_fields(
            excluded,
            "search exclusions",
            {"hits", "directories", "declined"},
        )
        for name in ("hits", "directories", "declined"):
            _response_int(excluded[name], "search exclusion count")

    raw_layers = root["layers"]
    if not isinstance(raw_layers, list):
        raise KnowledgeResponseError("GW knowledge layers are invalid")
    if len(raw_layers) != len(request["layers"]):
        raise KnowledgeResponseError("GW knowledge layers are incomplete")
    parsed_layers = []
    for expected_name, raw_layer in zip(request["layers"], raw_layers, strict=True):
        layer = _object(raw_layer, "search layer")
        _exact_fields(
            layer,
            "search layer",
            {"name", "total_results", "returned_results", "truncated", "documents"},
        )
        if layer["name"] != expected_name:
            raise KnowledgeResponseError("GW knowledge layer order is invalid")
        total = _response_int(layer["total_results"], "total results")
        returned = _response_int(layer["returned_results"], "returned results")
        documents = layer["documents"]
        if (not isinstance(documents, list) or returned != len(documents)
                or returned > request["max_results_per_layer"]
                or total < returned
                or not isinstance(layer["truncated"], bool)
                or layer["truncated"] is not (total > returned)):
            raise KnowledgeResponseError("GW knowledge result counts are invalid")
        parsed_documents = tuple(
            _parse_document(item, expected_name) for item in documents
        )
        parsed_layers.append(KnowledgeLayer(
            name=expected_name,
            total_results=total,
            truncated=layer["truncated"],
            documents=parsed_documents,
        ))
    return KnowledgeSearchResult(tuple(parsed_layers))


def _parse_execution_context(value: object, alias: str) -> ExecutionContext:
    root = _object(value, "execution context response")
    _exact_fields(
        root,
        "execution context response",
        {
            "schema", "schema_version", "ok", "alias", "revision",
            "variables",
        },
    )
    version = root["schema_version"]
    revision = root["revision"]
    if (
        root["schema"] != EXECUTION_CONTEXT_RESPONSE_SCHEMA
        or isinstance(version, bool)
        or version != EXECUTION_CONTEXT_SCHEMA_VERSION
        or root["ok"] is not True
        or root["alias"] != alias
        or not isinstance(revision, str)
        or not _DIGEST_RE.fullmatch(revision)
    ):
        raise KnowledgeResponseError(
            "GW execution context response identity is invalid"
        )
    variables = _object(root["variables"], "execution context variables")
    _exact_fields(
        variables,
        "execution context variables",
        {
            "display_name", "operator_context", "self_aliases",
            "institution_domains",
        },
    )
    display_name = _text(
        variables["display_name"], "execution context display name", 200
    )
    operator_context = _content_text(
        variables["operator_context"],
        "execution context operator text",
        32_768,
    )
    self_aliases = _context_text_list(
        variables["self_aliases"], "execution context self aliases", 32, 200
    )
    institution_domains = _context_text_list(
        variables["institution_domains"],
        "execution context institution domains",
        32,
        253,
    )
    if any(
        not _DOMAIN_RE.fullmatch(domain) for domain in institution_domains
    ):
        raise KnowledgeResponseError(
            "GW execution context institution domains are invalid"
        )
    canonical = json.dumps(
        {
            "display_name": display_name,
            "operator_context": operator_context,
            "self_aliases": list(self_aliases),
            "institution_domains": list(institution_domains),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if revision != hashlib.sha256(canonical).hexdigest():
        raise KnowledgeResponseError(
            "GW execution context revision does not match its variables"
        )
    return ExecutionContext(
        alias=alias,
        revision=revision,
        display_name=display_name,
        operator_context=operator_context,
        self_aliases=self_aliases,
        institution_domains=institution_domains,
    )


def _owner_meeting_request(
    *, alias: str, owner: object, owner_ref: Mapping[str, object]
) -> dict[str, object]:
    if not isinstance(owner_ref, Mapping):
        raise KnowledgeRequestError("owner meeting reference is invalid")
    fields = {
        "kind", "speaker_id", "canonical_speaker_id",
        "speaker_registry_id", "pinned", "provisional",
    }
    if set(owner_ref) != fields:
        raise KnowledgeRequestError("owner meeting reference is invalid")
    kind = owner_ref["kind"]
    scoped = (
        owner_ref["speaker_id"],
        owner_ref["canonical_speaker_id"],
        owner_ref["speaker_registry_id"],
    )
    if (
        kind not in {"person", "external"}
        or not isinstance(owner_ref["pinned"], bool)
        or owner_ref["provisional"] is not False
        or not (
            all(value is None for value in scoped)
            or all(
                isinstance(value, str)
                and value
                and value == value.strip()
                and len(value) <= 200
                for value in scoped
            )
        )
    ):
        raise KnowledgeRequestError("owner meeting reference is invalid")
    if (
        not isinstance(owner, str)
        or not owner
        or owner != owner.strip()
        or len(owner) > 200
        or any(ord(char) < 32 or ord(char) == 127 for char in owner)
    ):
        raise KnowledgeRequestError("owner meeting display is invalid")
    return {
        "schema": OWNER_MEETING_REQUEST_SCHEMA,
        "schema_version": OWNER_MEETING_SCHEMA_VERSION,
        "alias": alias,
        "owner": owner,
        "owner_ref": dict(owner_ref),
    }


def _parse_owner_meeting_response(value: object) -> OwnerUpcomingMeeting:
    root = _object(value, "owner meeting response")
    _exact_fields(
        root,
        "owner meeting response",
        {
            "schema", "schema_version", "ok", "match", "checked_at",
            "evidence_revision",
        },
    )
    version = root["schema_version"]
    checked_at = root["checked_at"]
    revision = root["evidence_revision"]
    if (
        root["schema"] != OWNER_MEETING_RESPONSE_SCHEMA
        or isinstance(version, bool)
        or version != OWNER_MEETING_SCHEMA_VERSION
        or root["ok"] is not True
        or not isinstance(root["match"], bool)
        or not isinstance(checked_at, str)
        or not isinstance(revision, str)
        or not _DIGEST_RE.fullmatch(revision)
    ):
        raise KnowledgeResponseError(
            "GW owner meeting response identity is invalid"
        )
    try:
        parsed_at = datetime.fromisoformat(checked_at)
    except ValueError:
        raise KnowledgeResponseError(
            "GW owner meeting response time is invalid"
        ) from None
    if parsed_at.tzinfo is None or parsed_at.utcoffset() is None:
        raise KnowledgeResponseError(
            "GW owner meeting response time is invalid"
        )
    return OwnerUpcomingMeeting(
        match=root["match"],
        checked_at=checked_at,
        evidence_revision=revision,
    )


def _context_text_list(
    value: object, field_name: str, maximum_items: int, maximum_chars: int
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum_items:
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    items = tuple(_text(item, field_name, maximum_chars) for item in value)
    if len(items) != len(set(items)):
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return items


def _parse_document(value: object, layer: str) -> KnowledgeDocument:
    document = _object(value, "search document")
    required = {"id", "path", "excerpt"}
    optional = {"kb_path", "section", "ranking"}
    if required - set(document) or set(document) - required - optional:
        raise KnowledgeResponseError("GW knowledge document fields are invalid")
    path = _relative_path(document["path"], "document path")
    identifier = _text(document["id"], "document identifier", 1_200)
    match = _DOCUMENT_ID_RE.fullmatch(identifier)
    if match is None or match.group(1) != layer or match.group(2) != path:
        raise KnowledgeResponseError("GW knowledge document identity is invalid")
    excerpt = _content_text(document["excerpt"], "document excerpt", 64_000)
    kb_path = (
        _relative_path(document["kb_path"], "knowledge-base path")
        if "kb_path" in document else None
    )
    section = (
        _text(document["section"], "document section", 1_000)
        if "section" in document else None
    )
    ranking_score = None
    if "ranking" in document:
        ranking = _object(document["ranking"], "document ranking")
        _exact_fields(ranking, "document ranking", {"method", "score"})
        score = ranking["score"]
        if (ranking["method"] != "rrf" or isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not math.isfinite(score) or not 0 <= score <= 1):
            raise KnowledgeResponseError("GW knowledge ranking is invalid")
        ranking_score = float(score)
    return KnowledgeDocument(
        id=identifier,
        path=path,
        excerpt=excerpt,
        kb_path=kb_path,
        section=section,
        ranking_score=ranking_score,
    )


def _relative_path(value: object, field_name: str) -> str:
    text = _text(value, field_name, 1_000)
    if (text.startswith("/") or "\\" in text or "//" in text
            or any(part in {"", ".", ".."} for part in text.split("/"))):
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return text


def _content_text(value: object, field_name: str, maximum: int) -> str:
    if (not isinstance(value, str) or len(value) > maximum
            or any(
                char not in "\t\n\r" and (ord(char) < 32 or ord(char) == 127)
                for char in value
            )):
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return value


def _text(value: object, field_name: str, maximum: int) -> str:
    if (not isinstance(value, str) or not value or value != value.strip()
            or len(value) > maximum
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return value


def _response_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return value


def _bounded_int(value: object, field_name: str, minimum: int, maximum: int) -> int:
    if (isinstance(value, bool) or not isinstance(value, int)
            or not minimum <= value <= maximum):
        raise KnowledgeRequestError(f"knowledge {field_name} is invalid")
    return value


def _object(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KnowledgeResponseError(f"GW knowledge {field_name} is invalid")
    return value


def _exact_fields(
    value: Mapping[str, Any], field_name: str, expected: set[str]
) -> None:
    if set(value) != expected:
        raise KnowledgeResponseError(f"GW knowledge {field_name} fields are invalid")


class _DuplicateField(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateField
        result[key] = value
    return result
