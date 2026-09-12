"""Authenticated loopback HTTP boundary for Foxhound task review cards."""

from __future__ import annotations

import argparse
import hmac
import ipaddress
import json
import logging
import os
import socket
import stat
import sys
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from .task_cards import (
    CardOperationResult,
    ScheduleResult,
    TaskCardService,
    render_task_review_card,
)
from .task_ledger import TaskLedgerError


log = logging.getLogger("foxhound.task_card_server")

SERVICE_VERSION = 1
REQUEST_SCHEMA = "foxhound.task-card-service.request"
ERROR_SCHEMA = "foxhound.task-card-service.error"
HEALTH_SCHEMA = "foxhound.task-card-service.health"
SCHEDULE_SCHEMA = "foxhound.task-card-service.schedule"
CLAIM_SCHEMA = "foxhound.task-card-service.claim"
OPERATION_SCHEMA = "foxhound.task-card-service.operation"
STATS_SCHEMA = "foxhound.task-card-service.stats"

ROUTES = {
    "/v1/task-cards/stats": "stats",
    "/v1/task-cards/schedule": "schedule",
    "/v1/task-cards/claim": "claim",
    "/v1/task-cards/delivered": "delivered",
    "/v1/task-cards/delivery-failed": "delivery_failed",
    "/v1/task-cards/action": "action",
}


class TaskCardServerConfigError(ValueError):
    pass


class TaskCardServerRequestError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        status: int = HTTPStatus.BAD_REQUEST,
    ) -> None:
        self.code = code
        self.message = message
        self.status = int(status)
        super().__init__(message)


class TaskCardServerResponseTooLarge(RuntimeError):
    pass


@dataclass(frozen=True)
class TaskCardServerLimits:
    max_body_bytes: int = 16 * 1024
    max_response_bytes: int = 64 * 1024
    request_timeout_seconds: float = 5.0

    def validate(self) -> None:
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1024
            for value in (self.max_body_bytes, self.max_response_bytes)
        ):
            raise TaskCardServerConfigError(
                "task card byte limits must be integers of at least 1024"
            )
        if (
            isinstance(self.request_timeout_seconds, bool)
            or not isinstance(self.request_timeout_seconds, (int, float))
            or not 0 < self.request_timeout_seconds <= 30
        ):
            raise TaskCardServerConfigError(
                "task card request timeout must be between 0 and 30 seconds"
            )


class TaskCardApplication:
    """Strict request validation around one task-card aggregate."""

    def __init__(
        self,
        cards: TaskCardService,
        token: str,
        *,
        limits: TaskCardServerLimits | None = None,
    ) -> None:
        if not isinstance(cards, TaskCardService):
            raise TaskCardServerConfigError("task card service is invalid")
        if not _valid_secret(token):
            raise TaskCardServerConfigError("task card bearer token is invalid")
        self.cards = cards
        self.token = token
        self.limits = limits or TaskCardServerLimits()
        self.limits.validate()

    def authorized(self, header: str | None) -> bool:
        prefix = "Bearer "
        return bool(
            header
            and header.startswith(prefix)
            and hmac.compare_digest(header[len(prefix):], self.token)
        )

    def dispatch(self, operation: str, payload: object) -> dict[str, Any]:
        if operation == "stats":
            _request(payload, required=set())
            stats = self.cards.stats()
            return {
                "schema": STATS_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": True,
                "pending": stats.pending,
                "delivering": stats.delivering,
                "delivered": stats.delivered,
                "snoozed": stats.snoozed,
                "active": stats.active,
            }
        if operation == "schedule":
            request = _request(payload, required={"limit"})
            limit = _integer(request["limit"], minimum=1, maximum=1_000)
            return _schedule_document(self.cards.schedule(limit=limit))
        if operation == "claim":
            request = _request(payload, required={"lease_seconds"})
            lease = _integer(request["lease_seconds"], minimum=5, maximum=300)
            claim = self.cards.claim_next(lease_seconds=lease)
            if claim is None:
                return {
                    "schema": CLAIM_SCHEMA,
                    "schema_version": SERVICE_VERSION,
                    "ok": True,
                    "status": "empty",
                    "claim": None,
                }
            body, reply_markup = render_task_review_card(claim.card)
            return {
                "schema": CLAIM_SCHEMA,
                "schema_version": SERVICE_VERSION,
                "ok": True,
                "status": "claimed",
                "claim": {
                    "card_id": claim.card.id,
                    "card_version": claim.card.version,
                    "claim_token": claim.token,
                    "expires_at": claim.expires_at,
                    "delivery_key": (
                        f"foxhound-task-card-{claim.card.id}-"
                        f"v{claim.card.version}"
                    ),
                    "body": body,
                    "reply_markup": reply_markup,
                },
            }
        if operation == "delivered":
            request = _request(
                payload,
                required={
                    "card_id", "card_version", "claim_token", "transport",
                    "delivery_ref",
                },
            )
            return _operation_document(self.cards.complete_delivery(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                claim_token=_secret(request["claim_token"]),
                transport=_opaque(request["transport"], maximum=64),
                delivery_ref=_opaque(request["delivery_ref"], maximum=200),
            ))
        if operation == "delivery_failed":
            request = _request(
                payload,
                required={"card_id", "card_version", "claim_token"},
            )
            return _operation_document(self.cards.fail_delivery(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                claim_token=_secret(request["claim_token"]),
            ))
        if operation == "action":
            request = _request(
                payload,
                required={"card_id", "card_version", "action"},
            )
            action = request["action"]
            if (
                not isinstance(action, str)
                or action not in {"done", "keep_open", "drop", "snooze"}
            ):
                raise TaskCardServerRequestError(
                    "invalid_request", "task card action is invalid"
                )
            return _operation_document(self.cards.act(
                _integer(request["card_id"], minimum=1),
                expected_version=_integer(request["card_version"], minimum=1),
                action=action,
            ))
        raise TaskCardServerRequestError(
            "not_found", "route not found", HTTPStatus.NOT_FOUND
        )


class _TaskCardHTTPServer(HTTPServer):
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], app: TaskCardApplication):
        self.app = app
        super().__init__(address, TaskCardRequestHandler)

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(self.app.limits.request_timeout_seconds)
        return connection, address


class TaskCardRequestHandler(BaseHTTPRequestHandler):
    server_version = "foxhound-task-cards/1"
    sys_version = ""

    @property
    def app(self) -> TaskCardApplication:
        return self.server.app  # type: ignore[attr-defined,no-any-return]

    def do_GET(self) -> None:  # noqa: N802
        started = time.monotonic()
        if self.path != "/healthz":
            self._error(HTTPStatus.NOT_FOUND, "not_found", "route not found")
            self._audit(HTTPStatus.NOT_FOUND, started)
            return
        self._json(HTTPStatus.OK, {
            "schema": HEALTH_SCHEMA,
            "schema_version": SERVICE_VERSION,
            "ok": True,
        })
        self._audit(HTTPStatus.OK, started)

    def do_POST(self) -> None:  # noqa: N802
        started = time.monotonic()
        operation = ROUTES.get(self.path)
        if operation is None:
            self._error(HTTPStatus.NOT_FOUND, "not_found", "route not found")
            self._audit(HTTPStatus.NOT_FOUND, started)
            return
        auth_headers = self.headers.get_all("Authorization") or []
        if len(auth_headers) != 1 or not self.app.authorized(auth_headers[0]):
            self._error(
                HTTPStatus.UNAUTHORIZED,
                "unauthorized",
                "authentication required",
            )
            self._audit(HTTPStatus.UNAUTHORIZED, started)
            return
        try:
            result = self.app.dispatch(operation, self._read_json_body())
            self._json(HTTPStatus.OK, result)
            self._audit(HTTPStatus.OK, started)
        except TaskCardServerRequestError as exc:
            self._error(exc.status, exc.code, exc.message)
            self._audit(exc.status, started)
        except TaskCardServerResponseTooLarge:
            self._error(
                HTTPStatus.BAD_GATEWAY,
                "response_too_large",
                "task card response exceeds the service limit",
            )
            self._audit(HTTPStatus.BAD_GATEWAY, started)
        except socket.timeout:
            self._error(
                HTTPStatus.REQUEST_TIMEOUT,
                "request_timeout",
                "request body timed out",
            )
            self._audit(HTTPStatus.REQUEST_TIMEOUT, started)
        except (BrokenPipeError, ConnectionResetError):
            self._audit(499, started)
        except Exception as exc:  # pragma: no cover - defensive boundary
            log.error("status=500 exception=%s", type(exc).__name__)
            self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal service error",
            )
            self._audit(HTTPStatus.INTERNAL_SERVER_ERROR, started)

    def do_PUT(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def do_DELETE(self) -> None:  # noqa: N802
        self._method_not_allowed()

    def _method_not_allowed(self) -> None:
        started = time.monotonic()
        self._error(
            HTTPStatus.METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "method not allowed",
            extra_headers={"Allow": "GET, POST"},
        )
        self._audit(HTTPStatus.METHOD_NOT_ALLOWED, started)

    def _read_json_body(self) -> object:
        if self.headers.get("Transfer-Encoding"):
            raise TaskCardServerRequestError(
                "unsupported_transfer_encoding",
                "transfer encoding is not supported",
            )
        media_type = self.headers.get("Content-Type", "").split(";", 1)[0]
        if media_type.strip().lower() != "application/json":
            raise TaskCardServerRequestError(
                "unsupported_media_type",
                "content type must be application/json",
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
            )
        lengths = self.headers.get_all("Content-Length") or []
        if len(lengths) != 1:
            raise TaskCardServerRequestError(
                "length_required",
                "one content length is required",
                HTTPStatus.LENGTH_REQUIRED,
            )
        try:
            length = int(lengths[0])
        except ValueError as exc:
            raise TaskCardServerRequestError(
                "invalid_request", "content length is invalid"
            ) from exc
        if length < 0 or length > self.app.limits.max_body_bytes:
            raise TaskCardServerRequestError(
                "request_too_large",
                "request body exceeds the service limit",
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
        body = self.rfile.read(length)
        if len(body) != length:
            raise TaskCardServerRequestError(
                "invalid_request", "request body is incomplete"
            )
        try:
            return json.loads(
                body.decode("utf-8"), object_pairs_hook=_strict_json_object
            )
        except (UnicodeDecodeError, ValueError) as exc:
            raise TaskCardServerRequestError(
                "invalid_json", "request body is not valid JSON"
            ) from exc

    def _json(
        self,
        status: int,
        document: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(
            document, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(body) > self.app.limits.max_response_bytes:
            if document.get("schema") == ERROR_SCHEMA:
                self.close_connection = True
                return
            raise TaskCardServerResponseTooLarge
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if extra_headers:
            for name, value in extra_headers.items():
                self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _error(
        self,
        status: int,
        code: str,
        message: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._json(status, {
            "schema": ERROR_SCHEMA,
            "schema_version": SERVICE_VERSION,
            "ok": False,
            "error": {"code": code, "message": message},
        }, extra_headers=extra_headers)

    def _audit(self, status: int, started: float) -> None:
        route = self.path.split("?", 1)[0]
        if route not in {"/healthz", *ROUTES}:
            route = "unknown"
        log.info(
            "method=%s route=%s status=%s duration_ms=%s",
            self.command,
            route,
            int(status),
            round((time.monotonic() - started) * 1000),
        )

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def _request(payload: object, *, required: set[str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TaskCardServerRequestError(
            "invalid_request", "request body must be a JSON object"
        )
    common = {"schema", "schema_version"}
    if set(payload) != common | required:
        raise TaskCardServerRequestError(
            "invalid_request", "request fields are invalid"
        )
    version = payload["schema_version"]
    if (
        payload["schema"] != REQUEST_SCHEMA
        or isinstance(version, bool)
        or version != SERVICE_VERSION
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "request contract is unsupported"
        )
    return payload


def _integer(value: object, *, minimum: int, maximum: int | None = None) -> int:
    upper = 9_223_372_036_854_775_807 if maximum is None else maximum
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= upper
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "integer request field is outside its limit"
        )
    return value


def _secret(value: object) -> str:
    if not _valid_secret(value):
        raise TaskCardServerRequestError(
            "invalid_request", "claim capability is invalid"
        )
    return value


def _opaque(value: object, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise TaskCardServerRequestError(
            "invalid_request", "opaque request field is invalid"
        )
    return value


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and 32 <= len(value) <= 4_096
        and not any(char.isspace() for char in value)
    )


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _schedule_document(result: ScheduleResult) -> dict[str, Any]:
    return {
        "schema": SCHEDULE_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.disposition != "refused",
        "disposition": result.disposition.value,
        "created": result.created,
        "cancelled": result.cancelled,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def _operation_document(result: CardOperationResult) -> dict[str, Any]:
    return {
        "schema": OPERATION_SCHEMA,
        "schema_version": SERVICE_VERSION,
        "ok": result.accepted,
        "disposition": result.disposition.value,
        "card_id": result.card_id,
        "card_version": result.version,
        "card_status": None if result.status is None else result.status.value,
        "task_version": result.task_version,
        "task_status": (
            None if result.task_status is None else result.task_status.value
        ),
        "wake_at": result.wake_at,
        "refusal": None if result.refusal is None else result.refusal.value,
    }


def load_token(path: str | os.PathLike[str]) -> str:
    token_path = Path(path).expanduser()
    if token_path.is_symlink():
        raise TaskCardServerConfigError("task card token file is unavailable")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(token_path, flags)
    except OSError as exc:
        raise TaskCardServerConfigError("task card token file is unavailable") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise TaskCardServerConfigError(
                "task card token file must be a regular file"
            )
        if os.name == "posix" and (
            info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise TaskCardServerConfigError(
                "task card token file must be owner-only mode 0600"
            )
        with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
            descriptor = -1
            token = handle.read().strip()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not _valid_secret(token):
        raise TaskCardServerConfigError("task card bearer token is invalid")
    return token


def is_canonical_loopback(host: object) -> bool:
    if not isinstance(host, str):
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.version == 4 and address.is_loopback and address.compressed == host


def make_server(
    host: str, port: int, app: TaskCardApplication
) -> _TaskCardHTTPServer:
    if not is_canonical_loopback(host):
        raise TaskCardServerConfigError(
            "task card service requires a canonical IPv4 loopback bind"
        )
    if (
        isinstance(port, bool)
        or not isinstance(port, int)
        or not 0 <= port <= 65_535
    ):
        raise TaskCardServerConfigError("task card service port is invalid")
    return _TaskCardHTTPServer((host, port), app)


def serve(host: str, port: int, app: TaskCardApplication) -> None:
    if port == 0:
        raise TaskCardServerConfigError("task card production port must be nonzero")
    server = make_server(host, port, app)
    log.info("task card service started")
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Serve Foxhound task cards on an authenticated loopback API"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--request-timeout", type=float, default=5.0)
    arguments = parser.parse_args(argv)
    try:
        cards = TaskCardService(arguments.database)
        cards.count()
        app = TaskCardApplication(
            cards,
            load_token(arguments.token_file),
            limits=TaskCardServerLimits(
                request_timeout_seconds=arguments.request_timeout
            ),
        )
        serve(arguments.bind, arguments.port, app)
    except (OSError, TaskLedgerError, TaskCardServerConfigError) as exc:
        print(f"foxhound task-card-server: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
