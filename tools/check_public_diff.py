#!/usr/bin/env python3
"""Reject high-confidence operational identifiers added by a public diff.

Ported from the sibling `gw` repository's `tools/check_public_diff.py` and
adapted for this repository. See `AGENTS.md` for the publication-safety rule
this script gives a mechanical floor to, and read the "silence is not
evidence" note there (or in CONFIDENTIALITY.md, if present) before trusting a
clean result.
"""

from __future__ import annotations

import ipaddress
import os
import re
import sys


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")
URL_HOST_RE = re.compile(r"https?://([A-Za-z0-9.-]+)(?::\d+)?(?:[/?#]|$)", re.I)
HOME_PATH_RE = re.compile(
    r"(?:/home/|/Users/)[A-Za-z0-9._-]+(?:/|\b)|"
    r"[A-Za-z]:\\Users\\[A-Za-z0-9._-]+(?:\\|\b)"
)
IPV4_RE = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
REMOTE_COMMAND_RE = re.compile(
    r"\b(?:ssh|scp|rsync)\s+(?:-[A-Za-z]+\s+)*(?:[^\s@]+@)?"
    r"([A-Za-z0-9][A-Za-z0-9.-]+)"
)

# Reviewed for this repository rather than copied from `gw` unchanged: the
# URL allowlist there also carried `claude.com`, `bitbucket.org`,
# `python.org`, and `pypi.org` for links that repository's own docs use.
# None of those domains appear anywhere in this repository, so they are left
# out here — an allowlist should reflect domains this repository actually
# has a reason to reference, not every domain a sibling project happened to
# need. Add a domain here only when a real, sanitized doc link needs it.
ALLOWED_EMAIL_DOMAINS = {
    "example.com",
    "example.net",
    "example.org",
    "users.noreply.github.com",
}
ALLOWED_HOSTS = {"host-a", "host-b", "localhost"}
ALLOWED_URL_DOMAINS = {
    "example.com",
    "example.net",
    "example.org",
    "github.com",
    "json-schema.org",
}
ALLOWED_NETWORKS = tuple(ipaddress.ip_network(value) for value in (
    "127.0.0.0/8",
    "192.0.2.0/24",
    "198.51.100.0/24",
    "203.0.113.0/24",
))


PRIVATE_TERMS_ENV = "FOXHOUND_PUBLIC_DIFF_PRIVATE_TERMS"


def _configured_private_terms(value: str | None = None) -> tuple[str, ...]:
    """Private literals supplied outside the repository, one per line.

    Committing the literals that this guard exists to keep private would make
    the guard self-defeating. There is no CI to inject this from a secret in
    this repository (see AGENTS.md and the note about Actions billing); this
    exists for local use — export the same variable before committing or
    running the guard by hand. Empty and one-character entries are ignored so
    an accidental blank cannot match every line.
    """
    raw = os.environ.get(PRIVATE_TERMS_ENV, "") if value is None else value
    return tuple(term.strip().casefold() for term in raw.splitlines()
                 if len(term.strip()) >= 2)


def _allowed_url_host(host: str) -> bool:
    host = host.lower().rstrip(".")
    return any(host == domain or host.endswith("." + domain)
               for domain in ALLOWED_URL_DOMAINS)


def findings(diff_text: str, *, private_terms: tuple[str, ...] | None = None) -> list[str]:
    """Return sanitized findings for added lines in a unified diff."""
    found: list[str] = []
    terms = (_configured_private_terms() if private_terms is None
             else tuple(term.casefold() for term in private_terms))
    added_line = 0
    for raw in diff_text.splitlines():
        if not raw.startswith("+") or raw.startswith("+++"):
            continue
        added_line += 1
        line = raw[1:]

        if any(term in line.casefold() for term in terms):
            found.append(f"added line {added_line}: configured private term")

        for match in EMAIL_RE.finditer(line):
            if match.group(1).lower() not in ALLOWED_EMAIL_DOMAINS:
                found.append(f"added line {added_line}: non-example email address")
                break

        for match in URL_HOST_RE.finditer(line):
            if not _allowed_url_host(match.group(1)):
                found.append(f"added line {added_line}: non-approved URL host")
                break

        if HOME_PATH_RE.search(line):
            found.append(f"added line {added_line}: user home path")

        for raw_ip in IPV4_RE.findall(line):
            try:
                address = ipaddress.ip_address(raw_ip)
            except ValueError:
                continue
            if not any(address in network for network in ALLOWED_NETWORKS):
                found.append(f"added line {added_line}: non-example IPv4 address")
                break

        command = REMOTE_COMMAND_RE.search(line)
        if command:
            host = command.group(1).lower()
            if (host not in ALLOWED_HOSTS and not host.startswith("example.")
                    and not host.startswith("<")):
                found.append(f"added line {added_line}: remote host identifier")

    return found


def main() -> int:
    found = findings(sys.stdin.read())
    if not found:
        print("public diff check passed")
        return 0
    print("Public diff may contain operational information:", file=sys.stderr)
    for item in found:
        print(f"- {item}", file=sys.stderr)
    print("Replace values with synthetic examples and review AGENTS.md.",
          file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
