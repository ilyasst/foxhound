"""Legacy root path translation for moved knowledge base directories."""

from __future__ import annotations

import os
import re
from typing import Sequence


class RootTranslations:
    """Translates obsolete absolute or home-relative path prefixes in text."""

    def __init__(
        self,
        translations: Sequence[tuple[str, str]],
        *,
        home: str | None = None,
    ) -> None:
        if len(translations) > 16:
            raise ValueError("At most 16 translation pairs are supported")

        norm_home: str | None = None
        if home is not None:
            if not home.startswith("/") or home != os.path.normpath(home):
                raise ValueError(f"Home directory must be absolute and normalised: {home!r}")
            norm_home = home.rstrip("/")

        validated_pairs: list[tuple[str, str]] = []
        seen_old: set[str] = set()

        for old_p, new_p in translations:
            if not isinstance(old_p, str) or not isinstance(new_p, str):
                raise TypeError("Translation prefixes must be strings")

            if not old_p.startswith("/") or not new_p.startswith("/"):
                raise ValueError("Prefixes must be absolute paths starting with '/'")

            if old_p != os.path.normpath(old_p) or new_p != os.path.normpath(new_p):
                raise ValueError("Prefixes must be normalised (no '..', no redundant slashes)")

            if (old_p.endswith("/") and old_p != "/") or (new_p.endswith("/") and new_p != "/"):
                raise ValueError("Prefixes must not have trailing slashes")

            if old_p == new_p:
                raise ValueError(f"Old and new prefix cannot be identical: {old_p!r}")

            if old_p in seen_old:
                raise ValueError(f"Duplicate old prefix: {old_p!r}")

            seen_old.add(old_p)
            validated_pairs.append((old_p, new_p))

        all_rules: list[tuple[str, str]] = []
        for old_p, new_p in validated_pairs:
            all_rules.append((old_p, new_p))
            if norm_home is not None and old_p.startswith(norm_home + "/"):
                rel_suffix = old_p[len(norm_home) :]
                home_old = f"~{rel_suffix}"
                if new_p.startswith(norm_home + "/"):
                    home_new = f"~{new_p[len(norm_home) :]}"
                else:
                    home_new = new_p
                all_rules.append((home_old, home_new))

        # Sort rules: longest old pattern first
        all_rules.sort(key=lambda item: len(item[0]), reverse=True)
        self._rules = all_rules
        self._rule_map = dict(all_rules)

        if all_rules:
            # Lookahead: path boundary must be '/' or non-path character (e.g. whitespace, quotes, punctuation) or end of string.
            # Lookbehind / start: old_prefix must be preceded by start-of-string or non-path character.
            delims = r"""[ \t\r\n`'"`)\x5d\x7d>:;,$!?.=]"""
            escaped = [re.escape(k) for k, _ in all_rules]
            pattern_str = rf"(?P<prefix>{'|'.join(escaped)})(?=/|{delims}|\Z)"
            self._pattern = re.compile(pattern_str)
        else:
            self._pattern = None

    def translate_path(self, path: str) -> str:
        """Translate a single path if it matches an old prefix boundary."""
        for old_p, new_p in self._rules:
            if path == old_p:
                return new_p
            if path.startswith(old_p + "/"):
                return new_p + path[len(old_p) :]
        return path

    def translate_text(self, text: str) -> str:
        """Single-pass replacement of old prefixes at path boundaries."""
        if not self._pattern or not text:
            return text

        def _replace(match: re.Match[str]) -> str:
            prefix = match.group("prefix")
            return self._rule_map[prefix]

        return self._pattern.sub(_replace, text)
