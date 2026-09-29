"""The one grammar for upstream speaker identifiers.

Producers emit either a legacy per-recording id (``SPK_`` followed by one or
more digits) or an authority cluster id (``CLU_`` followed by exactly six
digits). Both are opaque: nothing derives identity from the prefix, and the
two forms are never equal to each other.

``SPEAKER_ID_PATTERN`` is deliberately unanchored so each call site keeps the
anchoring it needs (full-match for contract fields, token search for display
hygiene).
"""

from __future__ import annotations

import re


SPEAKER_ID_PATTERN = r"(?:SPK_\d+|CLU_\d{6})"
SPEAKER_ID_RE = re.compile(rf"^{SPEAKER_ID_PATTERN}$")
