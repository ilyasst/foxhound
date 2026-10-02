import re
from typing import BinaryIO
from pathlib import Path

_SESSION_ID_LINE = re.compile(
    rb"(?m)^session_id:\s*([A-Za-z0-9][A-Za-z0-9._-]{0,127})\s*$"
)
#: The same identity, written in the closing block the runtime prints when a
#: pass stops because it exhausted its turn budget.  That block replaces the
#: plain ``session_id:`` line rather than accompanying it, so a runner that
#: reads only the plain form has no identity for exactly the runs most likely
#: to have left unrecorded work.  The following ``Duration:`` line is required:
#: it keeps a transcript that merely contains the word "Session:" -- agent
#: output is quoted into this file verbatim -- from being read as an identity.
_SESSION_SUMMARY_LINE = re.compile(
    rb"(?m)^Session:[ \t]+([A-Za-z0-9][A-Za-z0-9._-]{0,127})[ \t]*\r?\n"
    rb"Duration:[ \t]"
)
_SESSION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def extract_session_id_from_bytes(data: bytes) -> str | None:
    matches = [
        match
        for pattern in (_SESSION_ID_LINE, _SESSION_SUMMARY_LINE)
        for match in pattern.finditer(data)
    ]
    if not matches:
        return None
    last = max(matches, key=lambda match: match.start())
    try:
        return last.group(1).decode("ascii")
    except UnicodeDecodeError:
        return None


def extract_session_id_from_path(path: Path, transcript: BinaryIO | None = None) -> str | None:
    try:
        if transcript is not None:
            transcript.flush()
        data = path.read_bytes()
    except (OSError, AttributeError):
        return None
    return extract_session_id_from_bytes(data)
