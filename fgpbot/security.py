from __future__ import annotations

import logging
import re
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import quote, quote_plus


class Redactor:
    """Sanitize the *rendered* log line, including exception tracebacks."""

    def __init__(self) -> None:
        self._secrets: deque[str] = deque(maxlen=256)
        self._patterns = (
            (re.compile(r"(?i)([?&](?:access_token|refresh_token|client_secret|token|code)=)[^\s&#\"'<>]+"), r"\1[REDACTED]"),
            (re.compile(r'''(?i)((?:access_token|refresh_token|client_secret|token|code)["']?\s*[:=]\s*["']?)[^\s,}"'&<>]+'''), r"\1[REDACTED]"),
            (re.compile(r"(?i)(\b(?:Bearer|OAuth)\s+)[A-Za-z0-9._~+/%=-]{8,}"), r"\1[REDACTED]"),
            (re.compile(r"(https?://)[^/@\s]+:[^/@\s]+@"), r"\1[REDACTED]@"),
        )

    def add(self, *values: str) -> None:
        for value in values:
            if len(value) < 6:
                continue
            for item in {value, quote(value, safe=""), quote_plus(value)}:
                if item not in self._secrets:
                    self._secrets.append(item)

    def __call__(self, value: object) -> str:
        text = str(value)
        for secret in sorted(self._secrets, key=len, reverse=True):
            text = text.replace(secret, "[REDACTED]")
        for pattern, replacement in self._patterns:
            text = pattern.sub(replacement, text)
        return text


REDACT = Redactor()


class SafeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return REDACT(super().format(record))


def setup_logging(root: Path, level: str) -> None:
    logs = root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    formatter = SafeFormatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler()
    disk = RotatingFileHandler(logs / "fgpbot.log", maxBytes=2 * 1024 * 1024,
                               backupCount=4, encoding="utf-8")
    for handler in (console, disk):
        handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[console, disk], force=True)
    # OAuth callback URLs contain short-lived authorization codes.
    logging.getLogger("aiohttp.access").disabled = True
