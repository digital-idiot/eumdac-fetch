"""Bootstrap EUMDAC credentials from environment, .env file, or ~/.eumdac/credentials.

The module-level singleton :data:`ENV` is created once at import time.  It
tries each credential source in priority order and warns when nothing is found.

Priority chain
--------------
1. ``EUMDAC_KEY`` / ``EUMDAC_SECRET`` / ``EUMDAC_TOKEN_VALIDITY`` environment variables
2. ``.env`` file in the current working directory
3. ``~/.eumdac/credentials`` (comma-separated: ``key,secret``)

Note: token validity can only be configured via env var or ``.env`` file, not
via the credentials file (which only stores the key/secret pair).

Typical usage::

    from eumdac_fetch.auth import create_token, get_token

    token = get_token()   # uses ENV values by default
"""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_VALIDITY: int = 86400  # 24 hours in seconds


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_dotenv(path: Path) -> dict[str, str]:
    """Parse a .env file into a ``{key: value}`` dict.

    Handles ``KEY=value``, double/single-quoted values, comment lines, and
    blank lines.  Inline comments are *not* stripped (not standard dotenv).
    """
    result: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        if key:
            result[key] = value
    return result


def _parse_validity(raw: str, *, source: str) -> int | None:
    """Parse a validity string as a positive integer of seconds.

    Returns ``None`` and logs a warning if the value is invalid.
    """
    try:
        v = int(raw)
        if v > 0:
            return v
        logger.warning("EUMDAC_TOKEN_VALIDITY from %s must be positive, got %d; ignoring.", source, v)
    except ValueError:
        logger.warning("EUMDAC_TOKEN_VALIDITY from %s is not an integer (%r); ignoring.", source, raw)
    return None


def _load_credentials() -> tuple[str | None, str | None, int]:
    """Discover EUMDAC credentials and token validity through a priority chain.

    Credentials (key/secret) are read from env vars → ``.env`` →
    ``~/.eumdac/credentials``.  Token validity is only read from env vars or
    ``.env``; the credentials file always contains only ``key,secret``.

    Returns
    -------
    (key, secret, validity)
        ``key`` and ``secret`` may be ``None`` if a complete pair could not be
        found.  ``validity`` always has a value — it defaults to
        :data:`DEFAULT_VALIDITY` (86 400 s) if not configured.
    """
    validity: int = DEFAULT_VALIDITY

    # 1. Environment variables
    key: str | None = os.environ.get("EUMDAC_KEY") or None
    secret: str | None = os.environ.get("EUMDAC_SECRET") or None
    raw_validity = os.environ.get("EUMDAC_TOKEN_VALIDITY")
    if raw_validity:
        parsed = _parse_validity(raw_validity, source="environment variable")
        if parsed is not None:
            validity = parsed
    if key and secret:
        return key, secret, validity

    # 2. .env file in the current working directory
    dotenv_path = Path(".env")
    if dotenv_path.exists():
        try:
            env_vars = _parse_dotenv(dotenv_path)
            key = key or env_vars.get("EUMDAC_KEY") or None
            secret = secret or env_vars.get("EUMDAC_SECRET") or None
            raw_validity = env_vars.get("EUMDAC_TOKEN_VALIDITY")
            if raw_validity:
                parsed = _parse_validity(raw_validity, source=".env file")
                if parsed is not None:
                    validity = parsed
            if key and secret:
                return key, secret, validity
        except (OSError, ValueError):
            logger.debug("Failed to parse .env file", exc_info=True)

    # 3. ~/.eumdac/credentials  (format: "key,secret")
    cred_file = Path.home() / ".eumdac" / "credentials"
    if cred_file.exists():
        try:
            parts = [p.strip() for p in cred_file.read_text().strip().split(",")]
            key = key or (parts[0] if len(parts) >= 1 else None) or None
            secret = secret or (parts[1] if len(parts) >= 2 else None) or None
        except (OSError, ValueError):
            logger.debug("Failed to parse ~/.eumdac/credentials", exc_info=True)

    return key, secret, validity


# ---------------------------------------------------------------------------
# Credential singleton
# ---------------------------------------------------------------------------

# Suppress duplicate warnings: the credential warning should fire at most once
# per Python session regardless of how many _Env() instances are created.
_credentials_warning_emitted: bool = False


class _Env:
    """Holds the bootstrapped EUMDAC credentials discovered at import time."""

    def __init__(self) -> None:
        global _credentials_warning_emitted  # noqa: PLW0603
        key, secret, validity = _load_credentials()
        if (not key or not secret) and not _credentials_warning_emitted:
            warnings.warn(
                "EUMDAC credentials not found. "
                "Set EUMDAC_KEY/EUMDAC_SECRET environment variables, "
                "provide a .env file, or create ~/.eumdac/credentials.",
                UserWarning,
                stacklevel=2,
            )
            _credentials_warning_emitted = True
        self.key: str | None = key
        self.secret: str | None = secret
        self.validity: int = validity


#: Module-level credential singleton — bootstrapped once at import time.
ENV = _Env()
