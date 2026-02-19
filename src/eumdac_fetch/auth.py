"""Authentication with EUMETSAT Data Access Client.

This module is the single source of truth for EUMDAC token creation.
Credentials and token validity are always read from the :data:`ENV` singleton,
which discovers them in priority order:

  1. ``EUMDAC_KEY`` / ``EUMDAC_SECRET`` / ``EUMDAC_TOKEN_VALIDITY`` env vars
  2. ``.env`` file in the current working directory
  3. ``~/.eumdac/credentials``

If you need a token with credentials that differ from ENV (e.g. in tests or
multi-tenant use), construct one directly::

    import eumdac
    token = eumdac.AccessToken(credentials=(key, secret), validity=3600)

Or, without importing eumdac explicitly::

    from eumdac_fetch import AccessToken
    token = AccessToken(credentials=(key, secret), validity=3600)

Typical usage::

    from eumdac_fetch.auth import get_token

    token = get_token()   # shared process-level token, created from ENV on first call
"""

from __future__ import annotations

import logging

import eumdac

from eumdac_fetch.env import ENV

logger = logging.getLogger("eumdac_fetch")

# ---------------------------------------------------------------------------
# Process-level singleton
# ---------------------------------------------------------------------------

_token: eumdac.AccessToken | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_token() -> eumdac.AccessToken:
    """Create a **fresh** EUMDAC ``AccessToken`` from :data:`ENV` credentials.

    Credentials and validity come exclusively from
    :data:`~eumdac_fetch.env.ENV` — set ``EUMDAC_KEY``, ``EUMDAC_SECRET``,
    and optionally ``EUMDAC_TOKEN_VALIDITY`` in the environment (or a
    ``.env`` file / ``~/.eumdac/credentials``) before importing this module.

    For a token with custom credentials call ``eumdac.AccessToken`` (or the
    re-exported :class:`~eumdac_fetch.AccessToken`) directly.

    Returns
    -------
    eumdac.AccessToken
        A live, self-renewing token handle.

    Raises
    ------
    ValueError
        If :attr:`ENV.key` or :attr:`ENV.secret` are not set.
    eumdac.AccessTokenError
        If the EUMDAC OAuth2 endpoint rejects the credentials.
    """
    if not ENV.key or not ENV.secret:
        raise ValueError(
            "EUMDAC credentials not found. "
            "Set EUMDAC_KEY and EUMDAC_SECRET environment variables, "
            "provide a .env file, or create ~/.eumdac/credentials."
        )
    token = eumdac.AccessToken(
        credentials=(ENV.key, ENV.secret),
        validity=ENV.validity,
    )
    logger.info("Authenticated with EUMDAC (token expires: %s)", token.expiration)
    return token


def get_token() -> eumdac.AccessToken:
    """Return the process-level shared ``AccessToken``, creating it on first call.

    Once created the same ``AccessToken`` is returned for the lifetime of
    the process.  ``eumdac.AccessToken`` manages its own OAuth2 renewal
    internally, so a single instance is correct for long-running sessions.

    Returns
    -------
    eumdac.AccessToken
    """
    global _token  # noqa: PLW0603
    if _token is None:
        _token = create_token()
    return _token
