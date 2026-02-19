# noinspection SpellCheckingInspection
"""HTTP filesystem with automatic Bearer-token refresh on 401 responses.

``TokenRefreshingHTTPFileSystem`` is a drop-in fsspec filesystem that
transparently refreshes OAuth2 Bearer tokens on HTTP 401 responses, making
lazy remote access with xarray / h5netcdf safe across long-running sessions.

Typical usage::

    from eumdac_fetch.auth import get_token
    from eumdac_fetch.remote import TokenRefreshingHTTPFileSystem

    token = get_token()
    fs = TokenRefreshingHTTPFileSystem(token)

    f = fs.open(entry_url)          # Bearer-auth, no query-param token
    ds = xr.open_dataset(f, engine="h5netcdf")  # lazy — only metadata loaded
    chunk = ds["vis_06"].isel(x=slice(0, 64), y=slice(0, 64)).values  # range GET
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp
from fsspec.implementations.http import HTTPFileSystem

logger = logging.getLogger(__name__)


class TokenRefreshingHTTPFileSystem(HTTPFileSystem):
    """An HTTPFileSystem that transparently refreshes Bearer tokens on HTTP 401.

    Design notes
    ------------
    *   Token lifecycle is fully delegated to ``token_obj.access_token``.
        Any object that exposes a synchronous ``.access_token`` property
        returning the current Bearer token string is accepted — no coupling
        to a specific OAuth2 library.

    *   On a 401 response the aiohttp session is **closed and set to None**
        rather than mutated in-place.  fsspec rebuilds the session from
        ``self.kwargs`` on the next request via ``await self.set_session()``.
        This avoids the read-only ``CIMultiDictProxy`` on aiohttp sessions and
        ensures no stale connection state lingers.

    *   ``self.kwargs["headers"]`` is the single source of truth for the
        current auth value.  The race-condition check compares against it so
        that multiple coroutines queued behind ``_refresh_lock`` only perform
        one real refresh.

    *   ``token_obj.access_token`` may block (e.g. an internal retry loop
        calling ``time.sleep``).  It is always bridged to a thread via
        ``asyncio.to_thread`` to avoid stalling the event loop.

    *   ``encoded=True`` is set by default so that fsspec does not re-encode
        URLs that already contain percent-encoded characters (``%3A``,
        ``%2B``, …).  Double-encoding produces URLs the server cannot resolve,
        resulting in spurious 404 responses.

    Parameters
    ----------
    token_obj:
        Any object with a synchronous ``.access_token`` property that returns
        the current Bearer token as a ``str``.  Must remain alive for the
        duration of the filesystem's use.
    *args, **kwargs:
        Forwarded verbatim to :class:`fsspec.implementations.http.HTTPFileSystem`.
    """

    def __init__(self, token_obj, *args, **kwargs) -> None:
        self.token_obj = token_obj
        self._refresh_lock = asyncio.Lock()

        # Pre-populate so the very first request is already authenticated.
        # token_obj.access_token is a synchronous property — safe at __init__ time.
        kwargs.setdefault("headers", {})["Authorization"] = (
            f"Bearer {token_obj.access_token}"
        )

        # URLs served by many REST APIs already contain percent-encoded characters.
        # Without encoded=True fsspec re-encodes them (e.g. %3A → %253A),
        # producing URLs the server cannot recognize (→ 404).
        kwargs.setdefault("encoded", True)

        super().__init__(*args, **kwargs)

    # ------------------------------------------------------------------
    # Token refresh internals
    # ------------------------------------------------------------------

    async def _refresh_token_task(self) -> str:
        """Bridge a potentially blocking token renewal into the async event loop."""
        return await asyncio.to_thread(lambda: self.token_obj.access_token)

    async def _update_auth(self) -> None:
        """Refresh the token and invalidate the current aiohttp session.

        Idempotent under concurrent callers: only the first coroutine to
        acquire the lock performs real work; subsequent callers see the updated
        ``self.kwargs`` and return immediately.
        """
        async with self._refresh_lock:
            new_token = await self._refresh_token_task()
            new_auth = f"Bearer {new_token}"

            # All coroutines that queued behind the lock reach this check.
            # If the first one already refreshed successfully, the rest skip.
            if self.kwargs.get("headers", {}).get("Authorization") == new_auth:
                return

            # 1. Persist the new token — self.kwargs is the source of truth.
            self.kwargs.setdefault("headers", {})["Authorization"] = new_auth

            # 2. Invalidate the session.  On the next request fsspec calls
            #    await self.set_session(), rebuilding a fresh ClientSession
            #    from the updated self.kwargs.  This avoids mutating aiohttp's
            #    read-only CIMultiDictProxy entirely.
            if self._session is not None:
                await self._session.close()
                self._session = None

            logger.debug("Bearer token refreshed; session invalidated for recreation.")

    async def _run_with_refresh(self, coro_func, *args, **kwargs):
        """Execute a coroutine; retry exactly once after a token refresh on 401.

        Any non-401 HTTP error or non-HTTP exception propagates immediately.
        """
        try:
            return await coro_func(*args, **kwargs)
        except aiohttp.ClientResponseError as exc:
            if exc.status != 401:
                raise
            logger.info("HTTP 401 received; refreshing Bearer token and retrying.")
            await self._update_auth()
            # Sync the new auth value into any per-call headers the caller
            # passed explicitly so the retry uses a consistent header set.
            if "headers" in kwargs:
                kwargs["headers"]["Authorization"] = (
                    self.kwargs["headers"]["Authorization"]
                )
            return await coro_func(*args, **kwargs)

    # ------------------------------------------------------------------
    # Protected async entry points
    #
    # _cat_file  — byte-range reads: the hot path for lazy xarray/h5netcdf.
    # _info      — single-object stat; called internally by _open.
    # _ls_real   — directory listing.
    # _exists    — existence check.
    #
    # _open is intentionally not overridden: the parent HTTPFileSystem
    # implementation (with encoded=True set in __init__) handles URL encoding
    # correctly and supports lazy byte-range access via _cat_file.
    #
    # Write paths (_get_file, _put_file, _pipe_file) are intentionally not
    # wrapped: this filesystem is designed for read-only remote access.
    # ------------------------------------------------------------------

    async def _cat_file(self, url, start=None, end=None, **kwargs):
        return await self._run_with_refresh(
            super()._cat_file, url, start, end, **kwargs
        )

    async def _info(self, url, **kwargs):
        return await self._run_with_refresh(super()._info, url, **kwargs)

    async def _ls_real(self, url, detail=True, **kwargs):
        return await self._run_with_refresh(super()._ls_real, url, detail, **kwargs)

    async def _exists(self, path, **kwargs):
        return await self._run_with_refresh(super()._exists, path, **kwargs)
