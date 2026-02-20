"""High-level context-manager wrappers for authenticated remote EUMDAC files.

Classes
-------
RemoteData
    Context manager for a **single** authenticated remote URL.  Handles
    filesystem setup and file-handle lifecycle automatically.

RemoteDataset
    A collection of :class:`RemoteData` entries that share one authenticated
    :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem`, so Bearer
    token refreshes are coordinated across all concurrent reads.  Typically
    built from all ``.nc`` entries of a single EUMDAC product.

Typical usage::

    from eumdac_fetch.dataset import RemoteData, RemoteDataset
    import xarray as xr

    # Single file
    with RemoteData("https://…/entry?name=…") as f:
        ds = xr.open_dataset(f, engine="h5netcdf")

    # Whole product — entries share one authenticated session
    dataset = RemoteDataset({"VIS06": "https://…", "IR105": "https://…"})
    with dataset["VIS06"] as f:
        ds = xr.open_dataset(f, engine="h5netcdf")
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterator

from eumdac_fetch.auth import get_token
from eumdac_fetch.remote import TokenRefreshingHTTPFileSystem

# ---------------------------------------------------------------------------
# RemoteData — single-URL context manager
# ---------------------------------------------------------------------------


class RemoteData:
    """Context manager that opens a single authenticated remote file.

    On entering the context a file-like object backed by
    :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem` is returned.
    The handle is closed automatically on exit.

    Parameters
    ----------
    url:
        Fully-qualified HTTPS URL of the remote resource.  Percent-encoded
        characters (``%3A``, ``%2B``, …) are preserved as-is.
    token_manager:
        Any object with a synchronous ``.access_token`` property.  Defaults
        to :func:`~eumdac_fetch.auth.get_token` called with the
        bootstrapped credentials from :data:`~eumdac_fetch.env.ENV`.
        Ignored when ``fs`` is supplied.
    fs:
        A pre-built :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem`
        to reuse.  When provided ``token_manager`` and ``**kwargs`` are
        ignored.  :class:`RemoteDataset` uses this to share one filesystem
        across all its entries.
    **kwargs:
        Forwarded verbatim to
        :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem` when
        creating a new filesystem (i.e. when ``fs`` is not supplied).

    Examples
    --------
    ::

        with RemoteData(url) as f:
            ds = xr.open_dataset(f, engine="h5netcdf")
    """

    def __init__(
        self,
        url: str,
        token_manager=None,
        *,
        fs: TokenRefreshingHTTPFileSystem | None = None,
        **kwargs,
    ) -> None:
        if fs is not None:
            self._fs = fs
        else:
            if token_manager is None:
                token_manager = get_token()
            self._fs = TokenRefreshingHTTPFileSystem(token_obj=token_manager, **kwargs)
        self._url = url
        self._handle = None

    def open(self):
        """Open and return the file handle without the context-manager protocol."""
        return self._fs.open(self._url)

    def __enter__(self):
        self._handle = self.open()
        return self._handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._handle is not None:
            self._handle.close()
            self._handle = None
        return False

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._url!r})"


# ---------------------------------------------------------------------------
# RemoteDataset — a product's worth of entries
# ---------------------------------------------------------------------------


class RemoteDataset:
    """A collection of :class:`RemoteData` entries sharing one authenticated session.

    All entries use a single
    :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem`, so Bearer
    token refreshes are coordinated across all concurrent reads.

    Parameters
    ----------
    entries:
        Mapping of ``{entry_name: url}`` pairs.  URLs must already be
        percent-encoded (use :func:`urllib.parse.quote` if needed).
    token_manager:
        Any object with a synchronous ``.access_token`` property.  Defaults
        to :func:`~eumdac_fetch.auth.get_token` called with the
        bootstrapped credentials from :data:`~eumdac_fetch.env.ENV`.
    **kwargs:
        Forwarded verbatim to
        :class:`~eumdac_fetch.remote.TokenRefreshingHTTPFileSystem`.

    Examples
    --------
    ::

        dataset = RemoteDataset({"VIS06": url_vis, "IR105": url_ir})
        with dataset["VIS06"] as f:
            ds = xr.open_dataset(f, engine="h5netcdf")

        for name in dataset:
            print(name, dataset[name])
    """

    def __init__(
        self,
        entries: dict[str, str],
        token_manager=None,
        **kwargs,
    ) -> None:
        if token_manager is None:
            token_manager = get_token()
        shared_fs = TokenRefreshingHTTPFileSystem(token_obj=token_manager, **kwargs)
        self._entries: dict[str, RemoteData] = {name: RemoteData(url, fs=shared_fs) for name, url in entries.items()}

    # ------------------------------------------------------------------
    # Mapping-like interface
    # ------------------------------------------------------------------

    def __getitem__(self, name: str) -> RemoteData:
        return self._entries[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, name: object) -> bool:
        return name in self._entries

    @property
    def entries(self) -> list[str]:
        """Names of all available entries."""
        return list(self._entries.keys())

    def __repr__(self) -> str:
        return f"RemoteDataset({self.entries!r})"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_remote_dataset(product, token, entry_patterns: list[str] | None = None) -> RemoteDataset:
    """Build a RemoteDataset from a eumdac product object.

    Extracts per-entry URLs from product.links.
    If entry_patterns is given, only matching entry names are included.
    """
    entries: dict[str, str] = {}
    for link in product.links:
        name = link.title
        url = link.href
        if entry_patterns is None or any(fnmatch.fnmatch(name, p) for p in entry_patterns):
            entries[name] = url
    return RemoteDataset(entries, token_manager=token)
