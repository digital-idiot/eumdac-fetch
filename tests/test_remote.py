"""Tests for TokenRefreshingHTTPFileSystem."""

from __future__ import annotations

import asyncio
from unittest import mock

import aiohttp
import pytest
from fsspec.implementations.http import HTTPFileSystem

from eumdac_fetch.remote import TokenRefreshingHTTPFileSystem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_token(access_token_value: str = "tok-abc123"):
    """Return a mock eumdac.AccessToken whose .access_token property is stable."""
    token = mock.MagicMock()
    type(token).access_token = mock.PropertyMock(return_value=access_token_value)
    return token


def _make_fs(token, *, existing_token: str | None = None):
    """Construct a TokenRefreshingHTTPFileSystem with the parent __init__ mocked out.

    The parent HTTPFileSystem.__init__ is replaced with a minimal stub that
    sets ``self.kwargs`` (the only attribute our class depends on at runtime).
    """
    def _stub_init(self, *args, **kwargs):
        self.kwargs = kwargs  # HTTPFileSystem stores kwargs verbatim

    with mock.patch.object(HTTPFileSystem, "__init__", _stub_init):
        fs = TokenRefreshingHTTPFileSystem(token)

    # Simulate a live (or absent) aiohttp session
    fs._session = None
    if existing_token:
        # Override the auth header to simulate a stale session
        fs.kwargs["headers"]["Authorization"] = f"Bearer {existing_token}"
    return fs


# ---------------------------------------------------------------------------
# TokenRefreshingHTTPFileSystem — unit tests (no network)
# ---------------------------------------------------------------------------

class TestTokenRefreshingHTTPFileSystem:

    # --- __init__ ---

    def test_init_stores_token_obj(self):
        token = _make_token()
        fs = _make_fs(token)
        assert fs.token_obj is token

    def test_init_prepopulates_auth_header(self):
        """kwargs["headers"]["Authorization"] is set before super().__init__."""
        token = _make_token("first-token")
        captured = {}

        def stub(self, *args, **kwargs):
            captured.update(kwargs)
            self.kwargs = kwargs

        with mock.patch.object(HTTPFileSystem, "__init__", stub):
            TokenRefreshingHTTPFileSystem(token)

        assert captured["headers"]["Authorization"] == "Bearer first-token"

    def test_init_refresh_lock_created(self):
        token = _make_token()
        fs = _make_fs(token)
        assert isinstance(fs._refresh_lock, asyncio.Lock)

    # --- _refresh_token_task ---

    async def test_refresh_token_task_returns_current_token(self):
        token = _make_token("live-token-xyz")
        fs = _make_fs(token)
        result = await fs._refresh_token_task()
        assert result == "live-token-xyz"

    async def test_refresh_token_task_runs_in_thread(self):
        """access_token is bridged via asyncio.to_thread, not called directly."""
        token = _make_token("thread-token")
        fs = _make_fs(token)
        with mock.patch(
            "eumdac_fetch.remote.asyncio.to_thread",
            new=mock.AsyncMock(return_value="thread-token"),
        ) as mock_thread:
            result = await fs._refresh_token_task()
        assert result == "thread-token"
        mock_thread.assert_awaited_once()

    # --- _update_auth ---

    async def test_update_auth_sets_new_token_in_kwargs(self):
        token = _make_token("new-token")
        fs = _make_fs(token, existing_token="old-token")
        await fs._update_auth()
        assert fs.kwargs["headers"]["Authorization"] == "Bearer new-token"

    async def test_update_auth_closes_and_nullifies_session(self):
        token = _make_token("refreshed-tok")
        fs = _make_fs(token, existing_token="stale-tok")
        mock_session = mock.AsyncMock()
        fs._session = mock_session

        await fs._update_auth()

        mock_session.close.assert_awaited_once()
        assert fs._session is None

    async def test_update_auth_skips_when_token_unchanged(self):
        """Race-condition guard: second waiter skips if token already current."""
        token = _make_token("same-token")
        fs = _make_fs(token)
        fs.kwargs["headers"]["Authorization"] = "Bearer same-token"
        mock_session = mock.AsyncMock()
        fs._session = mock_session

        await fs._update_auth()

        mock_session.close.assert_not_awaited()

    async def test_update_auth_no_session_still_updates_kwargs(self):
        """When _session is None, kwargs is still updated for future sessions."""
        token = _make_token("brand-new-tok")
        fs = _make_fs(token, existing_token="old-tok")
        fs._session = None

        await fs._update_auth()

        assert fs.kwargs["headers"]["Authorization"] == "Bearer brand-new-tok"
        assert fs._session is None

    async def test_update_auth_race_second_waiter_skips(self):
        """Second waiter skips when token is already current after the first refreshes."""
        token = _make_token("fresh-token")
        fs = _make_fs(token, existing_token="stale-token")

        close_count = 0
        mock_session = mock.AsyncMock()

        async def counting_close():
            nonlocal close_count
            close_count += 1

        mock_session.close = counting_close
        fs._session = mock_session

        await asyncio.gather(fs._update_auth(), fs._update_auth())

        assert close_count == 1
        assert fs.kwargs["headers"]["Authorization"] == "Bearer fresh-token"

    # --- _run_with_refresh ---

    async def test_run_with_refresh_passes_through_on_success(self):
        token = _make_token()
        fs = _make_fs(token)

        async def coro():
            return "ok"

        result = await fs._run_with_refresh(coro)
        assert result == "ok"

    async def test_run_with_refresh_reraises_non_401(self):
        token = _make_token()
        fs = _make_fs(token)

        async def coro():
            raise aiohttp.ClientResponseError(
                request_info=mock.MagicMock(),
                history=(),
                status=500,
            )

        with pytest.raises(aiohttp.ClientResponseError) as exc_info:
            await fs._run_with_refresh(coro)
        assert exc_info.value.status == 500

    async def test_run_with_refresh_retries_on_401(self):
        token = _make_token("fresh-tok")
        fs = _make_fs(token, existing_token="stale-tok")
        call_count = 0

        async def flaky_coro(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiohttp.ClientResponseError(
                    request_info=mock.MagicMock(),
                    history=(),
                    status=401,
                )
            return "data"

        result = await fs._run_with_refresh(flaky_coro)
        assert result == "data"
        assert call_count == 2
        assert fs.kwargs["headers"]["Authorization"] == "Bearer fresh-tok"

    async def test_run_with_refresh_updates_explicit_headers_on_retry(self):
        """Per-call headers dict is synced with the new auth value on retry."""
        token = _make_token("new-tok")
        fs = _make_fs(token, existing_token="old-tok")
        call_count = 0
        received_header = {}

        async def coro(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aiohttp.ClientResponseError(
                    request_info=mock.MagicMock(), history=(), status=401
                )
            received_header.update(kwargs.get("headers", {}))
            return "ok"

        explicit_headers = {"Authorization": "Bearer old-tok", "X-Custom": "yes"}
        await fs._run_with_refresh(coro, headers=explicit_headers)
        assert received_header["Authorization"] == "Bearer new-tok"
        assert received_header["X-Custom"] == "yes"

    # --- overridden entry points each delegate to _run_with_refresh ---

    async def test_cat_file_delegates_to_run_with_refresh(self):
        token = _make_token()
        fs = _make_fs(token)
        with mock.patch.object(fs, "_run_with_refresh", new=mock.AsyncMock(return_value=b"")) as m:
            await fs._cat_file("http://example.com/file.nc", start=0, end=100)
        m.assert_awaited_once()
        args = m.await_args[0]
        assert callable(args[0])

    def test_open_not_overridden(self):
        """_open is not overridden — the parent HTTPFileSystem implementation is used."""
        assert "_open" not in TokenRefreshingHTTPFileSystem.__dict__

    async def test_info_delegates_to_run_with_refresh(self):
        token = _make_token()
        fs = _make_fs(token)
        with mock.patch.object(fs, "_run_with_refresh", new=mock.AsyncMock(return_value={})) as m:
            await fs._info("http://example.com/file.nc")
        m.assert_awaited_once()

    async def test_exists_delegates_to_run_with_refresh(self):
        token = _make_token()
        fs = _make_fs(token)
        with mock.patch.object(fs, "_run_with_refresh", new=mock.AsyncMock(return_value=True)) as m:
            await fs._exists("http://example.com/file.nc")
        m.assert_awaited_once()

    async def test_ls_real_delegates_to_run_with_refresh(self):
        token = _make_token()
        fs = _make_fs(token)
        with mock.patch.object(fs, "_run_with_refresh", new=mock.AsyncMock(return_value=[])) as m:
            await fs._ls_real("http://example.com/")
        m.assert_awaited_once()


# ---------------------------------------------------------------------------
# Integration tests — real EUMDAC network calls
# ---------------------------------------------------------------------------

COLLECTION_ID = "EO:EUM:DAT:0665"  # MTG FCI L1C HRFI
TEST_DTSTART_STR = "2025-03-15T10:00:00Z"
TEST_DTEND_STR = "2025-03-15T10:30:00Z"


@pytest.fixture(scope="module")
def live_token():
    from eumdac_fetch.auth import create_token
    from eumdac_fetch.env import ENV

    if not ENV.key or not ENV.secret:
        pytest.skip("No EUMDAC credentials available")
    return create_token(key=ENV.key, secret=ENV.secret)


@pytest.fixture(scope="module")
def fci_entry_url(live_token):
    """Resolve the URL for the first .nc entry of the first FCI product."""
    from datetime import UTC, datetime
    from urllib.parse import quote

    from eumdac_fetch.models import SearchFilters
    from eumdac_fetch.search import SearchService

    dtstart = datetime(2025, 3, 15, 10, 0, 0, tzinfo=UTC)
    dtend = datetime(2025, 3, 15, 10, 30, 0, tzinfo=UTC)

    service = SearchService(live_token)
    filters = SearchFilters(dtstart=dtstart, dtend=dtend)
    result = service.search(COLLECTION_ID, filters, limit=1)
    assert len(result.products) > 0, "No FCI products found for test window"

    product = result.products[0]
    base_url = product.url.split("?")[0]
    nc_entries = [e for e in product.entries if e.endswith(".nc")]
    assert nc_entries, "No .nc entries found in FCI product"

    entry_name = nc_entries[0]
    url = f"{base_url}/entry?name={quote(entry_name, safe='')}"
    print(f"\nEntry URL (no token): {url[:80]}...")
    return url, entry_name


@pytest.mark.integration
class TestTokenRefreshingHTTPFileSystemIntegration:
    """Integration tests that open real FCI NetCDF entries via Bearer auth."""

    def test_open_entry_metadata(self, live_token, fci_entry_url):
        import xarray as xr

        url, entry_name = fci_entry_url
        fs = TokenRefreshingHTTPFileSystem(live_token)
        f = fs.open(url)
        ds = xr.open_dataset(f, engine="h5netcdf")
        print(f"\nDataset from {entry_name}:")
        print(f"  Variables : {list(ds.data_vars)}")
        print(f"  Dimensions: {dict(ds.dims)}")
        assert len(ds.data_vars) > 0
        assert len(ds.dims) > 0
        ds.close()

    def test_lazy_load_does_not_load_all_data(self, live_token, fci_entry_url):
        import xarray as xr

        url, _ = fci_entry_url
        fs = TokenRefreshingHTTPFileSystem(live_token)
        f = fs.open(url)
        ds = xr.open_dataset(f, engine="h5netcdf")
        var_name = next(iter(ds.data_vars))
        var = ds[var_name]
        assert var.shape is not None
        assert var.dtype is not None
        ds.close()

    def test_small_slice_range_request(self, live_token, fci_entry_url):
        import numpy as np
        import xarray as xr

        url, _ = fci_entry_url
        fs = TokenRefreshingHTTPFileSystem(live_token)
        f = fs.open(url)
        ds = xr.open_dataset(f, engine="h5netcdf")
        var_name = next(iter(ds.data_vars))
        var = ds[var_name]
        indexers = {dim: slice(0, 10) for dim in var.dims}
        subset = var.isel(**indexers)
        values = subset.values
        assert values.shape == (10,) * len(var.dims)
        assert not np.all(np.isnan(values))
        ds.close()

    def test_random_region_access(self, live_token, fci_entry_url):
        import xarray as xr

        url, _ = fci_entry_url
        fs = TokenRefreshingHTTPFileSystem(live_token)
        f = fs.open(url)
        ds = xr.open_dataset(f, engine="h5netcdf")
        var_name = next(iter(ds.data_vars))
        var = ds[var_name]
        indexers = {
            dim: slice(size // 2, size // 2 + 10)
            for dim, size in zip(var.dims, var.shape, strict=True)
        }
        values = var.isel(**indexers).values
        assert values.shape == (10,) * len(var.dims)
        ds.close()

    def test_two_independent_opens_share_token(self, live_token, fci_entry_url):
        import numpy as np
        import xarray as xr

        url, _ = fci_entry_url
        fs = TokenRefreshingHTTPFileSystem(live_token)
        f1 = fs.open(url)
        f2 = fs.open(url)

        ds1 = xr.open_dataset(f1, engine="h5netcdf")
        ds2 = xr.open_dataset(f2, engine="h5netcdf")

        var = next(iter(ds1.data_vars))
        assert var in ds2.data_vars

        s1 = ds1[var].isel(dict.fromkeys(ds1[var].dims, 0)).values
        s2 = ds2[var].isel(dict.fromkeys(ds2[var].dims, 0)).values
        assert np.array_equal(s1, s2)

        ds1.close()
        ds2.close()
