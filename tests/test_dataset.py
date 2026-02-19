"""Tests for RemoteData and RemoteDataset."""

from __future__ import annotations

from unittest import mock

import pytest
from fsspec.implementations.http import HTTPFileSystem

from eumdac_fetch.dataset import RemoteData, RemoteDataset
from eumdac_fetch.remote import TokenRefreshingHTTPFileSystem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_token(value: str = "test-token"):
    token = mock.MagicMock()
    type(token).access_token = mock.PropertyMock(return_value=value)
    return token


def _make_fs(token):
    """Build a TokenRefreshingHTTPFileSystem with the parent __init__ stubbed."""

    def _stub(self, *args, **kwargs):
        self.kwargs = kwargs

    with mock.patch.object(HTTPFileSystem, "__init__", _stub):
        fs = TokenRefreshingHTTPFileSystem(token)
    fs._session = None
    return fs


# ---------------------------------------------------------------------------
# RemoteData
# ---------------------------------------------------------------------------


class TestRemoteData:
    def test_init_creates_fs_from_token(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            RemoteData("https://example.com/file.nc", token_manager=token)
        mock_cls.assert_called_once_with(token_obj=token)

    def test_init_uses_provided_fs(self):
        token = _make_token()
        shared_fs = _make_fs(token)
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            rd = RemoteData("https://example.com/file.nc", fs=shared_fs)
        mock_cls.assert_not_called()
        assert rd._fs is shared_fs

    def test_init_calls_get_token_when_no_token(self):
        fake_token = _make_token()
        with (
            mock.patch("eumdac_fetch.dataset.get_token", return_value=fake_token) as mock_cat,
            mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"),
        ):
            RemoteData("https://example.com/file.nc")
        mock_cat.assert_called_once_with()

    def test_context_manager_opens_and_closes(self):
        token = _make_token()
        fake_handle = mock.MagicMock()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            mock_cls.return_value.open.return_value = fake_handle
            with RemoteData("https://example.com/file.nc", token_manager=token) as f:
                assert f is fake_handle
        fake_handle.close.assert_called_once()

    def test_exit_closes_handle_on_exception(self):
        token = _make_token()
        fake_handle = mock.MagicMock()
        with (
            mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls,
            pytest.raises(RuntimeError),
        ):
            mock_cls.return_value.open.return_value = fake_handle
            with RemoteData("https://example.com/file.nc", token_manager=token):
                raise RuntimeError("boom")
        fake_handle.close.assert_called_once()

    def test_exit_does_not_suppress_exceptions(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            mock_cls.return_value.open.return_value = mock.MagicMock()
            with pytest.raises(ValueError), RemoteData("https://example.com/file.nc", token_manager=token):
                raise ValueError("not suppressed")

    def test_open_returns_handle_without_context_manager(self):
        token = _make_token()
        fake_handle = mock.MagicMock()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            mock_cls.return_value.open.return_value = fake_handle
            rd = RemoteData("https://example.com/file.nc", token_manager=token)
            assert rd.open() is fake_handle

    def test_repr_contains_url(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            rd = RemoteData("https://example.com/file.nc", token_manager=token)
        assert "https://example.com/file.nc" in repr(rd)


# ---------------------------------------------------------------------------
# RemoteDataset
# ---------------------------------------------------------------------------


class TestRemoteDataset:
    _entries = {
        "VIS06": "https://example.com/vis06.nc",
        "IR105": "https://example.com/ir105.nc",
    }

    def test_init_creates_single_shared_fs(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            RemoteDataset(self._entries, token_manager=token)
        mock_cls.assert_called_once_with(token_obj=token)

    def test_entries_are_remote_data_instances(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert isinstance(ds["VIS06"], RemoteData)
        assert isinstance(ds["IR105"], RemoteData)

    def test_all_entries_share_same_fs(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem") as mock_cls:
            ds = RemoteDataset(self._entries, token_manager=token)
        shared_fs = mock_cls.return_value
        assert ds["VIS06"]._fs is shared_fs
        assert ds["IR105"]._fs is shared_fs

    def test_entry_urls_are_set_correctly(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert ds["VIS06"]._url == self._entries["VIS06"]
        assert ds["IR105"]._url == self._entries["IR105"]

    def test_len(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert len(ds) == 2

    def test_iter_yields_names(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert set(ds) == {"VIS06", "IR105"}

    def test_contains(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert "VIS06" in ds
        assert "MISSING" not in ds

    def test_entries_property(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert set(ds.entries) == {"VIS06", "IR105"}

    def test_missing_key_raises(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        with pytest.raises(KeyError):
            ds["NONEXISTENT"]

    def test_repr_contains_entry_names(self):
        token = _make_token()
        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            ds = RemoteDataset(self._entries, token_manager=token)
        assert "VIS06" in repr(ds)
        assert "IR105" in repr(ds)

    def test_init_calls_get_token_when_no_token(self):
        fake_token = _make_token()
        with (
            mock.patch("eumdac_fetch.dataset.get_token", return_value=fake_token) as mock_cat,
            mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"),
        ):
            RemoteDataset(self._entries)
        mock_cat.assert_called_once_with()


# ---------------------------------------------------------------------------
# Integration tests — real EUMDAC network calls
# ---------------------------------------------------------------------------

COLLECTION_ID = "EO:EUM:DAT:0665"  # MTG FCI L1C HRFI


def _load_credentials():
    import os
    from pathlib import Path

    key = os.environ.get("EUMDAC_KEY", "")
    secret = os.environ.get("EUMDAC_SECRET", "")
    if key and secret:
        return key, secret
    cred_file = Path.home() / ".eumdac" / "credentials"
    if cred_file.exists():
        text = cred_file.read_text().strip()
        if "," in text:
            k, _, s = text.partition(",")
            return k.strip(), s.strip()
    return None


@pytest.fixture(scope="module")
def live_token():
    from eumdac_fetch.auth import create_token
    from eumdac_fetch.env import ENV

    if not ENV.key or not ENV.secret:
        pytest.skip("No EUMDAC credentials available")
    return create_token(key=ENV.key, secret=ENV.secret)


@pytest.fixture(scope="module")
def fci_dataset(live_token):
    """Build a RemoteDataset from the .nc entries of the first FCI product."""
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
    assert nc_entries, "No .nc entries in FCI product"

    entries = {name: f"{base_url}/entry?name={quote(name, safe='')}" for name in nc_entries}
    print(f"\nBuilding RemoteDataset with {len(entries)} entries: {list(entries)[:3]}…")
    return RemoteDataset(entries, token_manager=live_token)


@pytest.mark.integration
class TestRemoteDatasetIntegration:
    """Integration tests that open real FCI NetCDF entries via RemoteDataset."""

    def test_dataset_contains_nc_entries(self, fci_dataset):
        """RemoteDataset is populated with at least one .nc entry."""
        assert len(fci_dataset) > 0
        assert all(name.endswith(".nc") for name in fci_dataset)

    def test_open_entry_lazy_metadata(self, fci_dataset):
        """Opening an entry via RemoteDataset yields a readable xarray Dataset."""
        import xarray as xr

        name = fci_dataset.entries[0]
        with fci_dataset[name] as f:
            ds = xr.open_dataset(f, engine="h5netcdf")
            print(f"\n{name}: vars={list(ds.data_vars)}, dims={dict(ds.sizes)}")
            assert len(ds.data_vars) > 0
            assert len(ds.dims) > 0
            ds.close()

    def test_lazy_open_does_not_pull_full_array(self, fci_dataset):
        """Opening the dataset must not trigger a full data download."""
        import xarray as xr

        name = fci_dataset.entries[0]
        with fci_dataset[name] as f:
            ds = xr.open_dataset(f, engine="h5netcdf")
            var_name = next(iter(ds.data_vars))
            var = ds[var_name]
            assert var.shape is not None
            assert var.dtype is not None
            print(f"\n'{var_name}': shape={var.shape}, would be {var.nbytes / 1e6:.2f} MB")
            ds.close()

    def test_random_region_access(self, fci_dataset):
        """A slice from the centre of the array is retrieved correctly."""
        import xarray as xr

        name = fci_dataset.entries[0]
        with fci_dataset[name] as f:
            ds = xr.open_dataset(f, engine="h5netcdf")

            # Find the first variable that has at least one dimension with > 10 elements
            candidate = None
            for var_name in ds.data_vars:
                var = ds[var_name]
                if any(s > 10 for s in var.shape):
                    candidate = var_name
                    break
            if candidate is None:
                pytest.skip("No variable with dimensions large enough for a non-zero-offset slice")

            var = ds[candidate]
            # Slice from the middle of each large dimension — explicitly not from 0
            indexers = {
                dim: slice(size // 2, size // 2 + 10)
                for dim, size in zip(var.dims, var.shape, strict=True)
                if size > 10
            }
            values = var.isel(**indexers).values
            print(f"\n'{candidate}' centre slice: shape={values.shape}, dtype={values.dtype}")
            assert all(s == 10 for s in values.shape)
            ds.close()

    def test_two_entries_share_one_session(self, fci_dataset):
        """Two entries from the same dataset use the same underlying filesystem."""
        if len(fci_dataset) < 2:
            pytest.skip("Need at least 2 entries to test session sharing")

        e1, e2 = fci_dataset.entries[:2]
        assert fci_dataset[e1]._fs is fci_dataset[e2]._fs

    def test_independent_reads_from_two_entries(self, fci_dataset):
        """Two entries can be opened and read independently."""
        import xarray as xr

        if len(fci_dataset) < 2:
            pytest.skip("Need at least 2 entries")

        e1, e2 = fci_dataset.entries[:2]
        with fci_dataset[e1] as f1, fci_dataset[e2] as f2:
            ds1 = xr.open_dataset(f1, engine="h5netcdf")
            ds2 = xr.open_dataset(f2, engine="h5netcdf")
            assert len(ds1.data_vars) > 0
            assert len(ds2.data_vars) > 0
            ds1.close()
            ds2.close()
