"""Tests for download service."""

from __future__ import annotations

import asyncio
import hashlib
import http.client
import io
import time
from contextlib import contextmanager
from unittest import mock

import pytest
import requests.exceptions
import urllib3.exceptions

from eumdac_fetch.downloader import DownloadService
from eumdac_fetch.models import ProductRecord, ProductStatus
from eumdac_fetch.state import StateDB


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(tmp_path / "test.db")
    yield db
    db.close()


@pytest.fixture
def download_dir(tmp_path):
    d = tmp_path / "downloads"
    d.mkdir()
    return d


@pytest.fixture
def service(state_db, download_dir):
    return DownloadService(
        state_db=state_db,
        download_dir=download_dir,
        parallel=2,
        resume=True,
        verify_md5=False,
    )


def make_mock_product(product_id: str = "P1", content: bytes = b"test data", md5: str = ""):
    """Create a mock product with open() returning a context manager yielding IO[bytes]."""
    product = mock.MagicMock()
    product.__str__ = mock.MagicMock(return_value=product_id)
    product.size = 10  # KB
    product.md5 = md5 or hashlib.md5(content).hexdigest()

    @contextmanager
    def fake_open(**kwargs):
        yield io.BytesIO(content)

    product.open = fake_open
    return product, content


class TestDownloadService:
    def test_download_creates_file(self, service, state_db, download_dir):
        product, content = make_mock_product("P1")

        asyncio.run(service.download_all([product], "job1", "COL1"))

        path = download_dir / "P1"
        assert path.exists()
        assert path.read_bytes() == content

    def test_download_updates_state(self, service, state_db):
        product, _ = make_mock_product("P1")

        asyncio.run(service.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        # Without MD5 verification, should be VERIFIED
        assert record.status == ProductStatus.VERIFIED

    def test_download_skips_verified(self, service, state_db, download_dir):
        product, _ = make_mock_product("P1")

        # Pre-mark as verified
        state_db.upsert(
            ProductRecord(
                product_id="P1",
                job_name="job1",
                collection="COL1",
                status=ProductStatus.VERIFIED,
            )
        )

        asyncio.run(service.download_all([product], "job1", "COL1"))
        # Should not have created a file since it was already verified
        assert not (download_dir / "P1").exists()

    def test_download_multiple(self, service, state_db, download_dir):
        products = []
        for i in range(5):
            p, _ = make_mock_product(f"P{i}", content=f"data{i}".encode())
            products.append(p)

        asyncio.run(service.download_all(products, "job1", "COL1"))

        for i in range(5):
            assert (download_dir / f"P{i}").exists()

    def test_md5_verification_pass(self, state_db, download_dir):
        content = b"hello world"
        expected_md5 = hashlib.md5(content).hexdigest()
        product, _ = make_mock_product("P1", content=content, md5=expected_md5)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=True,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED

    def test_md5_verification_fail(self, state_db, download_dir):
        product, _ = make_mock_product("P1", content=b"real data", md5="wrong_md5")

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=True,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.FAILED
        assert "MD5" in record.error_message

    def test_download_handles_non_retryable_failure(self, state_db, download_dir):
        """Non-retryable errors fail immediately without retrying."""
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10

        call_count = 0

        def raise_on_open(**kwargs):
            nonlocal call_count
            call_count += 1
            raise ValueError("Bad value")

        product.open = raise_on_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=3,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.FAILED
        assert "Bad value" in record.error_message
        # Non-retryable: should only be called once
        assert call_count == 1

    def test_download_retries_on_connection_error(self, state_db, download_dir):
        """ConnectionError triggers retry with eventual success."""
        content = b"test data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        call_count = 0

        @contextmanager
        def flaky_open(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Connection reset")
            yield io.BytesIO(content)

        product.open = flaky_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=3,
            retry_backoff=0.01,  # Fast for tests
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        assert call_count == 3  # 2 failures + 1 success

    def test_download_exhausts_retries(self, state_db, download_dir):
        """When all retries are exhausted, product is marked FAILED."""
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10

        call_count = 0

        def always_fail(**kwargs):
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Network error")

        product.open = always_fail

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=2,
            retry_backoff=0.01,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.FAILED
        assert "3 attempts" in record.error_message
        # max_retries=2 means 3 total attempts (1 initial + 2 retries)
        assert call_count == 3

    def test_download_retries_on_timeout(self, state_db, download_dir):
        """TimeoutError is also retryable."""
        content = b"data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        call_count = 0

        @contextmanager
        def timeout_then_ok(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TimeoutError("Timed out")
            yield io.BytesIO(content)

        product.open = timeout_then_ok

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=2,
            retry_backoff=0.01,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        assert call_count == 2

    def test_download_retries_on_request_exception(self, state_db, download_dir):
        """requests.exceptions.RequestException is retryable (covers EUMDAC HTTP errors)."""
        content = b"satellite data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        call_count = 0

        @contextmanager
        def http_error_then_ok(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise requests.exceptions.ConnectionError("Connection aborted")
            yield io.BytesIO(content)

        product.open = http_error_then_ok

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=2,
            retry_backoff=0.01,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        assert call_count == 2

    def test_download_timeout(self, state_db, download_dir):
        """Download that exceeds timeout should fail (TimeoutError is retryable)."""
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10

        @contextmanager
        def slow_open(**kwargs):
            time.sleep(2)
            yield io.BytesIO(b"data")

        product.open = slow_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=0,
            timeout=0.5,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.FAILED

    def test_disk_space_warning(self, state_db, download_dir, caplog):
        """Disk space warning is logged when free space is insufficient."""
        product, content = make_mock_product("P1")
        product.size = 999_999_999  # ~1 TB in KB

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        import logging

        with caplog.at_level(logging.WARNING, logger="eumdac_fetch"):
            asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert any("Low disk space" in msg for msg in caplog.messages)

    def test_timeout_parameter_stored(self, state_db, download_dir):
        """Verify timeout parameter is stored on DownloadService."""
        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            timeout=600.0,
        )
        assert svc.timeout == 600.0

    def test_default_timeout(self, state_db, download_dir):
        """Default timeout should be 300 seconds."""
        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
        )
        assert svc.timeout == 300.0

    def test_product_size_exception_defaults_to_zero(self, state_db, download_dir):
        """Product with .size raising an exception should default to 0."""

        class NoSizeProduct:
            md5 = ""

            @property
            def size(self):
                raise AttributeError("no size")

            def __str__(self):
                return "P1"

            @contextmanager
            def open(self, **kwargs):
                yield io.BytesIO(b"data")

        product = NoSizeProduct()

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.size_kb == 0
        assert record.status == ProductStatus.VERIFIED

    def test_resume_with_existing_partial_file(self, state_db, download_dir):
        """Resume appends to existing partial download."""
        # Create a partial file
        partial = download_dir / "P1"
        partial.write_bytes(b"partial")

        full_content = b"remaining data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        open_kwargs_received = {}

        @contextmanager
        def resume_open(**kwargs):
            open_kwargs_received.update(kwargs)
            yield io.BytesIO(full_content)

        product.open = resume_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            resume=True,
            verify_md5=False,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        # Should have tried byte-range resume with chunk=(offset, "")
        assert "chunk" in open_kwargs_received

    def test_resume_fallback_on_range_not_supported(self, state_db, download_dir):
        """When byte-range resume fails, falls back to full download."""
        partial = download_dir / "P1"
        partial.write_bytes(b"partial")

        full_content = b"full new data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        call_count = [0]

        @contextmanager
        def resume_or_full(**kwargs):
            call_count[0] += 1
            if "chunk" in kwargs:
                raise Exception("Range not supported")
            yield io.BytesIO(full_content)

        product.open = resume_or_full

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            resume=True,
            verify_md5=False,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        # Should have been called twice: once with chunk (failed), once without
        assert call_count[0] == 2
        assert (download_dir / "P1").read_bytes() == full_content

    def test_resume_disabled_ignores_partial(self, state_db, download_dir):
        """With resume=False, existing partial files are overwritten."""
        partial = download_dir / "P1"
        partial.write_bytes(b"old partial data")

        new_content = b"fresh download"
        product, _ = make_mock_product("P1", content=new_content)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            resume=False,
            verify_md5=False,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert (download_dir / "P1").read_bytes() == new_content

    def test_shutdown_stops_download(self, state_db, download_dir):
        """Requesting shutdown stops pending downloads."""
        products = []
        for i in range(5):
            p, _ = make_mock_product(f"P{i}", content=f"data{i}".encode())
            products.append(p)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        # Set shutdown before download starts
        svc.request_shutdown()

        asyncio.run(svc.download_all(products, "job1", "COL1"))

        # Some or all products should not have been downloaded
        downloaded = [f"P{i}" for i in range(5) if (download_dir / f"P{i}").exists()]
        assert len(downloaded) < 5

    def test_product_not_in_search_results(self, state_db, download_dir, caplog):
        """Products in state DB but not in product_map are skipped with warning."""
        product, content = make_mock_product("P1")

        # Pre-register a product that won't be in the search results
        state_db.upsert(
            ProductRecord(
                product_id="P_ORPHAN",
                job_name="job1",
                collection="COL1",
                status=ProductStatus.PENDING,
            )
        )

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        import logging

        with caplog.at_level(logging.WARNING, logger="eumdac_fetch"):
            asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert any("P_ORPHAN" in msg and "not found" in msg for msg in caplog.messages)

    def test_md5_attribute_missing_skips_verification(self, state_db, download_dir):
        """When product.md5 raises, verification is skipped (returns True)."""
        content = b"test data"

        class NoMd5Product:
            size = 10

            @property
            def md5(self):
                raise AttributeError("no md5")

            def __str__(self):
                return "P1"

            @contextmanager
            def open(self, **kwargs):
                yield io.BytesIO(content)

        product = NoMd5Product()

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=True,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED

    def test_md5_empty_string_skips_verification(self, state_db, download_dir):
        """When product.md5 is empty string, verification passes."""
        content = b"test data"
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        @contextmanager
        def fake_open(**kwargs):
            yield io.BytesIO(content)

        product.open = fake_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=True,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED

    def test_skips_processed_products(self, state_db, download_dir):
        """Products with PROCESSED status are skipped."""
        product, _ = make_mock_product("P1")
        state_db.upsert(
            ProductRecord(
                product_id="P1",
                job_name="job1",
                collection="COL1",
                status=ProductStatus.PROCESSED,
            )
        )

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))
        assert not (download_dir / "P1").exists()

    def test_shutdown_between_retries(self, state_db, download_dir):
        """Shutdown flag checked between retry attempts aborts early."""
        attempt_count = [0]
        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=3,
            retry_backoff=0.01,
        )

        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        @contextmanager
        def failing_open(**kwargs):
            attempt_count[0] += 1
            # Set shutdown after first failed attempt so the retry-loop
            # checks _shutdown.is_set() before the second attempt.
            svc.request_shutdown()
            raise ConnectionError("network down")

        product.open = failing_open

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        # Should have tried only once; the shutdown flag prevents retry
        assert attempt_count[0] == 1
        record = state_db.get("P1", "job1")
        # Status stays DOWNLOADING because shutdown aborted before FAILED update
        assert record.status in (ProductStatus.DOWNLOADING, ProductStatus.PENDING)

    def test_shutdown_mid_stream(self, state_db, download_dir):
        """Shutdown flag during _stream_to_file stops reading and returns False."""
        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
        )

        # Create a stream that sets shutdown after yielding a chunk
        class ShutdownStream:
            def __init__(self, service):
                self._service = service
                self._calls = 0

            def read(self, size):
                self._calls += 1
                if self._calls == 1:
                    # First chunk succeeds, then set shutdown
                    self._service.request_shutdown()
                    return b"x" * size
                # Second read: shutdown is set, so _stream_to_file returns False
                # before reaching this point
                return b"y" * size

        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 100
        product.md5 = ""

        @contextmanager
        def stream_open(**kwargs):
            yield ShutdownStream(svc)

        product.open = stream_open

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        # Download should not complete — shutdown interrupted _stream_to_file
        assert record.status != ProductStatus.VERIFIED

    def test_incomplete_read_is_retried(self, state_db, download_dir):
        """http.client.IncompleteRead is retried instead of failing permanently."""
        content = b"full data"
        call_count = [0]

        @contextmanager
        def flaky_open(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise http.client.IncompleteRead(b"partial", 100)
            yield io.BytesIO(content)

        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""
        product.open = flaky_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=2,
            retry_backoff=0.01,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        assert call_count[0] == 2  # 1 failure + 1 success

    def test_protocol_error_is_retried(self, state_db, download_dir):
        """urllib3.exceptions.ProtocolError is retried instead of failing permanently."""
        content = b"full data"
        call_count = [0]

        @contextmanager
        def flaky_open(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise urllib3.exceptions.ProtocolError("Connection aborted", None)
            yield io.BytesIO(content)

        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""
        product.open = flaky_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            max_retries=2,
            retry_backoff=0.01,
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        record = state_db.get("P1", "job1")
        assert record.status == ProductStatus.VERIFIED
        assert call_count[0] == 2


class TestEntryMode:
    """Tests for entry-level (individual file) downloading."""

    def _make_entry_product(self, product_id: str, entries: list[str], content: bytes = b"nc data"):
        """Create a mock product that exposes entries."""
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value=product_id)
        product.size = 50  # KB
        product.md5 = ""
        product.entries = entries

        open_kwargs_log = []

        @contextmanager
        def fake_open(**kwargs):
            open_kwargs_log.append(kwargs)
            yield io.BytesIO(content)

        product.open = fake_open
        product._open_kwargs_log = open_kwargs_log
        return product

    def test_entry_mode_downloads_matched_entry(self, state_db, download_dir):
        """Entry mode downloads only entries matching the glob pattern."""
        entries = ["product_0001.nc", "product_0002.nc", "metadata.xml"]
        product = self._make_entry_product("P1", entries)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            entries=["*.nc"],
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        # Only .nc files should be downloaded
        assert (download_dir / "product_0001.nc").exists()
        assert (download_dir / "product_0002.nc").exists()
        assert not (download_dir / "metadata.xml").exists()

    def test_entry_mode_passes_entry_to_open(self, state_db, download_dir):
        """entry_name is forwarded to product.open(entry=...)."""
        entries = ["strip_0001.nc"]
        product = self._make_entry_product("P1", entries)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            entries=["*.nc"],
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert len(product._open_kwargs_log) >= 1
        assert product._open_kwargs_log[-1].get("entry") == "strip_0001.nc"

    def test_entry_mode_skips_unmatched(self, state_db, download_dir, caplog):
        """Warning logged when no entries match the pattern."""
        entries = ["metadata.xml", "quicklook.png"]
        product = self._make_entry_product("P1", entries)

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            entries=["*.nc"],
        )

        import logging

        with caplog.at_level(logging.WARNING, logger="eumdac_fetch"):
            asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert any("No entries matched" in msg for msg in caplog.messages)
        assert not any((download_dir / f).exists() for f in entries)

    def test_entry_mode_skips_md5_verification(self, state_db, download_dir):
        """MD5 is not verified for individual entries (hash covers whole product)."""
        entries = ["data_0001.nc"]
        product = self._make_entry_product("P1", entries, content=b"entry content")
        product.md5 = "badhash"  # Would fail if checked

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=True,  # Enabled globally but must be skipped per-entry
            entries=["*.nc"],
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        from eumdac_fetch.downloader import _encode_entry_key

        key = _encode_entry_key("P1", "data_0001.nc")
        record = state_db.get(key, "job1")
        assert record.status == ProductStatus.VERIFIED

    def test_entry_mode_state_key_encoding(self, state_db, download_dir):
        """State DB key encodes both product_id and entry_name."""
        from eumdac_fetch.downloader import _decode_entry_key, _encode_entry_key

        key = _encode_entry_key("EO:EUM:DAT:0665:P123", "body_0001.nc")
        assert "::entry::" in key

        decoded_id, decoded_entry = _decode_entry_key(key)
        assert decoded_id == "EO:EUM:DAT:0665:P123"
        assert decoded_entry == "body_0001.nc"

    def test_decode_whole_product_key(self):
        """Keys without entry suffix decode with entry_name=None."""
        from eumdac_fetch.downloader import _decode_entry_key

        product_id, entry_name = _decode_entry_key("EO:EUM:DAT:0665:P123")
        assert product_id == "EO:EUM:DAT:0665:P123"
        assert entry_name is None

    def test_entry_mode_handles_entries_error(self, state_db, download_dir, caplog):
        """When product.entries raises, product is skipped with warning."""
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        type(product).entries = mock.PropertyMock(side_effect=Exception("API error"))

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            entries=["*.nc"],
        )

        import logging

        with caplog.at_level(logging.WARNING, logger="eumdac_fetch"):
            asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert any("Could not list entries" in msg for msg in caplog.messages)

    def test_whole_product_mode_passes_none_entry(self, state_db, download_dir):
        """In whole-product mode, entry=None is passed to product.open()."""
        content = b"zip content"
        open_kwargs_log = []

        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="P1")
        product.size = 10
        product.md5 = ""

        @contextmanager
        def fake_open(**kwargs):
            open_kwargs_log.append(kwargs)
            yield io.BytesIO(content)

        product.open = fake_open

        svc = DownloadService(
            state_db=state_db,
            download_dir=download_dir,
            parallel=1,
            verify_md5=False,
            entries=None,  # Whole-product mode
        )

        asyncio.run(svc.download_all([product], "job1", "COL1"))

        assert len(open_kwargs_log) >= 1
        assert open_kwargs_log[-1].get("entry") is None
