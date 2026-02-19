"""Tests for SQLite state database."""

from __future__ import annotations

from unittest import mock

import pytest

from eumdac_fetch.models import ProductRecord, ProductStatus
from eumdac_fetch.state import StateDB


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(tmp_path / "test.db")
    yield db
    db.close()


class TestStateDB:
    def test_upsert_and_get(self, state_db):
        record = ProductRecord(
            product_id="P1",
            job_name="job1",
            collection="COL1",
            size_kb=5000.0,
        )
        state_db.upsert(record)
        got = state_db.get("P1", "job1")
        assert got is not None
        assert got.product_id == "P1"
        assert got.size_kb == 5000.0
        assert got.status == ProductStatus.PENDING

    def test_get_nonexistent(self, state_db):
        assert state_db.get("NOPE", "job1") is None

    def test_update_status(self, state_db):
        record = ProductRecord(product_id="P1", job_name="job1", collection="COL1")
        state_db.upsert(record)
        state_db.update_status("P1", "job1", ProductStatus.DOWNLOADING)
        got = state_db.get("P1", "job1")
        assert got.status == ProductStatus.DOWNLOADING

    def test_update_status_with_kwargs(self, state_db):
        record = ProductRecord(product_id="P1", job_name="job1", collection="COL1")
        state_db.upsert(record)
        state_db.update_status(
            "P1",
            "job1",
            ProductStatus.DOWNLOADED,
            bytes_downloaded=50000,
            download_path="/tmp/P1.zip",
        )
        got = state_db.get("P1", "job1")
        assert got.status == ProductStatus.DOWNLOADED
        assert got.bytes_downloaded == 50000
        assert got.download_path == "/tmp/P1.zip"

    def test_upsert_updates_existing(self, state_db):
        record = ProductRecord(product_id="P1", job_name="job1", collection="COL1", size_kb=100)
        state_db.upsert(record)

        record.size_kb = 200
        record.status = ProductStatus.VERIFIED
        state_db.upsert(record)

        got = state_db.get("P1", "job1")
        assert got.size_kb == 200
        assert got.status == ProductStatus.VERIFIED

    def test_get_by_status(self, state_db):
        for i in range(5):
            state_db.upsert(ProductRecord(product_id=f"P{i}", job_name="job1", collection="COL1"))
        state_db.update_status("P0", "job1", ProductStatus.VERIFIED)
        state_db.update_status("P1", "job1", ProductStatus.VERIFIED)

        pending = state_db.get_by_status("job1", ProductStatus.PENDING)
        assert len(pending) == 3

        verified = state_db.get_by_status("job1", ProductStatus.VERIFIED)
        assert len(verified) == 2

    def test_get_all(self, state_db):
        for i in range(3):
            state_db.upsert(ProductRecord(product_id=f"P{i}", job_name="job1", collection="COL1"))
        state_db.upsert(ProductRecord(product_id="P99", job_name="job2", collection="COL1"))

        all_job1 = state_db.get_all("job1")
        assert len(all_job1) == 3

    def test_get_resumable(self, state_db):
        state_db.upsert(ProductRecord(product_id="P0", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P1", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P2", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P3", job_name="j", collection="C"))
        state_db.update_status("P0", "j", ProductStatus.VERIFIED)
        state_db.update_status("P2", "j", ProductStatus.FAILED)
        state_db.update_status("P3", "j", ProductStatus.DOWNLOADING)

        resumable = state_db.get_resumable("j")
        ids = {r.product_id for r in resumable}
        assert ids == {"P1", "P2", "P3"}

    def test_get_resumable_excludes_completed_statuses(self, state_db):
        """VERIFIED, PROCESSED, DOWNLOADED should not be resumable."""
        state_db.upsert(ProductRecord(product_id="P0", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P1", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P2", job_name="j", collection="C"))
        state_db.update_status("P0", "j", ProductStatus.VERIFIED)
        state_db.update_status("P1", "j", ProductStatus.PROCESSED)
        state_db.update_status("P2", "j", ProductStatus.DOWNLOADED)

        resumable = state_db.get_resumable("j")
        assert len(resumable) == 0

    def test_composite_primary_key(self, state_db):
        state_db.upsert(ProductRecord(product_id="P1", job_name="job1", collection="COL1"))
        state_db.upsert(ProductRecord(product_id="P1", job_name="job2", collection="COL2"))
        assert state_db.get("P1", "job1").collection == "COL1"
        assert state_db.get("P1", "job2").collection == "COL2"

    def test_timestamps_set(self, state_db):
        record = ProductRecord(product_id="P1", job_name="j", collection="C")
        state_db.upsert(record)
        got = state_db.get("P1", "j")
        assert got.created_at != ""
        assert got.updated_at != ""

    def test_reset_stale_downloads(self, state_db):
        """DOWNLOADING products should be reset to PENDING."""
        state_db.upsert(ProductRecord(product_id="P0", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P1", job_name="j", collection="C"))
        state_db.upsert(ProductRecord(product_id="P2", job_name="j", collection="C"))
        state_db.update_status("P0", "j", ProductStatus.DOWNLOADING)
        state_db.update_status("P1", "j", ProductStatus.DOWNLOADING)
        state_db.update_status("P2", "j", ProductStatus.VERIFIED)

        count = state_db.reset_stale_downloads("j")
        assert count == 2

        assert state_db.get("P0", "j").status == ProductStatus.PENDING
        assert state_db.get("P1", "j").status == ProductStatus.PENDING
        assert state_db.get("P2", "j").status == ProductStatus.VERIFIED

    def test_reset_stale_downloads_none(self, state_db):
        """No DOWNLOADING products returns 0."""
        state_db.upsert(ProductRecord(product_id="P0", job_name="j", collection="C"))
        count = state_db.reset_stale_downloads("j")
        assert count == 0

    def test_reset_stale_downloads_only_affects_job(self, state_db):
        """Reset only affects the specified job."""
        state_db.upsert(ProductRecord(product_id="P0", job_name="j1", collection="C"))
        state_db.upsert(ProductRecord(product_id="P1", job_name="j2", collection="C"))
        state_db.update_status("P0", "j1", ProductStatus.DOWNLOADING)
        state_db.update_status("P1", "j2", ProductStatus.DOWNLOADING)

        count = state_db.reset_stale_downloads("j1")
        assert count == 1
        assert state_db.get("P0", "j1").status == ProductStatus.PENDING
        assert state_db.get("P1", "j2").status == ProductStatus.DOWNLOADING


class TestSearchResultsCache:
    def test_has_cached_search_empty(self, state_db):
        """Empty search_results table returns False."""
        assert state_db.has_cached_search() is False

    def test_cache_search_results(self, state_db):
        """Bulk insert and retrieve search results."""
        products = []
        for i in range(3):
            p = mock.MagicMock()
            p.__str__ = mock.MagicMock(return_value=f"PROD-{i}")
            p.size = 1000 + i
            p.sensing_start = f"2024-01-01T0{i}:00:00Z"
            p.sensing_end = f"2024-01-01T0{i}:15:00Z"
            products.append(p)

        state_db.cache_search_results(products, "COL1")
        results = state_db.get_cached_search_results()
        assert len(results) == 3
        assert results[0]["product_id"] == "PROD-0"
        assert results[0]["collection"] == "COL1"
        assert results[0]["size_kb"] == 1000
        assert results[0]["cached_at"] != ""

    def test_has_cached_search_populated(self, state_db):
        """After caching, has_cached_search returns True."""
        p = mock.MagicMock()
        p.__str__ = mock.MagicMock(return_value="P1")
        p.size = 500
        p.sensing_start = "2024-01-01T00:00:00Z"
        p.sensing_end = "2024-01-01T00:15:00Z"
        state_db.cache_search_results([p], "COL1")
        assert state_db.has_cached_search() is True

    def test_cache_search_results_replaces(self, state_db):
        """INSERT OR REPLACE updates existing rows."""
        p = mock.MagicMock()
        p.__str__ = mock.MagicMock(return_value="P1")
        p.size = 500
        p.sensing_start = "2024-01-01T00:00:00Z"
        p.sensing_end = "2024-01-01T00:15:00Z"
        state_db.cache_search_results([p], "COL1")

        # Update size
        p.size = 999
        state_db.cache_search_results([p], "COL1")

        results = state_db.get_cached_search_results()
        assert len(results) == 1
        assert results[0]["size_kb"] == 999

    def test_cache_handles_missing_attrs(self, state_db):
        """Products without size/sensing attrs use defaults."""

        class BareProduct:
            def __str__(self):
                return "P1"

        state_db.cache_search_results([BareProduct()], "COL1")
        results = state_db.get_cached_search_results()
        assert len(results) == 1
        assert results[0]["size_kb"] == 0
