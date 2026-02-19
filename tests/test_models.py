"""Tests for data models."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from eumdac_fetch.models import (
    AppConfig,
    DownloadConfig,
    JobConfig,
    PostProcessConfig,
    ProductRecord,
    ProductStatus,
    SearchFilters,
)


class TestSearchFilters:
    def test_defaults_are_none(self):
        f = SearchFilters()
        assert f.dtstart is None
        assert f.dtend is None
        assert f.geo is None
        assert f.sat is None
        assert f.sort == "start,time,1"

    def test_to_search_kwargs_empty(self):
        f = SearchFilters()
        # sort is always included (has a default)
        assert f.to_search_kwargs() == {"sort": "start,time,1"}

    def test_to_search_kwargs_partial(self):
        f = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            sat="MSG4",
            timeliness="NT",
        )
        kwargs = f.to_search_kwargs()
        assert kwargs == {
            "dtstart": datetime(2024, 1, 1, tzinfo=UTC),
            "sat": "MSG4",
            "timeliness": "NT",
            "sort": "start,time,1",
        }

    def test_to_search_kwargs_all_fields(self):
        f = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
            geo="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            sat="MSG4",
            timeliness="NT",
            filename="test.nc",
            cycle=1,
            orbit=100,
            relorbit=50,
            product_type="L1",
            publication="recent",
            download_coverage="full",
        )
        kwargs = f.to_search_kwargs()
        assert len(kwargs) == 13  # 12 explicit + sort default

    def test_to_search_kwargs_skips_none(self):
        f = SearchFilters(sat="MSG4", cycle=None)
        kwargs = f.to_search_kwargs()
        assert "cycle" not in kwargs
        assert kwargs == {"sat": "MSG4", "sort": "start,time,1"}


class TestProductStatus:
    def test_enum_values(self):
        assert ProductStatus.PENDING.value == "pending"
        assert ProductStatus.DOWNLOADING.value == "downloading"
        assert ProductStatus.DOWNLOADED.value == "downloaded"
        assert ProductStatus.VERIFIED.value == "verified"
        assert ProductStatus.PROCESSING.value == "processing"
        assert ProductStatus.PROCESSED.value == "processed"
        assert ProductStatus.FAILED.value == "failed"


class TestDownloadConfig:
    def test_defaults(self):
        cfg = DownloadConfig()
        assert cfg.directory == Path("./downloads")
        assert cfg.parallel == 4
        assert cfg.resume is True
        assert cfg.verify_md5 is True


class TestPostProcessConfig:
    def test_defaults(self):
        cfg = PostProcessConfig()
        assert cfg.enabled is False
        assert cfg.output_dir == Path("./output")


class TestJobConfig:
    def test_minimal(self):
        job = JobConfig(name="test", collection="EO:EUM:DAT:MSG:HRSEVIRI")
        assert job.name == "test"
        assert job.collection == "EO:EUM:DAT:MSG:HRSEVIRI"
        assert job.filters.dtstart is None
        assert job.download.parallel == 4
        assert job.post_process.enabled is False
        assert job.limit is None


class TestProductRecord:
    def test_defaults(self):
        rec = ProductRecord(product_id="P1", job_name="test", collection="COL1")
        assert rec.status == ProductStatus.PENDING
        assert rec.bytes_downloaded == 0
        assert rec.md5 == ""
        assert rec.download_path == ""

    def test_custom_status(self):
        rec = ProductRecord(
            product_id="P1",
            job_name="test",
            collection="COL1",
            status=ProductStatus.VERIFIED,
            size_kb=50000.0,
        )
        assert rec.status == ProductStatus.VERIFIED
        assert rec.size_kb == 50000.0


class TestAppConfig:
    def test_defaults(self):
        cfg = AppConfig()
        assert cfg.logging.level == "INFO"
        assert cfg.jobs == []
