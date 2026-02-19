"""Tests for pipeline module."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest import mock

import pytest

from eumdac_fetch.models import (
    AppConfig,
    DownloadConfig,
    JobConfig,
    PostProcessConfig,
    ProductRecord,
    ProductStatus,
    SearchFilters,
)
from eumdac_fetch.pipeline import Pipeline
from eumdac_fetch.state import StateDB


@pytest.fixture
def state_db(tmp_path):
    db = StateDB(tmp_path / "test.db")
    yield db
    db.close()


@pytest.fixture
def mock_token():
    return mock.MagicMock()


@pytest.fixture
def basic_config(tmp_path):
    return AppConfig(
        jobs=[
            JobConfig(
                name="test-job",
                collection="COL1",
                filters=SearchFilters(),
                download=DownloadConfig(directory=tmp_path / "downloads", parallel=1),
                post_process=PostProcessConfig(enabled=False),
            )
        ],
    )


@pytest.fixture
def post_process_config(tmp_path):
    return AppConfig(
        jobs=[
            JobConfig(
                name="test-job",
                collection="COL1",
                filters=SearchFilters(),
                download=DownloadConfig(directory=tmp_path / "downloads", parallel=1),
                post_process=PostProcessConfig(enabled=True, output_dir=tmp_path / "output"),
            )
        ],
    )


@pytest.fixture
def mock_session(tmp_path):
    """Create a mock Session that returns tmp_path-based paths."""
    session = mock.MagicMock()
    session.session_id = "abc123def456"
    session.session_dir = tmp_path / "sessions" / "abc123def456"
    session.download_dir = tmp_path / "downloads" / "COL1"
    session.state_db_path = tmp_path / "sessions" / "abc123def456" / "state.db"
    session.log_path = tmp_path / "sessions" / "abc123def456" / "session.log"
    session.is_new = True
    session.is_live = True
    session.initialize = mock.MagicMock()
    # Create the dirs so log handler can write
    session.session_dir.mkdir(parents=True, exist_ok=True)
    session.download_dir.mkdir(parents=True, exist_ok=True)
    return session


class TestPipeline:
    def test_pipeline_init(self, mock_token, basic_config):
        pipeline = Pipeline(token=mock_token, config=basic_config)
        assert not pipeline._shutdown.is_set()
        assert pipeline.post_processor is None

    def test_handle_signal(self, mock_token, basic_config):
        pipeline = Pipeline(token=mock_token, config=basic_config)
        pipeline._handle_signal()
        assert pipeline._shutdown.is_set()

    def test_run_empty_search(self, mock_token, basic_config, mock_session):
        pipeline = Pipeline(token=mock_token, config=basic_config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = []
            mock_log.return_value = mock.MagicMock()

            asyncio.run(pipeline.run())

            mock_search.iter_products.assert_called_once()

    def test_run_creates_session(self, mock_token, basic_config, mock_session):
        """Pipeline creates and initializes a Session for each job."""
        pipeline = Pipeline(token=mock_token, config=basic_config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session) as mock_session_cls,
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = []
            mock_log.return_value = mock.MagicMock()

            asyncio.run(pipeline.run())

            mock_session_cls.assert_called_once_with(basic_config.jobs[0])
            mock_session.initialize.assert_called_once()

    def test_run_download_only(self, mock_token, basic_config, mock_session):
        pipeline = Pipeline(token=mock_token, config=basic_config)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            mock_dl.download_all.assert_called_once()

    def test_run_uses_session_download_dir(self, mock_token, basic_config, mock_session):
        """DownloadService should use session.download_dir, not job.download.directory."""
        pipeline = Pipeline(token=mock_token, config=basic_config)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # Verify DownloadService was created with session download_dir
            call_kwargs = mock_dl_cls.call_args
            assert call_kwargs.kwargs["download_dir"] == mock_session.download_dir

    def test_run_with_post_processing(self, mock_token, post_process_config, mock_session):
        mock_post_processor = mock.MagicMock()
        pipeline = Pipeline(token=mock_token, config=post_process_config, post_processor=mock_post_processor)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()
            mock_state = mock_state_cls.return_value
            mock_state.get_by_status.return_value = []
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

    def test_post_processor_called(self, mock_token, post_process_config, mock_session):
        mock_post_processor = mock.MagicMock()
        pipeline = Pipeline(token=mock_token, config=post_process_config, post_processor=mock_post_processor)

        record = ProductRecord(
            product_id="P1",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.VERIFIED,
            download_path="/tmp/P1",
        )

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()
            mock_state = mock_state_cls.return_value
            mock_state.get_by_status.return_value = [record]
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            mock_post_processor.assert_called_once_with(Path("/tmp/P1"), "P1")

    def test_post_process_enabled_no_callable_downloads_only(self, mock_token, post_process_config, mock_session):
        """When post_process.enabled but no callable provided, should download only and warn."""
        pipeline = Pipeline(token=mock_token, config=post_process_config, post_processor=None)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            mock_dl.download_all.assert_called_once()

    def test_shutdown_stops_pipeline(self, mock_token, basic_config):
        pipeline = Pipeline(token=mock_token, config=basic_config)
        pipeline._shutdown.set()

        with mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls:
            asyncio.run(pipeline.run())
            mock_search_cls.return_value.iter_products.assert_not_called()

    def test_search_caching_on_fresh_search(self, mock_token, basic_config, mock_session):
        """Fresh search caches results in state DB."""
        pipeline = Pipeline(token=mock_token, config=basic_config)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # Verify search results were cached
            mock_state.cache_search_results.assert_called_once_with([mock_product], "COL1")

    def test_stale_downloads_reset_on_resume(self, mock_token, basic_config, mock_session):
        """Resumed sessions should reset stale DOWNLOADING products to PENDING."""
        mock_session.is_new = False
        pipeline = Pipeline(token=mock_token, config=basic_config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService"),
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = []
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.reset_stale_downloads.return_value = 3
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            mock_state.reset_stale_downloads.assert_called_once_with("test-job")

    def test_stale_downloads_not_reset_on_new_session(self, mock_token, basic_config, mock_session):
        """New sessions should NOT reset stale downloads (nothing to reset)."""
        mock_session.is_new = True
        pipeline = Pipeline(token=mock_token, config=basic_config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = []
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            mock_state.reset_stale_downloads.assert_not_called()

    def test_search_uses_cache_for_resumed_non_live(self, mock_token, basic_config, mock_session):
        """Resumed non-live session with cached search uses cache."""
        mock_session.is_new = False
        mock_session.is_live = False
        pipeline = Pipeline(token=mock_token, config=basic_config)

        resumable_record = ProductRecord(
            product_id="P1",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.PENDING,
        )

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = True
            mock_state.get_resumable.return_value = [resumable_record]
            mock_state.close = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # Should not cache again (using existing cache)
            mock_state.cache_search_results.assert_not_called()
            # Should still call iter_products to get eumdac objects
            mock_search.iter_products.assert_called_once()

    def test_search_cache_all_processed(self, mock_token, basic_config, mock_session):
        """Resumed session with cache but no resumable products skips download."""
        mock_session.is_new = False
        mock_session.is_live = False
        pipeline = Pipeline(token=mock_token, config=basic_config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService"),
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = True
            mock_state.get_resumable.return_value = []  # All processed
            mock_state.reset_stale_downloads.return_value = 0
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            # Download should never be called
            mock_dl_cls.return_value.download_all.assert_not_called()

    def test_shutdown_between_jobs(self, mock_token, mock_session):
        """Shutdown signal between jobs stops the pipeline."""
        two_job_config = AppConfig(
            jobs=[
                JobConfig(name="job1", collection="COL1", filters=SearchFilters()),
                JobConfig(name="job2", collection="COL2", filters=SearchFilters()),
            ],
        )
        pipeline = Pipeline(token=mock_token, config=two_job_config)

        job_count = [0]

        def count_jobs(*args, **kwargs):
            job_count[0] += 1
            # Set shutdown after first job
            pipeline._shutdown.set()
            return []

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.side_effect = count_jobs
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            # Only first job should have been processed
            assert job_count[0] == 1

    def test_post_process_failure_marks_failed(self, mock_token, post_process_config, mock_session):
        """Post-processor failure marks product as FAILED."""

        def failing_processor(path, pid):
            raise RuntimeError("processing error")

        pipeline = Pipeline(token=mock_token, config=post_process_config, post_processor=failing_processor)

        record = ProductRecord(
            product_id="P1",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.VERIFIED,
            download_path="/tmp/P1",
        )

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()
            mock_state = mock_state_cls.return_value
            mock_state.get_by_status.return_value = [record]
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            # Should have been marked FAILED
            mock_state.update_status.assert_any_call(
                "P1", "test-job", ProductStatus.FAILED, error_message="Post-processing failed: processing error"
            )

    def test_shutdown_during_post_processing(self, mock_token, post_process_config, mock_session):
        """Shutdown during post-processing consumer stops the loop."""
        pipeline = Pipeline(token=mock_token, config=post_process_config, post_processor=mock.MagicMock())

        record = ProductRecord(
            product_id="P1",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.VERIFIED,
            download_path="/tmp/P1",
        )

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()

            # Set shutdown when download_all is called, before producer sends to queue
            async def shutdown_on_download(*a, **kw):
                pipeline._shutdown.set()

            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock(side_effect=shutdown_on_download)
            mock_state = mock_state_cls.return_value
            mock_state.get_by_status.return_value = [record]
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()

            asyncio.run(pipeline.run())

            # Should complete without hanging
