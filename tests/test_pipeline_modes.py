"""Tests for download-toggle and remote-dataset pipeline modes."""

from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from click.testing import CliRunner

from eumdac_fetch.cli import cli
from eumdac_fetch.dataset import RemoteDataset
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

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_token():
    return mock.MagicMock()


@pytest.fixture
def mock_session(tmp_path):
    session = mock.MagicMock()
    session.session_id = "abc123def456"
    session.session_dir = tmp_path / "sessions" / "abc123def456"
    session.download_dir = tmp_path / "downloads" / "COL1"
    session.state_db_path = tmp_path / "sessions" / "abc123def456" / "state.db"
    session.log_path = tmp_path / "sessions" / "abc123def456" / "session.log"
    session.is_new = True
    session.is_live = True
    session.initialize = mock.MagicMock()
    session.session_dir.mkdir(parents=True, exist_ok=True)
    session.download_dir.mkdir(parents=True, exist_ok=True)
    return session


@pytest.fixture
def mock_product():
    p = mock.MagicMock()
    p.__str__ = mock.MagicMock(return_value="P1")
    p.size = 10
    return p


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def tmp_config(tmp_path):
    config = tmp_path / "job.yaml"
    config.write_text(
        """\
jobs:
  - name: test-job
    collection: "EO:EUM:DAT:TEST"
    filters:
      dtstart: "2024-01-01T00:00:00Z"
      dtend: "2024-01-02T00:00:00Z"
    download:
      directory: ./downloads
"""
    )
    return str(config)


@pytest.fixture
def tmp_config_no_download(tmp_path):
    config = tmp_path / "job_no_dl.yaml"
    config.write_text(
        """\
jobs:
  - name: test-job
    collection: "EO:EUM:DAT:TEST"
    filters:
      dtstart: "2024-01-01T00:00:00Z"
      dtend: "2024-01-02T00:00:00Z"
    download:
      enabled: false
      directory: ./downloads
"""
    )
    return str(config)


# ---------------------------------------------------------------------------
# 1. Search-only mode (download.enabled=False, no processor)
# ---------------------------------------------------------------------------


class TestSearchOnlyMode:
    def test_search_only_no_download(self, mock_token, tmp_path, mock_session, mock_product):
        config = AppConfig(
            jobs=[
                JobConfig(
                    name="test-job",
                    collection="COL1",
                    filters=SearchFilters(),
                    download=DownloadConfig(enabled=False, parallel=1),
                    post_process=PostProcessConfig(enabled=False),
                )
            ],
        )
        pipeline = Pipeline(token=mock_token, config=config)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get.return_value = None
            mock_state.close = mock.MagicMock()
            mock_dl_cls.return_value.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # download_all must never be called
            mock_dl_cls.return_value.download_all.assert_not_called()
            # products should be registered as PENDING
            mock_state.upsert.assert_called_once()
            upsert_record: ProductRecord = mock_state.upsert.call_args[0][0]
            assert upsert_record.product_id == "P1"
            assert upsert_record.status == ProductStatus.PENDING

    def test_search_only_skips_already_pending(self, mock_token, tmp_path, mock_session, mock_product):
        """If product is already in DB, do not upsert again."""
        config = AppConfig(
            jobs=[
                JobConfig(
                    name="test-job",
                    collection="COL1",
                    filters=SearchFilters(),
                    download=DownloadConfig(enabled=False, parallel=1),
                    post_process=PostProcessConfig(enabled=False),
                )
            ],
        )
        pipeline = Pipeline(token=mock_token, config=config)

        existing_record = ProductRecord(
            product_id="P1",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.PENDING,
        )

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get.return_value = existing_record  # already present
            mock_state.close = mock.MagicMock()
            mock_dl_cls.return_value.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            mock_dl_cls.return_value.download_all.assert_not_called()
            mock_state.upsert.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Remote mode: hook called with (RemoteDataset, product_id)
# ---------------------------------------------------------------------------


class TestRemoteMode:
    def _remote_config(self, tmp_path):
        return AppConfig(
            jobs=[
                JobConfig(
                    name="test-job",
                    collection="COL1",
                    filters=SearchFilters(),
                    download=DownloadConfig(parallel=1),
                    post_process=PostProcessConfig(enabled=True, mode="remote"),
                )
            ],
        )

    def test_remote_mode_calls_hook(self, mock_token, tmp_path, mock_session, mock_product):
        config = self._remote_config(tmp_path)
        mock_remote_hook = mock.MagicMock()
        pipeline = Pipeline(token=mock_token, config=config, remote_post_processor=mock_remote_hook)

        mock_dataset = mock.MagicMock(spec=RemoteDataset)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.build_remote_dataset", return_value=mock_dataset) as mock_build,
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get.return_value = None
            mock_state.close = mock.MagicMock()
            mock_dl_cls.return_value.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # build_remote_dataset must be called for the product
            mock_build.assert_called_once_with(mock_product, mock_token, None)
            # remote hook called with (dataset, product_id)
            mock_remote_hook.assert_called_once_with(mock_dataset, "P1")
            # no download
            mock_dl_cls.return_value.download_all.assert_not_called()
            # status updated to PROCESSED
            mock_state.update_status.assert_any_call("P1", "test-job", ProductStatus.PROCESSED)

    def test_remote_mode_no_hook_falls_through_to_download(self, mock_token, tmp_path, mock_session, mock_product):
        """mode=remote but no remote_post_processor: no remote processing, downloads normally."""
        config = self._remote_config(tmp_path)
        pipeline = Pipeline(token=mock_token, config=config, remote_post_processor=None)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get_by_status.return_value = []
            mock_state.close = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # Falls to download-only (post_process.enabled=True, no post_processor, download.enabled=True)
            mock_dl.download_all.assert_called_once()

    def test_remote_mode_failure_marks_failed(self, mock_token, tmp_path, mock_session, mock_product):
        config = self._remote_config(tmp_path)

        def failing_hook(dataset, product_id):
            raise RuntimeError("remote error")

        pipeline = Pipeline(token=mock_token, config=config, remote_post_processor=failing_hook)
        mock_dataset = mock.MagicMock(spec=RemoteDataset)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.build_remote_dataset", return_value=mock_dataset),
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_product]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get.return_value = None
            mock_state.close = mock.MagicMock()
            mock_dl_cls.return_value.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            mock_state.update_status.assert_any_call(
                "P1", "test-job", ProductStatus.FAILED, error_message="Remote processing failed: remote error"
            )


# ---------------------------------------------------------------------------
# 3. Remote mode resume: skips already PROCESSED products
# ---------------------------------------------------------------------------


class TestRemoteModeResume:
    def test_remote_mode_resume_skips_processed(self, mock_token, tmp_path, mock_session):
        config = AppConfig(
            jobs=[
                JobConfig(
                    name="test-job",
                    collection="COL1",
                    filters=SearchFilters(),
                    download=DownloadConfig(parallel=1),
                    post_process=PostProcessConfig(enabled=True, mode="remote"),
                )
            ],
        )
        mock_remote_hook = mock.MagicMock()
        pipeline = Pipeline(token=mock_token, config=config, remote_post_processor=mock_remote_hook)

        mock_p_pending = mock.MagicMock()
        mock_p_pending.__str__ = mock.MagicMock(return_value="P_PENDING")
        mock_p_processed = mock.MagicMock()
        mock_p_processed.__str__ = mock.MagicMock(return_value="P_PROCESSED")

        processed_record = ProductRecord(
            product_id="P_PROCESSED",
            job_name="test-job",
            collection="COL1",
            status=ProductStatus.PROCESSED,
        )

        mock_dataset = mock.MagicMock(spec=RemoteDataset)

        def fake_get(product_id, job_name):
            if product_id == "P_PROCESSED":
                return processed_record
            return None

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            mock.patch("eumdac_fetch.pipeline.build_remote_dataset", return_value=mock_dataset),
        ):
            mock_search_cls.return_value.iter_products.return_value = [mock_p_pending, mock_p_processed]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.get.side_effect = fake_get
            mock_state.close = mock.MagicMock()
            mock_dl_cls.return_value.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            # Hook called only for the PENDING product
            mock_remote_hook.assert_called_once_with(mock_dataset, "P_PENDING")


# ---------------------------------------------------------------------------
# 4 & 5. CLI flag tests
# ---------------------------------------------------------------------------


class TestCLIDownloadFlags:
    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_no_download_cli_flag_sets_enabled_false(self, mock_pipeline_cls, mock_get_token, runner, tmp_config):
        """--no-download overrides download.enabled to False on all jobs."""
        mock_pipeline = mock.MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run = mock.AsyncMock()

        result = runner.invoke(cli, ["run", "-c", tmp_config, "--no-download"])

        assert result.exit_code == 0
        call_kwargs = mock_pipeline_cls.call_args
        passed_config = call_kwargs.kwargs.get("config") or call_kwargs.args[1]
        for job in passed_config.jobs:
            assert job.download.enabled is False

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_download_flag_overrides_config(self, mock_pipeline_cls, mock_get_token, runner, tmp_config_no_download):
        """--download overrides download.enabled=false in YAML."""
        mock_pipeline = mock.MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run = mock.AsyncMock()

        result = runner.invoke(cli, ["run", "-c", tmp_config_no_download, "--download"])

        assert result.exit_code == 0
        call_kwargs = mock_pipeline_cls.call_args
        passed_config = call_kwargs.kwargs.get("config") or call_kwargs.args[1]
        for job in passed_config.jobs:
            assert job.download.enabled is True

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_no_flag_preserves_config_value(self, mock_pipeline_cls, mock_get_token, runner, tmp_config_no_download):
        """Without --download/--no-download, config value is preserved."""
        mock_pipeline = mock.MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run = mock.AsyncMock()

        result = runner.invoke(cli, ["run", "-c", tmp_config_no_download])

        assert result.exit_code == 0
        call_kwargs = mock_pipeline_cls.call_args
        passed_config = call_kwargs.kwargs.get("config") or call_kwargs.args[1]
        # Config has download.enabled: false — must be preserved
        for job in passed_config.jobs:
            assert job.download.enabled is False

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_remote_processor_invalid_format(self, mock_pipeline_cls, mock_get_token, runner, tmp_config):
        """--remote-processor without ':' should fail with an error message."""
        result = runner.invoke(cli, ["run", "-c", tmp_config, "--remote-processor", "no_colon"])
        assert result.exit_code == 1
        assert "module:function" in result.output

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_remote_processor_loaded_and_passed(self, mock_pipeline_cls, mock_get_token, runner, tmp_config, tmp_path):
        """--remote-processor loads callable and passes it to Pipeline."""
        mod_file = tmp_path / "myremote.py"
        mod_file.write_text("def process(dataset, pid): pass\n")

        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            mock_pipeline = mock.MagicMock()
            mock_pipeline_cls.return_value = mock_pipeline
            mock_pipeline.run = mock.AsyncMock()

            result = runner.invoke(cli, ["run", "-c", tmp_config, "--remote-processor", "myremote:process"])

            assert result.exit_code == 0
            call_kwargs = mock_pipeline_cls.call_args
            assert call_kwargs.kwargs.get("remote_post_processor") is not None
        finally:
            sys.path.pop(0)
            sys.modules.pop("myremote", None)


# ---------------------------------------------------------------------------
# 6. Unit test for build_remote_dataset
# ---------------------------------------------------------------------------


class TestBuildRemoteDataset:
    def test_build_remote_dataset_all_entries(self):
        """All entries included when no entry_patterns filter is given."""
        from eumdac_fetch.dataset import build_remote_dataset

        mock_product = mock.MagicMock()
        mock_product.url = "https://api.eumetsat.int/data/browse/products/PROD-001?foo=bar"
        mock_product.entries = ["file_A.nc", "file_B.nc"]

        mock_token = mock.MagicMock()

        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            dataset = build_remote_dataset(mock_product, mock_token)

        assert "file_A.nc" in dataset
        assert "file_B.nc" in dataset
        assert len(dataset) == 2

    def test_build_remote_dataset_url_construction(self):
        """Entry URLs are constructed from product.url with /entry?name= suffix."""
        from eumdac_fetch.dataset import build_remote_dataset

        mock_product = mock.MagicMock()
        mock_product.url = "https://api.eumetsat.int/data/browse/products/PROD-001?foo=bar"
        mock_product.entries = ["data file.nc"]  # space tests percent-encoding

        mock_token = mock.MagicMock()

        captured: dict = {}
        original_init = __import__("eumdac_fetch.dataset", fromlist=["RemoteDataset"]).RemoteDataset.__init__

        def _capture_init(self, entries, token_manager=None, **kw):
            captured.update(entries)
            original_init(self, {}, token_manager=token_manager)

        with (
            mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"),
            mock.patch("eumdac_fetch.dataset.RemoteDataset.__init__", _capture_init),
        ):
            build_remote_dataset(mock_product, mock_token)

        assert "data file.nc" in captured
        assert (
            captured["data file.nc"]
            == "https://api.eumetsat.int/data/browse/products/PROD-001/entry?name=data%20file.nc"
        )

    def test_build_remote_dataset_with_patterns(self):
        """Only matching entries included when entry_patterns is given."""
        from eumdac_fetch.dataset import build_remote_dataset

        mock_product = mock.MagicMock()
        mock_product.url = "https://api.eumetsat.int/data/browse/products/PROD-001"
        mock_product.entries = ["data.nc", "manifest.xml"]

        mock_token = mock.MagicMock()

        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            dataset = build_remote_dataset(mock_product, mock_token, entry_patterns=["*.nc"])

        assert "data.nc" in dataset
        assert "manifest.xml" not in dataset
        assert len(dataset) == 1

    def test_build_remote_dataset_empty_entries(self):
        """Empty entries list produces an empty RemoteDataset."""
        from eumdac_fetch.dataset import build_remote_dataset

        mock_product = mock.MagicMock()
        mock_product.url = "https://api.eumetsat.int/data/browse/products/PROD-001"
        mock_product.entries = []

        mock_token = mock.MagicMock()

        with mock.patch("eumdac_fetch.dataset.TokenRefreshingHTTPFileSystem"):
            dataset = build_remote_dataset(mock_product, mock_token)

        assert len(dataset) == 0
