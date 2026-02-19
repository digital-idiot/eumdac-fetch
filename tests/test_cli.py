"""Tests for CLI commands."""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner

from eumdac_fetch.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def tmp_config(tmp_path):
    """Create a minimal valid YAML config for testing."""
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


class TestInfoCommand:
    @mock.patch("eumdac.AccessToken")
    @mock.patch("eumdac_fetch.search.SearchService")
    def test_info_success(self, mock_search_cls, mock_access_token_cls, runner):
        from eumdac_fetch.search import CollectionInfo

        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.get_collection_info.return_value = CollectionInfo(
            collection_id="EO:EUM:DAT:TEST",
            title="Test Collection",
            abstract="A test collection",
            search_options={},
        )

        result = runner.invoke(cli, ["info", "EO:EUM:DAT:TEST", "--key", "k", "--secret", "s"])

        assert result.exit_code == 0
        assert "Test Collection" in result.output

    def test_info_missing_credentials(self, runner):
        result = runner.invoke(cli, ["info", "EO:EUM:DAT:TEST"])

        assert result.exit_code == 1
        assert (
            "credentials required" in result.output.lower() or "credentials required" in (result.stderr or "").lower()
        )

    @mock.patch("eumdac.AccessToken")
    @mock.patch("eumdac_fetch.search.SearchService")
    def test_info_api_error(self, mock_search_cls, mock_access_token_cls, runner):
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.get_collection_info.side_effect = Exception("API error")

        result = runner.invoke(cli, ["info", "EO:EUM:DAT:TEST", "--key", "k", "--secret", "s"])

        assert result.exit_code == 1
        assert "API error" in result.output


class TestSearchCommand:
    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    def test_search_success(self, mock_search_cls, mock_create_token, runner, tmp_config):
        from eumdac_fetch.search import SearchResult

        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.search.return_value = SearchResult(
            total=10,
            products=[],
            filters_used={"dtstart": "2024-01-01"},
        )

        result = runner.invoke(cli, ["search", "-c", tmp_config])

        assert result.exit_code == 0
        assert "test-job" in result.output

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    def test_search_count_only(self, mock_search_cls, mock_create_token, runner, tmp_config):
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.count.return_value = 42

        result = runner.invoke(cli, ["search", "-c", tmp_config, "--count-only"])

        assert result.exit_code == 0
        assert "42" in result.output

    def test_search_invalid_config(self, runner, tmp_path):
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text("not_valid: true\n")

        result = runner.invoke(cli, ["search", "-c", str(bad_config)])

        assert result.exit_code == 1

    def test_search_missing_config(self, runner):
        result = runner.invoke(cli, ["search", "-c", "/nonexistent.yaml"])

        assert result.exit_code == 2  # Click path validation error


class TestDownloadCommand:
    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.downloader.DownloadService")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_success(
        self,
        mock_log_handler,
        mock_dl_cls,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.iter_products.return_value = [mock.MagicMock()]

        mock_session = mock.MagicMock()
        mock_session.is_new = True
        mock_session.is_live = False
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = False
        mock_statedb_cls.return_value = mock_state_db

        mock_dl = mock.MagicMock()
        mock_dl_cls.return_value = mock_dl
        mock_dl.download_all = mock.AsyncMock()

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        mock_dl.download_all.assert_called_once()

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.downloader.DownloadService")
    def test_download_keyboard_interrupt(
        self, mock_dl_cls, mock_statedb_cls, mock_session_cls, mock_search_cls, mock_create_token, runner, tmp_config
    ):
        mock_create_token.side_effect = KeyboardInterrupt()

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 130


class TestRunCommand:
    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_run_success(self, mock_pipeline_cls, mock_create_token, runner, tmp_config):
        mock_pipeline = mock.MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run = mock.AsyncMock()

        result = runner.invoke(cli, ["run", "-c", tmp_config])

        assert result.exit_code == 0
        mock_pipeline.run.assert_called_once()

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_run_keyboard_interrupt(self, mock_pipeline_cls, mock_create_token, runner, tmp_config):
        mock_create_token.side_effect = KeyboardInterrupt()

        result = runner.invoke(cli, ["run", "-c", tmp_config])

        assert result.exit_code == 130

    @mock.patch("eumdac_fetch.auth.get_token")
    def test_run_invalid_post_processor_format(self, mock_create_token, runner, tmp_config):
        """--post-processor without ':' should fail."""
        result = runner.invoke(cli, ["run", "-c", tmp_config, "--post-processor", "no_colon_here"])

        assert result.exit_code == 1
        assert "module:function" in result.output

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_run_with_post_processor(self, mock_pipeline_cls, mock_create_token, runner, tmp_config, tmp_path):
        # Create a module with a callable
        mod_file = tmp_path / "myprocessor.py"
        mod_file.write_text("def process(path, pid): pass\n")

        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            mock_pipeline = mock.MagicMock()
            mock_pipeline_cls.return_value = mock_pipeline
            mock_pipeline.run = mock.AsyncMock()

            result = runner.invoke(cli, ["run", "-c", tmp_config, "--post-processor", "myprocessor:process"])

            assert result.exit_code == 0
            # Verify pipeline was created with the post_processor
            call_kwargs = mock_pipeline_cls.call_args
            assert call_kwargs.kwargs.get("post_processor") is not None or (
                len(call_kwargs.args) > 2 and call_kwargs.args[2] is not None
            )
        finally:
            sys.path.pop(0)
            sys.modules.pop("myprocessor", None)

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.downloader.DownloadService")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_live_session(
        self,
        mock_log_handler,
        mock_dl_cls,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        """Live session prints live session message."""
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.iter_products.return_value = [mock.MagicMock()]

        mock_session = mock.MagicMock()
        mock_session.is_new = True
        mock_session.is_live = True
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = False
        mock_statedb_cls.return_value = mock_state_db

        mock_dl = mock.MagicMock()
        mock_dl_cls.return_value = mock_dl
        mock_dl.download_all = mock.AsyncMock()

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        assert "Live session" in result.output

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.downloader.DownloadService")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_resumed_session_with_cache(
        self,
        mock_log_handler,
        mock_dl_cls,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        """Resumed non-live session uses cached search results."""
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service

        mock_session = mock.MagicMock()
        mock_session.is_new = False
        mock_session.is_live = False
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = True
        resumable = mock.MagicMock()
        resumable.product_id = "P1"
        mock_state_db.get_resumable.return_value = [resumable]
        mock_statedb_cls.return_value = mock_state_db

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_service.iter_products.return_value = [mock_product]

        mock_dl = mock.MagicMock()
        mock_dl_cls.return_value = mock_dl
        mock_dl.download_all = mock.AsyncMock()

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        assert "cached" in result.output.lower() or "Using cached" in result.output

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.downloader.DownloadService")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_resumed_all_done(
        self,
        mock_log_handler,
        mock_dl_cls,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        """Resumed session with all products already downloaded."""
        mock_session = mock.MagicMock()
        mock_session.is_new = False
        mock_session.is_live = False
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = True
        mock_state_db.get_resumable.return_value = []
        mock_statedb_cls.return_value = mock_state_db

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        assert "already downloaded" in result.output.lower()
        mock_dl_cls.assert_not_called()

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_stale_reset(
        self,
        mock_log_handler,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        """Resumed session resets stale downloads."""
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.iter_products.return_value = []

        mock_session = mock.MagicMock()
        mock_session.is_new = False
        mock_session.is_live = False
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = False
        mock_state_db.reset_stale_downloads.return_value = 3
        mock_statedb_cls.return_value = mock_state_db

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        assert "Reset 3" in result.output
        mock_state_db.reset_stale_downloads.assert_called_once()

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.search.SearchService")
    @mock.patch("eumdac_fetch.session.Session")
    @mock.patch("eumdac_fetch.state.StateDB")
    @mock.patch("eumdac_fetch.logging_config.add_session_log_handler")
    def test_download_no_products(
        self,
        mock_log_handler,
        mock_statedb_cls,
        mock_session_cls,
        mock_search_cls,
        mock_create_token,
        runner,
        tmp_config,
    ):
        """When search returns no products, download is skipped."""
        mock_service = mock.MagicMock()
        mock_search_cls.return_value = mock_service
        mock_service.iter_products.return_value = []

        mock_session = mock.MagicMock()
        mock_session.is_new = True
        mock_session.is_live = False
        mock_session_cls.return_value = mock_session

        mock_state_db = mock.MagicMock()
        mock_state_db.has_cached_search.return_value = False
        mock_statedb_cls.return_value = mock_state_db

        result = runner.invoke(cli, ["download", "-c", tmp_config])

        assert result.exit_code == 0
        assert "Found 0" in result.output

    def test_download_generic_error(self, runner, tmp_path):
        """Download with bad config shows error."""
        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text("not_valid: true\n")

        result = runner.invoke(cli, ["download", "-c", str(bad_config)])
        assert result.exit_code == 1

    @mock.patch("eumdac_fetch.auth.get_token")
    @mock.patch("eumdac_fetch.pipeline.Pipeline")
    def test_run_generic_error(self, mock_pipeline_cls, mock_create_token, runner, tmp_config):
        """Run command catches generic exceptions."""
        mock_pipeline = mock.MagicMock()
        mock_pipeline_cls.return_value = mock_pipeline
        mock_pipeline.run = mock.AsyncMock(side_effect=RuntimeError("pipeline broke"))

        result = runner.invoke(cli, ["run", "-c", tmp_config])

        assert result.exit_code == 1
        assert "pipeline broke" in result.output


class TestCollectionsCommand:
    @mock.patch("eumdac.AccessToken")
    @mock.patch("eumdac_fetch.search.SearchService")
    def test_collections_lists_all(self, mock_search_cls, mock_access_token_cls, runner):
        """collections command lists all available collections."""
        from eumdac_fetch.search import CollectionSummary

        mock_service = mock_search_cls.return_value
        mock_service.list_collections.return_value = [
            CollectionSummary("EO:EUM:DAT:MSG:HRSEVIRI", "High Rate SEVIRI"),
            CollectionSummary("EO:EUM:DAT:0665", "MTG FCI L1C HRFI"),
        ]

        result = runner.invoke(cli, ["collections", "--key", "test-key", "--secret", "test-secret"])

        assert result.exit_code == 0
        assert "EO:EUM:DAT:MSG:HRSEVIRI" in result.output
        assert "High Rate SEVIRI" in result.output
        assert "EO:EUM:DAT:0665" in result.output
        assert "MTG FCI L1C HRFI" in result.output
        assert "Found 2 collections" in result.output

    def test_collections_requires_credentials(self, runner):
        """collections command requires credentials."""
        result = runner.invoke(cli, ["collections"])
        assert result.exit_code == 1
        assert "credentials required" in result.output.lower()


class TestVersionOption:
    def test_version(self, runner):
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "eumdac-fetch" in result.output or "version" in result.output.lower()
