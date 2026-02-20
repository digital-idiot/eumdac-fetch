"""Tests for post-search filter registry and built-in filters."""

from __future__ import annotations

import asyncio
import textwrap
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest

from eumdac_fetch.filters import _REGISTRY, PostSearchFilterFn, build_filter, register

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_product(sensing_start: datetime) -> mock.MagicMock:
    """Create a mock eumdac product with a real datetime sensing_start."""
    p = mock.MagicMock()
    p.sensing_start = sensing_start
    p.__str__ = mock.MagicMock(return_value=str(sensing_start.timestamp()))
    p.size = 100
    return p


def _products_at_offsets(*hours: float, base: datetime | None = None) -> list:
    """Return mock products whose sensing_start is base + offset hours."""
    if base is None:
        base = datetime(2025, 1, 1, tzinfo=UTC)
    return [_make_product(base + timedelta(hours=h)) for h in hours]


# ---------------------------------------------------------------------------
# Registry: register + build_filter roundtrip
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_register_and_build_roundtrip(self):
        """Registered factory is callable via build_filter."""

        def my_factory(keep_every: int) -> PostSearchFilterFn:
            def _f(products):
                return products[::keep_every]

            return _f

        register("_test_every", my_factory)
        try:
            fn = build_filter("_test_every", {"keep_every": 2})
            result = fn([1, 2, 3, 4, 5])
            assert result == [1, 3, 5]
        finally:
            _REGISTRY.pop("_test_every", None)

    def test_build_filter_unknown_type_raises(self):
        """build_filter raises ValueError with helpful message for unknown types."""
        with pytest.raises(ValueError, match="Unknown post-search filter type 'nonexistent'"):
            build_filter("nonexistent", {})

    def test_build_filter_error_message_lists_available(self):
        """ValueError lists the available built-in filter names."""
        with pytest.raises(ValueError, match="sample_interval"):
            build_filter("no_such_filter", {})

    def test_build_filter_module_colon_callable(self):
        """build_filter with 'module:factory' dynamically imports and calls the factory."""
        sentinel_fn = lambda products: products  # noqa: E731

        fake_factory = mock.MagicMock(return_value=sentinel_fn)
        fake_module = mock.MagicMock()
        fake_module.my_factory = fake_factory

        with mock.patch("importlib.import_module", return_value=fake_module) as mock_import:
            result = build_filter("mymodule.sub:my_factory", {"keep_every": 3})

        mock_import.assert_called_once_with("mymodule.sub")
        fake_factory.assert_called_once_with(keep_every=3)
        assert result is sentinel_fn


# ---------------------------------------------------------------------------
# sample_interval built-in
# ---------------------------------------------------------------------------


class TestSampleInterval:
    def test_empty_list_returns_empty(self):
        fn = build_filter("sample_interval", {"interval_hours": 3})
        assert fn([]) == []

    def test_evenly_spaced_products_correct_subset(self):
        """With 10-min products and a 3-h bucket, keep one per 3-h window."""
        # 24 products, one every 10 minutes over 4 hours → 4 buckets of 1-h width
        # With 3-h interval: buckets are [0-3h), [3-6h)
        # Products at 0, 0.167, 0.333, ..., 3.83 hours
        # First bucket [0-3h): first product at 0 h
        # Second bucket [3-6h): first product at 3 h
        products = _products_at_offsets(*[i * (10 / 60) for i in range(25)])  # 0..4h in 10m steps
        fn = build_filter("sample_interval", {"interval_hours": 3})
        result = fn(products)
        assert len(result) == 2
        base = datetime(2025, 1, 1, tzinfo=UTC)
        assert result[0].sensing_start == base
        assert result[1].sensing_start == base + timedelta(hours=3)

    def test_products_within_same_bucket_only_first_kept(self):
        """Multiple products in the same bucket → only the earliest is kept."""
        products = _products_at_offsets(0, 0.5, 1.0, 1.5, 2.0)  # all within 3-h bucket
        fn = build_filter("sample_interval", {"interval_hours": 3})
        result = fn(products)
        assert len(result) == 1
        base = datetime(2025, 1, 1, tzinfo=UTC)
        assert result[0].sensing_start == base

    def test_each_product_in_own_bucket(self):
        """When products are each in their own bucket, all are kept."""
        products = _products_at_offsets(0, 3, 6, 9)
        fn = build_filter("sample_interval", {"interval_hours": 3})
        result = fn(products)
        assert len(result) == 4

    def test_unsorted_input_sorted_before_bucketing(self):
        """Products out of chronological order are sorted before bucketing."""
        base = datetime(2025, 1, 1, tzinfo=UTC)
        # Provide products in reverse order; the first in the bucket (t=0) must be kept
        products = _products_at_offsets(2, 1, 0)  # reversed
        fn = build_filter("sample_interval", {"interval_hours": 3})
        result = fn(products)
        assert len(result) == 1
        assert result[0].sensing_start == base


# ---------------------------------------------------------------------------
# Pipeline integration: filter applied before cache_search_results
# ---------------------------------------------------------------------------


class TestPipelineAppliesFilter:
    """Verify that _search_with_cache applies the post_search_filter before caching."""

    def _make_job(self, tmp_path, filter_cfg=None):
        from eumdac_fetch.models import (
            AppConfig,
            DownloadConfig,
            JobConfig,
            PostProcessConfig,
            SearchFilters,
        )

        job = JobConfig(
            name="test-job",
            collection="COL1",
            filters=SearchFilters(),
            download=DownloadConfig(directory=tmp_path / "downloads", parallel=1),
            post_process=PostProcessConfig(enabled=False),
            post_search_filter=filter_cfg,
        )
        return AppConfig(jobs=[job])

    def test_filter_applied_before_cache(self, tmp_path):
        """When post_search_filter is set, it runs before cache_search_results."""
        from eumdac_fetch.models import PostSearchFilterConfig
        from eumdac_fetch.pipeline import Pipeline

        filter_cfg = PostSearchFilterConfig(type="_pipeline_test_filter", params={"keep": 1})

        # A filter factory that keeps only the first product
        def _factory(keep: int):
            return lambda prods: prods[:keep]

        register("_pipeline_test_filter", _factory)

        try:
            app_config = self._make_job(tmp_path, filter_cfg)
            pipeline = Pipeline(token=mock.MagicMock(), config=app_config)

            mock_product = mock.MagicMock()
            mock_product.__str__ = mock.MagicMock(return_value="P1")
            mock_product.size = 10
            mock_product2 = mock.MagicMock()
            mock_product2.__str__ = mock.MagicMock(return_value="P2")
            mock_product2.size = 10

            mock_session = mock.MagicMock()
            mock_session.session_id = "abc"
            mock_session.session_dir = tmp_path / "sessions" / "abc"
            mock_session.download_dir = tmp_path / "downloads" / "COL1"
            mock_session.state_db_path = tmp_path / "sessions" / "abc" / "state.db"
            mock_session.log_path = tmp_path / "sessions" / "abc" / "session.log"
            mock_session.is_new = True
            mock_session.is_live = True
            mock_session.initialize = mock.MagicMock()
            mock_session.session_dir.mkdir(parents=True, exist_ok=True)
            mock_session.download_dir.mkdir(parents=True, exist_ok=True)

            with (
                mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
                mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
                mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
                mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
                mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
            ):
                mock_search = mock_search_cls.return_value
                mock_search.iter_products.return_value = [mock_product, mock_product2]
                mock_log.return_value = mock.MagicMock()
                mock_state = mock_state_cls.return_value
                mock_state.has_cached_search.return_value = False
                mock_state.close = mock.MagicMock()
                mock_dl = mock_dl_cls.return_value
                mock_dl.download_all = mock.AsyncMock()

                asyncio.run(pipeline.run())

                # Only the first product (after filter) should have been cached
                mock_state.cache_search_results.assert_called_once_with([mock_product], "COL1")
        finally:
            _REGISTRY.pop("_pipeline_test_filter", None)

    def test_no_filter_caches_all(self, tmp_path):
        """When no post_search_filter, all products are cached."""
        from eumdac_fetch.pipeline import Pipeline

        app_config = self._make_job(tmp_path, filter_cfg=None)
        pipeline = Pipeline(token=mock.MagicMock(), config=app_config)

        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")
        mock_product.size = 10
        mock_product2 = mock.MagicMock()
        mock_product2.__str__ = mock.MagicMock(return_value="P2")
        mock_product2.size = 10

        mock_session = mock.MagicMock()
        mock_session.session_id = "abc"
        mock_session.session_dir = tmp_path / "sessions" / "abc"
        mock_session.download_dir = tmp_path / "downloads" / "COL1"
        mock_session.state_db_path = tmp_path / "sessions" / "abc" / "state.db"
        mock_session.log_path = tmp_path / "sessions" / "abc" / "session.log"
        mock_session.is_new = True
        mock_session.is_live = True
        mock_session.initialize = mock.MagicMock()
        mock_session.session_dir.mkdir(parents=True, exist_ok=True)
        mock_session.download_dir.mkdir(parents=True, exist_ok=True)

        with (
            mock.patch("eumdac_fetch.pipeline.SearchService") as mock_search_cls,
            mock.patch("eumdac_fetch.pipeline.Session", return_value=mock_session),
            mock.patch("eumdac_fetch.pipeline.add_session_log_handler") as mock_log,
            mock.patch("eumdac_fetch.pipeline.StateDB") as mock_state_cls,
            mock.patch("eumdac_fetch.pipeline.DownloadService") as mock_dl_cls,
        ):
            mock_search = mock_search_cls.return_value
            mock_search.iter_products.return_value = [mock_product, mock_product2]
            mock_log.return_value = mock.MagicMock()
            mock_state = mock_state_cls.return_value
            mock_state.has_cached_search.return_value = False
            mock_state.close = mock.MagicMock()
            mock_dl = mock_dl_cls.return_value
            mock_dl.download_all = mock.AsyncMock()

            asyncio.run(pipeline.run())

            mock_state.cache_search_results.assert_called_once_with([mock_product, mock_product2], "COL1")


# ---------------------------------------------------------------------------
# Config parsing: post_search_filter block
# ---------------------------------------------------------------------------


class TestConfigParsing:
    def test_post_search_filter_parsed(self, tmp_path):
        """post_search_filter block is parsed with type and extra keys as params."""
        config_file = tmp_path / "job.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            jobs:
              - name: "filter-job"
                collection: "EO:EUM:DAT:0665"
                post_search_filter:
                  type: sample_interval
                  interval_hours: 3
            """)
        )
        from eumdac_fetch.config import load_config

        app_config = load_config(config_file)
        job = app_config.jobs[0]
        assert job.post_search_filter is not None
        assert job.post_search_filter.type == "sample_interval"
        assert job.post_search_filter.params == {"interval_hours": 3}

    def test_no_post_search_filter_is_none(self, tmp_path):
        """Jobs without post_search_filter have None."""
        config_file = tmp_path / "job.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            jobs:
              - name: "plain-job"
                collection: "EO:EUM:DAT:0665"
            """)
        )
        from eumdac_fetch.config import load_config

        app_config = load_config(config_file)
        assert app_config.jobs[0].post_search_filter is None

    def test_custom_filter_type_and_multiple_params(self, tmp_path):
        """Custom 'module:factory' type and multiple extra params are stored correctly."""
        config_file = tmp_path / "job.yaml"
        config_file.write_text(
            textwrap.dedent("""\
            jobs:
              - name: "custom-filter-job"
                collection: "EO:EUM:DAT:0665"
                post_search_filter:
                  type: "mymodule:my_factory"
                  keep_every: 5
                  min_size_kb: 100
            """)
        )
        from eumdac_fetch.config import load_config

        app_config = load_config(config_file)
        psf = app_config.jobs[0].post_search_filter
        assert psf is not None
        assert psf.type == "mymodule:my_factory"
        assert psf.params == {"keep_every": 5, "min_size_kb": 100}
