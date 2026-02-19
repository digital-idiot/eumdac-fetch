"""Tests for display utilities."""

from __future__ import annotations

from unittest import mock

from eumdac_fetch.display import display_collection_info, display_product_count, display_search_results
from eumdac_fetch.search import CollectionInfo


class TestDisplayCollectionInfo:
    def test_displays_title_and_abstract(self, capsys):
        info = CollectionInfo(
            collection_id="EO:EUM:DAT:TEST",
            title="Test Collection",
            abstract="A test abstract",
            search_options={},
        )
        display_collection_info(info)
        # Rich outputs to its own console, so we check it doesn't raise

    def test_displays_search_options(self):
        info = CollectionInfo(
            collection_id="EO:EUM:DAT:TEST",
            title="Test",
            abstract="Abstract",
            search_options={"sat": ["MSG4", "MSG3"], "timeliness": "NT"},
        )
        # Should not raise
        display_collection_info(info)


class TestDisplaySearchResults:
    def test_displays_empty_results(self):
        display_search_results([], 0, {})

    def test_displays_products(self):
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="PROD-001")
        product.size = 1234
        display_search_results([product], 1, {"dtstart": "2024-01-01"})

    def test_handles_product_without_size(self):
        product = mock.MagicMock()
        product.__str__ = mock.MagicMock(return_value="PROD-001")
        product.size = mock.PropertyMock(side_effect=AttributeError("no size"))
        display_search_results([product], 1, {})


class TestDisplayProductCount:
    def test_displays_count(self):
        display_product_count("EO:EUM:DAT:TEST", 42)
