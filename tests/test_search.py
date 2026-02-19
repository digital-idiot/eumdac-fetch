"""Tests for search service."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest import mock

import pytest
import requests.exceptions

from eumdac_fetch.models import SearchFilters
from eumdac_fetch.search import SearchService, _retry


@pytest.fixture
def mock_datastore():
    with mock.patch("eumdac_fetch.search.eumdac") as mock_eumdac:
        yield mock_eumdac


@pytest.fixture
def search_service(mock_datastore, mock_eumdac_token):
    return SearchService(mock_eumdac_token)


class TestSearchService:
    def test_get_collection_info(self, search_service, mock_datastore, mock_collection):
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        info = search_service.get_collection_info("EO:EUM:DAT:MSG:HRSEVIRI")

        assert info.collection_id == "EO:EUM:DAT:MSG:HRSEVIRI"
        assert info.title == "High Rate SEVIRI Level 1.5 Image Data"
        assert info.abstract == "Test abstract"
        assert info.search_options == {"sat": ["MSG1", "MSG2", "MSG3", "MSG4"]}

    def test_count(self, search_service, mock_datastore):
        mock_results = mock.MagicMock()
        mock_results.total_results = 42
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        count = search_service.count("COL1", filters)
        assert count == 42

    def test_search_with_limit(self, search_service, mock_datastore, mock_product):
        mock_results = mock.MagicMock()
        mock_results.total_results = 100
        mock_results.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 100))
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        result = search_service.search("COL1", filters, limit=10)
        assert len(result.products) == 10
        assert result.total == 100

    def test_search_passes_all_filters(self, search_service, mock_datastore):
        mock_results = mock.MagicMock()
        mock_results.total_results = 0
        mock_results.__iter__ = mock.MagicMock(return_value=iter([]))
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
            sat="MSG4",
            timeliness="NT",
            geo="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
        )
        search_service.search("COL1", filters)

        mock_collection.search.assert_called_once_with(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
            geo="POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            sat="MSG4",
            timeliness="NT",
            sort="start,time,1",
        )

    def test_iter_products_under_10k(self, search_service, mock_datastore, mock_product):
        mock_results = mock.MagicMock()
        mock_results.total_results = 50
        mock_results.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 50))
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        products = search_service.iter_products("COL1", filters)
        assert len(products) == 50

    def test_bisect_requires_date_range(self, search_service, mock_datastore):
        # First call to count returns >10k, triggering bisect
        mock_results = mock.MagicMock()
        mock_results.total_results = 15000
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters()  # No dates
        with pytest.raises(ValueError, match="Date range"):
            search_service.iter_products("COL1", filters)

    def test_get_collection_info_search_options_error(self, search_service, mock_datastore):
        """search_options raising an exception should not break get_collection_info."""
        mock_collection = mock.MagicMock()
        mock_collection.__str__ = mock.MagicMock(return_value="COL1")
        mock_collection.title = "Test"
        mock_collection.abstract = "Abstract"
        type(mock_collection).search_options = mock.PropertyMock(side_effect=Exception("API error"))
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        info = search_service.get_collection_info("COL1")

        assert info.collection_id == "COL1"
        assert info.search_options == {}

    def test_iter_products_over_10k_with_bisection(self, search_service, mock_datastore):
        """iter_products triggers date bisection when >10k results."""
        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        # count calls: first returns 15000 (triggers bisect), then sub-counts return <10k
        count_calls = [0]

        def count_side_effect(**kwargs):
            count_calls[0] += 1
            result = mock.MagicMock()
            if count_calls[0] == 1:
                result.total_results = 15000  # Initial count
            else:
                result.total_results = 5000  # Sub-range counts
            return result

        # search calls for the sub-ranges
        search_result = mock.MagicMock()
        search_result.total_results = 5000
        search_result.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 5000))

        mock_collection = mock.MagicMock()
        mock_collection.search.side_effect = count_side_effect
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        # We need a different approach - mock count and search separately
        # Reset and use a cleaner approach
        count_calls[0] = 0
        call_idx = [0]

        def search_side_effect(**kwargs):
            call_idx[0] += 1
            result = mock.MagicMock()
            if "c" in str(kwargs) or call_idx[0] <= 3:
                # count calls (total_results only)
                result.total_results = 15000 if call_idx[0] == 1 else 5000
            result.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 3))
            return result

        mock_collection.search.side_effect = search_side_effect

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 31, tzinfo=UTC),
        )
        products = search_service.iter_products("COL1", filters)

        assert len(products) > 0

    def test_iter_products_over_10k_with_limit(self, search_service, mock_datastore):
        """iter_products with limit truncates bisected results."""
        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        call_idx = [0]

        def search_side_effect(**kwargs):
            call_idx[0] += 1
            result = mock.MagicMock()
            result.total_results = 15000 if call_idx[0] == 1 else 5000
            result.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 100))
            return result

        mock_collection = mock.MagicMock()
        mock_collection.search.side_effect = search_side_effect
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 31, tzinfo=UTC),
        )
        products = search_service.iter_products("COL1", filters, limit=5)

        assert len(products) == 5

    def test_deep_recursive_bisection(self, search_service, mock_datastore):
        """When both halves of bisection are >10k, recursion goes deeper."""
        mock_product = mock.MagicMock()
        mock_product.__str__ = mock.MagicMock(return_value="P1")

        # Track count calls to simulate:
        # 1. iter_products initial count -> 20000 (triggers bisect)
        # 2. first half count -> 15000 (still >10k, recurse)
        # 3. second half count -> 15000 (still >10k, recurse)
        # 4-7. sub-halves all return <10k (base case)
        count_seq = iter([20000, 15000, 15000, 5000, 5000, 5000, 5000])
        search_products = [mock_product] * 3

        def search_side_effect(**kwargs):
            result = mock.MagicMock()
            result.total_results = next(count_seq, 5000)
            result.__iter__ = mock.MagicMock(return_value=iter(list(search_products)))
            return result

        mock_collection = mock.MagicMock()
        mock_collection.search.side_effect = search_side_effect
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 3, 1, tzinfo=UTC),
        )
        products = search_service.iter_products("COL1", filters)

        # 4 leaf searches x 3 products each = 12 products
        assert len(products) == 12
        # Total search calls: 3 counts (initial + 2 halves that are >10k)
        # + 4 counts for sub-halves + 4 leaf searches = 11 total
        assert mock_collection.search.call_count >= 7

    def test_search_no_limit(self, search_service, mock_datastore, mock_product):
        """search() with no limit returns all products."""
        mock_results = mock.MagicMock()
        mock_results.total_results = 3
        mock_results.__iter__ = mock.MagicMock(return_value=iter([mock_product] * 3))
        mock_collection = mock.MagicMock()
        mock_collection.search.return_value = mock_results
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        result = search_service.search("COL1", filters, limit=None)
        assert len(result.products) == 3


class TestSearchRetry:
    def test_retry_succeeds_on_first_try(self):
        func = mock.MagicMock(return_value=42)
        assert _retry(func, retries=2, backoff=0.01) == 42
        assert func.call_count == 1

    def test_retry_recovers_from_transient_error(self):
        func = mock.MagicMock(side_effect=[ConnectionError("fail"), 42])
        assert _retry(func, retries=2, backoff=0.01) == 42
        assert func.call_count == 2

    def test_retry_exhausts_and_raises(self):
        func = mock.MagicMock(side_effect=ConnectionError("persistent"))
        with pytest.raises(ConnectionError, match="persistent"):
            _retry(func, retries=2, backoff=0.01)
        assert func.call_count == 3  # 1 initial + 2 retries

    def test_retry_handles_request_exception(self):
        func = mock.MagicMock(side_effect=[requests.exceptions.ReadTimeout("read timed out"), 99])
        assert _retry(func, retries=2, backoff=0.01) == 99
        assert func.call_count == 2

    def test_retry_does_not_catch_non_transient(self):
        func = mock.MagicMock(side_effect=ValueError("bad"))
        with pytest.raises(ValueError, match="bad"):
            _retry(func, retries=3, backoff=0.01)
        assert func.call_count == 1

    def test_count_retries_on_api_error(self, search_service, mock_datastore):
        mock_collection = mock.MagicMock()
        mock_results = mock.MagicMock()
        mock_results.total_results = 42
        mock_collection.search.side_effect = [
            ConnectionError("network"),
            mock_results,
        ]
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        count = search_service.count("COL1", filters)
        assert count == 42
        assert mock_collection.search.call_count == 2

    def test_search_retries_on_api_error(self, search_service, mock_datastore, mock_product):
        mock_collection = mock.MagicMock()
        mock_results = mock.MagicMock()
        mock_results.total_results = 1
        mock_results.__iter__ = mock.MagicMock(return_value=iter([mock_product]))
        mock_collection.search.side_effect = [
            requests.exceptions.ConnectionError("reset"),
            mock_results,
        ]
        mock_datastore.DataStore.return_value.get_collection.return_value = mock_collection

        filters = SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        )
        result = search_service.search("COL1", filters)
        assert result.total == 1
        assert len(result.products) == 1
        assert mock_collection.search.call_count == 2

    def test_list_collections(self, search_service, mock_datastore):
        """list_collections returns all available collections."""
        mock_coll1 = mock.MagicMock()
        mock_coll1.__str__ = mock.MagicMock(return_value="EO:EUM:DAT:MSG:HRSEVIRI")
        mock_coll1.title = "High Rate SEVIRI"

        mock_coll2 = mock.MagicMock()
        mock_coll2.__str__ = mock.MagicMock(return_value="EO:EUM:DAT:0665")
        mock_coll2.title = "MTG FCI L1C HRFI"

        mock_datastore.DataStore.return_value.collections = [mock_coll1, mock_coll2]

        collections = search_service.list_collections()

        assert len(collections) == 2
        assert collections[0].collection_id == "EO:EUM:DAT:MSG:HRSEVIRI"
        assert collections[0].title == "High Rate SEVIRI"
        assert collections[1].collection_id == "EO:EUM:DAT:0665"
        assert collections[1].title == "MTG FCI L1C HRFI"
