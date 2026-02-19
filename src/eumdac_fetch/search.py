"""Search service with full eumdac filter support and >10k result handling."""

from __future__ import annotations

import itertools
import logging
import time
from dataclasses import dataclass

import eumdac
import requests.exceptions

from eumdac_fetch.models import SearchFilters

logger = logging.getLogger("eumdac_fetch")

# Transient exceptions worth retrying during search API calls
SEARCH_RETRYABLE = (
    ConnectionError,
    TimeoutError,
    OSError,
    requests.exceptions.RequestException,
)

DEFAULT_SEARCH_RETRIES = 3
DEFAULT_SEARCH_BACKOFF = 2.0


@dataclass
class CollectionInfo:
    """Information about a collection."""

    collection_id: str
    title: str
    abstract: str
    search_options: dict


@dataclass
class CollectionSummary:
    """Summary of a collection for listing."""

    collection_id: str
    title: str


@dataclass
class SearchResult:
    """Result of a product search."""

    total: int
    products: list
    filters_used: dict


def _retry(func, retries: int = DEFAULT_SEARCH_RETRIES, backoff: float = DEFAULT_SEARCH_BACKOFF):
    """Call *func* with retry on transient errors.

    Args:
        func: A zero-argument callable.
        retries: Maximum number of retries (total attempts = retries + 1).
        backoff: Base backoff in seconds (doubles each retry).

    Returns:
        The return value of *func*.
    """
    last_error = None
    for attempt in range(retries + 1):
        try:
            return func()
        except SEARCH_RETRYABLE as e:
            last_error = e
            if attempt < retries:
                wait = backoff * (2**attempt)
                logger.warning(
                    "Search API error (attempt %d/%d): %s. Retrying in %.1fs",
                    attempt + 1,
                    retries + 1,
                    e,
                    wait,
                )
                time.sleep(wait)
    raise last_error  # type: ignore[misc]


class SearchService:
    """Handles product searching with full filter support."""

    def __init__(self, token: eumdac.AccessToken):
        self.datastore = eumdac.DataStore(token)

    # noinspection PyUnresolvedReferences
    def get_collection(self, collection_id: str) -> eumdac.Collection:
        # noinspection SpellCheckingInspection
        """Get a collection by ID.

        Args:
            collection_id: The collection identifier (e.g. "EO:EUM:DAT:MSG:HRSEVIRI").

        Returns:
            The eumdac Collection object.
        """
        return self.datastore.get_collection(collection_id)

    def get_collection_info(self, collection_id: str) -> CollectionInfo:
        """Get detailed information about a collection.

        Args:
            collection_id: The collection identifier.

        Returns:
            CollectionInfo with title, abstract, and available search options.
        """
        collection = self.get_collection(collection_id)
        search_options = {}
        # noinspection PyBroadException
        try:
            search_options = collection.search_options
        except Exception:
            logger.warning("Could not retrieve search options for %s", collection_id)

        return CollectionInfo(
            collection_id=str(collection),
            title=collection.title,
            abstract=collection.abstract,
            search_options=search_options,
        )

    def list_collections(self) -> list[CollectionSummary]:
        """List all available collections.

        Returns:
            List of CollectionSummary objects with ID and title for each collection.
        """
        collections = []
        for collection in self.datastore.collections:
            collections.append(
                CollectionSummary(
                    collection_id=str(collection),
                    title=collection.title,
                )
            )
        return collections

    def count(self, collection_id: str, filters: SearchFilters) -> int:
        """Get total result count without fetching products (single API call with c=0).

        Args:
            collection_id: The collection identifier.
            filters: Search filter parameters.

        Returns:
            Total number of matching products.
        """
        collection = self.get_collection(collection_id)
        kwargs = filters.to_search_kwargs()

        def _do_count():
            results = collection.search(**kwargs)
            return results.total_results

        return _retry(_do_count)

    def search(self, collection_id: str, filters: SearchFilters, limit: int | None = None) -> SearchResult:
        """Search for products matching filters.

        Args:
            collection_id: The collection identifier.
            filters: Search filter parameters.
            limit: Maximum number of products to return. None for all.

        Returns:
            SearchResult with total count and product list.
        """
        collection = self.get_collection(collection_id)
        kwargs = filters.to_search_kwargs()

        def _do_search():
            results = collection.search(**kwargs)
            _total = results.total_results
            _prods = list(itertools.islice(results, limit)) if limit is not None else list(results)
            return _total, _prods

        total, products = _retry(_do_search)

        logger.info("Search returned %d/%d products for %s", len(products), total, collection_id)

        return SearchResult(
            total=total,
            products=products,
            filters_used=kwargs,
        )

    def iter_products(self, collection_id: str, filters: SearchFilters, limit: int | None = None) -> list:
        """Iterate matching products, handling >10k results via date bisection.

        When total_results exceeds 10,000 (the eumdac API limit per query),
        the date range is recursively bisected into smaller windows.

        Args:
            collection_id: The collection identifier.
            filters: Search filter parameters.
            limit: Maximum number of products to return. None for all.

        Returns:
            List of matching products.
        """
        total = self.count(collection_id, filters)

        if total <= 10000:
            result = self.search(collection_id, filters, limit=limit)
            return result.products

        logger.info("Total results (%d) exceeds 10k, using date bisection", total)
        products = self._bisect_search(collection_id, filters)
        if limit is not None:
            return products[:limit]
        return products

    def _bisect_search(self, collection_id: str, filters: SearchFilters) -> list:
        """Recursively bisect date range to handle >10k results."""
        if filters.dtstart is None or filters.dtend is None:
            # noinspection SpellCheckingInspection
            raise ValueError("Date range (dtstart, dtend) is required for >10k result handling")

        midpoint = filters.dtstart + (filters.dtend - filters.dtstart) / 2

        # First half
        # noinspection SpellCheckingInspection
        first_filters = SearchFilters(**{**filters.__dict__, "dtend": midpoint})
        first_count = self.count(collection_id, first_filters)

        # Second half
        # noinspection SpellCheckingInspection
        second_filters = SearchFilters(**{**filters.__dict__, "dtstart": midpoint})
        second_count = self.count(collection_id, second_filters)

        products = []

        if first_count <= 10000:
            result = self.search(collection_id, first_filters)
            products.extend(result.products)
        else:
            products.extend(self._bisect_search(collection_id, first_filters))

        if second_count <= 10000:
            result = self.search(collection_id, second_filters)
            products.extend(result.products)
        else:
            products.extend(self._bisect_search(collection_id, second_filters))

        return products
