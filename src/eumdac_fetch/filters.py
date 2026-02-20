"""Post-search filter registry and built-in filters."""

from __future__ import annotations

import importlib
import math
from collections.abc import Callable

# PostSearchFilterFn: takes a list of products, returns a filtered list
PostSearchFilterFn = Callable[[list], list]

# Registry: maps type name -> factory function
_REGISTRY: dict[str, Callable[..., PostSearchFilterFn]] = {}


def register(name: str, factory: Callable[..., PostSearchFilterFn]) -> None:
    """Register a post-search filter factory under the given name.

    Args:
        name: The filter type name used in YAML config.
        factory: A callable that accepts filter params as kwargs and returns a
            PostSearchFilterFn.
    """
    _REGISTRY[name] = factory


def build_filter(type_: str, params: dict) -> PostSearchFilterFn:
    """Build a PostSearchFilterFn from a type name and params dict.

    If *type_* contains ':', it is treated as ``'module:factory_callable'`` and
    dynamically imported, otherwise the built-in registry is consulted.

    Args:
        type_: Filter type name or ``'module:factory'`` import path.
        params: Keyword arguments forwarded to the factory.

    Returns:
        A PostSearchFilterFn ready to be called with a product list.

    Raises:
        ValueError: If *type_* is not found in the registry (and does not contain ':').
    """
    if ":" in type_:
        module_path, _, factory_name = type_.partition(":")
        module = importlib.import_module(module_path)
        factory = getattr(module, factory_name)
        return factory(**params)

    if type_ not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise ValueError(
            f"Unknown post-search filter type '{type_}'. "
            f"Available built-ins: {available}. "
            f"For custom filters use 'module:factory_name' syntax."
        )

    return _REGISTRY[type_](**params)


def _sample_interval_factory(interval_hours: float) -> PostSearchFilterFn:
    """Factory for temporal subsampling.

    Keeps one product per time bucket of *interval_hours* hours. Products are
    sorted ascending by ``sensing_start`` and the first product in each bucket
    is retained.

    Args:
        interval_hours: Bucket width in hours (e.g. 3.0 → one product per 3 h).
    """
    interval_secs = interval_hours * 3600.0

    def _filter(products: list) -> list:
        if not products:
            return []

        sorted_products = sorted(products, key=lambda p: p.sensing_start)

        seen_buckets: set[int] = set()
        result = []
        for product in sorted_products:
            bucket = math.floor(product.sensing_start.timestamp() / interval_secs)
            if bucket not in seen_buckets:
                seen_buckets.add(bucket)
                result.append(product)

        return result

    return _filter


# Register built-in filters at module load time
register("sample_interval", _sample_interval_factory)
