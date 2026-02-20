# Post-Search Filters

Post-search filters let you thin or transform the product list returned by a search
**before** it is cached and passed to the downloader. This is useful when a collection
publishes data far more frequently than you need — for example, MTG FCI produces one
product every 10 minutes, but you only want one per 3 hours.

The filter runs on every fresh search (and on live-session re-searches), so the cached
state always reflects the filtered set.

## Pipeline position

```{mermaid}
flowchart LR
    A[Search] --> B[Post-Search Filter]
    B --> C[Cache to State DB]
    C --> D[Downloader]
```

## YAML configuration

Add a `post_search_filter` block to any job:

```yaml
#file: noinspection SpellCheckingInspection
jobs:
  - name: mtg-fci-hrfi-3h
    collection: "EO:EUM:DAT:0665"
    filters:
      dtstart: "2025-01-01T00:00:00Z"
      dtend: "2026-01-01T00:00:00Z"
    post_search_filter:
      type: sample_interval
      interval_hours: 3
    download:
      directory: ./downloads
```

The `type` field selects the filter. All remaining keys are forwarded to the filter
factory as keyword arguments (`params`).

## Built-in filters

### `sample_interval`

Temporally subsamples a product list to at most one product per time bucket.

| Parameter        | Type  | Description                                            |
|------------------|-------|--------------------------------------------------------|
| `interval_hours` | float | Bucket width in hours (e.g. `3` → one product per 3 h) |

Products are sorted ascending by `sensing_start` and the **first** product that falls
into each bucket is kept. Products within the same bucket are discarded.

```yaml
post_search_filter:
  type: sample_interval
  interval_hours: 3      # keep one product every 3 hours
```

**Example** — MTG FCI at 10-min cadence, subsampled to 3 h:

| Sensing start | Bucket (3 h) | Kept? |
|---------------|--------------|-------|
| 00:00         | 0            | ✓     |
| 00:10         | 0            | —     |
| 00:20         | 0            | —     |
| …             | 0            | —     |
| 03:00         | 1            | ✓     |
| 03:10         | 1            | —     |
| …             |              |       |

## User-defined filters (programmatic API)

### Option A — Dynamic import via YAML (`module:factory`)

Any filter factory that is importable from the current Python environment can be
referenced in YAML without any registration step:

```yaml
post_search_filter:
  type: mypackage.filters:daytime_only_factory
  solar_elevation_min: 10
```

`build_filter` splits on `:`, imports `mypackage.filters`, and calls
`daytime_only_factory(solar_elevation_min=10)` — which must return a
`PostSearchFilterFn`.

### Option B — `register()` (Python API only)

When using eumdac-fetch as a Python library you can register a factory under a short
alias so your code can use a simple name rather than the full module path:

```python
from eumdac_fetch import register, PostSearchFilterFn


def daytime_only_factory(solar_elevation_min: float = 10) -> PostSearchFilterFn:
    # noinspection PyUnresolvedReferences
    def _filter(products: list) -> list:
        return [p for p in products if _solar_elevation(p) >= solar_elevation_min]

    return _filter


register("daytime_only", daytime_only_factory)
```

After registration, you can use `type: daytime_only` in YAML **or** build it
programmatically:

```python
from eumdac_fetch.filters import build_filter

fn = build_filter("daytime_only", {"solar_elevation_min": 15})
# noinspection PyUnresolvedReferences
filtered = fn(products)
```

## Filter function signature

A filter factory must be a callable that:

1. Accepts keyword arguments matching the `params` dict from YAML
2. Returns a `PostSearchFilterFn`:

```python
from eumdac_fetch import PostSearchFilterFn


# noinspection PyUnusedLocal
def my_factory(**params) -> PostSearchFilterFn:
    def _filter(products: list) -> list:
        # products: list of eumdac product objects
        # Return a subset (or transformed list)
        return [p for p in products if ...]

    return _filter
```

Products are standard `eumdac` product objects. Commonly used attributes:

| Attribute       | Type       | Description                 |
|-----------------|------------|-----------------------------|
| `sensing_start` | `datetime` | Start of the sensing window |
| `sensing_end`   | `datetime` | End of the sensing window   |
| `size`          | `int`      | Product size in KB          |

:::{note}
The filter receives all products that matched the search criteria (up to `limit`).
Filtering happens after `iter_products()` returns, before `cache_search_results()` is
called. The filtered list is what gets stored in the session state database.
:::
