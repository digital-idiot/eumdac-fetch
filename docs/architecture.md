# Architecture

eumdac-fetch is structured as a pipeline that flows from configuration through authentication, search, download, and optional post-processing.

## Pipeline Overview

```{mermaid}
flowchart LR
    A[YAML Config] --> B[Config Loader]
    B --> C[Authentication]
    C --> D[Search Service]
    D --> F[Post-Search Filter]
    F --> E[Session Manager]
    E --> G[State DB]
    G --> H[Async Downloader]
    H --> I[MD5 Verification]
    I --> J[Post-Processor]

    subgraph Session
        E
        G
    end

    subgraph Pipeline
        H
        I
        J
    end
```

## Module Overview

| Module | Responsibility |
|--------|---------------|
| `cli.py` | Click CLI entry point: `collections`, `info`, `search`, `download`, `run` |
| `config.py` | YAML loading, env var interpolation, path resolution |
| `filters.py` | Post-search filter registry, `build_filter()`, built-in `sample_interval` |
| `models.py` | Dataclasses for all configuration and state types |
| `auth.py` | `create_token()` and `get_token()` — lazy process-level token singleton backed by `ENV` |
| `env.py` | Credential discovery singleton: env vars → `.env` → `~/.eumdac/credentials` (key/secret only) |
| `remote.py` | `TokenRefreshingHTTPFileSystem`: fsspec HTTP filesystem with auto token refresh |
| `dataset.py` | `RemoteDataset` / `RemoteData`: high-level wrappers for authenticated remote file access |
| `search.py` | Collection info, product search, count, >10k date bisection |
| `session.py` | Deterministic session IDs, directory lifecycle, live detection |
| `state.py` | SQLite state database (thread-safe, WAL mode) |
| `downloader.py` | Async parallel downloads with resume, retry, MD5 |
| `pipeline.py` | Producer-consumer orchestration for `run` command |
| `display.py` | Rich console output (tables, progress) |
| `logging_config.py` | Structured logging with Rich and file handlers |

## Product Status State Machine

Each product moves through a defined set of states as it progresses through the pipeline:

```{mermaid}
stateDiagram-v2
    [*] --> PENDING
    PENDING --> DOWNLOADING
    DOWNLOADING --> DOWNLOADED
    DOWNLOADED --> VERIFIED
    VERIFIED --> PROCESSING
    PROCESSING --> PROCESSED
    PROCESSED --> [*]

    DOWNLOADING --> FAILED
    DOWNLOADED --> FAILED : MD5 mismatch
    PROCESSING --> FAILED

    DOWNLOADING --> PENDING : stale recovery
    FAILED --> PENDING : retry on re-run
```

## Producer-Consumer Pipeline (`run` command)

The `run` command uses an async producer-consumer architecture:

```{mermaid}
flowchart TB
    subgraph Producer
        S[Search] --> D[Download Workers]
        D --> Q[Async Queue]
    end

    subgraph Consumer
        Q --> PP[Post-Processor]
        PP --> Done[PROCESSED]
    end

    D -- "semaphore\n(parallel N)" --> D
    PP -- "sequential" --> PP
```

1. **Producer**: The download service runs N parallel workers (controlled by `parallel` setting). Each completed and verified product is pushed onto an `asyncio.Queue`.
2. **Consumer**: A single consumer pulls products from the queue and runs the post-processor function in a thread via `asyncio.to_thread()`.
3. **Sentinel**: When all downloads finish, a `None` sentinel is pushed to signal the consumer to stop.
4. **Graceful shutdown**: SIGINT/SIGTERM sets a shutdown event, allowing in-progress downloads to complete.

## Bearer Token Refresh

EUMDAC access tokens expire after a configurable validity period (default: 24 hours). `TokenRefreshingHTTPFileSystem` in `remote.py` wraps fsspec's `HTTPFileSystem` with transparent, async token refresh:

1. Every async file operation (`_cat_file`, `_info`, `_ls_real`, `_exists`) is wrapped with a single retry on HTTP 401.
2. On a 401, an `asyncio.Lock` serialises the refresh so concurrent workers don't all renew at once (thundering herd prevention).
3. After renewal, the aiohttp session is closed and set to `None` — forcing it to rebuild with the new `Authorization` header. The header in fsspec's kwargs dict is also updated for future sessions.
4. `encoded=True` is set at construction time to prevent double-encoding of percent-encoded characters (`%3A`, `%2F`) that appear in EUMDAC product URLs.

## Remote File Access (`RemoteData` / `RemoteDataset`)

`dataset.py` provides high-level context managers for authenticated streaming access to remote EUMDAC files (e.g. NetCDF products accessible via HTTPS).

**`RemoteData`** wraps a single URL with a `TokenRefreshingHTTPFileSystem`. It is a context manager that returns a file-like handle compatible with `xarray.open_dataset` and `h5netcdf`.

**`RemoteDataset`** holds a collection of named `RemoteData` entries and is the preferred public API. All entries share **one** `TokenRefreshingHTTPFileSystem` instance — meaning they share a single aiohttp session and coordinate token refresh. This is important for products split across multiple files (e.g. FCI granules).

```python
from eumdac_fetch import RemoteDataset

ds = RemoteDataset({
    "channel_06": "https://data.eumetsat.int/.../FCI_L1C...06.nc",
    "channel_09": "https://data.eumetsat.int/.../FCI_L1C...09.nc",
})

with ds["channel_06"] as f:
    import xarray as xr
    data = xr.open_dataset(f, engine="h5netcdf")
```

`RemoteDataset` accepts an optional `token_manager` argument. If omitted, it calls `create_access_token()` which uses credentials from the `ENV` singleton (env vars / `.env` / `~/.eumdac/credentials`).

## Search with >10k Results

The EUMETSAT API limits results to 10,000 per query. When `total_results` exceeds this, eumdac-fetch automatically bisects the date range:

```{mermaid}
flowchart TD
    A[Search with filters] --> B{total > 10k?}
    B -- No --> C[Return results]
    B -- Yes --> D[Compute midpoint]
    D --> E[Search first half]
    D --> F[Search second half]
    E --> G{> 10k?}
    F --> H{> 10k?}
    G -- No --> I[Collect results]
    G -- Yes --> J[Bisect again]
    H -- No --> K[Collect results]
    H -- Yes --> L[Bisect again]
```

**Requirements for bisection**: both `dtstart` and `dtend` must be set in the search filters. If they are not provided and results exceed 10,000, a `ValueError` is raised. Bisection continues recursively until each sub-range returns ≤ 10,000 results.

## Session Lifecycle

```{mermaid}
sequenceDiagram
    participant User
    participant CLI
    participant Session
    participant StateDB

    User->>CLI: eumdac-fetch download -c job.yaml
    CLI->>Session: Session(job_config)
    Session->>Session: Compute SHA-256 session ID
    alt New session
        Session->>Session: Create session directory
        CLI->>StateDB: Search & cache products
    else Existing session
        alt Live session
            CLI->>StateDB: Refresh search results
        else Non-live session
            CLI->>StateDB: Use cached results
        end
        CLI->>StateDB: Reset stale DOWNLOADING → PENDING
    end
    CLI->>StateDB: Get resumable products
    CLI->>CLI: Download with progress
```
