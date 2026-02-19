# State Database

eumdac-fetch uses a SQLite database to track the status of every product through the download pipeline. The database is stored per-session at `~/.eumdac-fetch/sessions/<session-id>/state.db`.

## Schema

### `products` table

Tracks per-product download and processing state.

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | TEXT | Product identifier (primary key with job_name) |
| `job_name` | TEXT | Job name (primary key with product_id) |
| `collection` | TEXT | Collection ID |
| `size_kb` | REAL | Product size in kilobytes (from metadata) |
| `md5` | TEXT | Expected MD5 hash |
| `bytes_downloaded` | INTEGER | Bytes downloaded so far |
| `status` | TEXT | Current status (see below) |
| `download_path` | TEXT | Path to downloaded file |
| `error_message` | TEXT | Error details if FAILED |
| `created_at` | TEXT | ISO 8601 creation timestamp |
| `updated_at` | TEXT | ISO 8601 last update timestamp |

### `search_results` table

Caches search result metadata for session resume.

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | TEXT | Product identifier (primary key) |
| `collection` | TEXT | Collection ID |
| `size_kb` | REAL | Product size in kilobytes |
| `sensing_start` | TEXT | Sensing start time |
| `sensing_end` | TEXT | Sensing end time |
| `cached_at` | TEXT | When the result was cached |

## Product Status Values

| Status | Description |
|--------|-------------|
| `pending` | Registered but not yet downloading |
| `downloading` | Download in progress |
| `downloaded` | Download complete, awaiting verification |
| `verified` | MD5 verification passed |
| `processing` | Post-processor is running |
| `processed` | Post-processing complete |
| `failed` | An error occurred (see `error_message`) |

## Thread Safety

The `StateDB` class uses thread-local SQLite connections, enabling safe concurrent access from multiple download worker threads. WAL (Write-Ahead Logging) mode is enabled for better concurrent read/write performance.

## Stale Download Recovery

When a process is killed (e.g. Ctrl+C, OOM), some products may be left in `downloading` status. On the next run, `reset_stale_downloads()` moves these back to `pending` so they are retried.
