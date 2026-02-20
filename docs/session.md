# Session Management

eumdac-fetch uses deterministic sessions to enable automatic resume across runs.

## Session Identity

A session ID is a 12-character hex string derived from an SHA-256 hash of the job configuration (excluding credentials). This means:

- The **same YAML config** always produces the **same session ID**
- Re-running the same config automatically resumes from where it left off
- Changing any job parameter (collection, filters, download settings) creates a new session

## Directory Structure

Sessions are stored under `~/.eumdac-fetch/` (configurable via `EUMDAC_FETCH_HOME`):

```
~/.eumdac-fetch/
├── sessions/
│   └── a1b2c3d4e5f6/          # Session ID
│       ├── state.db            # SQLite state database
│       ├── session.log         # Session-scoped log file
│       └── config.yaml         # Frozen job config (for reference)
└── downloads/
    └── EO:EUM:DAT:MSG:HRSEVIRI/  # Downloads organized by collection
        ├── product-001.zip
        └── product-002.zip
```

## Live Sessions

A session is classified as **live** when its date range extends into recent time:

- `dtend` is not set (open-ended search) — always live
- `dtend` is within **3 hours** of the current time — live

Live sessions **refresh search results** on every run: the search API is re-queried and the cached product list is replaced, ensuring any newly published products are included. Non-live sessions use cached search results from the state database to avoid redundant API calls.

## Resume Behavior

When a session already exists:

1. **Stale download recovery**: Products stuck in `DOWNLOADING` status (from a killed process) are automatically reset to `PENDING` before any work begins.
2. **Search caching**: For non-live sessions, cached search results are reused instead of re-querying the API.
3. **Skip completed**: Products already in `VERIFIED` or `PROCESSED` status are skipped.
4. **Resumable products**: Products in `PENDING`, `DOWNLOADING`, or `FAILED` status are queued for download. `FAILED` products are retried automatically on the next run — no manual intervention required.

```{mermaid}
flowchart TD
    A[Start session] --> B{Session exists?}
    B -- No --> C[Create new session]
    C --> D[Search API]
    D --> E[Cache results]
    E --> F[Download all]

    B -- Yes --> G[Resume session]
    G --> H[Reset stale downloads]
    H --> I{Live session?}
    I -- Yes --> J[Refresh search]
    I -- No --> K[Use cached results]
    J --> L[Filter to resumable]
    K --> L
    L --> M[Download remaining]
```

## Byte-Range Resume

For individual file downloads, eumdac-fetch supports byte-range resume:

1. If a partial file exists and `resume: true`, the download continues from the last byte using an HTTP `Range` request.
2. If the server doesn't support byte-range requests, the download falls back to re-downloading the file from scratch.
3. The `bytes_downloaded` field in the state database tracks progress and is used to determine the byte offset on resume.
4. Setting `resume: false` in the config disables this — any existing partial file is overwritten.

**MD5 verification scope**: MD5 verification (`verify_md5: true`) applies only to whole-product downloads. When downloading individual entries via the `entries` field, MD5 verification is skipped because the hash in EUMETSAT metadata covers the complete product archive, not individual entry files.
