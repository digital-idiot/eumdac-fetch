# Configuration Reference

eumdac-fetch uses a YAML configuration file that defines logging and one or more download jobs.
Credentials are **never stored in the config file** — see the Credentials section below.

## Full Example

```yaml
logging:
  level: INFO
  file: fetch.log

jobs:
  - name: seviri-europe
    collection: "EO:EUM:DAT:MSG:HRSEVIRI"
    limit: 500
    filters:
      dtstart: "2024-01-01T00:00:00Z"
      dtend: "2024-01-31T23:59:59Z"
      sat: "MSG4"
      timeliness: "NT"
    post_search_filter:
      type: sample_interval
      interval_hours: 3
    download:
      directory: ./downloads/seviri
      parallel: 4
      resume: true
      verify_md5: true
      max_retries: 3
      retry_backoff: 2.0
      timeout: 300
    post_process:
      enabled: true
      output_dir: ./output/seviri
```

## Credentials

Credentials are **not stored in the config file**. Including a `credentials:` key raises an error at load time.

eumdac-fetch discovers credentials automatically at startup in this priority order:

| Priority | Source | Variables |
|----------|--------|-----------|
| 1 | Environment variables | `EUMDAC_KEY`, `EUMDAC_SECRET`, `EUMDAC_TOKEN_VALIDITY` |
| 2 | `.env` file | Same variable names, `KEY=value` format |
| 3 | `~/.eumdac/credentials` | Single line: `key,secret` (key/secret only) |

`EUMDAC_TOKEN_VALIDITY` controls how long each access token is valid (in seconds, default 86400). It can only be set via env var or `.env` file — the credentials file always contains only the key/secret pair.

If no credentials are found, a warning is emitted and commands that require authentication will fail with a clear error.

```bash
# Recommended: environment variables
export EUMDAC_KEY="your-api-key"
export EUMDAC_SECRET="your-api-secret"
export EUMDAC_TOKEN_VALIDITY=3600   # optional: 1-hour tokens

# Alternative: .env file (add to .gitignore)
echo "EUMDAC_KEY=your-api-key" >> .env
echo "EUMDAC_SECRET=your-api-secret" >> .env
echo "EUMDAC_TOKEN_VALIDITY=3600" >> .env

# Alternative: credentials file (key and secret only)
mkdir -p ~/.eumdac
echo "your-api-key,your-api-secret" > ~/.eumdac/credentials
```

You can also override token validity per-invocation with the `--validity` flag on any command:

```bash
eumdac-fetch search -c job.yaml --validity 3600
eumdac-fetch download -c job.yaml --validity 7200
```

## Logging

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | `INFO` | Log level: DEBUG, INFO, WARNING, ERROR |
| `file` | string | null | Optional log file path |

## Jobs

Each job defines a collection to search and download from.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `default` | Job identifier (used in session and state tracking) |
| `collection` | string | **required** | EUMETSAT collection ID (e.g. `EO:EUM:DAT:MSG:HRSEVIRI`) |
| `limit` | integer | null | Max products to download (null = all matching) |

### Search Filters

All filters are optional. They map directly to the eumdac `collection.search()` API.

| Field | Type | Description |
|-------|------|-------------|
| `dtstart` | ISO 8601 datetime | Start of time range |
| `dtend` | ISO 8601 datetime | End of time range |
| `geo` | WKT string | Geographic filter (e.g. `POLYGON((...))`) |
| `sat` | string | Satellite name (e.g. `MSG4`, `Metop-C`) |
| `timeliness` | string | Timeliness class (e.g. `NT`, `NRT`) |
| `filename` | string | Filename pattern filter |
| `cycle` | integer | Repeat cycle number |
| `orbit` | integer | Orbit number |
| `relorbit` | integer | Relative orbit number |
| `product_type` | string | Product type filter |
| `publication` | string | Publication status |
| `download_coverage` | string | Download coverage filter |
| `sort` | string | Sort order (default: `start,time,1`) |

### Download Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `directory` | path | `./downloads` | Download directory (relative to config file) |
| `parallel` | integer | `4` | Number of concurrent downloads |
| `resume` | boolean | `true` | Resume interrupted downloads via byte-range requests |
| `verify_md5` | boolean | `true` | Verify MD5 checksum after download (whole products only) |
| `max_retries` | integer | `3` | Maximum retry attempts for transient errors |
| `retry_backoff` | float | `2.0` | Base backoff in seconds (doubles each retry) |
| `timeout` | float | `300.0` | Per-product download timeout in seconds |
| `entries` | list of strings | `null` | Glob patterns for entry-level downloads (see below) |

#### Entry-Level Downloads

By default eumdac-fetch downloads **whole products** — typically a ZIP archive containing all data files for that product.

The `entries` field lets you download **individual files from within a product** by providing a list of glob patterns matched against entry names:

```yaml
download:
  directory: ./downloads
  entries:
    - "*.nc"           # all NetCDF files
    - "*.nat"          # all native format files
    - "W_XX-EUMETSAT*" # files matching a naming convention
```

When `entries` is set:
- Only matching files are downloaded; the rest of the product archive is skipped.
- Each entry is tracked independently in the state database. The product status reflects the aggregate of its entries.
- MD5 verification is **not** applied to individual entries. The MD5 in EUMETSAT metadata covers the complete product archive, not individual files.
- If the pattern matches zero entries for a product, that product is skipped (no error).

Leave `entries` unset (or `null`) to download the full product archive.

### Post-Search Filter

An optional filter applied to the search results **before** they are cached and passed
to the downloader. See {doc}`filters` for full details.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | Filter type: a built-in name (e.g. `sample_interval`) or `module:factory` |
| *(extra keys)* | any | — | Forwarded as keyword arguments to the filter factory |

Built-in types:

| Type | Parameters | Description |
|------|-----------|-------------|
| `sample_interval` | `interval_hours: float` | Keep one product per N-hour time bucket |

Example:

```yaml
post_search_filter:
  type: sample_interval
  interval_hours: 3
```

### Post-Processing

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable post-processing in `run` mode |
| `output_dir` | path | `./output` | Output directory for processed files |

## Environment Variable Interpolation

Any string value in the YAML can use `${ENV_VAR}` syntax:

```yaml
download:
  directory: "${DATA_DIR}/satellite"
```

If the referenced variable is not set, configuration loading fails with an error message identifying the missing variable.

## Path Resolution

Relative paths in `download.directory` and `post_process.output_dir` are resolved relative to the directory containing the YAML config file, not the current working directory.
