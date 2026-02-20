# Post-Processing

The `run` command supports two post-processing modes:

- **Local mode** (default) — download products to disk, then call a hook with the local file path.
- **Remote mode** — stream products directly from EUMETSAT without writing to disk, passing a `RemoteDataset` to a hook.

There is also a **search-only** mode (no hook required) that caches search results for a later download run.

## Local Post-Processing

### Writing a Post-Processor

A post-processor is a Python function with this signature:

```python
from pathlib import Path


# noinspection PyUnusedLocal
def my_processor(download_path: Path, product_id: str) -> None:
    """Process a downloaded satellite product.

    Args:
        download_path: Path to the downloaded file.
        product_id: The EUMETSAT product identifier.

    Raises:
        Any exception will mark the product as FAILED.
    """
    # Your processing logic here
    ...
```

The function receives:
- `download_path`: a `Path` object pointing to the downloaded file
- `product_id`: the string identifier of the product

If the function raises an exception, the product is marked as `FAILED` with the error message recorded in the state database. Otherwise, it transitions to `PROCESSED`.

### Using a Post-Processor

Pass the function as `module:function` to the `--post-processor` option:

```bash
eumdac-fetch run -c job.yaml --post-processor mypackage.convert:to_cog
```

The module must be importable from the current Python environment.

### Example: Extracting a NetCDF Variable

```python
# processors.py
from pathlib import Path
import zipfile

def extract_nc(download_path: Path, product_id: str) -> None:
    """Unzip the product archive and keep only .nc files."""
    out_dir = download_path.parent / "extracted" / product_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(download_path) as zf:
        for name in zf.namelist():
            if name.endswith(".nc"):
                zf.extract(name, out_dir)
```

```bash
eumdac-fetch run -c job.yaml --post-processor processors:extract_nc
```

### Example: Converting to Cloud-Optimised GeoTIFF

```python
# cog_convert.py
from pathlib import Path


# noinspection PyUnusedLocal
def to_cog(download_path: Path, product_id: str) -> None:
    """Convert a GeoTIFF product to Cloud-Optimised GeoTIFF."""
    import subprocess
    out = download_path.with_suffix(".cog.tif")
    subprocess.run(
        ["gdal_translate", "-of", "COG", str(download_path), str(out)],
        check=True,
    )
    download_path.unlink()  # remove original after conversion
```

```bash
eumdac-fetch run -c job.yaml --post-processor cog_convert:to_cog
```

### Pipeline Architecture

```{mermaid}
flowchart TB
    subgraph "Download (Producer)"
        DW1[Worker 1] --> Q[Async Queue]
        DW2[Worker 2] --> Q
        DW3[Worker 3] --> Q
        DW4[Worker 4] --> Q
    end

    subgraph "Post-Process (Consumer)"
        Q --> PP[Post-Processor]
        PP --> DB[(State DB)]
    end
```

- **Downloads** run in parallel (controlled by `download.parallel`)
- **Post-processing** runs sequentially in a separate thread
- A sentinel value signals the consumer when all downloads are complete
- SIGINT/SIGTERM triggers graceful shutdown — in-progress operations finish before exit

### Configuration

Enable local post-processing in the job config:

```yaml
#file: noinspection SpellCheckingInspection
jobs:
  - name: my-job
    collection: "EO:EUM:DAT:MSG:HRSEVIRI"
    post_process:
      enabled: true
      mode: local          # default; can be omitted
      output_dir: ./output
```

:::{note}
`post_process.enabled` must be `true` **and** `--post-processor` must be provided on the command line. If enabled without a processor, a warning is logged and only downloads run.
:::

### Product States (local mode)

```
PENDING → DOWNLOADING → DOWNLOADED → VERIFIED → PROCESSING → PROCESSED
                                                            ↘ FAILED
```

Post-processing only runs on `VERIFIED` products — those that have been downloaded and passed MD5 verification.

---

## Remote Mode

Remote mode lets you process EUMETSAT products **without downloading them to disk**. The pipeline builds an authenticated `RemoteDataset` for each product and passes it directly to your hook. This is ideal for streaming workflows (e.g. reading a slice of a NetCDF over HTTPS with xarray).

### Writing a Remote Post-Processor

```python
from eumdac_fetch import RemoteDataset
import xarray as xr


# noinspection PyUnusedLocal
def my_remote_processor(dataset: RemoteDataset, product_id: str) -> None:
    """Process a product by streaming it directly from EUMETSAT.

    Args:
        dataset: Authenticated lazy reader for the product's entries.
        product_id: The EUMETSAT product identifier.

    Raises:
        Any exception will mark the product as FAILED.
    """
    with dataset["my_channel.nc"] as f:
        ds = xr.open_dataset(f, engine="h5netcdf")
        # process ds…
```

The `dataset` argument is a `RemoteDataset` — a mapping of `{entry_name: RemoteData}`. Each entry is a context manager that returns a file-like handle backed by an authenticated HTTP session with automatic token refresh.

### Using a Remote Post-Processor

```bash
eumdac-fetch run -c job.yaml --remote-processor mymodule:my_remote_processor
```

You can also restrict which entries are included by setting `download.entries` in the YAML — the same glob patterns used for entry-level downloads are applied when building the `RemoteDataset`.

### Configuration

```yaml
#file: noinspection SpellCheckingInspection
jobs:
  - name: my-job
    collection: "EO:EUM:DAT:MSG:HRSEVIRI"
    download:
      entries:
        - "*.nc"          # only expose .nc entries in the RemoteDataset
    post_process:
      enabled: true
      mode: remote
      output_dir: ./output   # available to your hook if needed
```

`mode: remote` always skips downloading regardless of `download.enabled`.

### Pipeline Architecture (remote mode)

```{mermaid}
flowchart TB
    subgraph "Remote Producer"
        P[For each product] --> RD[Build RemoteDataset]
        RD --> Q[Async Queue]
    end

    subgraph "Remote Consumer"
        Q --> RP[Remote Post-Processor]
        RP --> DB[(State DB)]
    end
```

### Product States (remote mode)

```
PENDING → PROCESSING → PROCESSED
                     ↘ FAILED
```

DOWNLOADING, DOWNLOADED, and VERIFIED are never set in remote mode — products go directly from PENDING to PROCESSING.

---

## Search-Only Mode

Set `download.enabled: false` to run a **search-and-cache** pass without writing any files. Products are registered as `PENDING` in the session state database. A subsequent run with `download.enabled: true` (or `--download` on the CLI) picks up the cached results and downloads them.

```yaml
download:
  enabled: false
```

```bash
# Phase 1: search and cache
eumdac-fetch run -c job.yaml --no-download

# Phase 2: download the cached results
eumdac-fetch run -c job.yaml --download
```
