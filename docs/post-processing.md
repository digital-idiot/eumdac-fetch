# Post-Processing

The `run` command supports pluggable post-processing hooks that run automatically after each product is downloaded and verified.

## Writing a Post-Processor

A post-processor is a Python function with this signature:

```python
from pathlib import Path

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

## Using a Post-Processor

Pass the function as `module:function` to the `--post-processor` option:

```bash
eumdac-fetch run -c job.yaml --post-processor mypackage.convert:to_cog
```

The module must be importable from the current Python environment.

## Example: Extracting a NetCDF Variable

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

## Example: Converting to Cloud-Optimised GeoTIFF

```python
# cog_convert.py
from pathlib import Path

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

## Pipeline Architecture

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

## Configuration

Enable post-processing in the job config:

```yaml
jobs:
  - name: my-job
    collection: "EO:EUM:DAT:MSG:HRSEVIRI"
    post_process:
      enabled: true
      output_dir: ./output
```

:::{note}
`post_process.enabled` must be `true` **and** `--post-processor` must be provided on the command line. If enabled without a processor, a warning is logged and only downloads run.
:::

## Product States

```
PENDING → DOWNLOADING → DOWNLOADED → VERIFIED → PROCESSING → PROCESSED
                                                            ↘ FAILED
```

Post-processing only runs on `VERIFIED` products — those that have been downloaded and passed MD5 verification.
