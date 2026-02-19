# Getting Started

## Prerequisites

- Python 3.14 or later
- [pixi](https://pixi.sh/) package manager (recommended) or pip
- EUMETSAT Data Store API credentials ([register here](https://api.eumetsat.int/))

## Installation

### With pixi (recommended)

```bash
git clone https://github.com/digital-idiot/eumdac-fetch.git
cd eumdac-fetch
pixi install
```

### With pip

```bash
pip install -e .
```

:::{note}
The `eumdac` library is only available from conda-forge, so pixi is the recommended installation method. If using pip, install eumdac separately via conda first.
:::

## Setting Up Credentials

You need an API key and secret from the [EUMETSAT API Portal](https://api.eumetsat.int/).

Credentials are **never stored in the config file** — eumdac-fetch discovers them automatically at startup from the following sources (in priority order):

### Option 1: Environment Variables (recommended)

```bash
export EUMDAC_KEY="your-api-key"
export EUMDAC_SECRET="your-api-secret"
export EUMDAC_TOKEN_VALIDITY=86400   # optional: token lifetime in seconds (default 86400)
```

### Option 2: `.env` File

Create a `.env` file in your working directory (add it to `.gitignore`):

```
EUMDAC_KEY=your-api-key
EUMDAC_SECRET=your-api-secret
EUMDAC_TOKEN_VALIDITY=86400
```

### Option 3: `~/.eumdac/credentials` File

```bash
mkdir -p ~/.eumdac
echo "your-api-key,your-api-secret" > ~/.eumdac/credentials
```

The file must contain the key and secret on a single line, separated by a comma. Token validity cannot be set here — use Option 1 or 2 for that.

:::{warning}
Never commit credentials to version control. Do not put them in your job YAML — the config loader will raise an error if a `credentials:` key is found.
:::

## First Run

### 1. Explore a Collection

```bash
eumdac-fetch info EO:EUM:DAT:MSG:HRSEVIRI --key $EUMDAC_KEY --secret $EUMDAC_SECRET
```

This shows the collection's title, description, and available search filters.

### 2. Create a Job Config

Create a file called `job.yaml`:

```yaml
jobs:
  - name: seviri-sample
    collection: "EO:EUM:DAT:MSG:HRSEVIRI"
    filters:
      dtstart: "2024-01-01T00:00:00Z"
      dtend: "2024-01-01T01:00:00Z"
    download:
      directory: ./downloads
      parallel: 4
      resume: true
      verify_md5: true
```

### 3. Search (Dry Run)

Preview matching products without downloading:

```bash
eumdac-fetch search -c job.yaml
```

Use `--count-only` to see just the total count:

```bash
eumdac-fetch search -c job.yaml --count-only
```

### 4. Download

```bash
eumdac-fetch download -c job.yaml
```

The download progress is displayed with Rich progress bars. If interrupted, re-run the same command to resume automatically.

### 5. Full Pipeline (Optional)

If you have a post-processing function:

```bash
eumdac-fetch run -c job.yaml --post-processor mymodule:convert_to_cog
```

See {doc}`post-processing` for details on writing post-processor hooks.
