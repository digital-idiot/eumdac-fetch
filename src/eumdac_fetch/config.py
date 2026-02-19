"""YAML configuration loading with environment variable interpolation."""

from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

from eumdac_fetch.models import (
    AppConfig,
    DownloadConfig,
    JobConfig,
    LoggingConfig,
    PostProcessConfig,
    SearchFilters,
)

JSONPrimitive = None | bool | int | float | str
JSONValue = JSONPrimitive | 'JSONList' | 'JSONObject'
JSONList = list[JSONValue]
JSONObject = dict[str, JSONValue]
JSONType = JSONList | JSONObject

ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)}")


def _interpolate_env_vars(value: str) -> str:
    """Replace ${ENV_VAR} patterns with environment variable values."""

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        env_val = os.environ.get(var_name, "")
        if not env_val:
            raise ValueError(f"Environment variable '{var_name}' is not set")
        return env_val

    return ENV_VAR_PATTERN.sub(replacer, value)


def _interpolate_recursive(obj: JSONPrimitive | JSONType) -> JSONPrimitive | JSONType:
    """Recursively interpolate env vars in strings within dicts/lists."""
    if isinstance(obj, str):
        return _interpolate_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _interpolate_recursive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_recursive(item) for item in obj]
    return obj


def _parse_datetime(value: str) -> datetime:
    """Parse ISO 8601 datetime string."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def _parse_filters(data: dict) -> SearchFilters:
    """Parse search filters from config dict."""
    filters = SearchFilters()
    # noinspection SpellCheckingInspection
    if "dtstart" in data:
        # noinspection SpellCheckingInspection
        filters.dtstart = _parse_datetime(data["dtstart"])

    # noinspection SpellCheckingInspection
    if "dtend" in data:
        # noinspection SpellCheckingInspection
        filters.dtend = _parse_datetime(data["dtend"])

    for str_field in (
        "geo",
        "bbox",
        "sat",
        "timeliness",
        "filename",
        "title",
        "product_type",
        "type",
        "publication",
        "download_coverage",
        "coverage",
        "repeatCycleIdentifier",
        "centerOfLongitude",
        "set",
        "sort",
    ):
        if str_field in data:
            setattr(filters, str_field, data[str_field])

    # noinspection SpellCheckingInspection
    for int_field in ("cycle", "orbit", "relorbit"):
        if int_field in data:
            setattr(filters, int_field, int(data[int_field]))
    return filters


def _parse_download_config(data: dict, base_dir: Path) -> DownloadConfig:
    """Parse download configuration."""
    cfg = DownloadConfig()
    if "directory" in data:
        cfg.directory = _resolve_path(data["directory"], base_dir)
    if "parallel" in data:
        cfg.parallel = int(data["parallel"])
    if "resume" in data:
        cfg.resume = bool(data["resume"])
    if "verify_md5" in data:
        cfg.verify_md5 = bool(data["verify_md5"])
    if "max_retries" in data:
        cfg.max_retries = int(data["max_retries"])
    if "retry_backoff" in data:
        cfg.retry_backoff = float(data["retry_backoff"])
    if "timeout" in data:
        cfg.timeout = float(data["timeout"])
    if "entries" in data:
        cfg.entries = list(data["entries"])
    return cfg


def _parse_post_process_config(data: dict, base_dir: Path) -> PostProcessConfig:
    """Parse post-processing configuration."""
    cfg = PostProcessConfig()
    if "enabled" in data:
        cfg.enabled = bool(data["enabled"])
    if "output_dir" in data:
        cfg.output_dir = _resolve_path(data["output_dir"], base_dir)
    return cfg


def _resolve_path(path_str: str, base_dir: Path) -> Path:
    """Resolve a path relative to base_dir if not absolute."""
    p = Path(path_str)
    if not p.is_absolute():
        return base_dir / p
    return p


def _parse_job(data: dict, base_dir: Path) -> JobConfig:
    """Parse a single job configuration."""
    if "collection" not in data:
        raise ValueError(f"Job '{data.get('name', '<unnamed>')}' is missing required 'collection' field")

    job = JobConfig(
        name=data.get("name", "default"),
        collection=data["collection"],
    )

    if "filters" in data:
        job.filters = _parse_filters(data["filters"])

    if "download" in data:
        job.download = _parse_download_config(data["download"], base_dir)

    if "post_process" in data:
        job.post_process = _parse_post_process_config(data["post_process"], base_dir)

    if "limit" in data:
        job.limit = int(data["limit"])

    return job


def load_config(path: str | Path) -> AppConfig:
    """Load and validate a YAML configuration file.

    Args:
        path: Path to the YAML config file.

    Returns:
        Parsed AppConfig.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If config is invalid.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    base_dir = config_path.parent.resolve()

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping")

    data = _interpolate_recursive(raw)

    if "credentials" in data:
        raise ValueError(
            "Credentials must not be stored in the config file. "
            "Set EUMDAC_KEY and EUMDAC_SECRET environment variables instead."
        )

    app_config = AppConfig()

    if "logging" in data:
        log_data = data["logging"]
        app_config.logging = LoggingConfig(
            level=log_data.get("level", "INFO"),
            file=log_data.get("file"),
        )

    if "jobs" in data:
        if not isinstance(data["jobs"], list):
            raise ValueError("'jobs' must be a list")
        for job_data in data["jobs"]:
            app_config.jobs.append(_parse_job(job_data, base_dir))

    if not app_config.jobs:
        raise ValueError("Config must contain at least one job")

    return app_config
