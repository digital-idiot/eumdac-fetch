"""Data models for eumdac-fetch."""

from __future__ import annotations

import enum
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eumdac_fetch.dataset import RemoteDataset

# Post-processor hook: receives (download_path, product_id)
PostProcessorFn = Callable[[Path, str], None]

# Remote post-processor hook: receives (RemoteDataset, product_id)
RemotePostProcessorFn = Callable[["RemoteDataset", str], None]


class ProductStatus(enum.Enum):
    """Status of a product in the download pipeline."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    VERIFIED = "verified"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


@dataclass
class SearchFilters:
    """All supported eumdac search parameters."""

    # noinspection SpellCheckingInspection
    dtstart: datetime | None = None
    # noinspection SpellCheckingInspection
    dtend: datetime | None = None
    geo: str | None = None
    bbox: str | None = None
    sat: str | None = None
    timeliness: str | None = None
    filename: str | None = None
    title: str | None = None
    cycle: int | None = None
    orbit: int | None = None
    # noinspection SpellCheckingInspection
    relorbit: int | None = None
    product_type: str | None = None
    type: str | None = None
    publication: str | None = None
    download_coverage: str | None = None
    coverage: str | None = None
    repeatCycleIdentifier: str | None = None
    centerOfLongitude: str | None = None
    set: str | None = None
    sort: str = "start,time,1"

    def to_search_kwargs(self) -> dict:
        """Convert to kwargs dict for collection.search(), dropping None values."""
        kwargs = {}
        # noinspection SpellCheckingInspection
        for f in (
            "dtstart",
            "dtend",
            "geo",
            "bbox",
            "sat",
            "timeliness",
            "filename",
            "title",
            "cycle",
            "orbit",
            "relorbit",
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
            val = getattr(self, f)
            if val is not None:
                kwargs[f] = val
        return kwargs


@dataclass
class DownloadConfig:
    """Download configuration for a job."""

    enabled: bool = True
    directory: Path = field(default_factory=lambda: Path("./downloads"))
    parallel: int = 4
    resume: bool = True
    verify_md5: bool = True
    max_retries: int = 3
    retry_backoff: float = 2.0  # Base seconds for exponential backoff
    timeout: float = 300.0  # Seconds per product download
    entries: list[str] | None = None  # Glob patterns for entries; None = whole product (ZIP)


@dataclass
class PostProcessConfig:
    """Post-processing configuration for a job."""

    enabled: bool = False
    mode: str = "local"
    output_dir: Path = field(default_factory=lambda: Path("./output"))


@dataclass
class PostSearchFilterConfig:
    """Configuration for a post-search filter."""

    type: str
    params: dict = field(default_factory=dict)


@dataclass
class JobConfig:
    """Configuration for a single download job."""

    name: str
    collection: str
    filters: SearchFilters = field(default_factory=SearchFilters)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    post_process: PostProcessConfig = field(default_factory=PostProcessConfig)
    post_search_filter: PostSearchFilterConfig | None = None
    limit: int | None = None


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    file: str | None = None


@dataclass
class AppConfig:
    """Top-level application configuration."""

    logging: LoggingConfig = field(default_factory=LoggingConfig)
    jobs: list[JobConfig] = field(default_factory=list)


@dataclass
class ProductRecord:
    """Per-product state tracking record."""

    product_id: str
    job_name: str
    collection: str
    size_kb: float = 0.0
    md5: str = ""
    bytes_downloaded: int = 0
    status: ProductStatus = ProductStatus.PENDING
    download_path: str = ""
    error_message: str = ""
    created_at: str = ""
    updated_at: str = ""
