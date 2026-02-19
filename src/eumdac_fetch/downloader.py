"""Async parallel downloader with resume, retry, and MD5 verification."""

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import http.client
import logging
import shutil
from pathlib import Path
from typing import Any

import requests.exceptions
import urllib3.exceptions
from rich.console import Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from eumdac_fetch.models import ProductRecord, ProductStatus
from eumdac_fetch.state import StateDB

logger = logging.getLogger("eumdac_fetch")

CHUNK_SIZE = 8192

# Separator used to encode an entry name into the state DB product_id key.
# Product IDs never contain this sequence, so it is safe to use as a delimiter.
_ENTRY_SEP = "::entry::"


def _encode_entry_key(product_id: str, entry_name: str) -> str:
    """Encode a (product_id, entry_name) pair as a single state DB key."""
    return f"{product_id}{_ENTRY_SEP}{entry_name}"


def _decode_entry_key(key: str) -> tuple[str, str | None]:
    """Return (product_id, entry_name). entry_name is None for whole-product keys."""
    if _ENTRY_SEP in key:
        product_id, entry_name = key.split(_ENTRY_SEP, 1)
        return product_id, entry_name
    return key, None


# Exception types that are considered transient and worth retrying.
# requests.exceptions.RequestException covers HTTP-level errors raised by
# the eumdac library (which uses requests internally).
# http.client.IncompleteRead and urllib3.exceptions.ProtocolError can occur
# when a connection drops mid-transfer without being wrapped by requests.
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
    requests.exceptions.RequestException,
    http.client.IncompleteRead,
    urllib3.exceptions.ProtocolError,
)


class DownloadService:
    """Manages parallel async downloads with resume, retry, and MD5 verification."""

    def __init__(
        self,
        state_db: StateDB,
        download_dir: Path,
        parallel: int = 4,
        resume: bool = True,
        verify_md5: bool = True,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
        timeout: float = 300.0,
        entries: list[str] | None = None,
    ):
        self.state_db = state_db
        self.download_dir = download_dir
        self.parallel = parallel
        self.resume = resume
        self.verify_md5 = verify_md5
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.timeout = timeout
        self.entries = entries  # Glob patterns; None = whole product (ZIP)
        self._shutdown = asyncio.Event()

    async def download_all(self, products: list, job_name: str, collection: str) -> None:
        """Download all products with parallel concurrency.

        Args:
            products: List of eumdac product objects.
            job_name: Name of the job for state tracking.
            collection: Collection ID.
        """
        # Register products (or their entries) in state DB
        for product in products:
            product_id = str(product)

            if self.entries is not None:
                # Entry-level mode: register one state row per matching entry
                # noinspection PyBroadException
                try:
                    all_entries = product.entries
                except Exception:
                    logger.warning("Could not list entries for %s, skipping", product_id)
                    continue
                matching = [
                    e
                    for e in all_entries
                    if any(fnmatch.fnmatch(e.split("/")[-1], pat) or fnmatch.fnmatch(e, pat) for pat in self.entries)
                ]
                if not matching:
                    logger.warning("No entries matched patterns %s for %s", self.entries, product_id)
                    continue
                for entry_name in matching:
                    key = _encode_entry_key(product_id, entry_name)
                    existing = self.state_db.get(key, job_name)
                    if existing and existing.status in (ProductStatus.VERIFIED, ProductStatus.PROCESSED):
                        logger.info("Skipping already verified/processed entry: %s", key)
                        continue
                    if not existing:
                        self.state_db.upsert(
                            ProductRecord(
                                product_id=key,
                                job_name=job_name,
                                collection=collection,
                                size_kb=0,  # Per-entry size not available from metadata
                            )
                        )
            else:
                # Whole-product mode: current behaviour
                existing = self.state_db.get(product_id, job_name)
                if existing and existing.status in (ProductStatus.VERIFIED, ProductStatus.PROCESSED):
                    logger.info("Skipping already verified/processed: %s", product_id)
                    continue
                if not existing:
                    # noinspection PyBroadException
                    try:
                        size_kb = product.size
                    except Exception:
                        size_kb = 0
                    self.state_db.upsert(
                        ProductRecord(
                            product_id=product_id,
                            job_name=job_name,
                            collection=collection,
                            size_kb=size_kb,
                        )
                    )

        # Get items to download
        to_download = self.state_db.get_resumable(job_name)
        if not to_download:
            logger.info("No products to download")
            return

        # Check disk space (best-effort; entry sizes are unknown so only checked for whole products)
        estimated_bytes = sum(r.size_kb * 1000 for r in to_download)
        if estimated_bytes > 0:
            free_bytes = shutil.disk_usage(self.download_dir).free
            if free_bytes < estimated_bytes:
                logger.warning(
                    "Low disk space: ~%.1f GB needed, %.1f GB free",
                    estimated_bytes / 1e9,
                    free_bytes / 1e9,
                )

        # Map product objects by actual product ID (strips entry suffix if present)
        product_map = {str(p): p for p in products}

        semaphore = asyncio.Semaphore(self.parallel)

        overall_progress = Progress(
            TextColumn("[bold blue]{task.fields[product_id]}"),
            BarColumn(),
            MofNCompleteColumn(),
        )
        download_progress = Progress(
            TextColumn("[bold blue]{task.fields[product_id]}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        )
        progress = download_progress

        with Live(Group(overall_progress, download_progress)):
            overall_task = overall_progress.add_task("Overall", total=len(to_download), product_id="Overall")

            tasks = []
            for record in to_download:
                actual_product_id, entry_name = _decode_entry_key(record.product_id)
                product = product_map.get(actual_product_id)
                if product is None:
                    logger.warning("Product %s not found in search results, skipping", actual_product_id)
                    continue
                tasks.append(
                    self._download_one(semaphore, product, entry_name, record, progress, overall_progress, overall_task)
                )

            await asyncio.gather(*tasks)

    async def _download_one(
        self,
        semaphore: asyncio.Semaphore,
        product: Any,
        entry_name: str | None,
        record: ProductRecord,
        progress: Progress,
        overall_progress: Progress,
        overall_task: TaskID,
    ) -> None:
        """Download a single product or entry with semaphore-controlled concurrency and retry."""
        if self._shutdown.is_set():
            return

        async with semaphore:
            # noinspection GrazieInspection
            db_key = record.product_id  # State DB key (may be encoded with entry suffix)
            # Use entry filename for display and on-disk filename; fall back to product ID
            filename = entry_name.split("/")[-1] if entry_name else db_key
            total_bytes = int(record.size_kb * 1000)
            task_id = progress.add_task(
                db_key,
                total=total_bytes or None,
                product_id=filename[:40],
            )

            last_error = None
            for attempt in range(self.max_retries + 1):
                if self._shutdown.is_set():
                    return

                try:
                    self.state_db.update_status(db_key, record.job_name, ProductStatus.DOWNLOADING)

                    download_path = self.download_dir / filename
                    downloaded = await asyncio.wait_for(
                        asyncio.to_thread(
                            self._download_blocking, product, entry_name, download_path, record, progress, task_id
                        ),
                        timeout=self.timeout,
                    )

                    if downloaded:
                        self.state_db.update_status(
                            db_key,
                            record.job_name,
                            ProductStatus.DOWNLOADED,
                            download_path=str(download_path),
                            bytes_downloaded=download_path.stat().st_size,
                        )

                        # MD5 is a whole-product hash; skip verification for individual entries
                        if self.verify_md5 and entry_name is None:
                            verified = await asyncio.to_thread(self._verify_md5, product, download_path)
                            if verified:
                                self.state_db.update_status(db_key, record.job_name, ProductStatus.VERIFIED)
                            else:
                                self.state_db.update_status(
                                    db_key,
                                    record.job_name,
                                    ProductStatus.FAILED,
                                    error_message="MD5 verification failed",
                                )
                        else:
                            self.state_db.update_status(db_key, record.job_name, ProductStatus.VERIFIED)

                    # Success — break out of retry loop
                    last_error = None
                    break

                except RETRYABLE_EXCEPTIONS as e:
                    last_error = e
                    if attempt < self.max_retries:
                        wait = self.retry_backoff * (2**attempt)
                        logger.warning(
                            "Retryable error downloading %s (attempt %d/%d): %s. Retrying in %.1fs",
                            filename,
                            attempt + 1,
                            self.max_retries + 1,
                            e,
                            wait,
                        )
                        await asyncio.sleep(wait)
                        # Reset progress for retry
                        progress.update(task_id, completed=0)
                    # If last attempt, fall through to mark FAILED below

                except Exception as e:
                    # Non-retryable error — fail immediately
                    logger.error("Failed to download %s: %s", filename, e)
                    self.state_db.update_status(
                        db_key,
                        record.job_name,
                        ProductStatus.FAILED,
                        error_message=str(e),
                    )
                    last_error = None  # Already handled
                    break

            # If we exhausted retries on a retryable error, mark FAILED
            if last_error is not None:
                logger.error(
                    "Failed to download %s after %d attempts: %s",
                    filename,
                    self.max_retries + 1,
                    last_error,
                )
                self.state_db.update_status(
                    db_key,
                    record.job_name,
                    ProductStatus.FAILED,
                    error_message=f"Failed after {self.max_retries + 1} attempts: {last_error}",
                )

            overall_progress.update(overall_task, advance=1)

    def _download_blocking(
        self,
        product: Any,
        entry_name: str | None,
        download_path: Path,
        record: ProductRecord,
        progress: Progress,
        task_id: TaskID,
    ) -> bool:
        """Blocking download function to run in a thread.

        Returns True if download completed successfully.
        """
        mode = "wb"
        offset = 0

        # Resume support
        if self.resume and download_path.exists():
            offset = download_path.stat().st_size
            if offset > 0:
                logger.info("Resuming %s from %d bytes", record.product_id, offset)
                mode = "ab"
                progress.update(task_id, completed=offset)

        # product.open() is a context manager yielding an IO[bytes] stream.
        # Byte-range resume: try chunk=(offset, ""), falling back to full
        # re-download if the server or library doesn't support it.
        if offset > 0:
            # noinspection PyBroadException
            try:
                with product.open(entry=entry_name, chunk=(offset, "")) as stream, open(download_path, mode) as f:
                    return self._stream_to_file(stream, f, progress, task_id)
            except Exception:
                # Fallback: restart from scratch
                logger.info("Byte-range resume not supported for %s, restarting", record.product_id)
                progress.update(task_id, completed=0)

        with product.open(entry=entry_name) as stream, open(download_path, "wb") as f:
            return self._stream_to_file(stream, f, progress, task_id)

    def _stream_to_file(self, stream, f, progress: Progress, task_id: TaskID) -> bool:
        """Read from stream and write to file, updating progress."""
        while True:
            if self._shutdown.is_set():
                return False
            data = stream.read(CHUNK_SIZE)
            if not data:
                break
            f.write(data)
            progress.update(task_id, advance=len(data))
        return True

    @staticmethod
    def _verify_md5(product: Any, download_path: Path) -> bool:
        """Verify downloaded file MD5 against product metadata."""
        # noinspection PyBroadException
        try:
            expected_md5 = product.md5
        except Exception:
            logger.warning("No MD5 available for %s, skipping verification", download_path.name)
            return True

        if not expected_md5:
            return True

        md5_hash = hashlib.md5()
        with open(download_path, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                md5_hash.update(chunk)

        computed = md5_hash.hexdigest()
        if computed != expected_md5:
            logger.error("MD5 mismatch for %s: expected %s, got %s", download_path.name, expected_md5, computed)
            return False

        logger.info("MD5 verified: %s", download_path.name)
        return True

    def request_shutdown(self) -> None:
        """Signal graceful shutdown."""
        self._shutdown.set()
