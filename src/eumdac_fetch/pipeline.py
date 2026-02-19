"""Daemon mode: asyncio producer-consumer pipeline for search -> download -> post-process."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from pathlib import Path

import eumdac

from eumdac_fetch.downloader import DownloadService
from eumdac_fetch.logging_config import add_session_log_handler
from eumdac_fetch.models import AppConfig, PostProcessorFn, ProductStatus
from eumdac_fetch.search import SearchService
from eumdac_fetch.session import Session
from eumdac_fetch.state import StateDB

logger = logging.getLogger("eumdac_fetch")

SENTINEL = None  # Signals end of queue


class Pipeline:
    """Orchestrates search -> download -> post-process as an async producer-consumer pipeline."""

    def __init__(
        self,
        token: eumdac.AccessToken,
        config: AppConfig,
        post_processor: PostProcessorFn | None = None,
    ):
        self.token = token
        self.config = config
        self.post_processor = post_processor
        self._shutdown = asyncio.Event()

    async def run(self) -> None:
        """Run the full pipeline for all jobs."""
        loop = asyncio.get_running_loop()

        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._handle_signal)  # type: ignore[arg-type]

        search_service = SearchService(self.token)

        for job in self.config.jobs:
            if self._shutdown.is_set():
                break

            # Create session for this job
            session = Session(job)
            session.initialize()

            logger.info(
                "Session: %s (%s)",
                session.session_id,
                "new" if session.is_new else "resuming",
            )
            logger.info("Session dir: %s", session.session_dir)
            if session.is_live:
                logger.info("Live session — search results will be refreshed")

            # Set up session-scoped logging
            log_handler = add_session_log_handler(session.log_path)

            # Set up state DB in session directory
            state_db = StateDB(session.state_db_path)

            try:
                logger.info("Starting pipeline for job: %s", job.name)

                # Reset stale DOWNLOADING products from previous killed runs
                if not session.is_new:
                    reset_count = state_db.reset_stale_downloads(job.name)
                    if reset_count:
                        logger.info("Reset %d stale downloading products to pending", reset_count)

                # Search with caching
                products = self._search_with_cache(
                    search_service,
                    session,
                    state_db,
                    job,
                )

                if not products:
                    continue

                # Use session download dir
                session.download_dir.mkdir(parents=True, exist_ok=True)

                if job.post_process.enabled and self.post_processor:
                    await self._run_with_post_processing(products, job, state_db, session)
                elif job.post_process.enabled and not self.post_processor:
                    logger.warning(
                        "Post-processing enabled for job '%s' but no post_processor callable provided; "
                        "downloading only",
                        job.name,
                    )
                    await self._run_download_only(products, job, state_db, session)
                else:
                    await self._run_download_only(products, job, state_db, session)
            finally:
                state_db.close()
                logging.getLogger("eumdac_fetch").removeHandler(log_handler)

            if self._shutdown.is_set():
                logger.info("Shutdown requested, stopping pipeline")
                break

        logger.info("Pipeline finished")

    @staticmethod
    def _search_with_cache(
        search_service: SearchService,
        session: Session,
        state_db: StateDB,
        job,
    ) -> list:
        """Search for products, using cached results for non-live resumed sessions."""
        if not session.is_new and not session.is_live and state_db.has_cached_search():
            logger.info("Using cached search results for job: %s", job.name)
            # For resumed non-live sessions, we still need eumdac product objects for download.
            # The products table tracks which ones still need downloading.
            resumable = state_db.get_resumable(job.name)
            if not resumable:
                logger.info("All products already processed for job: %s", job.name)
                return []
            # Re-search to get eumdac product objects, but we know the scope from cache
            logger.info("Re-fetching %d resumable products from API", len(resumable))
            products = search_service.iter_products(job.collection, job.filters, limit=job.limit)
            # Filter to only resumable product IDs
            resumable_ids = {r.product_id for r in resumable}
            products = [p for p in products if str(p) in resumable_ids]
            logger.info("Found %d resumable products", len(products))
            return products

        logger.info("Searching for products in %s", job.collection)
        products = search_service.iter_products(job.collection, job.filters, limit=job.limit)
        logger.info("Found %d products", len(products))

        if products:
            state_db.cache_search_results(products, job.collection)

        return products

    @staticmethod
    async def _run_download_only(products: list, job, state_db: StateDB, session: Session) -> None:
        """Download products without post-processing."""
        download_service = DownloadService(
            state_db=state_db,
            download_dir=session.download_dir,
            parallel=job.download.parallel,
            resume=job.download.resume,
            verify_md5=job.download.verify_md5,
            max_retries=job.download.max_retries,
            retry_backoff=job.download.retry_backoff,
            timeout=job.download.timeout,
            entries=job.download.entries,
        )
        await download_service.download_all(products, job.name, job.collection)

    async def _run_with_post_processing(
        self,
        products: list,
        job,
        state_db: StateDB,
        session: Session,
    ) -> None:
        """Run download -> post-process pipeline with async queues."""
        process_queue: asyncio.Queue = asyncio.Queue()

        download_service = DownloadService(
            state_db=state_db,
            download_dir=session.download_dir,
            parallel=job.download.parallel,
            resume=job.download.resume,
            verify_md5=job.download.verify_md5,
            max_retries=job.download.max_retries,
            retry_backoff=job.download.retry_backoff,
            timeout=job.download.timeout,
            entries=job.download.entries,
        )

        # Run download and post-process concurrently
        producer = asyncio.create_task(
            self._download_producer(download_service, products, job, state_db, process_queue)
        )
        consumer = asyncio.create_task(self._post_process_consumer(state_db, job.name, process_queue))

        await asyncio.gather(producer, consumer)

    async def _download_producer(
        self,
        download_service: DownloadService,
        products: list,
        job,
        state_db: StateDB,
        process_queue: asyncio.Queue,
    ) -> None:
        """Download products and push verified ones to the post-process queue."""
        try:
            await download_service.download_all(products, job.name, job.collection)

            # Push all verified products to post-process queue
            verified = state_db.get_by_status(job.name, ProductStatus.VERIFIED)
            for record in verified:
                if self._shutdown.is_set():
                    break
                await process_queue.put(record)
        finally:
            await process_queue.put(SENTINEL)

    async def _post_process_consumer(
        self,
        state_db: StateDB,
        job_name: str,
        process_queue: asyncio.Queue,
    ) -> None:
        """Consume downloaded products and post-process them."""
        while True:
            if self._shutdown.is_set():
                break

            record = await process_queue.get()
            if record is SENTINEL:
                break

            logger.info("Post-processing product: %s", record.product_id)
            state_db.update_status(record.product_id, job_name, ProductStatus.PROCESSING)

            try:
                download_path = Path(record.download_path)
                await asyncio.to_thread(self.post_processor, download_path, record.product_id)

                state_db.update_status(
                    record.product_id,
                    job_name,
                    ProductStatus.PROCESSED,
                )

            except Exception as e:
                logger.error("Post-processing failed for %s: %s", record.product_id, e)
                state_db.update_status(
                    record.product_id,
                    job_name,
                    ProductStatus.FAILED,
                    error_message=f"Post-processing failed: {e}",
                )

            process_queue.task_done()

    def _handle_signal(self) -> None:
        """Handle shutdown signal."""
        logger.info("Received shutdown signal")
        self._shutdown.set()
