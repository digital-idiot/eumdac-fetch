"""Structured logging configuration."""

from __future__ import annotations

import logging
from pathlib import Path

from rich.logging import RichHandler

from eumdac_fetch.models import LoggingConfig


def setup_logging(config: LoggingConfig | None = None) -> logging.Logger:
    """Configure logging with Rich console handler and optional file handler.

    Args:
        config: Logging configuration. Uses defaults if None.

    Returns:
        The root logger for eumdac_fetch.
    """
    if config is None:
        config = LoggingConfig()

    logger = logging.getLogger("eumdac_fetch")
    logger.setLevel(getattr(logging, config.level.upper(), logging.INFO))
    logger.handlers.clear()

    console_handler = RichHandler(
        show_time=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    if config.file:
        file_handler = logging.FileHandler(config.file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def add_session_log_handler(log_path: Path, level: str = "DEBUG") -> logging.Handler:
    """Add a file handler for session-scoped logging.

    Args:
        log_path: Path to the session log file.
        level: Logging level for the file handler.

    Returns:
        The created FileHandler (for later removal if needed).
    """
    logger = logging.getLogger("eumdac_fetch")
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(getattr(logging, level.upper(), logging.DEBUG))
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return file_handler
