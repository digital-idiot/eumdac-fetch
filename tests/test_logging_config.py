"""Tests for logging configuration."""

from __future__ import annotations

import logging

from eumdac_fetch.logging_config import add_session_log_handler, setup_logging
from eumdac_fetch.models import LoggingConfig


class TestSetupLogging:
    def test_default_config(self):
        logger = setup_logging()
        assert logger.name == "eumdac_fetch"
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1  # RichHandler

    def test_custom_level(self):
        logger = setup_logging(LoggingConfig(level="DEBUG"))
        assert logger.level == logging.DEBUG

    def test_with_file_handler(self, tmp_path):
        log_file = tmp_path / "test.log"
        logger = setup_logging(LoggingConfig(file=str(log_file)))
        assert len(logger.handlers) == 2  # RichHandler + FileHandler
        logger.info("test message")
        # Close handler to flush
        for h in logger.handlers[:]:
            if isinstance(h, logging.FileHandler):
                h.close()
        assert log_file.exists()
        assert "test message" in log_file.read_text()

    def test_clears_existing_handlers(self):
        logger = setup_logging()
        initial_count = len(logger.handlers)
        setup_logging()  # Call again
        assert len(logger.handlers) == initial_count


class TestAddSessionLogHandler:
    def test_adds_handler(self, tmp_path):
        log_path = tmp_path / "session.log"
        handler = add_session_log_handler(log_path)
        try:
            logger = logging.getLogger("eumdac_fetch")
            assert handler in logger.handlers
            logger.info("session test")
            handler.close()
            assert "session test" in log_path.read_text()
        finally:
            logging.getLogger("eumdac_fetch").removeHandler(handler)

    def test_custom_level(self, tmp_path):
        log_path = tmp_path / "session.log"
        handler = add_session_log_handler(log_path, level="WARNING")
        try:
            assert handler.level == logging.WARNING
        finally:
            handler.close()
            logging.getLogger("eumdac_fetch").removeHandler(handler)
