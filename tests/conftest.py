"""Shared test fixtures."""

from __future__ import annotations

import textwrap
from unittest import mock

import pytest


def pytest_addoption(parser):
    parser.addoption("--run-integration", action="store_true", default=False, help="Run integration tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(reason="Need --run-integration to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture
def tmp_config_dir(tmp_path):
    """Provide a temporary directory for config files."""
    return tmp_path


@pytest.fixture
def sample_job_yaml(tmp_path):
    """Create a sample YAML config file and return its path."""
    config_file = tmp_path / "job.yaml"
    config_file.write_text(
        textwrap.dedent("""\
        logging:
          level: DEBUG

        jobs:
          - name: "test-job"
            collection: "EO:EUM:DAT:MSG:HRSEVIRI"
            filters:
              dtstart: "2024-01-01T00:00:00Z"
              dtend: "2024-01-02T00:00:00Z"
              geo: "POLYGON((10 56, 11 56, 11 57, 10 57, 10 56))"
              sat: "MSG4"
              timeliness: "NT"
            download:
              directory: "./downloads"
              parallel: 4
              resume: true
              verify_md5: true
            post_process:
              enabled: true
              output_dir: "./output"
    """)
    )
    return config_file


@pytest.fixture
def env_var_yaml(tmp_path):
    """Create a config file that uses env var interpolation in non-credential fields."""
    config_file = tmp_path / "env_job.yaml"
    config_file.write_text(
        textwrap.dedent("""\
        logging:
          level: "${TEST_LOG_LEVEL}"

        jobs:
          - name: "env-job"
            collection: "EO:EUM:DAT:MSG:HRSEVIRI"
            filters:
              dtstart: "2024-01-01T00:00:00Z"
              dtend: "2024-01-02T00:00:00Z"
    """)
    )
    return config_file


@pytest.fixture
def mock_eumdac_token():
    """Create a mock eumdac access token."""
    token = mock.MagicMock()
    token.expiration = "2024-01-01T12:00:00Z"
    return token


@pytest.fixture
def mock_collection():
    """Create a mock eumdac collection."""
    collection = mock.MagicMock()
    collection.__str__ = mock.MagicMock(return_value="EO:EUM:DAT:MSG:HRSEVIRI")
    collection.title = "High Rate SEVIRI Level 1.5 Image Data"
    collection.abstract = "Test abstract"
    collection.search_options = {"sat": ["MSG1", "MSG2", "MSG3", "MSG4"]}
    return collection


@pytest.fixture
def mock_product():
    """Create a mock eumdac product."""
    product = mock.MagicMock()
    product.__str__ = mock.MagicMock(return_value="MSG4-SEVI-MSG15HRV-1234")
    product.sensing_start = "2024-01-01T00:00:00Z"
    product.sensing_end = "2024-01-01T00:15:00Z"
    product.size = 50000  # KB
    product.md5 = "abc123def456"
    return product
