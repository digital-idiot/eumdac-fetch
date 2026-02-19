"""Tests for YAML config loading."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest

from eumdac_fetch.config import (
    _interpolate_env_vars,
    _parse_datetime,
    load_config,
)


class TestEnvVarInterpolation:
    def test_simple_var(self):
        with mock.patch.dict(os.environ, {"MY_VAR": "hello"}):
            assert _interpolate_env_vars("${MY_VAR}") == "hello"

    def test_multiple_vars(self):
        with mock.patch.dict(os.environ, {"A": "foo", "B": "bar"}):
            assert _interpolate_env_vars("${A}:${B}") == "foo:bar"

    def test_missing_var_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            # Ensure the var is definitely not set
            os.environ.pop("NONEXISTENT_VAR", None)
            with pytest.raises(ValueError, match="not set"):
                _interpolate_env_vars("${NONEXISTENT_VAR}")

    def test_no_vars(self):
        assert _interpolate_env_vars("plain-string") == "plain-string"


class TestParseDatetime:
    def test_utc_z_suffix(self):
        dt = _parse_datetime("2024-01-01T00:00:00Z")
        assert dt == datetime(2024, 1, 1, tzinfo=UTC)

    def test_iso_with_offset(self):
        dt = _parse_datetime("2024-01-01T00:00:00+00:00")
        assert dt == datetime(2024, 1, 1, tzinfo=UTC)


class TestLoadConfig:
    def test_load_sample(self, sample_job_yaml):
        config = load_config(sample_job_yaml)
        assert config.logging.level == "DEBUG"
        assert len(config.jobs) == 1

        job = config.jobs[0]
        assert job.name == "test-job"
        assert job.collection == "EO:EUM:DAT:MSG:HRSEVIRI"
        assert job.filters.dtstart == datetime(2024, 1, 1, tzinfo=UTC)
        assert job.filters.dtend == datetime(2024, 1, 2, tzinfo=UTC)
        assert job.filters.sat == "MSG4"
        assert job.filters.timeliness == "NT"
        assert "POLYGON" in job.filters.geo
        assert job.download.parallel == 4
        assert job.download.resume is True
        assert job.post_process.enabled is True

    def test_credentials_in_yaml_raises(self, tmp_path):
        """A config file with a credentials section must be rejected."""
        f = tmp_path / "creds.yaml"
        f.write_text("credentials:\n  key: k\n  secret: s\njobs:\n  - collection: COL1\n")
        with pytest.raises(ValueError, match="Credentials must not be stored"):
            load_config(f)

    def test_load_with_env_vars(self, env_var_yaml):
        with mock.patch.dict(os.environ, {"TEST_LOG_LEVEL": "WARNING"}):
            config = load_config(env_var_yaml)
            assert config.logging.level == "WARNING"

    def test_load_env_var_missing_raises(self, env_var_yaml):
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("TEST_LOG_LEVEL", None)
            with pytest.raises(ValueError, match="not set"):
                load_config(env_var_yaml)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path.yaml")

    def test_missing_collection_raises(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("jobs:\n  - name: bad-job\n")
        with pytest.raises(ValueError, match="missing required 'collection'"):
            load_config(f)

    def test_no_jobs_raises(self, tmp_path):
        f = tmp_path / "empty.yaml"
        f.write_text("logging:\n  level: INFO\n")
        with pytest.raises(ValueError, match="at least one job"):
            load_config(f)

    def test_relative_paths_resolved(self, sample_job_yaml):
        config = load_config(sample_job_yaml)
        job = config.jobs[0]
        # Relative paths should be resolved against config file's parent dir
        assert job.download.directory.is_absolute()
        assert job.post_process.output_dir.is_absolute()

    def test_integer_filter_fields(self, tmp_path):
        f = tmp_path / "int.yaml"
        f.write_text(
            "jobs:\n"
            "  - name: int-test\n"
            "    collection: COL1\n"
            "    filters:\n"
            "      cycle: 5\n"
            "      orbit: 100\n"
            "      relorbit: 50\n"
            "      dtstart: '2024-01-01T00:00:00Z'\n"
            "      dtend: '2024-01-02T00:00:00Z'\n"
        )
        config = load_config(f)
        assert config.jobs[0].filters.cycle == 5
        assert config.jobs[0].filters.orbit == 100
        assert config.jobs[0].filters.relorbit == 50

    def test_download_config_all_fields(self, tmp_path):
        """All download config fields are parsed including max_retries, retry_backoff, timeout."""
        f = tmp_path / "dl.yaml"
        f.write_text(
            "jobs:\n"
            "  - name: dl-test\n"
            "    collection: COL1\n"
            "    download:\n"
            "      directory: ./data\n"
            "      parallel: 8\n"
            "      resume: false\n"
            "      verify_md5: false\n"
            "      max_retries: 5\n"
            "      retry_backoff: 3.5\n"
            "      timeout: 600\n"
        )
        config = load_config(f)
        dl = config.jobs[0].download
        assert dl.parallel == 8
        assert dl.resume is False
        assert dl.verify_md5 is False
        assert dl.max_retries == 5
        assert dl.retry_backoff == 3.5
        assert dl.timeout == 600.0

    def test_absolute_path_preserved(self, tmp_path):
        """Absolute paths in config are kept as-is."""
        abs_path = Path(tmp_path.anchor) / "absolute" / "path" / "downloads"
        f = tmp_path / "abs.yaml"
        f.write_text(
            f"jobs:\n  - name: abs-test\n    collection: COL1\n    download:\n      directory: {abs_path.as_posix()}\n"
        )
        config = load_config(f)
        assert config.jobs[0].download.directory == abs_path

    def test_job_with_limit(self, tmp_path):
        """Job limit is parsed correctly."""
        f = tmp_path / "limit.yaml"
        f.write_text("jobs:\n  - name: limit-test\n    collection: COL1\n    limit: 200\n")
        config = load_config(f)
        assert config.jobs[0].limit == 200

    def test_non_dict_yaml_raises(self, tmp_path):
        """YAML that parses to non-dict raises ValueError."""
        f = tmp_path / "list.yaml"
        f.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="YAML mapping"):
            load_config(f)

    def test_jobs_not_list_raises(self, tmp_path):
        """jobs as a non-list raises ValueError."""
        f = tmp_path / "bad_jobs.yaml"
        f.write_text("jobs:\n  name: single-job\n  collection: COL1\n")
        with pytest.raises(ValueError, match="must be a list"):
            load_config(f)

    def test_logging_config_parsed(self, tmp_path):
        """Logging config with file and level is parsed."""
        f = tmp_path / "log.yaml"
        f.write_text("logging:\n  level: WARNING\n  file: app.log\njobs:\n  - name: log-test\n    collection: COL1\n")
        config = load_config(f)
        assert config.logging.level == "WARNING"
        assert config.logging.file == "app.log"

    def test_post_process_config_parsed(self, tmp_path):
        """Post-process config with absolute output_dir."""
        abs_path = Path(tmp_path.anchor) / "absolute" / "output"
        f = tmp_path / "pp.yaml"
        f.write_text(
            "jobs:\n"
            "  - name: pp-test\n"
            "    collection: COL1\n"
            "    post_process:\n"
            "      enabled: true\n"
            f"      output_dir: {abs_path.as_posix()}\n"
        )
        config = load_config(f)
        assert config.jobs[0].post_process.enabled is True
        assert config.jobs[0].post_process.output_dir == abs_path

    def test_default_job_name(self, tmp_path):
        """Job without a name gets 'default'."""
        f = tmp_path / "no_name.yaml"
        f.write_text("jobs:\n  - collection: COL1\n")
        config = load_config(f)
        assert config.jobs[0].name == "default"

    def test_new_filter_fields(self, tmp_path):
        """New filter fields (coverage, bbox, title, etc.) are parsed correctly."""
        f = tmp_path / "new_filters.yaml"
        f.write_text(
            """
jobs:
  - name: test-new-filters
    collection: EO:EUM:DAT:0665
    filters:
      dtstart: "2024-01-01T00:00:00Z"
      dtend: "2024-01-02T00:00:00Z"
      coverage: FD
      bbox: "-180,-90,180,90"
      title: "*HRFI*"
      type: MTIFCI1CRRADHRFI
      repeatCycleIdentifier: "1"
      centerOfLongitude: "0.0"
      set: brief
"""
        )
        config = load_config(f)
        filters = config.jobs[0].filters
        assert filters.coverage == "FD"
        assert filters.bbox == "-180,-90,180,90"
        assert filters.title == "*HRFI*"
        assert filters.type == "MTIFCI1CRRADHRFI"
        assert filters.repeatCycleIdentifier == "1"
        assert filters.centerOfLongitude == "0.0"
        assert filters.set == "brief"
