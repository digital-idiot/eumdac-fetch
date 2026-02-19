"""Tests for session management module."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from eumdac_fetch.models import (
    DownloadConfig,
    JobConfig,
    PostProcessConfig,
    SearchFilters,
)
from eumdac_fetch.session import LIVE_THRESHOLD, Session


@pytest.fixture
def base_job():
    return JobConfig(
        name="test-job",
        collection="EO:EUM:DAT:MSG:HRSEVIRI",
        filters=SearchFilters(
            dtstart=datetime(2024, 1, 1, tzinfo=UTC),
            dtend=datetime(2024, 1, 2, tzinfo=UTC),
        ),
        download=DownloadConfig(parallel=4),
        post_process=PostProcessConfig(enabled=False),
        limit=100,
    )


class TestSessionId:
    def test_session_id_deterministic(self, tmp_path, base_job):
        """Same config produces the same session ID."""
        s1 = Session(base_job, base_dir=tmp_path)
        s2 = Session(base_job, base_dir=tmp_path)
        assert s1.session_id == s2.session_id

    def test_session_id_is_12_hex_chars(self, tmp_path, base_job):
        s = Session(base_job, base_dir=tmp_path)
        assert len(s.session_id) == 12
        assert all(c in "0123456789abcdef" for c in s.session_id)

    def test_session_id_changes_with_config(self, tmp_path, base_job):
        """Different filters produce a different session ID."""
        s1 = Session(base_job, base_dir=tmp_path)

        modified_job = JobConfig(
            name="test-job",
            collection="EO:EUM:DAT:MSG:HRSEVIRI",
            filters=SearchFilters(
                dtstart=datetime(2024, 6, 1, tzinfo=UTC),
                dtend=datetime(2024, 6, 2, tzinfo=UTC),
            ),
            download=DownloadConfig(parallel=4),
            post_process=PostProcessConfig(enabled=False),
            limit=100,
        )
        s2 = Session(modified_job, base_dir=tmp_path)
        assert s1.session_id != s2.session_id

    def test_session_id_changes_with_collection(self, tmp_path, base_job):
        """Different collection produces a different session ID."""
        s1 = Session(base_job, base_dir=tmp_path)

        modified_job = JobConfig(
            name="test-job",
            collection="EO:EUM:DAT:OTHER",
            filters=base_job.filters,
            download=base_job.download,
            post_process=base_job.post_process,
            limit=base_job.limit,
        )
        s2 = Session(modified_job, base_dir=tmp_path)
        assert s1.session_id != s2.session_id

    def test_session_id_changes_with_limit(self, tmp_path, base_job):
        """Different limit produces a different session ID."""
        s1 = Session(base_job, base_dir=tmp_path)

        modified_job = JobConfig(
            name=base_job.name,
            collection=base_job.collection,
            filters=base_job.filters,
            download=base_job.download,
            post_process=base_job.post_process,
            limit=200,
        )
        s2 = Session(modified_job, base_dir=tmp_path)
        assert s1.session_id != s2.session_id


class TestLiveDetection:
    def test_live_detection_no_dtend(self, tmp_path):
        """No dtend means session is live (open-ended)."""
        job = JobConfig(
            name="live-job",
            collection="COL1",
            filters=SearchFilters(dtstart=datetime(2024, 1, 1, tzinfo=UTC)),
        )
        s = Session(job, base_dir=tmp_path)
        assert s.is_live is True

    def test_live_detection_future_dtend(self, tmp_path):
        """dtend in the future means session is live."""
        job = JobConfig(
            name="live-job",
            collection="COL1",
            filters=SearchFilters(
                dtstart=datetime(2024, 1, 1, tzinfo=UTC),
                dtend=datetime.now(UTC) + timedelta(days=1),
            ),
        )
        s = Session(job, base_dir=tmp_path)
        assert s.is_live is True

    def test_live_detection_recent_dtend(self, tmp_path):
        """dtend within LIVE_THRESHOLD of now means session is live."""
        job = JobConfig(
            name="live-job",
            collection="COL1",
            filters=SearchFilters(
                dtstart=datetime(2024, 1, 1, tzinfo=UTC),
                dtend=datetime.now(UTC) - timedelta(hours=1),
            ),
        )
        s = Session(job, base_dir=tmp_path)
        assert s.is_live is True

    def test_live_detection_past_dtend(self, tmp_path):
        """dtend far in the past means session is not live."""
        job = JobConfig(
            name="old-job",
            collection="COL1",
            filters=SearchFilters(
                dtstart=datetime(2023, 1, 1, tzinfo=UTC),
                dtend=datetime(2023, 1, 2, tzinfo=UTC),
            ),
        )
        s = Session(job, base_dir=tmp_path)
        assert s.is_live is False

    def test_live_detection_boundary(self, tmp_path):
        """dtend exactly at LIVE_THRESHOLD boundary is not live."""
        boundary = datetime.now(UTC) - LIVE_THRESHOLD - timedelta(seconds=1)
        job = JobConfig(
            name="boundary-job",
            collection="COL1",
            filters=SearchFilters(
                dtstart=datetime(2024, 1, 1, tzinfo=UTC),
                dtend=boundary,
            ),
        )
        s = Session(job, base_dir=tmp_path)
        assert s.is_live is False


class TestSessionLifecycle:
    def test_initialize_creates_dirs(self, tmp_path, base_job):
        s = Session(base_job, base_dir=tmp_path)
        assert s.is_new is True
        s.initialize()
        assert s.session_dir.exists()
        assert s.download_dir.exists()

    def test_initialize_saves_config(self, tmp_path, base_job):
        s = Session(base_job, base_dir=tmp_path)
        s.initialize()
        assert s.config_path.exists()

        with open(s.config_path) as f:
            saved = yaml.safe_load(f)
        assert saved["name"] == "test-job"
        assert saved["collection"] == "EO:EUM:DAT:MSG:HRSEVIRI"

    def test_resume_existing_session(self, tmp_path, base_job):
        """Creating a session where the dir already exists → is_new == False."""
        s1 = Session(base_job, base_dir=tmp_path)
        s1.initialize()

        s2 = Session(base_job, base_dir=tmp_path)
        assert s2.is_new is False
        assert s2.session_id == s1.session_id

    def test_session_paths(self, tmp_path, base_job):
        s = Session(base_job, base_dir=tmp_path)
        assert s.state_db_path == s.session_dir / "state.db"
        assert s.log_path == s.session_dir / "session.log"
        assert s.config_path == s.session_dir / "config.yaml"

    def test_download_dir_uses_collection(self, tmp_path, base_job):
        s = Session(base_job, base_dir=tmp_path)
        assert s.download_dir == tmp_path / "downloads" / base_job.collection

    def test_base_dir_from_env(self, tmp_path, base_job, monkeypatch):
        """EUMDAC_FETCH_HOME env var overrides default base dir."""
        custom_base = tmp_path / "custom"
        monkeypatch.setenv("EUMDAC_FETCH_HOME", str(custom_base))
        s = Session(base_job)
        assert s.base_dir == custom_base

    def test_initialize_idempotent(self, tmp_path, base_job):
        """Calling initialize() twice doesn't overwrite config."""
        s = Session(base_job, base_dir=tmp_path)
        s.initialize()

        with open(s.config_path) as f:
            content1 = f.read()

        s.initialize()

        with open(s.config_path) as f:
            content2 = f.read()

        assert content1 == content2

    def test_sanitize_for_json_with_lists(self, tmp_path, base_job):
        """_sanitize_for_json handles lists containing Paths and datetimes."""
        s = Session(base_job, base_dir=tmp_path)
        result = s._sanitize_for_json(
            {
                "paths": [Path("/a"), Path("/b")],
                "dates": [datetime(2024, 1, 1, tzinfo=UTC)],
                "nested": [{"key": Path("/c")}],
            }
        )
        assert result == {
            "paths": ["/a", "/b"],
            "dates": ["2024-01-01T00:00:00+00:00"],
            "nested": [{"key": "/c"}],
        }
