"""Session management: identity, directories, and lifecycle."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from eumdac_fetch.models import JobConfig

# Sessions with dtend within this window of now are considered "live"
LIVE_THRESHOLD = timedelta(hours=3)

DEFAULT_BASE_DIR = Path.home() / ".eumdac-fetch"

# Characters forbidden in Windows directory/file names
_INVALID_DIRNAME_RE = re.compile(r'[<>:"/\\|?*]')


def _sanitize_dirname(name: str) -> str:
    """Replace characters invalid in Windows directory names with underscores."""
    return _INVALID_DIRNAME_RE.sub("_", name)


class Session:
    """Manages a session lifecycle: creation, resumption, and identity.

    A session is identified by a deterministic hash of the job configuration
    (excluding credentials). The same config always produces the same session ID,
    enabling automatic resumption.
    """

    def __init__(self, job: JobConfig, base_dir: Path | None = None):
        self.job = job
        self.base_dir = base_dir or Path(os.environ.get("EUMDAC_FETCH_HOME", str(DEFAULT_BASE_DIR)))
        self.session_id = self._compute_id()
        self.session_dir = self.base_dir / "sessions" / self.session_id
        self.download_dir = self.base_dir / "downloads" / _sanitize_dirname(job.collection)
        self.is_new = not self.session_dir.exists()
        self.is_live = self._check_live()

    # noinspection SpellCheckingInspection
    def _compute_id(self) -> str:
        """Hash job config (excluding credentials) to produce a 12-char hex session ID."""
        job_dict = asdict(self.job)
        # Convert Path objects and datetimes to strings for JSON serialization
        sanitized = self._sanitize_for_json(job_dict)
        canonical = json.dumps(sanitized, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()[:12]

    def _sanitize_for_json(self, obj: object) -> object:
        """Recursively convert non-JSON-serializable types to strings."""
        if isinstance(obj, dict):
            return {k: self._sanitize_for_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._sanitize_for_json(item) for item in obj]
        if isinstance(obj, Path):
            return obj.as_posix()
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def _check_live(self) -> bool:
        # noinspection SpellCheckingInspection
        """Check if the session's date range extends into recent or future time.

        A session is "live" if dtend is None (open-ended) or dtend is within
        LIVE_THRESHOLD of now (recent enough that new data may still arrive).
        """
        # noinspection SpellCheckingInspection
        dtend = self.job.filters.dtend
        if dtend is None:
            return True
        now = datetime.now(UTC)
        return dtend > (now - LIVE_THRESHOLD)

    def initialize(self) -> None:
        """Create session directory structure and save frozen config."""
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Write frozen config (job config only, no credentials)
        if not self.config_path.exists():
            job_dict = asdict(self.job)
            sanitized = self._sanitize_for_json(job_dict)
            with open(self.config_path, "w") as f:
                yaml.dump(sanitized, f, default_flow_style=False, sort_keys=False)

    @property
    def state_db_path(self) -> Path:
        return self.session_dir / "state.db"

    @property
    def log_path(self) -> Path:
        return self.session_dir / "session.log"

    @property
    def config_path(self) -> Path:
        return self.session_dir / "config.yaml"
