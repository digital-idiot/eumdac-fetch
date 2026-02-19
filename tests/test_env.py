"""Tests for eumdac_fetch.env — credential bootstrapping."""

from __future__ import annotations

import warnings
from unittest import mock

import eumdac_fetch.env as env_module
from eumdac_fetch.env import DEFAULT_VALIDITY, ENV, _Env, _load_credentials, _parse_dotenv

# ---------------------------------------------------------------------------
# _parse_dotenv
# ---------------------------------------------------------------------------


class TestParseDotenv:
    def test_simple_key_value(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text("EUMDAC_KEY=my-key\nEUMDAC_SECRET=my-secret\n")
        assert _parse_dotenv(f) == {"EUMDAC_KEY": "my-key", "EUMDAC_SECRET": "my-secret"}

    def test_double_quoted_value(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text('EUMDAC_KEY="quoted-key"\n')
        assert _parse_dotenv(f)["EUMDAC_KEY"] == "quoted-key"

    def test_single_quoted_value(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text("EUMDAC_KEY='quoted-key'\n")
        assert _parse_dotenv(f)["EUMDAC_KEY"] == "quoted-key"

    def test_ignores_comment_lines(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text("# this is a comment\nEUMDAC_KEY=k\n")
        result = _parse_dotenv(f)
        assert list(result.keys()) == ["EUMDAC_KEY"]

    def test_ignores_blank_lines(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text("\n\nEUMDAC_KEY=k\n\n")
        assert _parse_dotenv(f) == {"EUMDAC_KEY": "k"}

    def test_ignores_lines_without_equals(self, tmp_path):
        f = tmp_path / ".env"
        f.write_text("NOTANASSIGNMENT\nEUMDAC_KEY=k\n")
        assert _parse_dotenv(f) == {"EUMDAC_KEY": "k"}

    def test_value_with_equals_sign(self, tmp_path):
        """Only the first '=' is used as the separator."""
        f = tmp_path / ".env"
        f.write_text("TOKEN=abc=def\n")
        assert _parse_dotenv(f)["TOKEN"] == "abc=def"


# ---------------------------------------------------------------------------
# _load_credentials
# ---------------------------------------------------------------------------


class TestLoadCredentials:
    def test_reads_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("EUMDAC_KEY", "env-key")
        monkeypatch.setenv("EUMDAC_SECRET", "env-secret")
        key, secret, validity = _load_credentials()
        assert key == "env-key"
        assert secret == "env-secret"
        assert validity == DEFAULT_VALIDITY

    def test_reads_validity_from_env_var(self, monkeypatch):
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        monkeypatch.setenv("EUMDAC_TOKEN_VALIDITY", "3600")
        _, _, validity = _load_credentials()
        assert validity == 3600

    def test_reads_from_dotenv_file(self, tmp_path, monkeypatch):
        (tmp_path / ".env").write_text("EUMDAC_KEY=dotenv-key\nEUMDAC_SECRET=dotenv-secret\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        key, secret, validity = _load_credentials()
        assert key == "dotenv-key"
        assert secret == "dotenv-secret"
        assert validity == DEFAULT_VALIDITY

    def test_reads_validity_from_dotenv_file(self, tmp_path, monkeypatch):
        (tmp_path / ".env").write_text("EUMDAC_KEY=k\nEUMDAC_SECRET=s\nEUMDAC_TOKEN_VALIDITY=7200\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        _, _, validity = _load_credentials()
        assert validity == 7200

    def test_reads_from_credentials_file(self, tmp_path, monkeypatch):
        cred_dir = tmp_path / ".eumdac"
        cred_dir.mkdir()
        (cred_dir / "credentials").write_text("file-key, file-secret")
        monkeypatch.chdir(tmp_path)  # no .env here
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path):
            key, secret, validity = _load_credentials()
        assert key == "file-key"
        assert secret == "file-secret"
        assert validity == DEFAULT_VALIDITY

    def test_credentials_file_no_trailing_whitespace(self, tmp_path, monkeypatch):
        """Whitespace around key/secret is stripped."""
        cred_dir = tmp_path / ".eumdac"
        cred_dir.mkdir()
        (cred_dir / "credentials").write_text("  spaced-key  ,  spaced-secret  ")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path):
            key, secret, _ = _load_credentials()
        assert key == "spaced-key"
        assert secret == "spaced-secret"

    def test_env_vars_take_priority_over_dotenv(self, tmp_path, monkeypatch):
        (tmp_path / ".env").write_text("EUMDAC_KEY=dotenv-key\nEUMDAC_SECRET=dotenv-secret\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("EUMDAC_KEY", "env-key")
        monkeypatch.setenv("EUMDAC_SECRET", "env-secret")
        key, secret, _ = _load_credentials()
        assert key == "env-key"
        assert secret == "env-secret"

    def test_dotenv_takes_priority_over_credentials_file(self, tmp_path, monkeypatch):
        (tmp_path / ".env").write_text("EUMDAC_KEY=dotenv-key\nEUMDAC_SECRET=dotenv-secret\n")
        cred_dir = tmp_path / ".eumdac"
        cred_dir.mkdir()
        (cred_dir / "credentials").write_text("file-key, file-secret")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path):
            key, secret, _ = _load_credentials()
        assert key == "dotenv-key"
        assert secret == "dotenv-secret"

    def test_returns_none_when_nothing_found(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no .env
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path):
            key, secret, validity = _load_credentials()
        assert key is None
        assert secret is None
        assert validity == DEFAULT_VALIDITY  # default always present

    def test_empty_env_var_treated_as_missing(self, tmp_path, monkeypatch):
        """Empty env vars are treated as unset and do not short-circuit the chain."""
        monkeypatch.chdir(tmp_path)  # no .env
        monkeypatch.setenv("EUMDAC_KEY", "")
        monkeypatch.setenv("EUMDAC_SECRET", "")
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path):  # no cred file
            key, secret, _ = _load_credentials()
        assert not key
        assert not secret

    def test_invalid_validity_env_var_uses_default(self, monkeypatch):
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        monkeypatch.setenv("EUMDAC_TOKEN_VALIDITY", "not-a-number")
        _, _, validity = _load_credentials()
        assert validity == DEFAULT_VALIDITY

    def test_zero_validity_env_var_uses_default(self, monkeypatch):
        """Zero is not a valid validity; the default should be used."""
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        monkeypatch.setenv("EUMDAC_TOKEN_VALIDITY", "0")
        _, _, validity = _load_credentials()
        assert validity == DEFAULT_VALIDITY


# ---------------------------------------------------------------------------
# _Env singleton behaviour
# ---------------------------------------------------------------------------


class TestEnv:
    def test_env_has_key_secret_validity_attributes(self):
        assert hasattr(ENV, "key")
        assert hasattr(ENV, "secret")
        assert hasattr(ENV, "validity")

    def test_env_exposes_string_or_none(self):
        assert ENV.key is None or isinstance(ENV.key, str)
        assert ENV.secret is None or isinstance(ENV.secret, str)

    def test_env_validity_is_int(self):
        assert isinstance(ENV.validity, int)
        assert ENV.validity > 0

    def test_env_validity_defaults_to_86400_when_unset(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        env = _Env()
        assert env.validity == DEFAULT_VALIDITY

    def test_env_validity_reads_from_env_var(self, monkeypatch):
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        monkeypatch.setenv("EUMDAC_TOKEN_VALIDITY", "1800")
        env = _Env()
        assert env.validity == 1800

    def test_env_warns_when_no_credentials(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("EUMDAC_KEY", raising=False)
        monkeypatch.delenv("EUMDAC_SECRET", raising=False)
        monkeypatch.delenv("EUMDAC_TOKEN_VALIDITY", raising=False)
        monkeypatch.setattr(env_module, "_credentials_warning_emitted", False)
        with mock.patch("pathlib.Path.home", return_value=tmp_path), warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            env = _Env()
        assert env.key is None
        assert env.secret is None
        assert len(caught) == 1
        assert issubclass(caught[0].category, UserWarning)
        assert "EUMDAC credentials not found" in str(caught[0].message)

    def test_env_no_warning_when_credentials_present(self, monkeypatch):
        monkeypatch.setenv("EUMDAC_KEY", "k")
        monkeypatch.setenv("EUMDAC_SECRET", "s")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            env = _Env()
        assert env.key == "k"
        assert env.secret == "s"
        assert len(caught) == 0
