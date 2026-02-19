"""Tests for authentication module."""

from __future__ import annotations

from unittest import mock

import pytest

import eumdac_fetch.auth as auth_module
from eumdac_fetch.auth import create_token, get_token

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reset_singleton():
    """Reset the module-level token singleton between tests."""
    auth_module._token = None


# ---------------------------------------------------------------------------
# create_token
# ---------------------------------------------------------------------------


class TestCreateToken:
    def test_missing_key_raises(self):
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = None
            mock_env.secret = "s"
            with pytest.raises(ValueError, match="EUMDAC credentials not found"):
                create_token()

    def test_missing_secret_raises(self):
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = None
            with pytest.raises(ValueError, match="EUMDAC credentials not found"):
                create_token()

    def test_missing_both_raises(self):
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = None
            mock_env.secret = None
            with pytest.raises(ValueError, match="EUMDAC credentials not found"):
                create_token()

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_uses_env_key_and_secret(self, mock_cls):
        mock_cls.return_value = mock.MagicMock()
        mock_cls.return_value.expiration = "2099-01-01T00:00:00Z"
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "env-key"
            mock_env.secret = "env-secret"
            mock_env.validity = 86400
            create_token()
        mock_cls.assert_called_once_with(
            credentials=("env-key", "env-secret"),
            validity=86400,
        )

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_uses_env_validity(self, mock_cls):
        mock_cls.return_value = mock.MagicMock()
        mock_cls.return_value.expiration = "2099-01-01T00:00:00Z"
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = "s"
            mock_env.validity = 3600
            create_token()
        mock_cls.assert_called_once_with(credentials=("k", "s"), validity=3600)

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_auth_failure_propagates(self, mock_cls):
        mock_cls.side_effect = Exception("OAuth2 error: invalid_client")
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = "s"
            mock_env.validity = 86400
            with pytest.raises(Exception, match="invalid_client"):
                create_token()

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_logs_expiration(self, mock_cls):
        mock_token = mock.MagicMock()
        mock_token.expiration = "2099-01-01T00:00:00Z"
        mock_cls.return_value = mock_token
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = "s"
            mock_env.validity = 86400
            with mock.patch("eumdac_fetch.auth.logger") as mock_logger:
                create_token()
        mock_logger.info.assert_called_once()
        assert "2099-01-01T00:00:00Z" in mock_logger.info.call_args[0][1]


# ---------------------------------------------------------------------------
# get_token — singleton behaviour
# ---------------------------------------------------------------------------


class TestGetToken:
    def setup_method(self):
        _reset_singleton()

    def teardown_method(self):
        _reset_singleton()

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_first_call_creates_token(self, mock_cls):
        mock_cls.return_value = mock.MagicMock()
        mock_cls.return_value.expiration = "2099-01-01T00:00:00Z"
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = "s"
            mock_env.validity = 86400
            token = get_token()
        assert token is mock_cls.return_value

    @mock.patch("eumdac_fetch.auth.eumdac.AccessToken")
    def test_second_call_returns_same_instance(self, mock_cls):
        mock_cls.return_value = mock.MagicMock()
        mock_cls.return_value.expiration = "2099-01-01T00:00:00Z"
        with mock.patch("eumdac_fetch.auth.ENV") as mock_env:
            mock_env.key = "k"
            mock_env.secret = "s"
            mock_env.validity = 86400
            t1 = get_token()
            t2 = get_token()
        assert t1 is t2
        mock_cls.assert_called_once()  # AccessToken constructed only once
