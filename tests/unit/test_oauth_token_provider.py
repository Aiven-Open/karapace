"""
Tests for sasl_oauth_token_provider_class config and passthrough.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from unittest.mock import MagicMock, patch

import pytest

from karapace.core.config import Config
from karapace.core import kafka_utils
from karapace.core.kafka_utils import get_oauth_token_provider


@pytest.fixture(autouse=True)
def _clear_provider_cache():
    """Clear the singleton cache between tests."""
    kafka_utils._oauth_token_provider_cache.clear()
    yield
    kafka_utils._oauth_token_provider_cache.clear()


class StubTokenProvider:
    """A minimal token provider implementing the TokenWithExpiryProvider protocol."""

    def token_with_expiry(self, config=None):
        return ("fake-token", 9999999999)


def _make_config_with_provider():
    config = Config()
    config.sasl_mechanism = "OAUTHBEARER"
    config.security_protocol = "SASL_SSL"
    config.sasl_oauth_token_provider_class = StubTokenProvider
    return config


class TestGetOauthTokenProvider:
    def test_returns_none_when_not_configured(self):
        config = Config()
        assert get_oauth_token_provider(config) is None

    def test_returns_instance_when_configured(self):
        config = _make_config_with_provider()
        provider = get_oauth_token_provider(config)
        assert provider is not None
        assert isinstance(provider, StubTokenProvider)
        assert provider.token_with_expiry() == ("fake-token", 9999999999)

    def test_raises_when_missing_token_with_expiry(self):
        class BadProvider:
            pass

        config = Config()
        config.sasl_oauth_token_provider_class = BadProvider
        with pytest.raises(ValueError, match="must implement a token_with_expiry"):
            get_oauth_token_provider(config)

    def test_returns_cached_instance(self):
        config = _make_config_with_provider()
        first = get_oauth_token_provider(config)
        second = get_oauth_token_provider(config)
        assert first is second


class TestKafkaUtilsPassthrough:
    """Verify that factory functions pass sasl_oauth_token_provider to Kafka clients."""

    @patch("karapace.core.kafka_utils.KafkaAdminClient")
    def test_admin_passes_provider(self, mock_admin_cls):
        from karapace.core.kafka_utils import kafka_admin_from_config

        config = _make_config_with_provider()
        kafka_admin_from_config(config)

        call_kwargs = mock_admin_cls.call_args[1]
        assert "sasl_oauth_token_provider" in call_kwargs
        assert isinstance(call_kwargs["sasl_oauth_token_provider"], StubTokenProvider)

    @patch("karapace.core.kafka_utils.KafkaAdminClient")
    def test_admin_omits_provider_when_not_configured(self, mock_admin_cls):
        from karapace.core.kafka_utils import kafka_admin_from_config

        config = Config()
        kafka_admin_from_config(config)

        call_kwargs = mock_admin_cls.call_args[1]
        assert "sasl_oauth_token_provider" not in call_kwargs

    @patch("karapace.core.kafka_utils.KafkaConsumer")
    def test_consumer_passes_provider(self, mock_consumer_cls):
        from karapace.core.kafka_utils import kafka_consumer_from_config

        config = _make_config_with_provider()
        with kafka_consumer_from_config(config, "test-topic"):
            pass

        call_kwargs = mock_consumer_cls.call_args[1]
        assert "sasl_oauth_token_provider" in call_kwargs
        assert isinstance(call_kwargs["sasl_oauth_token_provider"], StubTokenProvider)

    @patch("karapace.core.kafka_utils.KafkaConsumer")
    def test_consumer_omits_provider_when_not_configured(self, mock_consumer_cls):
        from karapace.core.kafka_utils import kafka_consumer_from_config

        config = Config()
        with kafka_consumer_from_config(config, "test-topic"):
            pass

        call_kwargs = mock_consumer_cls.call_args[1]
        assert "sasl_oauth_token_provider" not in call_kwargs

    @patch("karapace.core.kafka_utils.KafkaProducer")
    def test_producer_passes_provider(self, mock_producer_cls):
        from karapace.core.kafka_utils import kafka_producer_from_config

        config = _make_config_with_provider()
        with kafka_producer_from_config(config):
            pass

        call_kwargs = mock_producer_cls.call_args[1]
        assert "sasl_oauth_token_provider" in call_kwargs
        assert isinstance(call_kwargs["sasl_oauth_token_provider"], StubTokenProvider)

    @patch("karapace.core.kafka_utils.KafkaProducer")
    def test_producer_omits_provider_when_not_configured(self, mock_producer_cls):
        from karapace.core.kafka_utils import kafka_producer_from_config

        config = Config()
        with kafka_producer_from_config(config):
            pass

        call_kwargs = mock_producer_cls.call_args[1]
        assert "sasl_oauth_token_provider" not in call_kwargs


class TestSchemaReaderPassthrough:
    """Verify that schema_reader internal factories also pass the provider.

    The schema_reader module transitively imports the protopace Go shared library,
    which may not be available in all test environments. We mock the protopace
    module before importing schema_reader to avoid that dependency.
    """

    @staticmethod
    def _import_schema_reader():
        """Import schema_reader with protopace mocked out."""
        import importlib
        import sys

        # If already imported, just return the cached module
        if "karapace.core.schema_reader" in sys.modules:
            return sys.modules["karapace.core.schema_reader"]

        # Mock the protopace shared library before importing schema_reader
        mock_protopace = MagicMock()
        mock_protopace.Proto = MagicMock
        modules_to_mock = {
            "karapace.core.protobuf.protopace.protopace": mock_protopace,
            "karapace.core.protobuf.protopace": mock_protopace,
        }
        with patch.dict(sys.modules, modules_to_mock):
            # Force re-import of dependency that triggers protopace
            if "karapace.core.dependency" in sys.modules:
                del sys.modules["karapace.core.dependency"]
            if "karapace.core.schema_reader" in sys.modules:
                del sys.modules["karapace.core.schema_reader"]
            mod = importlib.import_module("karapace.core.schema_reader")
        return mod

    def test_schema_reader_consumer_passes_provider(self):
        schema_reader = self._import_schema_reader()
        with patch.object(schema_reader, "KafkaConsumer") as mock_consumer_cls:
            config = _make_config_with_provider()
            schema_reader._create_consumer_from_config(config)

            call_kwargs = mock_consumer_cls.call_args[1]
            assert "sasl_oauth_token_provider" in call_kwargs
            assert isinstance(call_kwargs["sasl_oauth_token_provider"], StubTokenProvider)

    def test_schema_reader_consumer_omits_provider_when_not_configured(self):
        schema_reader = self._import_schema_reader()
        with patch.object(schema_reader, "KafkaConsumer") as mock_consumer_cls:
            config = Config()
            schema_reader._create_consumer_from_config(config)

            call_kwargs = mock_consumer_cls.call_args[1]
            assert "sasl_oauth_token_provider" not in call_kwargs

    def test_schema_reader_admin_passes_provider(self):
        schema_reader = self._import_schema_reader()
        with patch.object(schema_reader, "KafkaAdminClient") as mock_admin_cls:
            config = _make_config_with_provider()
            schema_reader._create_admin_client_from_config(config)

            call_kwargs = mock_admin_cls.call_args[1]
            assert "sasl_oauth_token_provider" in call_kwargs
            assert isinstance(call_kwargs["sasl_oauth_token_provider"], StubTokenProvider)

    def test_schema_reader_admin_omits_provider_when_not_configured(self):
        schema_reader = self._import_schema_reader()
        with patch.object(schema_reader, "KafkaAdminClient") as mock_admin_cls:
            config = Config()
            schema_reader._create_admin_client_from_config(config)

            call_kwargs = mock_admin_cls.call_args[1]
            assert "sasl_oauth_token_provider" not in call_kwargs


class TestConfigImportString:
    """Verify the config field works with importable string paths."""

    def test_config_accepts_none_by_default(self):
        config = Config()
        assert config.sasl_oauth_token_provider_class is None

    def test_config_accepts_class_directly(self):
        config = Config()
        config.sasl_oauth_token_provider_class = StubTokenProvider
        assert config.sasl_oauth_token_provider_class is StubTokenProvider
