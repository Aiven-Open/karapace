"""
karapace - configuration validation

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from karapace.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST, DEFAULT_SCHEMA_TOPIC
from karapace.typing import ElectionStrategy, NameStrategy
from karapace.utils import json_encode
from pathlib import Path
from pydantic import BaseModel, ImportString
from pydantic_settings import BaseSettings, SettingsConfigDict

import enum
import logging
import os
import socket
import ssl

HOSTNAME = socket.gethostname()

try:
    from opentelemetry import version as otel_version

    OTEL_VERSION = otel_version.__version__
except Exception:
    OTEL_VERSION = ""


class KarapaceTags(BaseModel):
    app: str = "Karapace"


class KarapaceTelemetryOTelExporter(str, enum.Enum):
    OTLP = "OTLP"
    CONSOLE = "CONSOLE"
    NOOP = "NOOP"


class KarapaceTelemetry(BaseModel):
    otel_endpoint_url: str | None = None
    otel_exporter: KarapaceTelemetryOTelExporter = KarapaceTelemetryOTelExporter.NOOP
    resource_service_name: str = "karapace"
    resource_service_instance_id: str = "karapace"
    resource_telemetry_sdk_name: str = "opentelemetry"
    resource_telemetry_sdk_language: str = "python"
    resource_telemetry_sdk_version: str = OTEL_VERSION


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="karapace_", env_ignore_empty=True, env_nested_delimiter="__")

    access_logs_debug: bool = False
    access_log_class: ImportString = "aiohttp.web_log.AccessLogger"
    advertised_hostname: str | None = None
    advertised_port: int | None = None
    advertised_protocol: str = "http"
    bootstrap_uri: str = "kafka:29092"
    sasl_bootstrap_uri: str | None = None
    client_id: str = "sr-1"
    compatibility: str = "BACKWARD"
    connections_max_idle_ms: int = 15000
    consumer_enable_auto_commit: bool = True
    consumer_request_timeout_ms: int = 11000
    consumer_request_max_bytes: int = 67108864
    consumer_idle_disconnect_timeout: int = 0
    fetch_min_bytes: int = 1
    force_key_correction: bool = False
    group_id: str = "schema-registry"
    http_request_max_size: int | None = None
    host: str = "127.0.0.1"
    port: int = 8081
    server_tls_certfile: str | None = None
    server_tls_keyfile: str | None = None
    registry_host: str = "karapace-schema-registry"
    registry_port: int = 8081
    registry_user: str | None = None
    registry_password: str | None = None
    registry_ca: str | None = None
    registry_authfile: str | None = None
    rest_authorization: bool = False
    rest_base_uri: str | None = None
    log_handler: str | None = "stdout"
    log_level: str = "DEBUG"
    log_format: str = "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s"
    master_eligibility: bool = True
    replication_factor: int = 1
    security_protocol: str = "PLAINTEXT"
    ssl_ciphers: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_check_hostname: bool = True
    ssl_crlfile: str | None = None
    ssl_password: str | None = None
    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    sasl_oauth_token: str | None = None
    topic_name: str = DEFAULT_SCHEMA_TOPIC
    metadata_max_age_ms: int = 60000
    admin_metadata_max_age: int = 5
    producer_acks: int = 1
    producer_compression_type: str | None = None
    producer_count: int = 5
    producer_linger_ms: int = 100
    producer_max_request_size: int = DEFAULT_PRODUCER_MAX_REQUEST
    session_timeout_ms: int = 10000
    karapace_rest: bool = False
    karapace_registry: bool = False
    name_strategy: str = "topic_name"
    name_strategy_validation: bool = True
    master_election_strategy: str = "lowest"
    protobuf_runtime_directory: str = "runtime"
    statsd_host: str | None = None
    statsd_port: int = 8125
    kafka_schema_reader_strict_mode: bool = False
    kafka_retriable_errors_silenced: bool = True
    use_protobuf_formatter: bool = False
    waiting_time_before_acting_as_master_ms: int = 5000

    sentry: Mapping[str, object] | None = None
    tags: KarapaceTags = KarapaceTags()
    telemetry: KarapaceTelemetry = KarapaceTelemetry()

    # add rest uri if not set
    # f"{new_config['advertised_protocol']}://{new_config['advertised_hostname']}:{new_config['advertised_port']}"

    # set tags if not set
    #  new_config["tags"]["app"] = "Karapace"

    def get_advertised_port(self) -> int:
        return self.advertised_port or self.port

    def get_advertised_hostname(self) -> str:
        return self.advertised_hostname or self.host

    def get_rest_base_uri(self) -> str:
        return (
            self.rest_base_uri
            or f"{self.advertised_protocol}://{self.get_advertised_hostname()}:{self.get_advertised_port()}"
        )

    def to_env_str(self) -> str:
        env_lines: list[str] = []
        for key, value in self.model_dump().items():
            if value is not None:
                env_lines.append(f"{key.upper()}={value}")
        return "\n".join(env_lines)

    def set_config_defaults(self, new_config: Mapping[str, str] | None = None) -> Config:
        config = deepcopy(self)
        if new_config:
            for key, value in new_config.items():
                setattr(config, key, value)

        # Fallback to default port if `advertised_port` is not set
        if config.advertised_port is None:
            config.advertised_port = config.port

        # Fallback to `advertised_*` constructed URI if not set
        if config.rest_base_uri is None:
            config.rest_base_uri = f"{config.advertised_protocol}://{config.advertised_hostname}:{config.advertised_port}"

        # Set the aiohttp client max size if REST Proxy is enabled and producer max request configuration is altered
        # from default and aiohttp client max size is not set
        # Use the http request max size from the configuration without altering if set.
        if (
            config.karapace_rest
            and config.producer_max_request_size > DEFAULT_PRODUCER_MAX_REQUEST
            and config.http_request_max_size is None
        ):
            # REST Proxy API configuration for producer max request size must be taken into account
            # also for the aiohttp.web.Application client max size.
            # Always add the aiohttp default client max size as the headroom above the producer max request size.
            # The input JSON size for REST Proxy is not easy to estimate, lot of small records in single request has
            # a lot of overhead due to JSON structure.
            config.http_request_max_size = config.producer_max_request_size + DEFAULT_AIOHTTP_CLIENT_MAX_SIZE
        elif config.http_request_max_size is None:
            # Set the default aiohttp client max size
            config.http_request_max_size = DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

        validate_config(config)
        return config


# class ConfigDefaults(Config, total=False):
#    ...


LOG = logging.getLogger(__name__)
HOSTNAME = socket.gethostname()
SASL_PLAIN_PASSWORD = "sasl_plain_password"
SECRET_CONFIG_OPTIONS = [SASL_PLAIN_PASSWORD]


class InvalidConfiguration(Exception):
    pass


def parse_env_value(value: str) -> str | int | bool:
    # we only have ints, strings and bools in the config
    try:
        return int(value)
    except ValueError:
        pass
    if value.lower() == "false":
        return False
    if value.lower() == "true":
        return True
    return value


# def set_settings_from_environment(config: Config) -> None:
#    """The environment variables have precedence and overwrite the configuration settings."""
#    for config_name in DEFAULTS:
#        config_name_with_prefix = config_name if config_name.startswith("karapace") else f"karapace_{config_name}"
#        env_name = config_name_with_prefix.upper()
#        env_val = os.environ.get(env_name)
#        if env_val is not None:
#            if config_name not in SECRET_CONFIG_OPTIONS:
#                LOG.info(
#                    "Populating config value %r from env var %r with %r instead of config file",
#                    config_name,
#                    env_name,
#                    env_val,
#                )
#            else:
#                LOG.info(
#                    "Populating config value %r from env var %r instead of config file",
#                    config_name,
#                    env_name,
#                )
#
#            config[config_name] = parse_env_value(env_val)


def set_sentry_dsn_from_environment(config: Config) -> None:
    sentry_config = config.setdefault("sentry", {"dsn": None})

    # environment variable has precedence
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn is not None:
        sentry_config["dsn"] = sentry_dsn


def validate_config(config: Config) -> None:
    master_election_strategy = config.master_election_strategy
    try:
        ElectionStrategy(master_election_strategy.lower())
    except ValueError:
        valid_strategies = [strategy.value for strategy in ElectionStrategy]
        raise InvalidConfiguration(
            f"Invalid master election strategy: {master_election_strategy}, valid values are {valid_strategies}"
        ) from None

    name_strategy = config.name_strategy
    try:
        NameStrategy(name_strategy)
    except ValueError:
        valid_strategies = list(NameStrategy)
        raise InvalidConfiguration(
            f"Invalid default name strategy: {name_strategy}, valid values are {valid_strategies}"
        ) from None

    if config.rest_authorization and config.sasl_bootstrap_uri is None:
        raise InvalidConfiguration(
            "Using 'rest_authorization' requires configuration value for 'sasl_bootstrap_uri' to be set"
        )


def write_config(config_path: Path, custom_values: Config) -> None:
    config_path.write_text(json_encode(custom_values))


def write_env_file(dot_env_path: Path, config: Config) -> None:
    dot_env_path.write_text(config.to_env_str())


def read_env_file(env_file_path: str) -> Config:
    return Config(_env_file=env_file_path, _env_file_encoding="utf-8")


def create_client_ssl_context(config: Config) -> ssl.SSLContext | None:
    # taken from conn.py, as it adds a lot more logic to the context configuration than the initial version
    if config.security_protocol in ("PLAINTEXT", "SASL_PLAINTEXT"):
        return None
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3
    ssl_context.options |= ssl.OP_NO_TLSv1
    ssl_context.options |= ssl.OP_NO_TLSv1_1
    ssl_context.verify_mode = ssl.CERT_OPTIONAL
    if config.ssl_check_hostname:
        ssl_context.check_hostname = True
    if config.ssl_cafile:
        ssl_context.load_verify_locations(config.ssl_cafile)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    if config.ssl_certfile and config.ssl_keyfile:
        ssl_context.load_cert_chain(
            certfile=config.ssl_certfile,
            keyfile=config.ssl_keyfile,
            password=config.ssl_password,
        )
    if config.ssl_crlfile:
        if not hasattr(ssl, "VERIFY_CRL_CHECK_LEAF"):
            raise RuntimeError("This version of Python does not support ssl_crlfile!")
        ssl_context.load_verify_locations(config.ssl_crlfile)
        ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    if config.ssl_ciphers:
        ssl_context.set_ciphers(config.ssl_ciphers)
    return ssl_context


def create_server_ssl_context(config: Config) -> ssl.SSLContext | None:
    tls_certfile = config.server_tls_certfile
    tls_keyfile = config.server_tls_keyfile
    if tls_certfile is None:
        if tls_keyfile is None:
            # Neither config value set, do not use TLS
            return None
        raise InvalidConfiguration("`server_tls_keyfile` defined but `server_tls_certfile` not defined")
    if tls_keyfile is None:
        raise InvalidConfiguration("`server_tls_certfile` defined but `server_tls_keyfile` not defined")
    if not isinstance(tls_certfile, str):
        raise InvalidConfiguration("`server_tls_certfile` is not a string")
    if not isinstance(tls_keyfile, str):
        raise InvalidConfiguration("`server_tls_keyfile` is not a string")
    if not os.path.exists(tls_certfile):
        raise InvalidConfiguration("`server_tls_certfile` file does not exist")
    if not os.path.exists(tls_keyfile):
        raise InvalidConfiguration("`server_tls_keyfile` file does not exist")

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3
    ssl_context.options |= ssl.OP_NO_TLSv1
    ssl_context.options |= ssl.OP_NO_TLSv1_1

    ssl_context.load_cert_chain(certfile=tls_certfile, keyfile=tls_keyfile)
    return ssl_context
