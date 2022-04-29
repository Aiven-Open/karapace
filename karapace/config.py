"""
karapace - configuration validation

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from aiohttp.web_log import AccessLogger
from enum import Enum, unique
from karapace.constants import DEFAULT_SCHEMA_TOPIC
from pathlib import Path
from typing import Dict, IO, List, Optional, Union

import logging
import os
import socket
import ssl
import ujson

Config = Dict[str, Union[None, str, int, bool, List[str], AccessLogger]]
LOG = logging.getLogger(__name__)
HOSTNAME = socket.gethostname()
SASL_PLAIN_PASSWORD = "sasl_plain_password"
DEFAULTS = {
    "access_logs_debug": False,
    "access_log_class": None,
    "advertised_hostname": HOSTNAME,
    "advertised_port": None,
    "advertised_protocol": "http",
    "bootstrap_uri": "127.0.0.1:9092",
    "client_id": "sr-1",
    "compatibility": "BACKWARD",
    "connections_max_idle_ms": 15000,
    "consumer_enable_auto_commit": True,
    "consumer_request_timeout_ms": 11000,
    "consumer_request_max_bytes": 67108864,
    "fetch_min_bytes": -1,
    "group_id": "schema-registry",
    "host": "127.0.0.1",
    "port": 8081,
    "server_tls_certfile": None,
    "server_tls_keyfile": None,
    "registry_host": "127.0.0.1",
    "registry_port": 8081,
    "registry_ca": None,
    "log_level": "DEBUG",
    "log_format": "%(name)-20s\t%(threadName)s\t%(levelname)-8s\t%(message)s",
    "master_eligibility": True,
    "replication_factor": 1,
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": None,
    "ssl_certfile": None,
    "ssl_keyfile": None,
    "ssl_check_hostname": True,
    "ssl_crlfile": None,
    "ssl_password": None,
    "sasl_mechanism": None,
    "sasl_plain_username": None,
    SASL_PLAIN_PASSWORD: None,
    "topic_name": DEFAULT_SCHEMA_TOPIC,
    "metadata_max_age_ms": 60000,
    "admin_metadata_max_age": 5,
    "producer_acks": 1,
    "producer_compression_type": None,
    "producer_count": 5,
    "producer_linger_ms": 100,
    "session_timeout_ms": 10000,
    "karapace_rest": False,
    "karapace_registry": False,
    "master_election_strategy": "lowest",
    "protobuf_runtime_directory": "runtime",
}
SECRET_CONFIG_OPTIONS = [SASL_PLAIN_PASSWORD]


class InvalidConfiguration(Exception):
    pass


@unique
class ElectionStrategy(Enum):
    highest = "highest"
    lowest = "lowest"


def parse_env_value(value: str) -> Union[str, int, bool]:
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


def set_config_defaults(config: Config) -> Config:
    new_config = DEFAULTS.copy()
    new_config.update(config)

    set_settings_from_environment(new_config)
    set_sentry_dsn_from_environment(new_config)
    validate_config(new_config)
    return new_config


def set_settings_from_environment(config: Config) -> None:
    """The environment variables have precedence and overwrite the configuration settings."""
    for config_name in DEFAULTS:
        config_name_with_prefix = config_name if config_name.startswith("karapace") else f"karapace_{config_name}"
        env_name = config_name_with_prefix.upper()
        env_val = os.environ.get(env_name)
        if env_val is not None:
            if config_name not in SECRET_CONFIG_OPTIONS:
                LOG.info(
                    "Populating config value %r from env var %r with %r instead of config file",
                    config_name,
                    env_name,
                    env_val,
                )
            else:
                LOG.info(
                    "Populating config value %r from env var %r instead of config file",
                    config_name,
                    env_name,
                )

            config[config_name] = parse_env_value(env_val)


def set_sentry_dsn_from_environment(config: Config) -> None:
    sentry_config = config.setdefault("sentry", {"dsn": None})

    # environment variable has precedence
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if sentry_dsn is not None:
        sentry_config["dsn"] = sentry_dsn

    # Tag app should always be karapace
    sentry_config.setdefault("tags", {})
    sentry_config["tags"]["app"] = "Karapace"


def validate_config(config: Config) -> None:
    master_election_strategy = config["master_election_strategy"]
    try:
        ElectionStrategy(master_election_strategy.lower())
    except ValueError:
        valid_strategies = [strategy.value for strategy in ElectionStrategy]
        raise InvalidConfiguration(
            f"Invalid master election strategy: {master_election_strategy}, valid values are {valid_strategies}"
        ) from None


def write_config(config_path: Path, custom_values: Config) -> None:
    config_path.write_text(ujson.dumps(custom_values))


def read_config(config_handler: IO) -> Config:
    try:
        config = ujson.load(config_handler)
    except ValueError as ex:
        raise InvalidConfiguration("Configuration is not a valid JSON") from ex

    return set_config_defaults(config)


def create_client_ssl_context(config: Config) -> Optional[ssl.SSLContext]:
    # taken from conn.py, as it adds a lot more logic to the context configuration than the initial version
    if config["security_protocol"] == "PLAINTEXT":
        return None
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_context.options |= ssl.OP_NO_SSLv2
    ssl_context.options |= ssl.OP_NO_SSLv3
    ssl_context.options |= ssl.OP_NO_TLSv1
    ssl_context.options |= ssl.OP_NO_TLSv1_1
    ssl_context.verify_mode = ssl.CERT_OPTIONAL
    if config["ssl_check_hostname"]:
        ssl_context.check_hostname = True
    if config["ssl_cafile"]:
        ssl_context.load_verify_locations(config["ssl_cafile"])
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    if config["ssl_certfile"] and config["ssl_keyfile"]:
        ssl_context.load_cert_chain(
            certfile=config["ssl_certfile"],
            keyfile=config["ssl_keyfile"],
            password=config["ssl_password"],
        )
    if config["ssl_crlfile"]:
        if not hasattr(ssl, "VERIFY_CRL_CHECK_LEAF"):
            raise RuntimeError("This version of Python does not support ssl_crlfile!")
        ssl_context.load_verify_locations(config["ssl_crlfile"])
        ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    if config.get("ssl_ciphers"):
        ssl_context.set_ciphers(config["ssl_ciphers"])
    return ssl_context


def create_server_ssl_context(config: Config) -> Optional[ssl.SSLContext]:
    tls_certfile = config["server_tls_certfile"]
    tls_keyfile = config["server_tls_keyfile"]
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
