"""
karapace - configuration validation

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from pathlib import Path
from typing import Dict, IO, Union

import json
import os
import socket
import ssl

CONFIG_TYPE = Dict[str, Union[None, str, int, bool]]
DEFAULTS: CONFIG_TYPE = {
    "advertised_hostname": socket.gethostname(),
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
    "registry_host": "127.0.0.1",
    "log_level": "DEBUG",
    "registry_port": 8081,
    "port": 8081,
    "master_eligibility": True,
    "replication_factor": 1,
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": None,
    "ssl_certfile": None,
    "ssl_keyfile": None,
    "topic_name": "_schemas",
    "metadata_max_age_ms": 60000,
    "admin_metadata_max_age": 5,
    "producer_acks": 1,
    "producer_compression_type": None,
    "producer_count": 5,
    "producer_linger_ms": 0,
    "session_timeout_ms": 10000,
    "karapace_rest": False,
    "karapace_registry": False,
    "master_election_strategy": "lowest"
}


class InvalidConfiguration(Exception):
    pass


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


def set_config_defaults(config: CONFIG_TYPE) -> CONFIG_TYPE:
    for k, v in DEFAULTS.items():
        if k.startswith("karapace"):
            env_name = k.upper()
        else:
            env_name = f"karapace_{k}".upper()
        if env_name in os.environ:
            val = os.environ[env_name]
            print(f"Populating config value {k} from env var {env_name} with {val} instead of config file")
            config[k] = parse_env_value(os.environ[env_name])
        config.setdefault(k, v)

    strat = config["master_election_strategy"]
    msg = 'master_election_strategy must be "highest" or "lowest"'
    assert isinstance(strat, str), msg
    assert strat.lower() in {"highest", "lowest"}, msg

    return config


def write_config(config_path: Path, custom_values: Dict[str, Union[str, int, bool]]):
    config_path.write_text(json.dumps(custom_values))


def read_config(config_handler: IO) -> Dict[str, Union[str, int, bool]]:
    try:
        config = json.load(config_handler)
        config = set_config_defaults(config)
        return config
    except Exception as ex:
        raise InvalidConfiguration(ex) from None


def create_ssl_context(config: CONFIG_TYPE) -> ssl.SSLContext:
    # taken from conn.py, as it adds a lot more logic to the context configuration than the initial version
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_TLSv1  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_TLSv1_1  # pylint: disable=no-member
    ssl_context.verify_mode = ssl.CERT_OPTIONAL

    if config.get('ssl_check_hostname'):
        ssl_context.check_hostname = True

    if isinstance(config['ssl_cafile'], str):
        ssl_context.load_verify_locations(config['ssl_cafile'])
        ssl_context.verify_mode = ssl.CERT_REQUIRED

    if isinstance(config['ssl_certfile'], str) and isinstance(config['ssl_keyfile'], str):
        password = config.get('ssl_password')
        if not isinstance(password, str):
            raise ValueError("Invalid password type, it should be a string")

        ssl_context.load_cert_chain(certfile=config['ssl_certfile'], keyfile=config['ssl_keyfile'], password=password)

    if config.get('ssl_crlfile'):
        if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
            raise RuntimeError('This version of Python does not support ssl_crlfile!')

        ssl_crlfile = config['ssl_crlfile']
        if not isinstance(ssl_crlfile, str):
            raise ValueError("Invalid ssl_crlfile type, it should be a string")

        ssl_context.load_verify_locations(ssl_crlfile)
        ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF  # pylint: disable=no-member

    if config.get('ssl_ciphers'):
        ssl_ciphers = config['ssl_ciphers']
        if not isinstance(ssl_ciphers, str):
            raise ValueError("Invalid ssl_ciphers type, it should be a string")

        ssl_context.set_ciphers(ssl_ciphers)

    return ssl_context
