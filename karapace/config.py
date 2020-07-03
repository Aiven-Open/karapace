"""
karapace - configuration validation

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import json
import socket
import ssl

DEFAULTS = {
    "advertised_hostname": socket.gethostname(),
    "bootstrap_uri": "127.0.0.1:9092",
    "client_id": "sr-1",
    "compatibility": "BACKWARD",
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
    "karapace_rest": False,
    "karapace_registry": False,
}


class InvalidConfiguration(Exception):
    pass


def set_config_defaults(config):
    for k, v in DEFAULTS.items():
        config.setdefault(k, v)
    return config


def write_config(config_path, custom_values):
    with open(config_path, "w") as fp:
        fp.write(json.dumps(custom_values))


def read_config(config_path):
    with open(config_path, "r") as cf:
        try:
            config = json.loads(cf.read())
            config = set_config_defaults(config)
            return config
        except Exception as ex:
            raise InvalidConfiguration(ex)


def create_ssl_context(config):
    # taken from conn.py, as it adds a lot more logic to the context configuration than the initial version
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
    ssl_context.verify_mode = ssl.CERT_OPTIONAL
    if config.get('ssl_check_hostname'):
        ssl_context.check_hostname = True
    if config['ssl_cafile']:
        ssl_context.load_verify_locations(config['ssl_cafile'])
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    if config['ssl_certfile'] and config['ssl_keyfile']:
        ssl_context.load_cert_chain(
            certfile=config['ssl_certfile'], keyfile=config['ssl_keyfile'], password=config.get('ssl_password')
        )
    if config.get('ssl_crlfile'):
        if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
            raise RuntimeError('This version of Python does not support ssl_crlfile!')
        ssl_context.load_verify_locations(config['ssl_crlfile'])
        # pylint: disable=no-member
        ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    if config.get('ssl_ciphers'):
        ssl_context.set_ciphers(config['ssl_ciphers'])
    return ssl_context
