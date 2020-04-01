"""
karapace - configuration validation

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
import socket
import ssl


def set_config_defaults(config):
    config.setdefault("advertised_hostname", socket.gethostname())
    config.setdefault("bootstrap_uri", "127.0.0.1:9092")
    config.setdefault("client_id", "sr-1")
    config.setdefault("compatibility", "BACKWARD")
    config.setdefault("group_id", "schema-registry")
    config.setdefault("host", "127.0.0.1")
    config.setdefault("log_level", "DEBUG")
    config.setdefault("port", 8081)
    config.setdefault("master_eligibility", True)
    config.setdefault("replication_factor", 1)
    config.setdefault("security_protocol", "PLAINTEXT")
    config.setdefault("ssl_cafile", None)
    config.setdefault("ssl_certfile", None)
    config.setdefault("ssl_keyfile", None)
    config.setdefault("topic_name", "_schemas")
    config.setdefault("metadata_max_age_ms", 60000)
    return config


def create_ssl_context(config):
    # taken from conn.py, as it adds a lot more logic to the context configuration than the initial version
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
    ssl_context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
    ssl_context.verify_mode = ssl.CERT_OPTIONAL
    if config['ssl_check_hostname']:
        ssl_context.check_hostname = True
    if config['ssl_cafile']:
        ssl_context.load_verify_locations(config['ssl_cafile'])
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    if config['ssl_certfile'] and config['ssl_keyfile']:
        ssl_context.load_cert_chain(
            certfile=config['ssl_certfile'],
            keyfile=config['ssl_keyfile'],
            password=config['ssl_password'])
    if config['ssl_crlfile']:
        if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
            raise RuntimeError('This version of Python does not support ssl_crlfile!')
        ssl_context.load_verify_locations(config['ssl_crlfile'])
        # pylint: disable=no-member
        ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
    if config['ssl_ciphers']:
        ssl_context.set_ciphers(config['ssl_ciphers'])
    return ssl_context
