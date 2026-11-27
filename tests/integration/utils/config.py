"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ZKConfig:
    client_port: int
    admin_port: int
    path: str  # can't be a Path instance because it needs to be JSON serializable

    @staticmethod
    def from_dict(data: dict) -> "ZKConfig":
        return ZKConfig(
            data["client_port"],
            data["admin_port"],
            data["path"],
        )


@dataclass(frozen=True)
class KafkaDescription:
    version: str
    kafka_tgz: Path
    install_dir: Path
    download_url: str
    protocol_version: str


@dataclass
class KafkaConfig:
    datadir: str
    logdir: str
    plaintext_port: int
    # Listener authenticating with SASL/OAUTHBEARER through Kafka's built-in unsecured JWT
    # validator; used to exercise `sasl_oauth_token_provider_class` end to end.
    sasl_oauthbearer_port: int

    @staticmethod
    def from_dict(data: dict) -> "KafkaConfig":
        return KafkaConfig(
            datadir=data["datadir"],
            logdir=data["logdir"],
            plaintext_port=data["plaintext_port"],
            sasl_oauthbearer_port=data["sasl_oauthbearer_port"],
        )
