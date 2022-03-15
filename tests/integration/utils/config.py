"""
Copyright (c) 2022 Aiven Ltd
See LICENSE for details
"""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ZKConfig:
    client_port: int
    admin_port: int
    path: str

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
    install_dir: Path
    download_url: str
    protocol_version: str
