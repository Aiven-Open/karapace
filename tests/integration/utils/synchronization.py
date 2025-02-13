"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from pathlib import Path


def lock_path_for(path: Path) -> Path:
    """Append .lock to path"""
    suffixes = path.suffixes
    suffixes.append(".lock")
    return path.with_suffix("".join(suffixes))
