"""
Path normalization utilities for preventing unbounded metric label cardinality.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import re
from enum import Enum


class PathPattern(Enum):
    """Regex patterns and their replacements for normalizing dynamic path segments.

    Each member is a (pattern, replacement) pair. Patterns are applied in
    declaration order -- put broader matches (e.g. UUIDs) before narrower ones.
    """

    UUID = (r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "{uuid}")
    TOPIC = (r"(/topics/)([^/]+)", r"\1{topic}")
    SCHEMA_ID = (r"(/schemas/ids/)(\d+)", r"\1{id}")
    SUBJECT = (r"(/subjects/)([^/]+)", r"\1{subject}")
    VERSION = (r"(/versions/)(\d+)", r"\1{version}")
    CONFIG_SUBJECT = (r"(/config/)([^/]+)", r"\1{subject}")
    MODE_SUBJECT = (r"(/mode/)([^/]+)", r"\1{subject}")

    def __init__(self, pattern: str, replacement: str) -> None:
        self.regex = re.compile(pattern)
        self.replacement = replacement


def normalize_path(path: str) -> str:
    """Normalize a request path by replacing dynamic segments with placeholders.

    This prevents unbounded label cardinality in metrics which would
    cause memory growth over time as each unique path creates a new time series.

    Examples:
        /topics/my-topic -> /topics/{topic}
        /consumers/3f410583-cfa3-48e8-bd9f-22eff1881c00/instances/0333f5a1-d242-42bc-ba35-d7c5d67fc121 -> /consumers/{uuid}/instances/{uuid}
        /schemas/ids/42 -> /schemas/ids/{id}
        /subjects/my-subject/versions/3 -> /subjects/{subject}/versions/{version}
        /config/my-subject -> /config/{subject}
        /mode/my-subject -> /mode/{subject}
    """
    normalized = path
    for p in PathPattern:
        normalized = p.regex.sub(p.replacement, normalized)
    return normalized
