"""
Path normalization utilities for preventing unbounded metric label cardinality.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import re

# UUIDs: 8-4-4-4-12 hex format
UUID_PATTERN = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
# Topic names after /topics/ - match until next slash or end
TOPIC_PATTERN = re.compile(r"(/topics/)([^/]+)")
# Schema IDs (numeric) after /schemas/ids/
SCHEMA_ID_PATTERN = re.compile(r"(/schemas/ids/)(\d+)")
# Subject names after /subjects/
SUBJECT_PATTERN = re.compile(r"(/subjects/)([^/]+)")
# Version numbers after /versions/
VERSION_PATTERN = re.compile(r"(/versions/)(\d+)")
# Subject names after /config/
CONFIG_SUBJECT_PATTERN = re.compile(r"(/config/)([^/]+)")
# Subject names after /mode/
MODE_SUBJECT_PATTERN = re.compile(r"(/mode/)([^/]+)")


def normalize_path(path: str) -> str:
    """Normalize a request path by replacing dynamic segments with placeholders.

    This prevents unbounded label cardinality in metrics which would
    cause memory growth over time as each unique path creates a new time series.

    Examples:
        /topics/my-topic -> /topics/{topic}
        /consumers/abc-123-def/instances/xyz-456 -> /consumers/{uuid}/instances/{uuid}
        /schemas/ids/42 -> /schemas/ids/{id}
        /subjects/my-subject/versions/3 -> /subjects/{subject}/versions/{version}
        /config/my-subject -> /config/{subject}
        /mode/my-subject -> /mode/{subject}
    """
    normalized = UUID_PATTERN.sub("{uuid}", path)
    normalized = TOPIC_PATTERN.sub(r"\1{topic}", normalized)
    normalized = SCHEMA_ID_PATTERN.sub(r"\1{id}", normalized)
    normalized = SUBJECT_PATTERN.sub(r"\1{subject}", normalized)
    normalized = VERSION_PATTERN.sub(r"\1{version}", normalized)
    normalized = CONFIG_SUBJECT_PATTERN.sub(r"\1{subject}", normalized)
    normalized = MODE_SUBJECT_PATTERN.sub(r"\1{subject}", normalized)
    return normalized
