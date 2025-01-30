"""
karapace - convert_to_int

Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from http import HTTPStatus


def convert_to_int(object_: dict, key: str, content_type: str) -> None:
    if object_.get(key) is None:
        return
    try:
        object_[key] = int(object_[key])
    except ValueError:
        from karapace.rapu import http_error

        http_error(
            message=f"{key} is not a valid int: {object_[key]}",
            content_type=content_type,
            code=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
