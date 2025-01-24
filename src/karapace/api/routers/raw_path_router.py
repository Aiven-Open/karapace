"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

"""
The MIT License (MIT)

Copyright (c) 2018 Sebastián Ramírez
"""

from fastapi import HTTPException  # noqa: E402
from fastapi.routing import APIRoute  # noqa: E402
from starlette.routing import Match  # noqa: E402
from starlette.types import Scope  # noqa: E402

import re  # noqa: E402


class RawPathRoute(APIRoute):
    """The subject in the paths can contain url encoded forward slash

    See explanation and origin of the solution:
     - https://github.com/fastapi/fastapi/discussions/7328#discussioncomment-8443865
    Starlette defect discussion:
     - https://github.com/encode/starlette/issues/826
    """

    def matches(self, scope: Scope) -> tuple[Match, Scope]:
        raw_path: str | None = None

        if "raw_path" in scope and scope["raw_path"] is not None:
            raw_path = scope["raw_path"].decode("utf-8")

        if raw_path is None:
            raise HTTPException(status_code=500, detail="Internal routing error")

        # Drop the last forward slash if present. e.g. '/path/' -> '/path', but from path '/'
        if len(raw_path) > 1:
            raw_path = raw_path if raw_path[-1] != "/" else raw_path[:-1]

        new_path = re.sub(r"\?.*", "", raw_path)
        scope["path"] = new_path
        return super().matches(scope)
