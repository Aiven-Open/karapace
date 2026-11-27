"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

from karapace.core.contexts import DEFAULT_CONTEXT, normalize_context, qualify
from starlette.types import ASGIApp, Receive, Scope, Send
from urllib.parse import parse_qsl, quote, unquote, urlencode

# Path segments followed by a subject: /contexts/{ctx}/subjects/{subject}/... and so on.
_SUBJECT_RESOURCES = frozenset({"subjects", "config", "mode"})


def rewrite_context_path(raw_path: str, query_string: str) -> tuple[str, str] | None:
    """Map a /contexts/{ctx}/... request onto the existing routes, in the given context.

    - ``/contexts/.A/subjects/s/...`` (also ``config/s``, ``mode/s``) becomes ``/subjects/:.A:s/...``;
    - ``/contexts/.A/schemas/ids/1...`` becomes ``/schemas/ids/1...?subject=:.A:``, the subject then
      selects the context of the id.

    ``raw_path`` is percent-encoded. Return the rewritten (raw path, query string), or None when the
    path is not a context path this mapping covers: it is then left to the routes, as any other path.
    """
    segments = raw_path.split("?", 1)[0].split("/")
    # ["", "contexts", "{ctx}", resource, ...]
    if len(segments) < 4 or segments[0] != "" or segments[1] != "contexts":
        return None
    context = normalize_context(unquote(segments[2]))
    rest = segments[3:]

    if rest[0] in _SUBJECT_RESOURCES and len(rest) >= 2 and rest[1]:
        if context != DEFAULT_CONTEXT:
            rest[1] = quote(qualify(context, ""), safe=":.") + rest[1]
        return "/" + "/".join(rest), query_string

    if rest[0] == "schemas" and len(rest) >= 3 and rest[1] == "ids":
        params = parse_qsl(query_string, keep_blank_values=True)
        subject = next((value for key, value in params if key == "subject"), "")
        query = [(key, value) for key, value in params if key != "subject"]
        query.append(("subject", qualify(context, subject)))
        return "/" + "/".join(rest), urlencode(query)

    return None


class ContextPathMiddleware:
    """Serves the /contexts/{ctx}/... paths by rewriting them before routing.

    The routes match on ``raw_path`` (see RawPathRoute), so both ``path`` and ``raw_path`` are rewritten.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            raw = scope.get("raw_path")
            raw_path = raw.decode("utf-8") if raw is not None else quote(scope["path"])
            rewritten = rewrite_context_path(raw_path, scope.get("query_string", b"").decode("utf-8"))
            if rewritten is not None:
                new_raw_path, new_query = rewritten
                scope = dict(scope)
                scope["raw_path"] = new_raw_path.encode("utf-8")
                scope["path"] = unquote(new_raw_path)
                scope["query_string"] = new_query.encode("utf-8")
        await self.app(scope, receive, send)
