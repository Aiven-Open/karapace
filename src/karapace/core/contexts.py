"""
karapace - Schema contexts

A context is an independent namespace of schema ids and subjects. It is carried by the subject name
itself: ``:.ctx:subject`` is ``subject`` in context ``.ctx``, an unqualified subject is in the default
context ``.``. The qualified name is what is stored in the schemas topic, so a context needs no field
of its own.

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

DEFAULT_CONTEXT = "."
CONTEXT_DELIMITER = ":"
CONTEXT_SEPARATOR = "."


def split_subject(subject: str) -> tuple[str, str]:
    """Return (context, unqualified subject) of a subject, qualified or not.

    ``:.A:orders`` gives ``(".A", "orders")``, ``:.A:`` gives ``(".A", "")``, ``orders`` gives
    ``(".", "orders")``. A malformed prefix (no closing delimiter, no leading dot) is no context:
    the whole string is the subject, in the default context.
    """
    if subject.startswith(CONTEXT_DELIMITER + CONTEXT_SEPARATOR):
        end = subject.find(CONTEXT_DELIMITER, 1)
        if end > 0:
            return subject[1:end], subject[end + 1 :]
    return DEFAULT_CONTEXT, subject


def context_of(subject: str) -> str:
    return split_subject(subject)[0]


def normalize_context(context: str) -> str:
    """``A``, ``.A``, ``:.A:`` give ``.A`` ; an empty context or ``.`` is the default context."""
    context = context.strip(CONTEXT_DELIMITER)
    if not context.startswith(CONTEXT_SEPARATOR):
        context = CONTEXT_SEPARATOR + context
    return context


def qualify(context: str, subject: str) -> str:
    """Subject name in the given context, unqualified in the default context."""
    context = normalize_context(context)
    if context == DEFAULT_CONTEXT:
        return subject
    return f"{CONTEXT_DELIMITER}{context}{CONTEXT_DELIMITER}{subject}"
