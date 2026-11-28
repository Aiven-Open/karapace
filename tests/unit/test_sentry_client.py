"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from karapace.core.sentry.sentry_client import SentryClient


def test_unexpected_exception_is_captured_with_tags() -> None:
    events: list = []
    client = SentryClient(
        sentry_config={
            "dsn": "https://key@localhost/1",
            "default_integrations": False,
            # returning None drops the event instead of sending it
            "before_send": lambda event, hint: events.append(event),
        }
    )

    client.unexpected_exception(ValueError("boom"), where="unit-test", tags={"extra": "1"})
    client.close()

    assert events[0]["tags"] == {"where": "unit-test", "extra": "1"}
