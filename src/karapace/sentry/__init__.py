from __future__ import annotations

from karapace.sentry.sentry_client_api import SentryClientAPI, SentryNoOpClient

import logging

LOG = logging.getLogger(__name__)


def _get_sentry_noop_client(sentry_dsn: str) -> SentryClientAPI:
    return SentryNoOpClient(sentry_dsn=sentry_dsn)


_get_sentry_client = _get_sentry_noop_client


try:
    from karapace.sentry.sentry_client import SentryClient

    # If Sentry SDK can be imported in SentryClient the Sentry SDK can be initialized.
    def _get_actual_sentry_client(sentry_dsn: str) -> SentryClientAPI:
        return SentryClient(sentry_dsn=sentry_dsn)

    _get_sentry_client = _get_actual_sentry_client
except ImportError:
    LOG.warning("Cannot enable Sentry.io sending: importing 'sentry_sdk' failed")


def get_sentry_client(sentry_dsn: str) -> SentryClientAPI:
    return _get_sentry_client(sentry_dsn=sentry_dsn)
