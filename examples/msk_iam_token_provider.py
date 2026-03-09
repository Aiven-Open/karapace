"""Example: AWS MSK IAM token provider for Karapace OAUTHBEARER auth.

Copyright (c) 2024 Aiven Ltd
See LICENSE for details

This module provides a token provider that generates short-lived AWS MSK
authentication tokens using IAM credentials. It implements the
``TokenWithExpiryProvider`` protocol expected by Karapace's Kafka clients.

Usage
-----
Configure Karapace via environment variables::

    KARAPACE_SECURITY_PROTOCOL=SASL_SSL
    KARAPACE_SASL_MECHANISM=OAUTHBEARER
    KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=examples.msk_iam_token_provider:MSKIAMTokenProvider

Or pass ``sasl_oauth_token_provider_class`` in any config source that
Karapace supports (JSON config, pydantic-settings, etc.).

Requirements
------------
Install the AWS MSK auth helper::

    pip install aws-msk-iam-sasl-signer-python

The provider uses the default boto3 credential chain (env vars, instance
profile, ECS task role, etc.). For a specific profile or role, subclass
and override ``_generate_token``.

Protocol
--------
Karapace expects a class whose **instances** expose::

    def token_with_expiry(self, config: str | None) -> tuple[str, int | None]

The first element is the OAuth token string; the second is an optional
UNIX-epoch expiry timestamp (``int``) or ``None`` for no expiry.
"""

from __future__ import annotations

import logging

LOG = logging.getLogger(__name__)


class MSKIAMTokenProvider:
    """Generates MSK IAM auth tokens via ``aws_msk_iam_sasl_signer``.

    Instantiated once by Karapace; ``token_with_expiry`` is called by
    confluent-kafka's ``oauth_cb`` each time a token is needed (including
    automatic refresh before expiry).
    """

    def __init__(self, region: str | None = None) -> None:
        # Import here so the dependency is only required when this provider is used
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # type: ignore[import-untyped]

        self._msk_provider = MSKAuthTokenProvider
        self._region = region

    def _get_region(self) -> str:
        """Resolve the AWS region, falling back to boto3 session default."""
        if self._region:
            return self._region
        import boto3

        session = boto3.session.Session()
        region = session.region_name
        if not region:
            raise ValueError(
                "AWS region could not be determined. "
                "Set AWS_DEFAULT_REGION, pass region= to the provider, "
                "or configure a default region in your AWS profile."
            )
        return region

    def token_with_expiry(self, config: str | None = None) -> tuple[str, int | None]:
        """Return a fresh MSK auth token and its expiry.

        :param config: Unused; required by the ``oauth_cb`` callback signature.
        :returns: ``(token, expiry_epoch_ms)`` tuple.
        """
        region = self._get_region()
        token, expiry_ms = self._msk_provider.generate_auth_token(region)
        LOG.debug("Generated MSK IAM token for region=%s, expires=%s", region, expiry_ms)
        return token, expiry_ms
