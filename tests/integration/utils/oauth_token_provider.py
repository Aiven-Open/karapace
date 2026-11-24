"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import base64
import json
import time


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def unsecured_jwt(subject: str, lifetime_s: int) -> tuple[str, int]:
    """An ``alg=none`` JWS accepted by Kafka's ``OAuthBearerUnsecuredValidatorCallbackHandler``.

    Returns the compact serialization and its expiry as UNIX seconds.
    """
    now = int(time.time())
    expiry = now + lifetime_s
    header = _b64url(json.dumps({"alg": "none"}).encode())
    claims = _b64url(json.dumps({"sub": subject, "iat": now, "exp": expiry}).encode())
    return f"{header}.{claims}.", expiry


class UnsecuredJwtTokenProvider:
    """``sasl_oauth_token_provider_class`` for the integration broker's OAUTHBEARER listener.

    Implements ``TokenWithExpiryProvider`` so the same instance serves confluent-kafka's
    ``oauth_cb`` and, through Karapace's adapter, aiokafka's ``AbstractTokenProvider``.
    """

    SUBJECT = "karapace-integration"
    LIFETIME_S = 3600

    def token_with_expiry(self, _config: str | None = None) -> tuple[str, int | None]:
        return unsecured_jwt(self.SUBJECT, self.LIFETIME_S)
