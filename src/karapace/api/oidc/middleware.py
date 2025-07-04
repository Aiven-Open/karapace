"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import jwt
import logging
from fastapi import FastAPI
from jwt import PyJWKClient, InvalidTokenError
from starlette.responses import JSONResponse
from karapace.core.auth import AuthenticationError
from karapace.core.config import Config
from starlette.types import Scope, Receive, Send

log = logging.getLogger(__name__)


class OIDCMiddleware:
    def __init__(self, app: FastAPI, config: Config) -> None:
        self.app = app
        self.config = config

        self._jwks_client: PyJWKClient | None
        self.jwks_url = config.sasl_oauthbearer_jwks_endpoint_url
        self.issuer = config.sasl_oauthbearer_expected_issuer
        self.audience = config.sasl_oauthbearer_expected_audience
        self.claim_name = config.sasl_oauthbearer_sub_claim_name

        # Hardcoded default algorithms
        self.algorithms = ["RS256", "RS384", "RS512"]

        # Validate required fields if JWKS URL is set
        if self.jwks_url:
            if not self.issuer or not self.audience:
                raise ValueError(
                    "OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
                )
            self._jwks_client = PyJWKClient(self.jwks_url)
        else:
            self._jwks_client = None
            log.warning("OIDC middleware initialized without JWKS URL — token validation will be skipped.")

    def validate_jwt(self, token: str) -> dict:
        if not self._jwks_client:
            log.warning("Skipping JWT validation due to missing JWKS configs. Ignoring authentication.")
            return {}

        try:
            signing_key = self._jwks_client.get_signing_key_from_jwt(token)
            # Split the comma-separated audience string into a list and strip whitespace
            if self.audience:
                audiences = [aud.strip() for aud in self.audience.split(",") if aud.strip()]
            else:
                audiences = None  # or leave it out
            payload = jwt.decode(
                token,
                signing_key.key,
                algorithms=self.algorithms,
                audience=audiences,
                issuer=self.issuer,
            )
            return payload
        except InvalidTokenError:
            log.error("JWT validation failed")
            raise AuthenticationError("Invalid OIDC token")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode()
        if auth_header.startswith("Bearer "):
            try:
                payload = self.validate_jwt(auth_header.split(" ", 1)[1])
                scope["user"] = payload  # inject user info into scope
            except AuthenticationError as e:
                response = JSONResponse({"error": "Authentication error", "reason": str(e)}, status_code=401)
                await response(scope, receive, send)
                return

        await self.app(scope, receive, send)
