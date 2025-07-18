"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

from typing import Any

import jwt
import logging
from fastapi import FastAPI, HTTPException
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
        self.authorization_enabled = config.sasl_oauthbearer_authorization_enabled
        self.client_id = config.sasl_oauthbearer_client_id
        self.sasl_oauthbearer_method_roles: dict[str, list[str]] = config.sasl_oauthbearer_method_roles
        self.sasl_oauthbearer_roles_claim_path = config.sasl_oauthbearer_roles_claim_path

        # Hardcoded default algorithms
        self.algorithms = ["RS256", "RS384", "RS512"]

        # Validate required fields if JWKS URL is set
        if self.jwks_url:
            if not self.issuer or not self.audience:
                raise ValueError(
                    "OIDC config error: 'issuer' and 'audience' must be set if 'jwks_endpoint_url' is provided."
                )
            log.info(
                "OIDC middleware initialized — Bearer token validation enabled. " "jwks_url=%s issuer=%s audience=%s",
                self.jwks_url,
                self.issuer,
                self.audience,
            )

            self._jwks_client = PyJWKClient(self.jwks_url)

            log.info("OIDC Authorization enabled: %s", self.authorization_enabled)
            if self.authorization_enabled:
                if self.client_id is None or self.sasl_oauthbearer_roles_claim_path is None:
                    raise HTTPException(
                        status_code=403,
                        detail="Invalid configuration: client_id and roles_claim_path are required when authorization is enabled.",
                    )

                required_http_methods = set(self.sasl_oauthbearer_method_roles.keys())
                # Validate required HTTP methods in method_roles
                missing_methods = required_http_methods - self.sasl_oauthbearer_method_roles.keys()
                if missing_methods:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Invalid configuration: method_roles is missing definitions for: {', '.join(sorted(missing_methods))}",
                    )
                log.info(
                    "OIDC Authorization configured. — client_id: %s, method_roles: %s, roles_claim_path: %s",
                    self.client_id,
                    self.sasl_oauthbearer_method_roles,
                    self.sasl_oauthbearer_roles_claim_path,
                )
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

    def authorize_request(self, payload: dict, request_method: str) -> bool:
        request_method = request_method.upper()
        allowed_roles = self.sasl_oauthbearer_method_roles.get(request_method, [])

        # Dynamic role extraction based on configured path
        if self.sasl_oauthbearer_roles_claim_path is not None and self.client_id is not None:
            roles_claim_path = self.sasl_oauthbearer_roles_claim_path.replace("[client_id]", self.client_id)
        else:
            raise HTTPException(status_code=403, detail="Insufficient roles. Invalid roles_claim_path.")

        user_roles = self.get_roles_from_claim_path(payload, roles_claim_path)

        if not any(role in user_roles for role in allowed_roles):
            log.warning(
                "Authorization failed for method %s. User roles: %s, required: %s",
                request_method,
                user_roles,
                allowed_roles,
            )
            raise HTTPException(status_code=403, detail="Insufficient roles")
        return True

    @staticmethod
    def get_roles_from_claim_path(payload: dict[str, Any], path: str) -> list[str]:
        try:
            parts = path.split(".")
            value: Any = payload
            for part in parts:
                if not isinstance(value, dict):
                    return []  # path broken
                value = value.get(part)

            # value might not be a list of strings, so check
            if isinstance(value, list):
                if all(isinstance(role, str) for role in value):
                    return value
        except Exception:
            pass
        return []

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
