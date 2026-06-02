"""
karapace - OIDC startup smoke test

Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

from __future__ import annotations

import os
import subprocess
import sys


# Assumes OIDC validation runs synchronously before any I/O; ~1s locally, 10s is CI slack.
STARTUP_TIMEOUT_SECONDS = 10


def test_karapace_startup_fails_when_method_roles_incomplete() -> None:
    """End-to-end smoke: a misconfigured ``method_roles`` env must surface as a
    non-zero exit through ``python -m karapace``, proving env-var -> Pydantic ->
    DI -> ``setup_middlewares`` -> validation all hold together. Branch coverage
    of the validation itself lives in ``test_oidc.py``.
    """
    env = os.environ.copy()
    env.update(
        {
            "KARAPACE_SASL_OAUTHBEARER_AUTHENTICATION_ENABLED": "true",
            "KARAPACE_SASL_OAUTHBEARER_AUTHORIZATION_ENABLED": "true",
            "KARAPACE_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL": "https://idp.example.invalid/realms/r/protocol/openid-connect/certs",
            "KARAPACE_SASL_OAUTHBEARER_EXPECTED_ISSUER": "https://idp.example.invalid/realms/r",
            "KARAPACE_SASL_OAUTHBEARER_EXPECTED_AUDIENCE": "test-audience",
            "KARAPACE_SASL_OAUTHBEARER_CLIENT_ID": "karapace-client",
            "KARAPACE_SASL_OAUTHBEARER_ROLES_CLAIM_PATH": "resource_access.karapace-client.roles",
            # Missing DELETE — should be rejected at startup.
            "KARAPACE_SASL_OAUTHBEARER_METHOD_ROLES": '{"GET": ["r"], "POST": ["w"], "PUT": ["w"]}',
        }
    )
    result = subprocess.run(
        [sys.executable, "-m", "karapace"],
        env=env,
        capture_output=True,
        text=True,
        timeout=STARTUP_TIMEOUT_SECONDS,
    )

    # Exit code 1 = uncaught Python exception (the ValueError from our validation).
    # Pinning the exact code prevents the test from green-washing unrelated failures
    # such as ImportError (exit 1 too in CPython, but stderr would not contain our message).
    assert result.returncode == 1, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    combined = result.stdout + result.stderr
    assert "method_roles is missing definitions for: DELETE" in combined, combined
