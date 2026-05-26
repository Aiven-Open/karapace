# `src/karapace/api/` — Agent notes

This directory hosts the FastAPI-based Schema Registry. The notes below cover behaviors that aren't obvious from the code — anchors are file + symbol names, not line numbers.

The REST Proxy lives in `../kafka_rest_apis/`; shared logic (basic auth, the schema-client + token-forwarding contextvar) is in `../core/`. See `../core/AGENTS.md`.

## OIDC validation gate — `api/oidc/middleware.py` + `api/middlewares/__init__.py`

OIDC runs as a **Starlette HTTP middleware**, before any FastAPI route handler. Setup is in `setup_middlewares()`.

### `OIDCMiddleware.validate_jwt`

1. `PyJWKClient.get_signing_key_from_jwt(token)` — JWKS is cached: `PyJWKClient(self.jwks_url, cache_keys=True, lifespan=300, max_cached_keys=16)`. `lifespan=300` caps how long a key stays cached after IdP rotation/revocation.
2. `jwt.decode()` with:
   - `algorithms=["RS256","RS384","RS512"]` (hardcoded, RSA only).
   - `audience` — comma-separated `sasl_oauthbearer_expected_audience` is split into a list.
   - `issuer` — `sasl_oauthbearer_expected_issuer`.
   - `options={"require": ["exp","iss","aud"]}` — PyJWT does **not** require these by default; we do, explicitly.
3. `ExpiredSignatureError` → custom `TokenExpiredError` (subclass of `AuthenticationError`) so the 401 reason can distinguish "Token expired" from "Invalid token/payload".

### HTTPS enforcement — `OIDCMiddleware.__init__`
JWKS over plain HTTP is rejected unless `sasl_oauthbearer_allow_insecure_jwks=true`. Reason: an in-path attacker could swap signing keys and forge valid tokens. The override is dev-only and logs a loud warning.

### Subject exposure — `_authenticate_and_authorize`
After validation, **only the configured subject claim** (`claim_name`, default `"sub"`) is set on `request.state.user`. The full payload is intentionally *not* exposed to handlers — prevents downstream code from picking up attacker-controlled claims (e.g. `roles`) and using them for authz decisions.

### Skip paths — `setup_middlewares`
- Always skipped: `/docs`, `/docs/oauth2-redirect`, `/redoc`, `/openapi.json`.
- Configurable: `sasl_oauthbearer_skip_auth_paths` (default `["/_health", "/metrics"]`).

## Authorization (RBAC) — `OIDCMiddleware.authorize_request`

- Off when `sasl_oauthbearer_authorization_enabled=false` — returns `True` immediately.
- Allowed roles per HTTP method come from `sasl_oauthbearer_method_roles`, e.g. `{"GET": ["reader","admin"], "POST": ["admin"], ...}`.
- Roles are extracted via `sasl_oauthbearer_roles_claim_path` (dot-path, e.g. `realm_access.roles`). Supports a `[client_id]` token in the path that's substituted with `sasl_oauthbearer_client_id` (Keycloak `resource_access.<client>.roles` style).
- Mismatch **or any misconfig** ⇒ **403 with the same body** as a real role mismatch. This is intentional — an attacker with a valid token must not be able to distinguish "bad config" from "missing role" by the response.
- Required config when authz is on: `sasl_oauthbearer_client_id` and `sasl_oauthbearer_roles_claim_path`. Missing either ⇒ `ValueError` at startup (fail-closed).

## Error response shape

Match the existing patterns:
- 401 (authn): `{"error": "Unauthorized", "reason": "..."}`
- 403 (authz): `{"error": "Authorization error", "reason": "Forbidden"}`

Keep the 403 body identical to the misconfig branch (see `authorize_request`) — distinguishing them in the response would leak information.

## Layout reminders

- `api/routers/` — FastAPI route handlers (subjects, schemas, config, compatibility, health, mode, metrics).
- `api/controller.py` → `KarapaceSchemaRegistryController`: bridges routes to `KarapaceSchemaRegistry` (in `../core/`).
- `api/forward_client.py` → `ForwardClient`: forwards SR cluster requests to the master node. Includes the `Authorization` header among forwardable headers (`_HEADERS_TO_FORWARD`) — bearer tokens travel cluster-to-cluster transparently.
- `api/container.py` → `SchemaRegistryContainer` (DI; wired in `../__main__.py`).
- `api/telemetry/` — OpenTelemetry middleware + container.

## Tests

- `tests/unit/test_oidc.py` — `OIDCMiddleware` unit tests (config validation, `validate_jwt`, role extraction, `authorize_request`).
- `tests/e2e/schema_registry/test_oidc.py` — SR e2e (valid/invalid/expired tokens, missing headers, skip paths).
- `tests/e2e/kafka_rest_apis/test_oidc_forwarding.py` — RP→SR e2e (AVRO/JSON publish + consumer fetch with bearer, invalid bearer, no-auth fallthrough).

## Checklist when extending

- **New SR route?** It's automatically gated by `setup_middlewares`. If it should be public, add it to `sasl_oauthbearer_skip_auth_paths`. **Do not read claims other than the configured subject** off `request.state` — only `request.state.user` is exposed deliberately.
- **New authz rule?** Prefer extending `sasl_oauthbearer_method_roles` over inventing a parallel system. If you must inspect the JWT payload, do it inside `OIDCMiddleware` — don't leak the payload to handlers.
- **Touching the forwarding contextvar?** That logic lives in `../core/serialization.py`; see `../core/AGENTS.md`.
