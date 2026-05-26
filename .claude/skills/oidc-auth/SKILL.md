---
name: oidc-auth
description: Context for Karapace's OIDC + basic-auth implementation across Schema Registry (SR) and REST Proxy (RP). Use when working on token validation, JWKS, token forwarding from RP to SR, schema-client cache partitioning, role-based authz, or basic-auth coexistence. Triggers - files under src/karapace/api/oidc/, src/karapace/api/middlewares/, src/karapace/core/auth.py, src/karapace/core/serialization.py (sr_authorization_ctx, _token_fingerprint), src/karapace/kafka_rest_apis/__init__.py (publish/fetch gates), or any change to sasl_oauthbearer_* config.
---

# Karapace OIDC & Auth — Architecture Reference

Karapace ships two services (Schema Registry on FastAPI, REST Proxy on aiohttp) with **three independent auth systems** that interact in specific ways:

1. **SR OIDC** — SR validates bearer JWTs against the IdP's JWKS endpoint (authn) and optionally enforces role-based authz.
2. **SR Basic Auth** — file-based users + ACLs, selected when `registry_authfile` is set; coexists with OIDC but the two don't run together in the same request pipeline.
3. **RP OIDC pass-through** — RP does **not** validate tokens itself. Kafka brokers validate OAUTHBEARER tokens for topic ops; SR validates the same token for schema ops. RP only **forwards** the inbound `Authorization` header to SR.

This file is the load-bearing context for those flows. Anchors below are file paths + symbol names (class/function/identifier) — line numbers are deliberately omitted because they rot. When you need a precise location, grep for the symbol.

---

## 1. SR OIDC — Token Validation

### Files
- `src/karapace/api/oidc/middleware.py` — `OIDCMiddleware` (validation + authz)
- `src/karapace/api/middlewares/__init__.py` — HTTP middleware that runs the gate (`_authenticate_and_authorize`, `setup_middlewares`)

### Validation flow — `OIDCMiddleware.validate_jwt`
1. `PyJWKClient.get_signing_key_from_jwt(token)` fetches the signing key (cached).
2. `jwt.decode()` with:
   - `algorithms=["RS256","RS384","RS512"]` (hardcoded, RSA only)
   - `audience` — comma-separated `sasl_oauthbearer_expected_audience` is split into a list
   - `issuer` — `sasl_oauthbearer_expected_issuer`
   - `options={"require": ["exp","iss","aud"]}` — PyJWT does **not** require these by default; we do, explicitly.
3. `ExpiredSignatureError` → custom `TokenExpiredError` (subclass of `AuthenticationError`) so the 401 reason can say "Token expired" vs. "Invalid token/payload".

### JWKS client cache — `OIDCMiddleware.__init__` (`self._jwks_client`)
```python
PyJWKClient(self.jwks_url, cache_keys=True, lifespan=300, max_cached_keys=16)
```
- `lifespan=300` caps how long a key stays cached after IdP key rotation/revocation.
- `max_cached_keys=16` — IdPs typically expose 1-3; 16 is generous.

### HTTPS enforcement — `OIDCMiddleware.__init__` (search for `allow_insecure_jwks`)
JWKS over plain HTTP is rejected unless `sasl_oauthbearer_allow_insecure_jwks=true` (dev-only override; logs a loud warning). Reason: an in-path attacker could swap signing keys and forge valid tokens.

### Subject exposure — `_authenticate_and_authorize` (search for `request.state.user`)
After validation, only the configured subject claim (`claim_name`, default `"sub"`) is set on `request.state.user`. **The full payload is intentionally not exposed** to handlers — prevents downstream code from picking up attacker-controlled claims like `roles` and using them for authz decisions.

### Skip paths — `setup_middlewares` (search for `skip_auth_paths`)
- Always skipped: `/docs`, `/docs/oauth2-redirect`, `/redoc`, `/openapi.json`
- Configurable skip: `sasl_oauthbearer_skip_auth_paths` (default `["/_health", "/metrics"]`)

---

## 2. SR OIDC — Authorization (RBAC)

### Flow — `OIDCMiddleware.authorize_request`
- Off when `sasl_oauthbearer_authorization_enabled=false` — returns `True` immediately.
- Looks up allowed roles for the request method in `sasl_oauthbearer_method_roles`, e.g. `{"GET": ["reader","admin"], "POST": ["admin"], ...}`.
- Extracts user roles via `sasl_oauthbearer_roles_claim_path` (dot-path, e.g. `realm_access.roles`). Supports `[client_id]` token in the path that gets replaced with `sasl_oauthbearer_client_id` (Keycloak `resource_access.<client>.roles` style).
- Mismatch (or any misconfig) → **403 with the same body** as a real role mismatch. This is intentional — an attacker with a valid token must not be able to distinguish "bad config" from "missing role" via response differences.

### Required config when authz is on (`OIDCMiddleware.__init__`)
- `sasl_oauthbearer_client_id`
- `sasl_oauthbearer_roles_claim_path`

Missing either → `ValueError` at startup (fail-closed).

### Backwards-compat shim — `Config._enforce_authn_when_authz_enabled`
If `sasl_oauthbearer_authorization_enabled=true` but `sasl_oauthbearer_authentication_enabled=false`, we auto-enable authn and log a deprecation warning. Authz without authn is meaningless; the prior single-flag config is preserved with a warning so operators migrate.

---

## 3. RP → SR Token Forwarding

This is the heart of the recent work (commit `27b61e11` "Handle token fwd from rest proxy to sr", merged 2026-05-20 in PR #1287). Read this section before touching anything that crosses the RP/SR boundary.

### The contextvar — `sr_authorization_ctx` in `src/karapace/core/serialization.py`
```python
sr_authorization_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar("sr_authorization", default=None)
```
- **Asyncio-safe** per-request slot. Threads/locals don't work because asyncio runs many coroutines on one thread.
- `None` (default) means "no forwarded token" → `SchemaRegistryClient` falls back to `session_auth` (the `registry_user`/`registry_password` basic-auth pair), which is the legacy behavior.

### Where the contextvar is **set** (RP side, `src/karapace/kafka_rest_apis/__init__.py`)
Only two entry points set it, both gated on `config.sasl_oauthbearer_authentication_enabled`:
- `UserRestProxy.publish()`
- `UserRestProxy.fetch()`

To find every site, grep: `rg 'sr_authorization_ctx\.set'`.

```python
if self.config.sasl_oauthbearer_authentication_enabled:
    sr_authorization_ctx.set(request.headers.get("Authorization"))
```

**Why only these two?** They're the RP endpoints that internally trigger schema lookups via `SchemaRegistryClient`. Other RP endpoints (offsets, broker info, consumer create/delete, etc.) don't reach the schema client, so forwarding would be wasted. **If you add a new RP endpoint that hits the schema client, set the contextvar there too** — there's a comment on the contextvar reminding you of this list.

### Where the contextvar is **read** (SR client side, `src/karapace/core/serialization.py`)
- `_authorization_headers()` — returns `{"Authorization": auth}` or `None`.
- All `SchemaRegistryClient` outbound calls merge it into headers:
  - `post_new_schema()` — merges with `Content-Type: application/vnd.schemaregistry.v1+json` (the merge matters: `Client.post` only injects the vendor content-type when `headers` is falsy; if you pass auth without merging, the request silently demotes to `application/json`).
  - `_get_schema_recursive()` and `get_schema_for_id()` — pass `headers=_authorization_headers()`.

To audit every call site, grep: `rg '_authorization_headers\('`.

### What "RP doesn't validate" actually means
RP only decodes the JWT **without verifying the signature** to read `exp` — see `get_expiration_timestamp_from_jwt` in `src/karapace/kafka_rest_apis/authentication.py`:
```python
jwt.decode(token, options={"verify_signature": False}).get("exp")
```
This `exp` is used **only** to evict the per-user proxy when the token expires. SR's `sasl_oauthbearer_*` validation fields don't apply on RP — Kafka validates signature/issuer/audience at the broker, SR validates them again for schema ops.

---

## 4. Schema-Client Cache Partitioning by Token Fingerprint

### Why
The `SchemaRegistryClient` LRU caches `(subject, version) → (id, schema, version)`. Without partitioning, a request with no auth could hit a cache slot warmed by an authenticated request — effectively bypassing SR's gate. Conversely, two principals with different roles must not share a slot.

### Fingerprint — `_token_fingerprint()` in `src/karapace/core/serialization.py`
```python
def _token_fingerprint() -> str:
    auth = sr_authorization_ctx.get()
    if not auth:
        return ""
    return hashlib.sha256(auth.encode("utf-8")).hexdigest()[:16]
```
- SHA-256, truncated to 16 hex chars. **Never log the raw token; never store the raw token in the LRU.**
- Empty string is the sentinel for unauthenticated (covers session_auth fallback paths).

### Cache key
```
(subject, version, _token_fingerprint()) → (SchemaId, ValidatedTypedSchema, Version)
```
Per-instance decoration so cache size is configurable:
```python
self._get_schema_cached = alru_cache(maxsize=cache_maxsize)(self._get_schema_cached)
```

### Tunable
`Config.schema_registry_client_cache_maxsize` (default 100). Raise for multi-tenant deployments — each principal gets its own cache slot per `(subject, version)`, so heavy fan-out can blow the LRU.

### Known limitation
Cache does **not** invalidate on token revocation — schema bytes are immutable, so a cached `(subject, version, fp)` entry is still correct material; it just means a revoked token's cache entry will live up to the LRU eviction window. SR re-validates the token on every request before any cache lookup happens (the gate runs in HTTP middleware, before the route handler), so this is benign for authn purposes.

---

## 5. SR Basic Auth (Coexistence)

### Files
- `src/karapace/core/auth.py` — `HTTPAuthorizer`, `User`, `ACLEntry`, `Operation`
- `src/karapace/core/config.py` — `get_authorizer` (selector)

### Selection — `get_authorizer`
```python
return http_authorizer if config.registry_authfile else no_auth_authorizer
```
- `registry_authfile` set → `HTTPAuthorizer` (basic auth from JSON file).
- Unset → `NoAuthAndAuthz` (no-op).

### Auth file format (auth.py)
```json
{
  "users": [
    {"username": "...", "algorithm": "sha512", "salt": "...", "password_hash": "..."}
  ],
  "permissions": [
    {"username": "...", "operation": "Read|Write", "resource": "<regex>"}
  ]
}
```
- Algorithms: `sha1`, `sha256`, `sha512`, `scrypt`.
- `Write` implies `Read`. `resource` is a regex pattern matched against the resource path.
- File is hot-reloaded via `watchfiles.awatch()` — edits take effect without restart.

### Basic Auth ↔ OIDC orthogonality
- They are **independent systems** — different config paths, different code paths.
- OIDC runs as Starlette HTTP middleware (before routing).
- Basic Auth runs through the FastAPI dependency injection layer in route handlers.
- A deployment can enable either, both (technically), or neither. In practice, OIDC-on deployments don't set `registry_authfile`. Tests use distinct fixtures: `registry_async_client_oidc` (Bearer) vs `registry_async_client_basic` (BasicAuth).

---

## 6. Config Reference — fields on `Config` in `src/karapace/core/config.py`

| Field | Default | Purpose |
|---|---|---|
| `sasl_oauthbearer_authentication_enabled` | `False` | Master switch for SR OIDC validation gate AND for RP→SR token forwarding gate |
| `sasl_oauthbearer_authorization_enabled` | `False` | Adds RBAC check after authn |
| `sasl_oauthbearer_jwks_endpoint_url` | `None` | JWKS endpoint; required if either flag above is on |
| `sasl_oauthbearer_allow_insecure_jwks` | `False` | Dev-only escape hatch for HTTP JWKS |
| `sasl_oauthbearer_expected_issuer` | `None` | Required when JWKS is set |
| `sasl_oauthbearer_expected_audience` | `None` | Comma-separated list; required when JWKS is set |
| `sasl_oauthbearer_sub_claim_name` | `"sub"` | Claim to expose as `request.state.user` |
| `sasl_oauthbearer_client_id` | `None` | Required for authz; substituted into `[client_id]` in roles path |
| `sasl_oauthbearer_roles_claim_path` | `None` | Dot-path to roles list in JWT |
| `sasl_oauthbearer_method_roles` | `{GET/POST/PUT/DELETE: []}` | Per-method allowed roles |
| `sasl_oauthbearer_skip_auth_paths` | `["/_health","/metrics"]` | Bypass paths |
| `schema_registry_client_cache_maxsize` | `100` | LRU cap on `(subject, version, fp)` |
| `registry_authfile` | `None` | Selects basic-auth `HTTPAuthorizer` |

All env vars are prefixed `KARAPACE_` (Pydantic `BaseSettings`). The flag double-duty of `sasl_oauthbearer_authentication_enabled` (gates BOTH SR validation AND RP forwarding) is deliberate — it means an OIDC-enabled deployment is consistent across both services with a single switch.

---

## 7. Tests — where to look

| Path | What it covers |
|---|---|
| `tests/unit/test_oidc.py` | `OIDCMiddleware` unit tests: config validation, `validate_jwt`, role extraction, `authorize_request` |
| `tests/unit/kafka_rest_apis/test_sr_authorization_forwarding.py` | RP gate: enabled/disabled, with/without header, concurrent isolation across coroutines |
| `tests/e2e/schema_registry/test_oidc.py` | SR e2e: valid/invalid/expired tokens, missing headers, skip paths |
| `tests/e2e/kafka_rest_apis/test_oidc_forwarding.py` | RP→SR e2e: AVRO/JSON publish + consumer fetch with bearer, invalid bearer, no-auth fallthrough |
| `tests/unit/test_rest_auth.py` | RP per-user proxy janitor (token-expiry eviction) |
| `tests/conftest.py` | Fixtures: `registry_async_client_oidc`, `registry_async_client_basic`, `reset_sr_authorization_ctx` |

The `reset_sr_authorization_ctx` fixture is required for any test that touches `sr_authorization_ctx` — contextvars persist across tests in the same event-loop scope and will leak state otherwise.

---

## 8. Mental model when extending

When you add a feature that crosses the auth boundary, walk through these checks:

1. **New SR route?** It's automatically gated by `setup_middlewares`. If it should be public, add it to `sasl_oauthbearer_skip_auth_paths`. **Don't read claims other than the configured subject** off `request.state` — only `request.state.user` is exposed deliberately.
2. **New RP endpoint that calls `SchemaRegistryClient`?** Set `sr_authorization_ctx` from the inbound `Authorization` header, gated on `config.sasl_oauthbearer_authentication_enabled`. Update the comment on the contextvar listing the entry points.
3. **New `SchemaRegistryClient` outbound call?** Pass `headers=_authorization_headers()` (or merge it if you already have headers — see the `post_new_schema` content-type pitfall).
4. **New cacheable schema lookup?** Include `_token_fingerprint()` in the cache key. Don't store the raw token anywhere.
5. **New authz rule?** Prefer extending `sasl_oauthbearer_method_roles` over inventing a parallel system. If you must inspect the JWT payload, do it inside `OIDCMiddleware` — don't leak the payload to handlers.
6. **Error responses on auth failure?** Match the existing pattern: 401 with `{"error": "Unauthorized", "reason": "..."}` for authn, 403 with `{"error": "Authorization error", "reason": "Forbidden"}` for authz. Keep the 403 body identical to the misconfig branch — see `OIDCMiddleware.authorize_request` for the rationale (do not let the response distinguish bad config from a missing role).
