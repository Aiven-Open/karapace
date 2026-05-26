# `src/karapace/core/` — Agent notes

This directory holds shared business logic used by both services (Schema Registry and REST Proxy). The notes below cover behaviors that aren't obvious from the code alone — anchors are file + symbol names, never line numbers (those rot).

## Auth surface in `core/`

Three independent auth concerns intersect here. The OIDC validation gate itself lives in `../api/oidc/` (see `../api/AGENTS.md`); `core/` owns the **basic-auth system** and the **token-forwarding mechanics** used by the schema client.

### Basic auth — `core/auth.py`

- `HTTPAuthorizer` loads users + ACLs from a JSON file and watches for changes via `watchfiles.awatch()` — edits take effect without restart.
- Selection is in `core/config.py` → `get_authorizer`: `registry_authfile` set ⇒ `HTTPAuthorizer`, otherwise `NoAuthAndAuthz`.
- ACL model: `User`, `ACLEntry { username, operation, resource: re.Pattern }`. `Operation.Write` implies `Read`. `resource` is a regex matched against the resource path.
- Auth file format:
  ```json
  {
    "users":       [{"username": "...", "algorithm": "sha512", "salt": "...", "password_hash": "..."}],
    "permissions": [{"username": "...", "operation": "Read|Write", "resource": "<regex>"}]
  }
  ```
  Algorithms supported: `sha1`, `sha256`, `sha512`, `scrypt`.

Basic auth and OIDC are **independent systems**. OIDC is a Starlette HTTP middleware (runs before routing); basic auth runs through FastAPI dependency injection inside route handlers. A deployment typically picks one — fixtures `registry_async_client_oidc` (Bearer) vs. `registry_async_client_basic` (BasicAuth) reflect this.

### Token forwarding RP → SR — `core/serialization.py`

When OIDC is on, the REST Proxy forwards the inbound `Authorization` header to SR for schema operations. SR validates the token; RP does not (Kafka brokers validate it for topic ops).

**The contextvar — `sr_authorization_ctx`**
```python
sr_authorization_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar("sr_authorization", default=None)
```
- Asyncio-safe per-request slot. Threads/locals don't work because asyncio runs many coroutines on one thread.
- `None` (default) ⇒ `SchemaRegistryClient` falls back to `session_auth` (the `registry_user`/`registry_password` basic-auth pair). That's the legacy behavior.
- Set sites live in `../kafka_rest_apis/__init__.py` — `UserRestProxy.publish()` and `UserRestProxy.fetch()`. Both gated on `config.sasl_oauthbearer_authentication_enabled`. To audit: `rg 'sr_authorization_ctx\.set'`.
- **If you add a new RP endpoint that calls `SchemaRegistryClient`, set the contextvar there too.**

**Read-side helpers — `_authorization_headers()` and `_token_fingerprint()`**
- `_authorization_headers()` returns `{"Authorization": auth}` or `None`. Every outbound `SchemaRegistryClient` call merges it. Audit: `rg '_authorization_headers\('`.
- `post_new_schema()` has a content-type pitfall: `Client.post` only injects `application/vnd.schemaregistry.v1+json` when `headers` is falsy. Pass auth via merge, not replace, or the request silently demotes to `application/json`.

### Schema-client cache partitioning — `_token_fingerprint()`, `SchemaRegistryClient._get_schema_cached`

`SchemaRegistryClient` LRU-caches `(subject, version) → (id, schema, version)`. Without partitioning, an unauthenticated request could hit a slot warmed by an authenticated one — bypassing SR's gate. So we add the principal:

```python
def _token_fingerprint() -> str:
    auth = sr_authorization_ctx.get()
    if not auth:
        return ""
    return hashlib.sha256(auth.encode("utf-8")).hexdigest()[:16]
```

- Cache key: `(subject, version, _token_fingerprint())`.
- SHA-256 truncated to 16 hex chars. **Never log the raw token; never store it in the LRU.** Empty string is the sentinel for unauthenticated (covers `session_auth` fallback).
- Decoration is per-instance so cache size is configurable: `self._get_schema_cached = alru_cache(maxsize=cache_maxsize)(self._get_schema_cached)`.
- `Config.schema_registry_client_cache_maxsize` defaults to 100 — raise it for multi-tenant deployments where many principals fan out across `(subject, version)`.
- The cache does **not** invalidate on token revocation. Schema bytes are immutable, so the cached entry is still the right material; SR re-validates the token on every request before any cache lookup happens (the gate runs in HTTP middleware, before the route handler), so this is benign for authn purposes.

## Config (Pydantic `BaseSettings`) — `core/config.py`

All env vars are prefixed `KARAPACE_`. The OIDC fields live on `Config`:

| Field | Default | Purpose |
|---|---|---|
| `sasl_oauthbearer_authentication_enabled` | `False` | Master switch — gates **both** SR validation **and** RP→SR forwarding. Single-flag deliberate so the two services stay consistent. |
| `sasl_oauthbearer_authorization_enabled` | `False` | Adds RBAC after authn. See `../api/AGENTS.md`. |
| `sasl_oauthbearer_jwks_endpoint_url` | `None` | Required if either flag above is on. |
| `sasl_oauthbearer_allow_insecure_jwks` | `False` | Dev-only escape hatch for HTTP JWKS. |
| `sasl_oauthbearer_expected_issuer` | `None` | Required when JWKS is set. |
| `sasl_oauthbearer_expected_audience` | `None` | Comma-separated list; required when JWKS is set. |
| `sasl_oauthbearer_sub_claim_name` | `"sub"` | Claim exposed as `request.state.user`. |
| `sasl_oauthbearer_client_id` | `None` | Required for authz. Substituted into `[client_id]` in roles path. |
| `sasl_oauthbearer_roles_claim_path` | `None` | Dot-path to roles list in JWT. |
| `sasl_oauthbearer_method_roles` | `{GET/POST/PUT/DELETE: []}` | Per-method allowed roles. |
| `sasl_oauthbearer_skip_auth_paths` | `["/_health","/metrics"]` | Bypass paths. |
| `schema_registry_client_cache_maxsize` | `100` | LRU cap on `(subject, version, fp)`. |
| `registry_authfile` | `None` | Selects basic-auth `HTTPAuthorizer`. |

`Config._enforce_authn_when_authz_enabled` (model validator): if authz is on but authn isn't, auto-enable authn with a deprecation warning. Authz without authn is meaningless; the prior single-flag config is preserved with a warning so operators migrate.

## Other notable modules in `core/`

- `schema_registry.py` → `KarapaceSchemaRegistry`: schema CRUD + compatibility checks.
- `schema_reader.py` → `KafkaSchemaReader`: consumer of the `_schemas` Kafka topic.
- `in_memory_database.py` → `InMemoryDatabase`: in-memory schema store fed by `KafkaSchemaReader`.
- `coordinator/` → master election via Kafka consumer groups (only the master handles writes).
- `compatibility/` → Avro / JSON Schema / Protobuf compatibility logic.

## Tests touching this directory

- `tests/unit/test_oidc.py` — `OIDCMiddleware` (lives in `api/`, but config validation is exercised here).
- `tests/unit/kafka_rest_apis/test_sr_authorization_forwarding.py` — RP gate behavior (enabled/disabled, with/without header, concurrent isolation across coroutines).
- `tests/conftest.py` — fixtures `registry_async_client_oidc`, `registry_async_client_basic`, `reset_sr_authorization_ctx`. **The `reset_sr_authorization_ctx` fixture is required for any test that touches `sr_authorization_ctx`** — contextvars persist across tests in the same event-loop scope and will leak state otherwise.

## Checklist when extending

- **New `SchemaRegistryClient` outbound call?** Pass `headers=_authorization_headers()` (merge, don't replace — see `post_new_schema` pitfall above).
- **New cacheable schema lookup?** Include `_token_fingerprint()` in the cache key. Never store the raw token.
- **New basic-auth resource?** Match it via the regex on `ACLEntry.resource`. Remember `Write` implies `Read`.
