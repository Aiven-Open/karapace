---
title: Authentication & authorization
---

# Authentication & authorization

Karapace supports two independent authentication systems for the Schema Registry:
**HTTP basic auth** (file-based users and access rules) and **OAuth2 / OIDC** (bearer
token validation). The REST proxy forwards OAuth2 tokens to Kafka and the Schema
Registry rather than validating them itself.

## HTTP basic authentication

Set `registry_authfile` to the path of a JSON file that lists authorized users and
access-control rules. When set, the Schema Registry requires authentication for most
endpoints and applies per-endpoint authorization rules. The file is hot-reloaded, so
edits take effect without a restart.

Each user entry contains:

| Field           | Description                                                   |
| --------------- | ------------------------------------------------------------- |
| `username`      | The user name.                                                |
| `algorithm`     | One of `scrypt`, `sha1`, `sha256`, or `sha512`.               |
| `salt`          | Salt used for hashing the password.                           |
| `password_hash` | The password hash computed with the given algorithm and salt. |

Generate an entry with the `karapace_mkpasswd` tool (or `python -m karapace.core.auth`).
It prints a ready-to-use JSON user entry:

```bash
karapace_mkpasswd -u user -a sha512 secret
# Response:
# {
#     "username": "user",
#     "algorithm": "sha512",
#     "salt": "iuLouaExTeg9ypqTxqP-dw",
#     "password_hash": "R6ghYSXdLGsq6hkQcg8wT4..."
# }
```

Each access-control rule contains:

| Field       | Description                                                                                                    |
| ----------- | -------------------------------------------------------------------------------------------------------------- |
| `username`  | Matched against the authenticated user.                                                                        |
| `operation` | `Read` or `Write`. `Write` implies read; it covers all mutable operations, including deleting schema versions. |
| `resource`  | A regular expression matched against the accessed resource.                                                    |

Supported resources are `Config:` (global configuration) and `Subject:<subject_name>`
(where `<subject_name>` is a regex matched against the accessed subject).

### Example authorization file

```json
{
  "users": [
    {
      "username": "admin",
      "algorithm": "scrypt",
      "salt": "<salt>",
      "password_hash": "<hash>"
    },
    {
      "username": "plainuser",
      "algorithm": "sha256",
      "salt": "<salt>",
      "password_hash": "<hash>"
    }
  ],
  "permissions": [
    { "username": "admin", "operation": "Write", "resource": ".*" },
    {
      "username": "plainuser",
      "operation": "Read",
      "resource": "Subject:general.*"
    },
    { "username": "plainuser", "operation": "Read", "resource": "Config:" }
  ]
}
```

## OAuth2 / OIDC (Schema Registry)

When OAuth2 is enabled, Karapace extracts the bearer token from the `Authorization`
header (`Authorization: Bearer $JWT`) and validates it against your identity provider's
JWKS endpoint before serving the request.

```yaml
sasl_oauthbearer_authentication_enabled: true
sasl_oauthbearer_jwks_endpoint_url: "https://idp.example.com/realms/karapace/protocol/openid-connect/certs"
sasl_oauthbearer_expected_issuer: "https://idp.example.com/realms/karapace"
sasl_oauthbearer_expected_audience: "account"
sasl_oauthbearer_sub_claim_name: "sub"
```

The token's signature, issuer, audience, expiry and the configured subject claim are all
verified. Optional hardening flags:

| Flag                                        | Default | Description                                                                        |
| ------------------------------------------- | ------- | ---------------------------------------------------------------------------------- |
| `sasl_oauthbearer_leeway_seconds`           | `0`     | Clock-skew tolerance for `exp`/`nbf`/`iat`. Must be `>= 0`.                        |
| `sasl_oauthbearer_require_access_token_typ` | `false` | Require the token header type to be an access-token type.                          |
| `sasl_oauthbearer_allow_insecure_jwks`      | `false` | Allow a plain-HTTP JWKS endpoint. Dev/test only — startup fails on HTTP otherwise. |

### Role-based authorization

Authorization is opt-in on top of authentication. It maps HTTP methods to the roles a
token must carry.

```yaml
sasl_oauthbearer_authentication_enabled: true
sasl_oauthbearer_authorization_enabled: true
sasl_oauthbearer_roles_claim_path: "resource_access.karapace-client.roles"
sasl_oauthbearer_method_roles:
  GET: ["karapace.schema:read", "karapace.subject:read"]
  POST: ["karapace.schema:write", "karapace.subject:write"]
  PUT: []
  DELETE: []
```

`sasl_oauthbearer_roles_claim_path` is a dot-path into the token pointing at the list of
roles. Write the literal client id into the path.

:::note
Enabling authorization requires authentication. Enabling authorization without explicitly
enabling authentication auto-enables it and logs a deprecation warning — set both flags
explicitly.
:::

### Docker example

```yaml
KARAPACE_SASL_OAUTHBEARER_AUTHENTICATION_ENABLED: true
KARAPACE_SASL_OAUTHBEARER_JWKS_ENDPOINT_URL: https://keycloak:8080/realms/karapace/protocol/openid-connect/certs
KARAPACE_SASL_OAUTHBEARER_EXPECTED_ISSUER: https://keycloak:8080/realms/karapace
KARAPACE_SASL_OAUTHBEARER_EXPECTED_AUDIENCE: "account"
KARAPACE_SASL_OAUTHBEARER_SUB_CLAIM_NAME: sub
```

### Production hardening

- `sasl_oauthbearer_jwks_endpoint_url` must use `https://`; startup fails otherwise.
- `/docs`, `/redoc` and `/openapi.json` bypass the auth gate by design (for Swagger UI).
  Block them at your reverse proxy in production to avoid exposing the API surface.

## OAuth2 (REST proxy)

The REST proxy can pass OAuth2 credentials to the underlying Kafka service defined by
`sasl_bootstrap_uri`. When a bearer token is present, the Kafka clients managed by
Karapace use the SASL `OAUTHBEARER` mechanism and forward the token. The REST proxy does
**not** verify the token — Kafka validates it, and the Schema Registry validates the same
token for schema operations.

OAuth2 token forwarding depends on `rest_authorization` being `true`.

```yaml
sasl_mechanism: "OAUTHBEARER"
security_protocol: "SASL_SSL"
ssl_cafile: "ca.pem"
```

If `sasl_mechanism` is `PLAIN`:

```yaml
sasl_mechanism: "PLAIN"
security_protocol: "SASL_PLAINTEXT"
sasl_plain_username: "your_username"
sasl_plain_password: "your_password"
```

### Token expiry

The REST proxy manages producer and consumer clients keyed by the OAuth2 token. They are
cleaned up periodically when idle, and before the token expires. Before refreshing its
token, a client is expected to remove its running consumers (after committing offsets)
and producers that use the current token.
