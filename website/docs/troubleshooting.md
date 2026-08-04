---
title: Troubleshooting
---

# Troubleshooting

## Source install fails to build

- Make sure you have an up-to-date version of [wheel](https://pypi.org/project/wheel/).
- Updated versions of `go` and `rust` are required to build Karapace from source.
- Create and activate a virtual environment (venv) to manage dependencies.

## Service will not start

- At least one of `karapace_registry` and `karapace_rest` must be enabled, otherwise the
  service refuses to start.
- Make sure Kafka is running and reachable at `bootstrap_uri` before starting Karapace.
- The `runtime` directory (see `protobuf_runtime_directory`) **must** exist — Karapace
  fails if it does not.

## Empty or unexpected subject list

`GET /subjects` returns an empty array when no schemas are registered yet. If you expect
data, confirm that the reader is consuming the correct `topic_name` (`_schemas` by
default) and that the instance has access to that topic.

## OAuth2 startup failures

- `sasl_oauthbearer_jwks_endpoint_url` must use `https://`; startup fails on a plain-HTTP
  URL unless `sasl_oauthbearer_allow_insecure_jwks` is set (dev/test only).
- Enabling `sasl_oauthbearer_authorization_enabled` requires
  `sasl_oauthbearer_roles_claim_path` to be set, or startup fails.

## 401 / 403 responses with OAuth2

- A `401` with `{"error": "Unauthorized", ...}` means the token was missing or invalid
  (bad signature, issuer, audience, or expired).
- A `403` means the token is valid but lacks the role required for the request method.
  Check `sasl_oauthbearer_method_roles` and `sasl_oauthbearer_roles_claim_path`.

## Protobuf compiler version

Karapace requires a Protobuf compiler whose major version is not ahead of the pinned
`protobuf` runtime library. An older `protoc` is fine.
