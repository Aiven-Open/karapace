# PingFederate

PingFederate is treated as a **cloud provider** in this repo: there is no local
PingFederate container. To exercise Karapace against PingFederate, point the
stack at a real tenant via env vars and run the existing e2e target — the local
Keycloak still starts but is unused.

No source changes are needed. The OIDC middleware
(`src/karapace/api/oidc/middleware.py`) validates any RS256/384/512 JWT against
the configured JWKS URL, issuer, audience, and claim paths.

## Required env vars

| Variable | Example | Notes |
|---|---|---|
| `OIDC_PROVIDER` | `pingfederate` | Any non-`keycloak` value → cloud flow |
| `OIDC_TOKEN_URL` | `https://<tenant>/as/token.oauth2` | |
| `OIDC_JWKS_ENDPOINT_URL` | `https://<tenant>/pf/JWKS` | |
| `OIDC_EXPECTED_ISSUER` | `https://<tenant>` | |
| `OIDC_EXPECTED_AUDIENCE` | `karapace-audience` | |
| `OIDC_CLIENT_ID` | `karapace-client` | |
| `OIDC_CLIENT_SECRET` | `<from your tenant>` | |
| `OIDC_SUB_CLAIM_NAME` | `client_id` | PingFederate can't emit the reserved `sub` for client_credentials |
| `OIDC_ROLES_CLAIM_PATH` | `roles` | Flat claim (vs Keycloak's `resource_access.karapace-client.roles`) |

## Tenant setup

In the PingFederate admin console:

1. Create an Access Token Manager (JWT). Set issuer, audience
   (`karapace-audience`), and signing algorithm (RS256).
2. Create an OAuth client (`karapace-client`) with the `client_credentials`
   grant type and a client secret.
3. Configure a client-credentials mapping that emits a `roles` claim
   (space-delimited) with the Karapace role values your tests need. The
   compose `METHOD_ROLES` mapping expects the `karapace.` prefix:
   `karapace.schema:read`, `karapace.schema:write`, `karapace.subject:read`,
   `karapace.subject:write`, `karapace.config_subject:update`,
   `karapace.config_global:update`, `karapace.schema:delete`,
   `karapace.subject:delete`.

## Run e2e

```sh
OIDC_PROVIDER=pingfederate \
OIDC_TOKEN_URL=https://<tenant>/as/token.oauth2 \
OIDC_JWKS_ENDPOINT_URL=https://<tenant>/pf/JWKS \
OIDC_EXPECTED_ISSUER=https://<tenant> \
OIDC_EXPECTED_AUDIENCE=karapace-audience \
OIDC_CLIENT_ID=karapace-client \
OIDC_CLIENT_SECRET=<secret> \
OIDC_SUB_CLAIM_NAME=client_id \
OIDC_ROLES_CLAIM_PATH=roles \
make e2e-tests-in-docker
```
