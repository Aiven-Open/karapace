# Microsoft Entra ID

Entra is a **cloud-only** provider — there is no local container. To run
Karapace e2e tests against Entra, register an app in your tenant and supply the
env vars below. The local Keycloak still starts but is unused.

No source changes are needed. The OIDC middleware
(`src/karapace/api/oidc/middleware.py`) validates any RS256/384/512 JWT against
the configured JWKS URL, issuer, audience, and claim paths.

## Required env vars

| Variable | Example | Notes |
|---|---|---|
| `OIDC_PROVIDER` | `entra` | Any non-`keycloak` value → cloud flow |
| `OIDC_TOKEN_URL` | `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token` | |
| `OIDC_JWKS_ENDPOINT_URL` | `https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys` | |
| `OIDC_EXPECTED_ISSUER` | `https://login.microsoftonline.com/<tenant-id>/v2.0` | |
| `OIDC_EXPECTED_AUDIENCE` | `api://<client-id>` | v2.0 audience is the app id URI or client id |
| `OIDC_CLIENT_ID` | `<app-registration-client-id>` | |
| `OIDC_CLIENT_SECRET` | `<app-registration-secret>` | |
| `OIDC_SCOPE` | `api://<client-id>/.default` | |
| `OIDC_SUB_CLAIM_NAME` | `oid` | Entra `sub` is pairwise; `oid` is the stable object id |
| `OIDC_ROLES_CLAIM_PATH` | `roles` | Entra app roles land in a flat top-level `roles` claim |

## Tenant setup

In the Azure portal:

1. Register an application. Note the tenant ID and application (client) ID.
2. Add a client secret under **Certificates & secrets**.
3. Under **Expose an API**, set an Application ID URI (e.g. `api://<client-id>`)
   — this becomes the audience.
4. Under **App roles**, define the Karapace roles you need. The compose
   `METHOD_ROLES` mapping expects the `karapace.` prefix: `karapace.schema:read`,
   `karapace.schema:write`, `karapace.subject:read`, `karapace.subject:write`,
   `karapace.config_subject:update`, `karapace.config_global:update`,
   `karapace.schema:delete`, `karapace.subject:delete`. Assign the app's
   service principal to those roles in **Enterprise applications**.
5. The `roles` claim is emitted automatically once roles are assigned.

## Run e2e

```sh
OIDC_PROVIDER=entra \
OIDC_TOKEN_URL=https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token \
OIDC_JWKS_ENDPOINT_URL=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys \
OIDC_EXPECTED_ISSUER=https://login.microsoftonline.com/<tenant-id>/v2.0 \
OIDC_EXPECTED_AUDIENCE=api://<client-id> \
OIDC_CLIENT_ID=<client-id> \
OIDC_CLIENT_SECRET=<secret> \
OIDC_SCOPE=api://<client-id>/.default \
OIDC_SUB_CLAIM_NAME=oid \
OIDC_ROLES_CLAIM_PATH=roles \
make e2e-tests-in-docker
```
