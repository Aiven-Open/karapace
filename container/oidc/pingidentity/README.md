# PingIdentity / PingFederate OIDC Setup

Keycloak is still the default local OIDC provider. Use PingFederate when you want Karapace to validate tokens against the local PingFederate service instead.

For local PingFederate, use the same `OIDC_*` values for both Karapace runtime validation and the e2e fixtures. That keeps the service config and test token source aligned.

## 1. Required env

For a persistent PingFederate setup, add these values to `.env`:

```env
PING_IDENTITY_DEVOPS_USER=<your-ping-devops-user>
PING_IDENTITY_DEVOPS_KEY=<your-ping-devops-key>
OIDC_JWKS_ENDPOINT_URL=https://pingfederate:9031/pf/JWKS
OIDC_ALLOW_INSECURE_JWKS=true
OIDC_EXPECTED_ISSUER=https://pingfederate:9031
OIDC_EXPECTED_AUDIENCE=karapace-audience
OIDC_SUB_CLAIM_NAME=client_id
OIDC_CLIENT_ID=karapace-client
OIDC_ROLES_CLAIM_PATH=roles
OIDC_METHOD_ROLES={"GET": ["schema:read", "subject:read"], "POST": ["schema:write", "subject:write"], "PUT": ["config_subject:update","config_global:update"], "DELETE": ["schema:delete", "subject:delete"]}
```

Use `OIDC_ALLOW_INSECURE_JWKS=true` only for local self-signed PingFederate.

You still need your Ping DevOps credentials in the shell:

```sh
export PING_IDENTITY_DEVOPS_USER=<your-ping-devops-user>
export PING_IDENTITY_DEVOPS_KEY=<your-ping-devops-key>
```

For a one-shot flip, use this command:

```sh
make \
  PING_IDENTITY_DEVOPS_USER="$PING_IDENTITY_DEVOPS_USER" \
  PING_IDENTITY_DEVOPS_KEY="$PING_IDENTITY_DEVOPS_KEY" \
  OIDC_JWKS_ENDPOINT_URL=https://pingfederate:9031/pf/JWKS \
  OIDC_ALLOW_INSECURE_JWKS=true \
  OIDC_EXPECTED_ISSUER=https://pingfederate:9031 \
  OIDC_EXPECTED_AUDIENCE=karapace-audience \
  OIDC_SUB_CLAIM_NAME=client_id \
  OIDC_CLIENT_ID=karapace-client \
  OIDC_CLIENT_SECRET=karapace-secret \
  OIDC_TOKEN_URL=https://pingfederate:9031/as/token.oauth2 \
  OIDC_SCOPE=openid \
  OIDC_VERIFY_TLS=false \
  OIDC_ROLES_CLAIM_PATH=roles \
  OIDC_METHOD_ROLES='{"GET": ["schema:read", "subject:read"], "POST": ["schema:write", "subject:write"], "PUT": ["config_subject:update","config_global:update"], "DELETE": ["schema:delete", "subject:delete"]}' \
  provision-pingfederate-oidc
```

That starts the `auth` and `pingfederate` profiles and provisions the OAuth objects Karapace needs.

If PingFederate fails during bootstrap with messages like `License File absent` or `No credentials were provided to retrieve an evaluation license`, the container is failing before the engine starts. In that case, provide valid `PING_IDENTITY_DEVOPS_USER` and `PING_IDENTITY_DEVOPS_KEY`, or mount a valid `pingfederate.lic` file into the service.

## 2. Useful endpoints

Relevant local endpoints:

- Admin UI: `https://localhost:9999/pingfederate/app`
- Admin API: `https://localhost:9999/pf-admin-api/v1`
- Issuer: `https://localhost:9031`
- JWKS: `https://localhost:9031/pf/JWKS`
- Token endpoint: `https://localhost:9031/as/token.oauth2`

Reference docs:

- [PingIdentity docs portal](https://docs.pingidentity.com/)
- [OAuth 2.0 client credentials grant](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)
- [JWT claims reference (`sub`, `iss`, `aud`)](https://datatracker.ietf.org/doc/html/rfc7519)
- [JWK / JWKS format](https://datatracker.ietf.org/doc/html/rfc7517)
- [OpenID Connect discovery metadata](https://openid.net/specs/openid-connect-discovery-1_0.html)

PingIdentity's deep documentation URLs move around often. When you need product-specific detail, search the docs portal for `PingFederate access token managers`, `OAuth clients`, and `access token mappings`.

## 3. Manual provisioning

The `provision-pingfederate-oidc` target already creates the OAuth objects. Run the script directly only if you need to reprovision or customize them after PingFederate is healthy:

```sh
PINGFEDERATE_ADMIN_URL=https://localhost:9999 \
PINGFEDERATE_ADMIN_USER=administrator \
PINGFEDERATE_ADMIN_PASSWORD=2FederateM0re \
PINGFEDERATE_VERIFY_TLS=false \
PINGFEDERATE_CLIENT_ID=karapace-client \
PINGFEDERATE_CLIENT_SECRET=karapace-secret \
PINGFEDERATE_TOKEN_ISSUER=https://pingfederate:9031 \
PINGFEDERATE_TOKEN_AUDIENCE=karapace-audience \
python3 bin/oidc/provision_pingfederate_oidc.py
```

The script creates:

- a JWT access token manager
- an OAuth client using `client_credentials`
- a client-credentials mapping that emits `roles`

By default, it configures the JWT access token manager to emit the client identity in the `client_id` claim for this local flow. You can override that with `PINGFEDERATE_CLIENT_ID_CLAIM` if you need a different non-reserved claim name.

Default roles:

```json
[
  "schema:read",
  "schema:write",
  "schema:delete",
  "subject:read",
  "subject:write",
  "subject:delete",
  "config_subject:update",
  "config_global:update"
]
```

You can override those values through `PINGFEDERATE_*` env vars.

## 4. Token contract

Karapace expects these token properties from PingFederate:

- `iss`: `https://pingfederate:9031`
- `aud`: `karapace-audience`
- `client_id`: the caller identity used by Karapace for this local client-credentials flow
- `roles`: the roles used by `OIDC_METHOD_ROLES`

For the local PingFederate flow in this repository, `roles` is provisioned as a multi-valued claim containing a single space-delimited string, for example `[`schema:read schema:write subject:read`]`. Karapace splits that entry into individual roles during authorization. The middleware also accepts a comma-delimited single entry for compatibility in tests and alternate local setups.

If you already provisioned an older local setup that emits `karapace.roles`, switching the claim name in place can fail on PingFederate's existing access token mapping. Recreate the local PingFederate stack from a clean state before changing the claim name.

For the local PingFederate setup in this repository, the provisioner configures the JWT access token manager so the client identity is emitted in `client_id`. PingFederate rejects reserved JWT claim names such as `sub` for `Client ID Claim Name`. If you override the claim name with `PINGFEDERATE_CLIENT_ID_CLAIM`, keep `OIDC_SUB_CLAIM_NAME` aligned with that value and use a non-reserved claim name.

Common local failure: issuer mismatch. If PingFederate emits `https://localhost:9031` but Karapace validates `https://pingfederate:9031`, authentication fails.

## 5. Quick checks

Fetch a client-credentials token with the helper script:

```sh
OIDC_PROVIDER=pingfederate \
OIDC_TOKEN_URL=https://pingfederate:9031/as/token.oauth2 \
OIDC_CLIENT_ID=karapace-client \
OIDC_CLIENT_SECRET=karapace-secret \
OIDC_VERIFY_TLS=false \
python3 bin/oidc/get_oidc_token.py
```

Fetch one manually from the host:

```sh
curl -sk -X POST "https://localhost:9031/as/token.oauth2" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=karapace-client" \
  -d "client_secret=karapace-secret" \
  -d "scope=openid"
```

Use the resulting token against Schema Registry:

```sh
ACCESS_TOKEN="$(OIDC_PROVIDER=pingfederate OIDC_TOKEN_URL=https://pingfederate:9031/as/token.oauth2 OIDC_CLIENT_ID=karapace-client OIDC_CLIENT_SECRET=karapace-secret OIDC_VERIFY_TLS=false python3 bin/oidc/get_oidc_token.py)"
curl --insecure -H "Authorization: Bearer $ACCESS_TOKEN" https://localhost:8081/subjects
```

## 6. Run e2e tests against PingFederate

Use the same `OIDC_*` values from section 1 when running e2e against PingFederate.

```sh
docker compose -f container/compose.yml -f container/compose-auth.yml --profile auth --profile pingfederate exec -T karapace-cli \
  /venv/bin/python3 /opt/karapace/bin/oidc/get_oidc_token.py

docker compose -f container/compose.yml -f container/compose-auth.yml --profile auth --profile pingfederate exec -T karapace-cli \
  /venv/bin/python3 -m pytest -s -vv tests/e2e/schema_registry/test_oidc.py tests/e2e/kafka_rest_apis/test_oidc_forwarding.py
```

For the full e2e suite, reuse the same provisioned stack:

```sh
make e2e-tests-in-docker-pingfederate
```

If you want to stay explicit instead of using the make target, the provider switch is `OIDC_PROVIDER=pingfederate`.

Quick checks for PingFederate readiness:

```sh
curl -sk https://localhost:9031/.well-known/openid-configuration | jq .issuer
curl -sk https://localhost:9031/pf/JWKS | jq .keys[0].kid
curl -sk -u administrator:2FederateM0re https://localhost:9999/pf-admin-api/v1/oauth/clients | jq .items[].clientId
```

## Getting Ping DevOps credentials

The Ping DevOps user and key are not generated by the container. Obtain them from PingIdentity:

1. Go to `https://devops.pingidentity.com/how-to/devopsRegistration/`.
2. Register for Ping DevOps access or sign in with your PingIdentity account.
3. Accept the relevant developer or evaluation terms.
4. Copy your DevOps user and DevOps key from the Ping DevOps portal.
5. Place them in `.env` as `PING_IDENTITY_DEVOPS_USER` and `PING_IDENTITY_DEVOPS_KEY`.

If you cannot obtain those credentials, the alternative is to mount an existing `pingfederate.lic` file into the PingFederate service.
