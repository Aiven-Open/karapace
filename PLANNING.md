# OIDC Integration Plan for Karapace

## 1. Problem Description

The goal is to integrate OpenID Connect (OIDC) as an authentication mechanism in Karapace. This will allow users to authenticate using external OIDC providers like Google, Azure, or GitHub, and manage access to Karapace resources (e.g., Schema Registry) based on OIDC tokens and roles. The solution should be flexible enough to support different OIDC providers and allow for easy configuration.

## 2. Architecture and Design

The OIDC integration will be built upon the existing authentication and authorization framework in Karapace. We will introduce a new `OIDCAuthorizer` class that implements the `AuthenticatorAndAuthorizer` protocol. This new class will handle the OIDC token validation and role extraction.

### 2.1. Configuration

We will add the following configuration parameters to the `Config` class in `src/karapace/core/config.py`:

- `sasl_oauthbearer_jwks_endpoint_url` (Mandatory): The JWKS endpoint URL of the OIDC provider. If this is provided, the OIDC authorizer will be enabled.
- `sasl_oauthbearer_expected_issuer` (Mandatory): The expected issuer of the OIDC token.
- `sasl_oauthbearer_expected_audience` (Mandatory): The expected audience of the OIDC token. This is required to prevent token reuse attacks where a token intended for one service is used to access Karapace.
- `sasl_oauthbearer_roles_claim_path` (Optional): A JMESPath expression to extract roles from the OIDC token. If not provided, role-based authorization will not be performed.

Note: Role-to-permission mappings will be stored in the existing auth file format rather than configuration, ensuring better scalability and consistency with the current permission system.

### 2.2. OIDC Authorizer

The `OIDCAuthorizer` class will be responsible for the full lifecycle of OIDC-based authorization. When a request with an OIDC token is received:

1.  The authorizer will first receive the OIDC token from the `Authorization` header.
2.  It will then fetch the JSON Web Key Set (JWKS) from the `sasl_oauthbearer_jwks_endpoint_url`, caching the result to minimize latency.
3.  Using the `python-jose` library, it will decode and validate the token. This validation is critical and includes:
    -   **Signature verification** using the fetched JWKS.
    -   **Issuer (`iss`) claim verification** against the configured `sasl_oauthbearer_expected_issuer`.
    -   **Audience (`aud`) claim verification** against the configured `sasl_oauthbearer_expected_audience`.
4.  If the token is valid, the authorizer will extract the user's roles using the JMESPath expression from `sasl_oauthbearer_roles_claim_path`.
5.  These roles are then translated into Karapace-specific permissions by looking them up in the `oidc_role_mappings` section of the auth file. This process is done in collaboration with the `HTTPAuthorizer`.
6.  Finally, the request is authorized based on the resolved permissions.

The `OIDCAuthorizer` will be integrated into the existing `AuthContainer` and will be used if the `sasl_oauthbearer_jwks_endpoint_url` configuration option is provided.

#### 2.2.1. OIDCAuthorizer Interface and Requirements

The `OIDCAuthorizer` will implement the `AuthenticatorAndAuthorizer` protocol. The primary interface change will be the new configuration options in `src/karapace/core/config.py`, but the authorizer's adherence to the existing protocol ensures seamless integration with the rest of the application. Given that OIDC relies on bearer tokens instead of username/password, the `authenticate` method's signature might need to be adapted or handled in the web request layer to extract the token.

**Requirements:**

- It must be initialized with the application configuration (`Config`) to access OIDC settings.
- It must fetch and cache the JSON Web Key Set (JWKS) from the OIDC provider.
- It must perform token validation in a fail-fast manner.
- All external network requests (e.g., to the OIDC provider) must be asynchronous to avoid blocking.

### 2.3. Role-to-Permission Mapping

Instead of using configuration for role mappings, we will extend the existing file-based authentication system to include OIDC role mappings. This approach is more scalable and follows the established pattern used for user permissions.

The existing auth file format will be extended to include an `oidc_role_mappings` section:

```json
{
  "users": [
    {
      "username": "admin",
      "algorithm": "scrypt", 
      "salt": "adminsalt",
      "password_hash": "..."
    }
  ],
  "permissions": [
    {
      "username": "admin",
      "operation": "Write",
      "resource": ".*"
    }
  ],
  "oidc_role_mappings": [
    {
      "role": "schema-reader",
      "permissions": ["schema:read"]
    },
    {
      "role": "karapace-admin",
      "permissions": ["schema:read", "schema:write", "subject:read", "subject:write", "subject:delete"]
    }
  ]
}
```

The `HTTPAuthorizer` class will be extended to load and manage OIDC role mappings alongside users and permissions. The `OIDCAuthorizer` will retrieve role mappings from the auth file system instead of configuration. For example, if a user has the `schema-reader` role, they will be granted `schema:read` permission based on the role mapping defined in the auth file.

### 2.4. Local and Integration Testing

For local and integration testing, we will use a `quay.io/keycloak/keycloak` container as the OIDC provider. This will allow us to test the OIDC integration in a controlled environment without relying on external providers.

## 3. Files to be Modified

- **`src/karapace/core/config.py`**: Add new OIDC configuration parameters (excluding role mappings).
- **`src/karapace/core/auth.py`**: 
  - Implement the `OIDCAuthorizer` class.
  - Extend `AuthData`, `HTTPAuthorizer` to support OIDC role mappings from auth files.
  - Add new `OIDCRoleMappingData` TypedDict for role mapping structure.
- **`src/karapace/core/auth_container.py`**: Integrate the `OIDCAuthorizer` into the dependency injection container.
- **Auth files** (e.g., `tests/integration/config/karapace.auth.json`): Extend format to include `oidc_role_mappings` section.
- **`tests/`**: Add new unit and integration tests for the OIDC integration and auth file format extension.

## 4. Validation Rules

The following validation rules will be implemented:

**Configuration validation:**
- The `sasl_oauthbearer_jwks_endpoint_url` must be a valid URL.
- The `sasl_oauthbearer_expected_issuer` must be a non-empty string.
- The `sasl_oauthbearer_expected_audience` must be a non-empty string.

**Auth file validation:**
- The `oidc_role_mappings` section must contain valid role mapping objects.
- Each role mapping must have a `role` (string) and `permissions` (array of strings).
- Role names must be non-empty strings.
- Permission values must follow the established format (e.g., `schema:read`, `subject:write`).

## 5. Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant KarapaceWebLayer as Karapace (Web Layer)
    participant OIDCAuthorizer
    participant HTTPAuthorizer
    participant OIDCProvider

    Client->>KarapaceWebLayer: Request with OIDC token
    activate KarapaceWebLayer
    KarapaceWebLayer->>OIDCAuthorizer: Authenticate and authorize request
    activate OIDCAuthorizer

    OIDCAuthorizer->>OIDCAuthorizer: Check for cached JWKS
    alt JWKS not in cache or expired
        OIDCAuthorizer->>OIDCProvider: Fetch JWKS (with single-flight lock)
        activate OIDCProvider
        OIDCProvider-->>OIDCAuthorizer: JWKS Response
        deactivate OIDCProvider
        OIDCAuthorizer->>OIDCAuthorizer: Cache JWKS (with TTL)
    end

    OIDCAuthorizer->>OIDCAuthorizer: Validate token (signature, iss, aud)
    alt Token is invalid
        OIDCAuthorizer-->>KarapaceWebLayer: Authentication failure
        deactivate OIDCAuthorizer
        KarapaceWebLayer-->>Client: 401 Unauthorized
        deactivate KarapaceWebLayer
    end

    OIDCAuthorizer->>OIDCAuthorizer: Extract roles via JMESPath from token claim
    OIDCAuthorizer->>HTTPAuthorizer: Get permissions for extracted roles
    activate HTTPAuthorizer
    HTTPAuthorizer->>HTTPAuthorizer: Load `oidc_role_mappings` from auth file
    HTTPAuthorizer-->>OIDCAuthorizer: Return mapped permissions
    deactivate HTTPAuthorizer

    OIDCAuthorizer->>OIDCAuthorizer: Authorize request against permissions
    alt Request is authorized
        OIDCAuthorizer-->>KarapaceWebLayer: Authorization success
        KarapaceWebLayer->>KarapaceWebLayer: Process request
        KarapaceWebLayer-->>Client: Response
    else Request is not authorized
        OIDCAuthorizer-->>KarapaceWebLayer: Authorization failure
        KarapaceWebLayer-->>Client: 403 Forbidden
    end
    deactivate OIDCAuthorizer
    deactivate KarapaceWebLayer
```

## 6. Resilience and Error Handling

To ensure high availability and robust performance, especially under high load (e.g., 1000 requests per second), the following strategies will be implemented.

### 6.1. Asynchronous Operations

All network I/O, specifically requests to the OIDC provider's JWKS endpoint, will be fully asynchronous using `aiohttp`. This ensures that waiting for the OIDC provider does not block the main application thread, allowing Karapace to handle other requests concurrently.

### 6.2. TTL-Based Caching

The JSON Web Key Set (JWKS) fetched from the OIDC provider will be cached in memory. This avoids the latency of a network request for every incoming API call that requires token validation.
- **Cache Key:** The JWKS endpoint URL.
- **Caching Strategy:** A Time-To-Live (TTL) based cache. The TTL will be determined by the `Cache-Control` or `Expires` headers in the OIDC provider's response. If these headers are not present, a configurable default TTL (e.g., 24 hours) will be used.

#### 6.2.1. Cache Stampede Protection

To prevent cache stampede scenarios where multiple requests simultaneously attempt to fetch the JWKS when the cache expires, a **single-flight mechanism** will be implemented:

- **Problem:** When many tokens expire simultaneously (e.g., during a burst of traffic), multiple concurrent requests might all find an empty cache and simultaneously attempt to fetch the JWKS from the OIDC provider. This creates a load spike on the provider and wastes resources.
- **Solution:** Implement a single-flight pattern where:
  1. When a cache miss occurs, the first request creates an `asyncio.Task` to fetch the JWKS and stores it in a temporary "in-flight" dictionary, keyed by the JWKS URL.
  2. Subsequent concurrent requests finding the task in the "in-flight" dictionary will `await` this existing task instead of starting a new fetch operation.
  3. Once the fetch task completes, its result is placed in the main cache, and the task is removed from the "in-flight" dictionary.
  4. If the fetch task fails, it is removed from the "in-flight" dictionary, allowing a subsequent request to retry the operation.

This ensures that regardless of how many concurrent requests need the JWKS, only one network request is made to the OIDC provider per cache refresh cycle.

### 6.3. Circuit Breaker

A circuit breaker pattern will be implemented for requests to the OIDC provider.
- **Purpose:** If the OIDC provider becomes slow or unavailable, the circuit breaker will "trip" (open).
- **Behavior:** While the circuit is open, all requests to the OIDC provider will fail immediately without attempting a network call. This prevents system resources from being consumed by requests that are likely to fail and allows the service to fail-fast. After a configured timeout, the circuit will move to a "half-open" state to check if the OIDC provider has recovered.

### 6.4. Fail-Fast Token Validation

Token validation will be performed as early as possible in the request lifecycle. If a token is invalid for any reason (e.g., invalid signature, incorrect issuer, expired), the request will be rejected immediately with a `401 Unauthorized` or `403 Forbidden` response. This prevents further processing of an invalid request.

## 7. Configuration Example for Google OIDC

Here is an example of how to configure Karapace to use Google as an OIDC provider. The user would need to create OAuth 2.0 credentials in their Google Cloud project to get a client ID, which would be the audience.

```json
{
  "karapace": {
    "sasl_oauthbearer_jwks_endpoint_url": "https://www.googleapis.com/oauth2/v3/certs",
    "sasl_oauthbearer_expected_issuer": "https://accounts.google.com",
    "sasl_oauthbearer_expected_audience": "your-google-client-id.apps.googleusercontent.com",
    "sasl_oauthbearer_roles_claim_path": "roles"
  }
}
```