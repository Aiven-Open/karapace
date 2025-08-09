# OIDC Implementation Task List

This document breaks down the implementation of OIDC authentication, as designed in `PLANNING.md`, into a series of iterative steps. Each step includes a set of quality gates that must be passed before moving to the next.

---

### Step 1: Configuration and Core Interfaces

**Goal:** Define all necessary configuration settings and the core `OIDCAuthorizer` interface without implementing business logic. This provides a stable foundation for the next steps.

**Tasks:**
1.  **Modify `src/karapace/core/config.py`**:
    -   Add the new OIDC configuration fields to the `Config` model using `pydantic`.
    -   Mark `sasl_oauthbearer_jwks_endpoint_url`, `sasl_oauthbearer_expected_issuer`, and `sasl_oauthbearer_expected_audience` as mandatory (`str`).
    -   Mark `sasl_oauthbearer_roles_claim_path` as optional (`Optional[str]`).
2.  **Extend auth file format in `src/karapace/core/auth.py`**:
    -   Add new `OIDCRoleMappingData` TypedDict with `role: str` and `permissions: list[str]` fields.
    -   Extend `AuthData` TypedDict to include `oidc_role_mappings: list[OIDCRoleMappingData]` field (optional).
    -   Modify `HTTPAuthorizer._load_authfile` method to load OIDC role mappings from auth files.
    -   Add validation for OIDC role mappings format.
3.  **Define the `OIDCAuthorizer` class**:
    -   Ensure it inherits from `AuthenticatorAndAuthorizer`.
    -   Create stubs for all required methods (`__init__`, `start`, `close`, `authenticate`, `check_authorization`) with `pass` as the implementation.
    -   Ensure all method signatures are fully typed.
    -   Add a method to access role mappings from the auth file system.
4.  **Create placeholder `User` representation**:
    -   For now, the `authenticate` method can return a temporary `User` object for type-correctness. The actual user data will be populated from the token in a later step.

**Quality Gates:**
-   [ ] **Unit Tests:** 
    -   New unit tests for the `Config` model pass, verifying that default values are set correctly and that environment variable overrides work.
    -   Unit tests for auth file loading with OIDC role mappings work correctly.
    -   Validation tests for malformed role mappings in auth files.
    -   Add tests for logic only, without testing logging.
-   [ ] **Type Checking:** The project passes `mypy` checks without errors.
-   [ ] **Linting:** The project passes `ruff` checks without errors.
-   [ ] **Error Handling:** Proper error handling for malformed auth files with invalid OIDC role mappings.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** Only `src/karapace/core/config.py`, `src/karapace/core/auth.py`, sample auth files, and corresponding test files are modified.

---

### Step 2: Auth File Format Extension and Sample Files

**Goal:** Create sample auth files with OIDC role mappings and verify the extended format works correctly.

**Tasks:**
1.  **Create sample auth files with OIDC role mappings**:
    -   Update `tests/integration/config/karapace.auth.json` to include an `oidc_role_mappings` section.
    -   Create additional sample files for different OIDC provider scenarios (Google, Azure, etc.).
    -   Ensure the examples follow the new format defined in Step 1.
2.  **Test auth file loading with OIDC role mappings**:
    -   Verify that `HTTPAuthorizer._load_authfile` correctly loads the extended format.
    -   Test that role mappings are accessible from the loaded auth data.
    -   Test backward compatibility (auth files without `oidc_role_mappings` should still work).
3.  **Add validation for OIDC role mappings**:
    -   Ensure proper validation of role mapping structure.
    -   Test error handling for malformed role mappings.

**Quality Gates:**
-   [ ] **Unit Tests:**
    -   Test loading auth files with and without OIDC role mappings.
    -   Test validation of role mapping format.
    -   Test error cases (invalid role names, malformed permissions, etc.).
    -   Add tests for logic only, without testing logging.
-   [ ] **Integration Tests:** Test that sample auth files load correctly in a running instance.
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Error Handling:** Proper error messages for malformed auth files.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** Sample auth files, `src/karapace/core/auth.py`, and corresponding test files.

---

### Step 3: Asynchronous JWKS Fetching with Caching and Circuit Breaker

**Goal:** Implement the mechanism for securely and resiliently fetching the JSON Web Key Set from the OIDC provider.

**Tasks:**
1.  **Implement `OIDCAuthorizer.start` and `OIDCAuthorizer.__init__`**:
    -   Initialize an `aiohttp.ClientSession` for making HTTP requests.
    -   Initialize a TTL-based cache (e.g., using the `async-lru` library or a custom implementation).
    -   Initialize a circuit breaker (e.g., using the `pybreaker` library).
    -   Initialize a dictionary to track in-flight JWKS fetch tasks for the single-flight mechanism.
2.  **Create a private method `_fetch_jwks`**:
    -   This method, wrapped by the circuit breaker, will asynchronously fetch the JWKS from `sasl_oauthbearer_jwks_endpoint_url`.
    -   Store the fetched keys in the TTL cache. The TTL should be derived from the response's `Cache-Control` header or a default fallback.
    -   The `start` method will call this once to pre-warm the cache.
3.  **Implement single-flight mechanism**:
    -   Create a private method `_get_jwks_with_single_flight` that uses an in-flight request map to ensure only one concurrent `asyncio.Task` fetches the JWKS when the cache expires.
    -   Multiple concurrent requests should await the same in-flight `Task` to get the result.
    -   Handle the case where the fetch `Task` fails, allowing a subsequent request to retry.

**Quality Gates:**
-   [ ] **Unit Tests:**
    -   Test that `_fetch_jwks` correctly fetches and parses a valid JWKS from a mocked endpoint (`aresponses` is a good choice for this).
    -   Test that the caching works: a second call within the TTL period should not trigger a network request.
    -   Test that the circuit breaker opens after a series of failed requests and closes after a successful one.
    -   Test the single-flight mechanism: multiple concurrent requests for an expired JWKS should result in only one network call to the OIDC provider.
    -   Test single-flight failure handling: if the first `Task` fails, a subsequent request should be able to create a new `Task` to retry.
    -   Add tests for logic only, without testing logging.
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Error Handling:** The `_fetch_jwks` method must handle network errors, timeouts, and non-200 status codes gracefully.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** Only `src/karapace/core/auth.py` and its corresponding test file are modified.

---

### Step 4: Full Token Validation

**Goal:** Implement the core security logic to validate an incoming OIDC token.

**Tasks:**
1.  **Implement the `OIDCAuthorizer.authenticate` method**:
    -   This method will take the bearer token `str` as input.
    -   Use the `python-jose` library to decode the JWT.
    -   The `key` for decoding will come from the cached JWKS.
    -   Perform all required validations:
        -   Signature verification using the JWKS.
        -   Issuer (`iss`) claim matches `sasl_oauthbearer_expected_issuer`.
        -   Audience (`aud`) claim matches `sasl_oauthbearer_expected_audience`.
        -   Token is not expired (`exp` claim).
2.  **Refine `User` object creation**:
    -   Upon successful validation, create a `User` object populated with claims from the token (e.g., `sub` for username).

**Quality Gates:**
-   [ ] **Unit Tests:**
    -   Test with a valid token that passes all validations.
    -   Test with an invalid signature, incorrect issuer, incorrect audience, and an expired token. Each should raise an `AuthenticationError`.
    -   Test with a token signed by an unknown key (not in the JWKS).
    -   Add tests for logic only, without testing logging.
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Error Handling:** The method must implement fail-fast validation and raise a specific `AuthenticationError` for any validation failure.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** Only `src/karapace/core/auth.py` and its test file are modified.

---

### Step 5: Role Extraction and Authorization Logic

**Goal:** Translate the validated token into application-specific permissions using the auth file-based role mapping system.

**Tasks:**
1.  **Add role extraction to `authenticate`**:
    -   After successful token validation, use the `jmespath` library to extract roles from the token claims based on the `sasl_oauthbearer_roles_claim_path` pattern.
    -   Store the extracted roles on the `User` object.
2.  **Implement `OIDCAuthorizer.check_authorization`**:
    -   Access the OIDC role mappings from the auth file system (loaded by `HTTPAuthorizer`).
    -   Use these role mappings to translate the user's roles into a set of Karapace `Operation` permissions.
    -   Check if the requested operation on the given resource is permitted for the user.
3.  **Implement collaboration with HTTPAuthorizer**:
    -   Create methods to access the loaded OIDC role mappings from `HTTPAuthorizer`.
    -   Ensure proper error handling when auth file doesn't contain OIDC role mappings section.

**Quality Gates:**
-   [ ] **Unit Tests:**
    -   Test role extraction from various example token payloads.
    -   Test `check_authorization` for users with sufficient, insufficient, and no permissions.
    -   Test the case where a user has a role that is not in the auth file's `oidc_role_mappings`.
    -   Test proper handling when auth file doesn't contain OIDC role mappings section.
    -   Add tests for logic only, without testing logging.
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Error Handling:** Handle cases where the roles claim is missing or has an unexpected format, and when role mappings are not available in auth file.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** Only `src/karapace/core/auth.py` and its test file are modified.

---

### Step 6: Integration with Dependency Injection

**Goal:** Wire the `OIDCAuthorizer` into the application so it is used when configured.

**Tasks:**
1.  **Modify `src/karapace/core/auth_container.py`**:
    -   Create a new `providers.Singleton` for the `OIDCAuthorizer`.
    -   Modify the `get_authorizer` factory function. It should inspect the `config` and return the `OIDCAuthorizer` singleton if `config.sasl_oauthbearer_jwks_endpoint_url` is set. Otherwise, it should fall back to the existing authorizers.
2.  **Integrate with the request layer**:
    -   Identify the middleware or request handler that currently deals with authentication.
    -   Modify it to extract the `Authorization: Bearer <token>` and call the authorizer's `authenticate` method.

**Quality Gates:**
-   [ ] **Integration Tests:** Add integration tests that send requests with valid and invalid mock OIDC tokens to a running Karapace instance and assert the correct HTTP response codes (200, 401, 403).
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** `src/karapace/core/auth_container.py`, `tests/integration/`, and the relevant API/middleware files are modified.

---

### Step 7: OIDC Authorizer Metrics and Telemetry

**Goal:** Add comprehensive metrics and OpenTelemetry support to the `OIDCAuthorizer` for observability.

**Tasks:**
1.  **Define OIDC-specific metrics**:
    -   Create an `OIDCAuthMetrics` class, likely in `src/karapace/core/auth.py`, to encapsulate all OIDC-related metrics.
    -   Define the following metrics using the `opentelemetry-python` API:
        -   `karapace_oidc_token_validation_total`: A `Counter` to track token validation attempts, with attributes for the result (e.g., `success`, `failure_reason`).
        -   `karapace_oidc_token_validation_duration_seconds`: A `Histogram` to measure the latency of token validation.
        -   `karapace_oidc_jwks_fetch_total`: A `Counter` for JWKS fetch attempts, with attributes for `success` or `failure`.
        -   `karapace_oidc_jwks_cache_total`: A `Counter` for JWKS cache lookups, with an attribute for `hit` or `miss`.
        -   `karapace_oidc_authorization_total`: A `Counter` for authorization checks, with attributes for the result (`granted`, `denied`).
2.  **Integrate metrics into `OIDCAuthorizer`**:
    -   Inject the `OIDCAuthMetrics` instance into the `OIDCAuthorizer`.
    -   In the `authenticate` method, record validation duration and outcome.
    -   In the JWKS fetching logic (`_fetch_jwks`), instrument cache lookups and fetch operations.
    -   In the `check_authorization` method, record the outcome of permission checks.
3.  **Wire up dependency injection**:
    -   Ensure the `OIDCAuthMetrics` class is instantiated as a singleton and provided by the dependency injection container.

**Quality Gates:**
-   [ ] **Unit Tests:**
    -   Test that metrics are correctly recorded for various scenarios (e.g., valid token, invalid token, JWKS fetch failure, cache hit/miss).
    -   Verify that metric attributes (labels) are correctly set.
    -   Add tests for logic only, without testing logging.
-   [ ] **Integration Tests:**
    -   Verify that the new metrics are correctly exported to the configured backend (e.g., Prometheus via the OpenTelemetry collector).
-   [ ] **Type Checking:** Passes `mypy`.
-   [ ] **Linting:** Passes `ruff`.
-   [ ] **Logging:** There are at least one error log for each unhappy case and at most one log for successful completion.
-   [ ] **Scope:** `src/karapace/core/auth.py`, `src/karapace/core/auth_container.py`, and corresponding test files are modified.