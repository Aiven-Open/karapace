# Karapace Development Agents

Reusable agent prompts for Claude Code to streamline common development workflows in this codebase. Copy these into `.claude/agents/<name>.md` files to make them invocable.

---

## test-runner

**File:** `.claude/agents/test-runner.md`

```markdown
---
description: Run tests for changed code and report results
---

Identify which files were changed and run the relevant tests:

1. Check `git diff --name-only` to find modified source files.
2. Map source files to test files:
   - `src/karapace/core/<module>.py` → `tests/unit/test_<module>.py`
   - `src/karapace/api/routers/<module>.py` → `tests/unit/api/test_<module>.py`
   - `src/karapace/core/compatibility/` → `tests/unit/compatibility/`
   - `src/karapace/core/protobuf/` → `tests/unit/protobuf/`
   - `src/karapace/kafka_rest_apis/` → `tests/unit/kafka_rest_apis/`
3. Run: `pytest <test_files> -x -v --tb=short --no-header`
4. If tests fail, report the failure with file path, test name, and error summary.
5. If no matching test files exist, report that and suggest running the full unit suite.
```

---

## lint-fix

**File:** `.claude/agents/lint-fix.md`

```markdown
---
description: Lint and format code, then fix any issues
---

Run linting and formatting on the codebase:

1. Run `ruff check . --fix` to auto-fix linting issues.
2. Run `ruff format .` to format code.
3. If there are remaining unfixable lint errors, report them with file paths and line numbers.
4. Do NOT modify test data files or generated files.
```

---

## schema-change-reviewer

**File:** `.claude/agents/schema-change-reviewer.md`

```markdown
---
description: Review changes to schema handling code for correctness
---

When reviewing changes to schema-related code, check these concerns:

1. **Compatibility logic** (`src/karapace/core/compatibility/`): Ensure changes don't break backward/forward/full compatibility checks for Avro, JSON Schema, or Protobuf.
2. **Schema models** (`src/karapace/core/schema_models.py`): Verify that `ParsedTypedSchema`, `ValidatedTypedSchema`, and `TypedSchema` changes maintain the parse→validate→store pipeline.
3. **Schema reader** (`src/karapace/core/schema_reader.py`): Changes here affect how schemas are consumed from the `_schemas` Kafka topic. Verify offset handling and database updates.
4. **In-memory database** (`src/karapace/core/in_memory_database.py`): Check thread safety and consistency of subject/schema/version lookups.
5. **References** (`src/karapace/core/schema_references.py`): Schema references must resolve correctly for all three schema types.
6. **Normalization**: Protobuf normalization is the only type currently supported. Don't assume normalization works for Avro or JSON Schema.

Run `pytest tests/unit/test_schema_models.py tests/unit/compatibility/ -x -v` to validate.
```

---

## di-container-helper

**File:** `.claude/agents/di-container-helper.md`

```markdown
---
description: Help with dependency injection container wiring
---

This project uses `dependency-injector` with declarative containers. When adding or modifying injected dependencies:

1. **Container hierarchy** (most specific to least):
   - `SchemaRegistryContainer` (api/container.py) → depends on KarapaceContainer, MetricsContainer, TelemetryContainer
   - `TelemetryContainer` (api/telemetry/container.py) → depends on KarapaceContainer, MetricsContainer
   - `AuthContainer` (core/auth_container.py) → depends on KarapaceContainer
   - `MetricsContainer` (core/metrics_container.py) → depends on KarapaceContainer
   - `KarapaceContainer` (core/container.py) → root container (Config, ForwardClient, Prometheus)

2. **Wiring rules**: Every module that uses `@inject` or `Provide[]` MUST be listed in the container's `.wire(modules=[...])` call in `__main__.py`. Missing wiring is a silent failure — dependencies will be `None` at runtime.

3. **Test fixtures**: Tests set up their own containers in `tests/conftest.py`. When adding new providers, update test container setup too.

4. **Pattern**: Use `Depends(Provide[Container.dependency])` in FastAPI route functions and lifespan. Use `@inject` decorator on non-FastAPI functions.

Verify wiring by checking `src/karapace/__main__.py` and running `pytest tests/unit/test_main.py -x -v`.
```

---

## api-route-helper

**File:** `.claude/agents/api-route-helper.md`

```markdown
---
description: Help add or modify Schema Registry API routes
---

When working with Schema Registry API routes:

1. **Route files** are in `src/karapace/api/routers/`. Each file handles a resource domain (subjects, schemas, config, compatibility, mode, health, metrics).
2. **Router setup**: New routers must be registered in `src/karapace/api/routers/setup.py`.
3. **Controller**: Business logic calls go through `KarapaceSchemaRegistryController` (`src/karapace/api/controller.py`), which delegates to `KarapaceSchemaRegistry` (`src/karapace/core/schema_registry.py`).
4. **Request/response models**: Route request schemas are in `src/karapace/api/routers/requests.py`. Errors are in `src/karapace/api/routers/errors.py`.
5. **Content types**: Schema Registry uses `application/vnd.schemaregistry.v1+json`. Content type handling is in `src/karapace/api/content_type.py`.
6. **Auth**: Routes that need auth use injected `AuthenticatorAndAuthorizer` from `AuthContainer`. Check existing routes for the pattern.
7. **Forwarding**: Write operations on non-master nodes must be forwarded to the master via `ForwardClient`.

Test with: `pytest tests/unit/api/ -x -v`
```
