# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Karapace

Karapace is an open-source implementation of Kafka Schema Registry and Kafka REST Proxy. It supports Avro, JSON Schema, and Protobuf schemas. The Schema Registry is built on FastAPI; the REST Proxy uses aiohttp.

## Build & Development Commands

### Local setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"     # development install (includes test/lint deps)
```

Requires Python 3.12+, Go, Rust, and protoc < 3.20.0.

### Running services locally

```bash
python3 -m karapace                  # Schema Registry (FastAPI, port 8081)
python3 -m karapace.kafka_rest_apis  # REST Proxy (aiohttp, port 8082)
```

Config is loaded from env vars prefixed `KARAPACE_`, or override defaults in `src/karapace/core/config.py` during local dev.

### Linting, formatting, type checking

```bash
ruff check .
ruff format .
pre-commit run --all-files  # runs ruff + isort + other hooks
mypy
```

### Running tests

```bash
pytest                                                          # all tests (pytest-xdist --numprocesses auto)
pytest tests/unit/                                              # unit (no Kafka required)
pytest tests/unit/test_schema_models.py::test_function_name -x  # single test
pytest tests/integration/                                       # requires running Kafka
pytest tests/e2e/
```

Pytest config in `pytest.ini`: `--numprocesses auto`, `--import-mode=importlib`, 90s timeout.

### Docker-based development

```bash
make cli                              # interactive shell in container
make start-karapace-docker-resources  # start all containers
make stop-karapace-docker-resources   # stop all containers

make unit-tests-in-docker
make integration-tests-in-docker
make e2e-tests-in-docker
make smoke-test-schema-registry
make smoke-test-rest-proxy
make type-check-mypy-in-docker
```

Common env overrides for the `*-in-docker` targets: `DOCKER_COMPOSE`, `RUNNER_UID`, `RUNNER_GID`, `PYTHON_VERSION`, `PYTEST_ARGS`.

For ad-hoc compose usage, see `container/compose.yml` (registry on :8081, REST proxy on :8082).

For manual API curl examples, see the project README.

## Architecture

### Source layout: `src/karapace/`

**Two main services with separate entry points:**
- **Schema Registry** (`karapace/__main__.py`): FastAPI app via uvicorn. Entry point: `karapace` CLI command.
- **REST Proxy** (`karapace/kafka_rest_apis/__main__.py`): aiohttp app. Entry point: `karapace_rest_proxy` CLI command.

**Dependency Injection** uses `dependency-injector` with declarative containers:
- `core/container.py` → `KarapaceContainer` (config, forward client, prometheus)
- `api/container.py` → `SchemaRegistryContainer` (schema registry, controller)
- `core/auth_container.py` → `AuthContainer`
- `core/metrics_container.py` → `MetricsContainer`
- `api/telemetry/container.py` → `TelemetryContainer`

Containers must be wired to modules that use injected dependencies (see `__main__.py` for wiring patterns).

### Key modules

- `core/schema_registry.py` → `KarapaceSchemaRegistry`: main business logic (schema CRUD, compatibility checks)
- `core/schema_reader.py` → `KafkaSchemaReader`: reads schemas from Kafka `_schemas` topic
- `core/in_memory_database.py` → `InMemoryDatabase`: in-memory schema storage
- `core/coordinator/` → Master election via Kafka consumer groups
- `core/compatibility/` → Schema compatibility checking (Avro, JSON Schema, Protobuf)
- `core/config.py` → `Config` (Pydantic `BaseSettings`, env vars prefixed `KARAPACE_`)
- `api/routers/` → FastAPI route handlers (subjects, schemas, config, compatibility, health, mode, metrics)
- `api/controller.py` → `KarapaceSchemaRegistryController`: bridges routes to registry logic
- `kafka_rest_apis/` → REST proxy (aiohttp-based, separate from Schema Registry)
- `rapu.py` → Custom aiohttp middleware layer used by REST Proxy

### Schema flow

Schemas are stored in a Kafka topic (`_schemas`). `KafkaSchemaReader` consumes this topic and populates `InMemoryDatabase`. Write operations go through `KarapaceSchemaRegistry` → `KarapaceProducer` → Kafka topic. Master coordination ensures only one instance handles writes.

### Tests

- `tests/unit/` → Unit tests (no Kafka required)
- `tests/integration/` → Integration tests (require Kafka; fixtures in `tests/integration/conftest.py`)
- `tests/e2e/` → End-to-end tests
- `tests/conftest.py` → Shared fixtures, DI container setup for tests

### Code style

- Ruff for linting/formatting, line length 125
- isort config: profile=black, force_alphabetical_sort, line_length=125
- Uses a patched Avro library from `github.com/aiven/avro`
- The `runtime/` directory must exist (used by protoc); not cleaned between test runs automatically
