# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Karapace

Karapace is an open-source implementation of Kafka Schema Registry and Kafka REST Proxy. It supports Avro, JSON Schema, and Protobuf schemas. The Schema Registry is built on FastAPI; the REST Proxy uses aiohttp.

## Build & Development Commands

### Local setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install .               # production install
pip install -e ".[dev]"     # development install (includes test/lint deps)
```

Requires Python 3.12+, Go, Rust, and protoc < 3.20.0.

### Running services locally

```bash
# Schema Registry (FastAPI, port 8081)
python3 -m karapace

# REST Proxy (aiohttp, port 8082)
python3 -m karapace.kafka_rest_apis
```

Config is loaded from env vars prefixed `KARAPACE_`, or override `bootstrap_uri`/`port` in `src/karapace/core/config.py` defaults during local dev.

### Linting & formatting

```bash
# Ruff (line length: 125)
ruff check .
ruff format .

# Or use pre-commit (runs ruff + isort + other hooks)
pre-commit run --all-files
```

### Type checking

```bash
mypy
```

### Running tests

```bash
# All tests (uses pytest-xdist with --numprocesses auto)
pytest

# Unit tests only (no Kafka required)
pytest tests/unit/

# Single test file
pytest tests/unit/test_schema_models.py

# Single test function
pytest tests/unit/test_schema_models.py::test_function_name -x

# Integration tests (requires running Kafka)
pytest tests/integration/
pytest tests/integration/test_dependencies_compatibility_protobuf.py  # single integration file

# E2E tests
pytest tests/e2e/
```

Pytest config is in `pytest.ini`: uses `--numprocesses auto`, `--import-mode=importlib`, 90s timeout.

### Docker-based development

```bash
cp .env.example .env  # then edit: PYTHON_VERSION, RUNNER_UID, RUNNER_GID, etc.

# Container management
make cli                              # interactive shell in container
make start-karapace-docker-resources  # start all containers
make stop-karapace-docker-resources   # stop all containers

# Tests in Docker (common env vars: DOCKER_COMPOSE, RUNNER_UID, RUNNER_GID, PYTHON_VERSION, PYTEST_ARGS)
make unit-tests-in-docker
make integration-tests-in-docker
make e2e-tests-in-docker
make smoke-test-schema-registry
make smoke-test-rest-proxy
make type-check-mypy-in-docker

# Example with full env overrides
PYTEST_ARGS="--cov=src/karapace --cov-append --numprocesses 4" \
  DOCKER_COMPOSE="docker-compose" RUNNER_UID=503 RUNNER_GID=20 \
  PYTHON_VERSION=3.12 make unit-tests-in-docker
```

### Docker compose

```bash
# Start everything (registry on :8081, REST proxy on :8082)
docker compose -f ./container/compose.yml up -d

# Start individual services
docker compose -f ./container/compose.yml up -d kafka
docker compose -f ./container/compose.yml up -d karapace-schema-registry

# Build local image
docker build --build-arg PYTHON_VERSION=3.12 -t karapacelocal -f ./container/Dockerfile .
```

### Reinstalling after code changes

```bash
pip cache purge
pip uninstall karapace
pip install .
```

### Useful API curl commands for manual testing

```bash
# List subjects
curl http://localhost:8081/subjects

# Register Avro schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
  http://localhost:8081/subjects/testtopic1-value/versions

# Register Protobuf schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schemaType": "PROTOBUF", "schema": "syntax = \"proto3\";\n\nmessage Obj {\n  int32 age = 1;\n}"}' \
  http://localhost:8081/subjects/testtopic-value/versions

# Register JSON Schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schemaType": "JSON", "schema": "{\"type\": \"object\", \"properties\": {\"age\": {\"type\": \"integer\"}}, \"required\": [\"age\"]}"}' \
  http://localhost:8081/subjects/accountstopic-value/versions

# Get schema version
curl "http://localhost:8081/subjects/testtopic1-value/versions/1"

# Set compatibility level
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' http://localhost:8081/config

# REST Proxy: list topics
curl "http://localhost:8082/topics"

# REST Proxy: produce Avro message
curl -H "Content-Type: application/vnd.kafka.avro.v2+json" -X POST -d \
  '{"value_schema": "{\"type\": \"record\", \"name\": \"simple\", \"namespace\": \"example.avro\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "name0"}}]}' \
  http://localhost:8082/topics/testtopic

# REST Proxy: create consumer, subscribe, consume
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
  --data '{"name": "my_consumer", "format": "avro", "auto.offset.reset": "earliest"}' \
  http://localhost:8082/consumers/avro_consumers

curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
  --data '{"topics":["testtopic"]}' \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer/subscription

curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer/records

# Auth: basic auth
curl -X GET -u "user:mypwd" http://localhost:8081/subjects

# Auth: OIDC Bearer token
curl -H "Authorization: Bearer $ACCESS_TOKEN" http://localhost:8081/subjects
```

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
