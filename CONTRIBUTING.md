# Welcome

Contributions are very welcome on Karapace. When contributing please keep this in mind:

- Open an issue to discuss new bigger features.
- Write code consistent with the project style and make sure the tests are passing.
- Stay in touch with us if we have follow up questions or requests for further changes.

# Development

## Local Environment

There is very little you need to get started coding for Karapace:

- Use [one of the supported python versions](https://github.com/Aiven-Open/karapace/blob/master/pyproject.toml)
  documented in the `project:requires-python`.
- [Install](https://go.dev/doc/install) Go (needed for the `protopace` helpers built during `pip install .`). The Go module declares `go 1.21` (toolchain 1.24 is supported).
- Create [a virtual environment](https://docs.python.org/3/tutorial/venv.html) and install the dev dependencies in it:

```python
python -m venv <path_to_venv>
source <path_to_venv>/bin/activate
pip install .
pip install -e .[dev,typing]
```

## Local Environment with Docker & Compose

For isolated testing and development using Docker, we provide convenient make commands that handle all setup automatically.
This ensures consistency across development environments.

### Quick Setup

**Set `.env`:**

```bash
cp .env.example .env
```

Edit `.env` to customize:

- `PYTHON_VERSION` - Python version for Docker builds (default: 3.12)
- `KARAPACE_VERSION` - Karapace version (default: 5.0.3)
- `DOCKER_COMPOSE` - Docker compose command (default: docker-compose)
- `RUNNER_UID` / `RUNNER_GID` - Container user IDs (check .env.example on how to get)
- `PYTEST_ARGS` - Pytest arguments for test runs

### Available Commands

**Run any make command** - `make` populates the environment with `.env`:

Some commands run `start-karapace-docker-resources`, so resources are available.

- `make cli` - Open interactive shell inside Karapace container
- `make start-karapace-docker-resources` - Start all Docker containers (Kafka, Schema Registry, etc.)
- `make stop-karapace-docker-resources` - Stop all Docker containers
- `make type-check-mypy-in-docker` - Run type checking in Docker
- `make unit-tests-in-docker` - Run unit tests in Docker
- `make integration-tests-in-docker` - Run integration tests in Docker
- `make e2e-tests-in-docker` - Run end-to-end tests in Docker
- `make smoke-test-schema-registry` - Run schema registry smoke tests
- `make smoke-test-rest-proxy` - Run REST proxy smoke tests

## Website

Karapace documentation is part of this GitHub repository. You can find it in `website` folder. Its `README` file contains instructions explaining how you can run documentation website locally.

## Tests

Tests are written with the [pytest](https://docs.pytest.org/) framework, and All PRs are tested for
each supported Python version using [GitHub Flow](https://guides.github.com/introduction/flow/).

There are two flavors of tests, unit tests and integration tests:

- Unit: These are faster and very useful for quick iterations. They are usually testing pure
  functions.
- Integration: Are slower but more complete. These tests run Karapace, ZooKeeper, and Kafka servers,
  pytest's fixtures are used to start/stop these for you.

Both flavors run in parallel using [pytest-xdist](https://github.com/pytest-dev/pytest-xdist). New
tests should be engineered with this in mind:

- Don't reuse schema/subject/topic names
- Expect other clients to be interacting with the servers at the same time.

Before running the tests make sure you have `protoc` installed. `protoc` is part of the protobuf-compiler package.
In Fedora® distributions you can install it using:
```
dnf install protobuf-compiler
```

### Running Tests with Docker (Recommended)

Use the make commands for consistent, isolated test environments:

```sh
make unit-tests-in-docker
make integration-tests-in-docker
make e2e-tests-in-docker
```

### Running Tests Locally

To run the tests locally in your virtual environment use the binary `pytest` available in the virtualenv.
It will download Kafka to be used in the tests for you:

```sh
pytest tests/unit
pytest tests/integration
```

The integration tests can be configured with the use of a few parameters:

- `--kafka-version`: allows to change the version of the Kafka server used by the tests. Example
    versions: `2.7.2`, `2.8.1`, `3.0.0`.
- `--kafka-bootstrap-servers`: A comma separated list of servers. This option allows to use an
    external server (the tests won't start a server for you)

Other options can be seen with `pytest test/integration --help`

## Static checking and Linting

The code is statically checked and formatted using [a few tools][requirements-dev].
To run these automatically on each commit please enable the [pre-commit](https://pre-commit.com)
hooks.

[requirements-dev]: https://github.com/Aiven-Open/karapace/blob/master/requirements/requirements-dev.txt

## Manual testing

To use your development code, you need to set up a Kafka server and run Karapace from you
virtual environment:

```
docker compose -f ./container/compose.yml up -d kafka
karapace karapace.config.json
```

### Configuration

To see descriptions of configuration keys see our
[README](https://github.com/Aiven-Open/karapace#configuration-keys).

Each configuration key can be overridden with an environment variable prefixed with `KARAPACE_`,
exception being configuration keys that actually start with the `karapace` string. For example, to
override the `bootstrap_uri` config value, one would use the environment variable
`KARAPACE_BOOTSTRAP_URI`.

# Opening a PR

- Commit messages should describe the changes, not the filenames. Win our admiration by following
  the [excellent advice from Chris Beams](https://chris.beams.io/posts/git-commit/) when composing
  commit messages.
- Choose a meaningful title for your pull request.
- The pull request description should focus on what changed and why.
- Check that the tests pass (and add test coverage for your changes if appropriate).

## Trademarks:
Fedora and the Infinity design logo are trademarks of Red Hat, Inc.
