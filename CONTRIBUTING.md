# Welcome!

Contributions are very welcome on Karapace. When contributing please keep this in mind:

- Open an issue to discuss new bigger features.
- Write code consistent with the project style and make sure the tests are passing.
- Stay in touch with us if we have follow up questions or requests for further changes.

# Development

## Local Environment

There is very little you need to get started coding for Karapace:

- Use [one of the supported python versions](https://github.com/aiven/karapace/blob/master/setup.py)
  documented in the `setup.py` classifiers.
- Create a local virtual env, then install karapace and the dev dependencies in it:

```python
python -m venv <path_to_venv>
source <path_to_venv>/bin/activate
pip install -r ./requirements-dev.txt
pip install -e .
```

## Tests

Tests are written with the [pytest](https://docs.pytest.org/) framework, and All PRs are test for
each supported python version using [GitHub Flow](https://guides.github.com/introduction/flow/).

There are two flavors of tests, unit tests and integration tests:

- Unit: These are faster and very useful for quick iterations. They are usually testing pure
  functions.
- Integration: Are slower but more complete. These tests run Karapace, ZooKeeper, and Kafka servers,
  pytest's fixtures are used to start/stop these for you.

Both flavors run in parallel using [pytest-xdist](https://github.com/pytest-dev/pytest-xdist). New
tests should be engineered with this in mind:

- Don't reuse schema/subject/topic names
- Expect other clients to be interacting with the servers at the same time.

To run the tests use the Makefile, it will download Kafka to be used in the tests for you:

```sh
make unittest
make integrationtest
```

### PyCharm

If you want to run the tests from within the IDE, first download Kafka using `make fetch-kafka`, and
make use the project root as for working directory.

### Compatibility tests

The integration tests can be configured to use an external REST (:code:`--rest-url`) or Registry
(:code:`--registry-url`) service. This is be used to make sure our tests conform to the Kafka REST
or Schema Registry APIs, and then that Karapace conform to the tests:

```sh
docker-compose -f ./tests/integration/confluent-docker-compose.yml up -d
pytest --kafka-bootstrap-servers localhost:9092 --registry-url http://locahost:8081 --rest-url http://localhost:8082/ tests/integration
```

## Static checking and Linting

The code is statically checked and formatted using [a few
tools](https://github.com/aiven/karapace/blob/master/requirements-dev.txt). To run these
automatically on before each commit please enable the [pre-commit](https://pre-commit.com) hooks.
Alternatively you can run it manually with `make pre-commit`.

## Manual testing

To use your development code, you just need to set up a Kafka server and run Karapace from you
virtual environment:

```
docker-compose -f ./container/docker-compose.yml up -d kafka
karapace karapace.config.json
```

### Configuration

To see descriptions of configuration keys see our
[README](https://github.com/aiven/karapace#configuration-keys).

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
