SHELL := /usr/bin/env bash

VENV_DIR ?= $(CURDIR)/venv
PIP      ?= pip3 --disable-pip-version-check --no-input --require-virtualenv
PYTHON   ?= python3
PYTHON_VERSION ?= 3.10
DOCKER_COMPOSE ?= docker compose
KARAPACE-CLI   ?= $(DOCKER_COMPOSE) -f container/compose.yml run --rm karapace-cli

define PIN_VERSIONS_COMMAND
pip install pip-tools && \
	python -m piptools compile --upgrade -o /karapace/requirements/requirements.txt /karapace/pyproject.toml && \
	python -m piptools compile --upgrade --extra dev -o /karapace/requirements/requirements-dev.txt /karapace/pyproject.toml && \
	python -m piptools compile --upgrade --extra typing -o /karapace/requirements/requirements-typing.txt /karapace/pyproject.toml
endef


export PATH   := $(VENV_DIR)/bin:$(PATH)
export PS4    := \e[0m\e[32m==> \e[0m
export LC_ALL := C
MAKEFLAGS     += --warn-undefined-variables
MAKEFLAGS     += --no-builtin-rules
SHELL         := bash
.SHELLFLAGS   := -euxo pipefail -O globstar -c
.SILENT:
.SUFFIXES:

.PHONY: all
all: version

.PHONY: venv
venv: venv/.make
venv/.make:
	rm -fr '$(VENV_DIR)'
	$(PYTHON) -m venv '$(VENV_DIR)'
	$(PIP) install --upgrade pip
	touch '$(@)'

.PHONY: install
install: venv/.deps
venv/.deps: venv/.make
	set +x
	source ./bin/get-java
	source ./bin/get-protoc
	source ./bin/get-snappy
	set -x
	$(PIP) install --use-pep517 .
	$(PIP) check
	touch '$(@)'

.PHONY: install-dev
install-dev: venv/.deps-dev
venv/.deps-dev: venv/.make
	set +x
	source ./bin/get-java
	source ./bin/get-protoc
	source ./bin/get-snappy
	set -x
	$(PIP) install -e .[dev]
	$(PIP) check
	touch '$(@)'


.PHONY: test
tests: unit-tests integration-tests

.PHONY: unit-tests
unit-tests: export PYTEST_ARGS ?=
unit-tests: venv/.deps-dev
	rm -fr runtime/*
	$(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/unit/
	rm -fr runtime/*

.PHONY: integration-tests
integration-tests: export PYTEST_ARGS ?=
integration-tests: venv/.deps-dev
	rm -fr runtime/*
	$(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/integration/
	rm -fr runtime/*

.PHONY: clean
clean:
	rm -fr ./kafka_* ./*.egg-info/ ./dist/ ./karapace/version.py

.PHONY: cleaner
cleaner: clean
	rm -fr ./.*cache*/

.PHONY: cleanest
cleanest: cleaner
	rm -fr '$(VENV_DIR)'

.PHONY: requirements
requirements:
requirements:
	$(PIP) install --upgrade pip setuptools pip-tools
	$(PIP) install .[dev,typing]

.PHONY: schema
schema: against := origin/main
schema:
	$(PYTHON) -m karapace.backup.backends.v3.schema_tool --against=$(against)

.PHONY: pin-requirements
pin-requirements:
	docker run -e CUSTOM_COMPILE_COMMAND='make pin-requirements' -it -v .:/karapace --security-opt label=disable python:$(PYTHON_VERSION)-bullseye /bin/bash -c "$(PIN_VERSIONS_COMMAND)"

.PHONY: start-karapace-docker-resources
start-karapace-docker-resources: export KARAPACE_VERSION ?= 4.1.1.dev44+gac20eeed.d20241205
start-karapace-docker-resources:
	sudo touch .coverage.3.10 .coverage.3.11 .coverage.3.12
	sudo chown ${RUNNER_UID}:${RUNNER_GID} .coverage.3.10 .coverage.3.11 .coverage.3.12
	$(DOCKER_COMPOSE) -f container/compose.yml up -d --build --wait --detach

.PHONY: unit-tests-in-docker
unit-tests-in-docker: export PYTEST_ARGS ?=
unit-tests-in-docker: start-karapace-docker-resources
	rm -fr runtime/*
	$(KARAPACE-CLI) $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/unit/
	rm -fr runtime/*

.PHONY: e2e-tests-in-docker
e2e-tests-in-docker: export PYTEST_ARGS ?=
e2e-tests-in-docker: start-karapace-docker-resources
	rm -fr runtime/*
	sleep 10
	$(KARAPACE-CLI) $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/e2e/test_karapace.py
	rm -fr runtime/*

.PHONY: integration-tests-in-docker
integration-tests-in-docker: export PYTEST_ARGS ?=
integration-tests-in-docker: start-karapace-docker-resources
	rm -fr runtime/*
	sleep 10
	$(KARAPACE-CLI) $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/integration/
	rm -fr runtime/*

.PHONY: type-check-mypy-in-docker
type-check-mypy-in-docker: start-karapace-docker-resources
	$(KARAPACE-CLI) $(PYTHON) -m mypy src

.PHONY: cli
cli: start-karapace-docker-resources
	$(KARAPACE-CLI) bash
