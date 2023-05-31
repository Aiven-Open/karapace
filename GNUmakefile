SHELL := /usr/bin/env bash

VENV_DIR ?= $(CURDIR)/venv
PIP      ?= pip3 --disable-pip-version-check --no-input --require-virtualenv
PYTHON   ?= python3
ifdef CI
PYENV    ?= $(PYTHON)
else
PYENV    ?= pyenv exec python
endif

PYTHON_VERSION ?= 3.8

export PATH   := $(VENV_DIR)/bin:$(PATH)
export PS4    := \e[0m\e[32m==> \e[0m
export LC_ALL := C
MAKEFLAGS     += --warn-undefined-variables
MAKEFLAGS     += --no-builtin-rules
SHELL         := bash
.SHELLFLAGS   := -euxo pipefail -O globstar -c
.ONESHELL:
.SILENT:
.SUFFIXES:

.PHONY: all
all: version

.PHONY: venv
venv: venv/.make
venv/.make:
	rm -fr '$(VENV_DIR)'
	$(PYENV) -m venv '$(VENV_DIR)'
	$(PIP) install --upgrade pip
	touch '$(@)'

.PHONY: install
install: venv/.deps
venv/.deps: requirements/requirements-dev.txt requirements/requirements.txt | venv/.make
	set +x
	source ./bin/get-java
	source ./bin/get-protoc
	source ./bin/get-snappy
	set -x
	$(PIP) install --use-pep517 -r '$(<)'
	$(PIP) install --use-pep517 .
	$(PIP) check
	touch '$(@)'


karapace/version.py:
	$(PYTHON) version.py

.PHONY: version
version: venv/.make | karapace/version.py

.PHONY: test
tests: unit-tests integration-tests

.PHONY: unit-tests
unit-tests: export PYTEST_ARGS ?=
unit-tests: karapace/version.py venv/.deps
	rm -fr runtime/*
	$(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/unit/
	rm -fr runtime/*

.PHONY: integration-tests
unit-tests: export PYTEST_ARGS ?=
integration-tests: karapace/version.py venv/.deps
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
requirements: export CUSTOM_COMPILE_COMMAND='make requirements'
requirements:
	pip install --upgrade pip setuptools pip-tools
	cd requirements && pip-compile --upgrade --resolver=backtracking requirements.in
	cd requirements && pip-compile --upgrade --resolver=backtracking requirements-dev.in
	cd requirements && pip-compile --upgrade --resolver=backtracking requirements-typing.in

.PHONY: schema
schema: against := origin/main
schema:
	python3 -m karapace.backup.backends.v3.schema_tool --against=$(against)
