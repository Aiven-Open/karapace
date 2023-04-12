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
venv/.deps: requirements-dev.txt requirements.txt | venv/.make
	set +x
	source ./bin/get-java
	source ./bin/get-protoc
	source ./bin/get-snappy
	set -x
	$(PIP) install -r '$(<)' --use-pep517
	touch '$(@)'

.PHONY: version
version: karapace/version.py
karapace/version.py: version.py | venv/.make
	$(PYTHON) '$(<)' '$(@)'

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

BUF := buf

ifeq (, $(shell which $(BUF)))
	BUF := docker run --volume "$$(pwd):/workspace" --workdir /workspace bufbuild/buf
endif

.PHONY: proto
protoc:
	$(BUF) generate
	$(BUF) format -w
	$(BUF) lint

.PHONY: proto-breaking
proto-breaking:
	$(BUF) breaking --against '.git#branch=main'
