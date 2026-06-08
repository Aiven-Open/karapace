ifneq ($(wildcard .env),)
	include .env
endif

VENV_DIR ?= $(CURDIR)/venv
PYTEST_ARGS ?=
OIDC_PROVIDER ?= keycloak
DOCKER_COMPOSE ?= docker compose
DOCKER_COMPOSE_STANDARD ?= $(DOCKER_COMPOSE) -f container/compose.yml
DOCKER_COMPOSE_AUTH ?= $(DOCKER_COMPOSE) -f container/compose.yml -f container/compose-auth.yml --profile auth
DOCKER_COMPOSE_AUTH_PINGFEDERATE ?= $(DOCKER_COMPOSE) -f container/compose.yml -f container/compose-auth.yml --profile auth --profile pingfederate
DOCKER_CONTAINERS ?= \
	karapace-cli \
	karapace-schema-registry \
	karapace-schema-registry-follower \
	karapace-rest-proxy \
	kafka \
	prometheus \
	grafana \
	statsd-exporter \
	opentelemetry-collector \
	jaeger \
	karapace-schema-registry-authn-only \
	karapace-rest-proxy-oidc \
	karapace-rest-proxy-no-forward \
	karapace-schema-registry-basic \
	karapace-rest-proxy-basic \
	keycloak \
	pingfederate
PIP      ?= pip3 --disable-pip-version-check --no-input --require-virtualenv
PYTHON   ?= python3
PYTHON_VERSION ?= 3.12
KARAPACE_VERSION ?= 5.0.3
RUNNER_UID ?=
RUNNER_GID ?=
COVERAGE_FILE ?= .coverage.${PYTHON_VERSION}
KARAPACE_CLI   ?= $(DOCKER_COMPOSE_STANDARD) run --rm karapace-cli
KARAPACE_CLI_EXEC ?= $(DOCKER_COMPOSE_STANDARD) exec -T karapace-cli
CERTS_FOLDER ?= /opt/karapace/certs

# Export variables needed by docker compose
export PYTHON_VERSION KARAPACE_VERSION RUNNER_UID RUNNER_GID COVERAGE_FILE
export OIDC_JWKS_ENDPOINT_URL OIDC_ALLOW_INSECURE_JWKS OIDC_EXPECTED_ISSUER OIDC_EXPECTED_AUDIENCE OIDC_SUB_CLAIM_NAME OIDC_CLIENT_ID OIDC_ROLES_CLAIM_PATH OIDC_METHOD_ROLES
export OIDC_PROVIDER OIDC_TOKEN_URL OIDC_CLIENT_SECRET OIDC_SCOPE OIDC_VERIFY_TLS OIDC_REALM KEYCLOAK_URL
export PING_IDENTITY_DEVOPS_USER PING_IDENTITY_DEVOPS_KEY

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


.PHONY: tests
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
	$(PIP) install --upgrade pip setuptools pip-tools
	$(PIP) install .[dev,typing]

.PHONY: pin-requirements
pin-requirements:
	docker run -e CUSTOM_COMPILE_COMMAND='make pin-requirements' -t -v "$(CURDIR):/karapace" --security-opt label=disable python:$(PYTHON_VERSION)-bookworm /bin/bash -c "$(PIN_VERSIONS_COMMAND)"

.PHONY: stop-karapace-docker-resources
stop-karapace-docker-resources:
	$(DOCKER_COMPOSE_STANDARD) down -v --remove-orphans || true
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) down -v --remove-orphans || true
	docker rm -f $(DOCKER_CONTAINERS) >/dev/null 2>&1 || true

.PHONY: prepare-docker-resources
prepare-docker-resources:
	touch .coverage.${PYTHON_VERSION} || sudo touch .coverage.${PYTHON_VERSION}
	chown ${RUNNER_UID}:${RUNNER_GID} .coverage.${PYTHON_VERSION} 2>/dev/null || sudo chown ${RUNNER_UID}:${RUNNER_GID} .coverage.${PYTHON_VERSION}
	mkdir -p test-tmp.${PYTHON_VERSION} || sudo mkdir -p test-tmp.${PYTHON_VERSION}
	chown -R ${RUNNER_UID}:${RUNNER_GID} test-tmp.${PYTHON_VERSION} 2>/dev/null || sudo chown -R ${RUNNER_UID}:${RUNNER_GID} test-tmp.${PYTHON_VERSION}

.PHONY: start-karapace-docker-resources
start-karapace-docker-resources: prepare-docker-resources
	$(DOCKER_COMPOSE_STANDARD) up -d --build --wait --detach

.PHONY: start-karapace-docker-auth-resources
start-karapace-docker-auth-resources: prepare-docker-resources
	$(DOCKER_COMPOSE_AUTH) up -d --build --wait

.PHONY: start-karapace-docker-auth-pingfederate-resources
start-karapace-docker-auth-pingfederate-resources: prepare-docker-resources
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) up -d --build --wait

.PHONY: provision-pingfederate-oidc
provision-pingfederate-oidc: start-karapace-docker-auth-pingfederate-resources
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) exec -T karapace-cli $(PYTHON) /opt/karapace/bin/oidc/provision_pingfederate_oidc.py

.PHONY: print-keycloak-oidc-token
print-keycloak-oidc-token: start-karapace-docker-auth-resources
	$(DOCKER_COMPOSE_AUTH) exec -T karapace-cli env -u OIDC_CLIENT_SECRET \
		OIDC_PROVIDER="keycloak" \
		KEYCLOAK_URL="http://keycloak:8080" \
		OIDC_REALM="karapace" \
		OIDC_CLIENT_ID="karapace-client" \
		OIDC_TOKEN_URL="http://keycloak:8080/realms/karapace/protocol/openid-connect/token" \
		OIDC_SCOPE="openid" \
		OIDC_VERIFY_TLS="true" \
		$(PYTHON) /opt/karapace/bin/oidc/get_oidc_token.py

.PHONY: print-pingfederate-oidc-token
print-pingfederate-oidc-token: start-karapace-docker-auth-pingfederate-resources
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) exec -T karapace-cli env \
		PINGFEDERATE_CLIENT_ID_CLAIM="client_id" \
		$(PYTHON) /opt/karapace/bin/oidc/provision_pingfederate_oidc.py >/dev/null
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) exec -T karapace-cli env \
		OIDC_PROVIDER="pingfederate" \
		OIDC_SUB_CLAIM_NAME="client_id" \
		OIDC_CLIENT_ID="karapace-client" \
		OIDC_CLIENT_SECRET="karapace-secret" \
		OIDC_TOKEN_URL="https://pingfederate:9031/as/token.oauth2" \
		OIDC_SCOPE="openid" \
		OIDC_VERIFY_TLS="false" \
		$(PYTHON) /opt/karapace/bin/oidc/get_oidc_token.py

.PHONY: smoke-test-schema-registry
smoke-test-schema-registry: stop-karapace-docker-resources start-karapace-docker-auth-resources
	$(DOCKER_COMPOSE_AUTH) exec -T karapace-cli /opt/karapace/bin/smoke-test-schema-registry.sh

.PHONY: smoke-test-rest-proxy
smoke-test-rest-proxy: stop-karapace-docker-resources start-karapace-docker-resources
	$(KARAPACE_CLI_EXEC) /opt/karapace/bin/smoke-test-rest-proxy.sh

.PHONY: unit-tests-in-docker
unit-tests-in-docker: start-karapace-docker-resources
	rm -fr runtime/*
	$(KARAPACE_CLI_EXEC) $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/unit/
	rm -fr runtime/*

.PHONY: e2e-tests-in-docker
e2e-tests-in-docker: stop-karapace-docker-resources
	$(DOCKER_COMPOSE_AUTH) up -d --build --wait
	rm -fr runtime/*
	sleep 10
	$(DOCKER_COMPOSE_AUTH) exec -T karapace-cli $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/e2e/
	rm -fr runtime/*

.PHONY: e2e-tests-in-docker-keycloak
e2e-tests-in-docker-keycloak: export OIDC_PROVIDER=keycloak
e2e-tests-in-docker-keycloak: e2e-tests-in-docker

.PHONY: e2e-tests-in-docker-pingfederate
e2e-tests-in-docker-pingfederate: export OIDC_PROVIDER=pingfederate
e2e-tests-in-docker-pingfederate: stop-karapace-docker-resources
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) up -d --build --wait
	rm -fr runtime/*
	sleep 10
	$(DOCKER_COMPOSE_AUTH_PINGFEDERATE) exec -T karapace-cli $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/e2e/
	rm -fr runtime/*

.PHONY: integration-tests-in-docker
integration-tests-in-docker: start-karapace-docker-resources
	rm -fr runtime/*
	sleep 10
	$(KARAPACE_CLI_EXEC) $(PYTHON) -m pytest -s -vvv $(PYTEST_ARGS) tests/integration/
	rm -fr runtime/*

.PHONY: type-check-mypy-in-docker
type-check-mypy-in-docker: start-karapace-docker-resources
	$(KARAPACE_CLI_EXEC) $(PYTHON) -m mypy src/karapace

.PHONY: cli
cli: start-karapace-docker-resources
	$(KARAPACE_CLI) bash

.PHONY: generate-sr-https-certs
generate-sr-https-certs:
	$(info ====> Generating self-signed certificates <====)
	$(KARAPACE_CLI) mkcert -key-file $(CERTS_FOLDER)/key.pem -cert-file $(CERTS_FOLDER)/cert.pem \
		localhost \
		127.0.0.1 \
		0.0.0.0 \
		::1 \
		karapace-schema-registry \
		karapace-schema-registry-follower
	$(KARAPACE_CLI) mkcert -install

.PHONY:  curl-sr-https
curl-sr-https: header ?= 'Content-Type: application/vnd.schemaregistry.v1+json'
curl-sr-https:
	$(info ====> Sending HTTPS $(method) request with data to $(url) <====)
	$(KARAPACE_CLI) curl -i -X $(method) --location $(url) --cacert $(CERTS_FOLDER)/ca/rootCA.pem \
		--header $(header) \
		--data '$(data)'
