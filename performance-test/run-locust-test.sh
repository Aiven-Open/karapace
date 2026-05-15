#!/usr/bin/env bash
set -Eeuo pipefail

# The target address params
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

BASE_URL=${BASE_URL:-http://localhost:8082}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-localhost:9092}
TOPIC=${TOPIC:-test-topic}
KEYCLOAK_URL=${KEYCLOAK_URL:-http://keycloak:8080}
KEYCLOAK_CONNECT_URL=${KEYCLOAK_CONNECT_URL:-http://localhost:8383}
OIDC_TOKEN_SCRIPT=${OIDC_TOKEN_SCRIPT:-"${SCRIPT_DIR}/../bin/get_oidc_token.py"}
AUTO_GET_SCHEMA_REGISTRY_OIDC_TOKEN=${AUTO_GET_SCHEMA_REGISTRY_OIDC_TOKEN:-0}

DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-50}
LOCUST_FILE=${LOCUST_FILE:-"rest-proxy-produce-consume-test.py"}

GUI_PARAMS=""
if [[ -n ${LOCUST_GUI-} ]]; then
    GUI_PARAMS="--autostart"
fi

export BOOTSTRAP_SERVER

if [[ ${AUTO_GET_SCHEMA_REGISTRY_OIDC_TOKEN} == "1" ]] && [[ -z ${SCHEMA_REGISTRY_OIDC_TOKEN-} ]] && [[ ${LOCUST_FILE} == *"schema-registry"* ]]; then
    SCHEMA_REGISTRY_OIDC_TOKEN=$(KEYCLOAK_URL="${KEYCLOAK_URL}" KEYCLOAK_CONNECT_URL="${KEYCLOAK_CONNECT_URL}" "${OIDC_TOKEN_SCRIPT}")
    export SCHEMA_REGISTRY_OIDC_TOKEN
    echo "Schema registry Locust test using OIDC bearer token"
fi

locust ${GUI_PARAMS:+${GUI_PARAMS}} \
    --run-time "${DURATION}" \
    --users "${CONCURRENCY}" \
    -H "${BASE_URL}" \
    --spawn-rate 0.50 \
    --stop-timeout 5 \
    --locustfile "${LOCUST_FILE}"
