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
COLLECT_DOCKER_STATS=${COLLECT_DOCKER_STATS:-0}
DOCKER_STATS_INTERVAL=${DOCKER_STATS_INTERVAL:-1}
DOCKER_STATS_SCRIPT=${DOCKER_STATS_SCRIPT:-"${SCRIPT_DIR}/collect-docker-stats.sh"}
DOCKER_STATS_CONTAINERS=${DOCKER_STATS_CONTAINERS-}
DOCKER_STATS_POST_RUN_DELAY=${DOCKER_STATS_POST_RUN_DELAY:-10}
CLEAN_RESULTS=${CLEAN_RESULTS:-0}

DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-50}
LOCUST_SPAWN_RATE=${LOCUST_SPAWN_RATE:-10}
LOCUST_FILE=${LOCUST_FILE:-"rest-proxy-produce-consume-test.py"}
LOCUST_GUI=${LOCUST_GUI:-1}
LOCUST_AUTOQUIT_SECONDS=${LOCUST_AUTOQUIT:-${LOCUST_AUTOQUIT_SECONDS:-0}}
LOCUST_STOP_TIMEOUT=${LOCUST_STOP_TIMEOUT:-30}

RESULTS_ROOT=${SCRIPT_DIR}/results
DOCKER_STATS_PID=""
SHUTTING_DOWN=0
DEFAULT_DOCKER_STATS_CONTAINERS=(karapace-rest-proxy karapace-schema-registry karapace-schema-registry-follower kafka keycloak)

GUI_PARAMS=""
HEADLESS_PARAMS="--headless"
if [[ ${LOCUST_GUI} == "1" ]]; then
    GUI_PARAMS="--autostart"
    if [[ ${LOCUST_AUTOQUIT_SECONDS} != "0" ]]; then
        GUI_PARAMS+=" --autoquit ${LOCUST_AUTOQUIT_SECONDS}"
    fi
    HEADLESS_PARAMS=""
fi

export BOOTSTRAP_SERVER
export PYTHONDONTWRITEBYTECODE=1
unset LOCUST_AUTOQUIT
unset LOCUST_AUTOQUIT_SECONDS

if [[ ${AUTO_GET_SCHEMA_REGISTRY_OIDC_TOKEN} == "1" ]] && [[ -z ${SCHEMA_REGISTRY_OIDC_TOKEN-} ]] && [[ ${LOCUST_FILE} == *"schema-registry"* ]]; then
    SCHEMA_REGISTRY_OIDC_TOKEN=$(KEYCLOAK_URL="${KEYCLOAK_URL}" KEYCLOAK_CONNECT_URL="${KEYCLOAK_CONNECT_URL}" "${OIDC_TOKEN_SCRIPT}")
    export SCHEMA_REGISTRY_OIDC_TOKEN
    echo "Schema registry Locust test using OIDC bearer token"
fi

if [[ ${LOCUST_FILE} == *"schema-registry"* ]]; then
    RESULTS_SUBDIR="schema-registry"
else
    RESULTS_SUBDIR="rest-proxy"
fi

if [[ ${CLEAN_RESULTS} == "1" ]]; then
    rm -rf "${RESULTS_ROOT:?}/${RESULTS_SUBDIR:?}"
fi

# shellcheck disable=SC2317
cleanup() {
    if [[ ${SHUTTING_DOWN} == "1" ]]; then
        return
    fi
    SHUTTING_DOWN=1

    if [[ -n ${DOCKER_STATS_PID} ]]; then
        kill "${DOCKER_STATS_PID}" 2>/dev/null || true
        wait "${DOCKER_STATS_PID}" 2>/dev/null || true
        DOCKER_STATS_PID=""
    fi
}

# shellcheck disable=SC2317
handle_signal() {
    cleanup
    exit 130
}

trap cleanup EXIT
trap handle_signal INT TERM

if [[ ${COLLECT_DOCKER_STATS} == "1" ]]; then
    mkdir -p "${RESULTS_ROOT}/${RESULTS_SUBDIR}"
    timestamp=$(date -u +"%Y%m%dT%H%M%SZ")
    docker_stats_file="${RESULTS_ROOT}/${RESULTS_SUBDIR}/docker-stats-${timestamp}.csv"

    if [[ -n ${DOCKER_STATS_CONTAINERS} ]]; then
        read -r -a docker_stats_containers <<<"${DOCKER_STATS_CONTAINERS}"
    else
        docker_stats_containers=("${DEFAULT_DOCKER_STATS_CONTAINERS[@]}")
    fi

    DOCKER_STATS_PARENT_PID=$$ bash "${DOCKER_STATS_SCRIPT}" "${docker_stats_file}" "${DOCKER_STATS_INTERVAL}" "${docker_stats_containers[@]}" &
    DOCKER_STATS_PID=$!
    echo "Collecting Docker stats into ${docker_stats_file}"
fi

LOCUST_EXIT_CODE=0
locust ${GUI_PARAMS:+${GUI_PARAMS}} \
    ${HEADLESS_PARAMS:+${HEADLESS_PARAMS}} \
    --run-time "${DURATION}" \
    --users "${CONCURRENCY}" \
    -H "${BASE_URL}" \
    --spawn-rate "${LOCUST_SPAWN_RATE}" \
    --stop-timeout "${LOCUST_STOP_TIMEOUT}" \
    --locustfile "${LOCUST_FILE}" || LOCUST_EXIT_CODE=$?

if [[ ${COLLECT_DOCKER_STATS} == "1" ]] && [[ ${DOCKER_STATS_POST_RUN_DELAY} != "0" ]]; then
    echo "Waiting ${DOCKER_STATS_POST_RUN_DELAY}s before stopping Docker stats collection"
    sleep "${DOCKER_STATS_POST_RUN_DELAY}"
fi

exit "${LOCUST_EXIT_CODE}"
