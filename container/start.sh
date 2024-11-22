#!/usr/bin/env bash
set -Eeuo pipefail

# Configuration is done using environment variables. The environment variable
# names are the same as the configuration keys, all letters in caps, and always
# start with `KARAPACE_`.

# In the code below the expression ${var+isset} is used to check if the
# variable was defined, and ${var-isunset} if not.
#
# Ref: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/V3_chap02.html#tag_18_06_02

case $1 in
rest)
    # Reexport variables for compatibility
    [[ -n ${KARAPACE_REST_ADVERTISED_HOSTNAME+isset} ]] && export KARAPACE_ADVERTISED_HOSTNAME="${KARAPACE_REST_ADVERTISED_HOSTNAME}"
    [[ -n ${KARAPACE_REST_BOOTSTRAP_URI+isset} ]] && export KARAPACE_BOOTSTRAP_URI="${KARAPACE_REST_BOOTSTRAP_URI}"
    [[ -n ${KARAPACE_REST_REGISTRY_HOST+isset} ]] && export KARAPACE_REGISTRY_HOST="${KARAPACE_REST_REGISTRY_HOST}"
    [[ -n ${KARAPACE_REST_REGISTRY_PORT+isset} ]] && export KARAPACE_REGISTRY_PORT="${KARAPACE_REST_REGISTRY_PORT}"
    [[ -n ${KARAPACE_REST_HOST+isset} ]] && export KARAPACE_HOST="${KARAPACE_REST_HOST}"
    [[ -n ${KARAPACE_REST_PORT+isset} ]] && export KARAPACE_PORT="${KARAPACE_REST_PORT}"
    [[ -n ${KARAPACE_REST_ADMIN_METADATA_MAX_AGE+isset} ]] && export KARAPACE_ADMIN_METADATA_MAX_AGE="${KARAPACE_REST_ADMIN_METADATA_MAX_AGE}"
    [[ -n ${KARAPACE_REST_LOG_LEVEL+isset} ]] && export KARAPACE_LOG_LEVEL="${KARAPACE_REST_LOG_LEVEL}"
    export KARAPACE_REST=1
    echo "{}" >/opt/karapace/rest.config.json

    echo "Starting Karapace REST API"
    exec python3 -m karapace.karapace_all /opt/karapace/rest.config.json
    ;;
registry)
    echo "Starting Karapace Schema Registry"
    exec python3 -m schema_registry
    ;;
*)
    echo "usage: start-karapace.sh <registry|rest>"
    exit 0
    ;;
esac

wait
