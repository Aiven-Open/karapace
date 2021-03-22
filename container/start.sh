#!/bin/bash
set -e

# keep in sync with karapace/config.py
KARAPACE_REGISTRY_PORT_DEFAULT=8081
KARAPACE_REGISTRY_HOST_DEFAULT=0.0.0.0
KARAPACE_REGISTRY_CLIENT_ID_DEFAULT=sr-1
KARAPACE_REGISTRY_GROUP_ID_DEFAULT=schema-registry
KARAPACE_REGISTRY_MASTER_ELIGIBITY_DEFAULT=true
KARAPACE_REGISTRY_TOPIC_NAME_DEFAULT=_schemas
KARAPACE_REGISTRY_LOG_LEVEL_DEFAULT=INFO
# Variables without defaults:
# KARAPACE_REGISTRY_ADVERTISED_HOSTNAME
# KARAPACE_REGISTRY_BOOTSTRAP_URI

# keep in sync with karapace/config.py
KARAPACE_REST_PORT_DEFAULT=8082
KARAPACE_REST_ADMIN_METADATA_MAX_AGE_DEFAULT=5
KARAPACE_REST_HOST_DEFAULT=0.0.0.0
KARAPACE_REST_LOG_LEVEL_DEFAULT=INFO
# Variables without defaults:
# KARAPACE_REST_ADVERTISED_HOSTNAME
# KARAPACE_REST_BOOTSTRAP_URI
# KARAPACE_REST_REGISTRY_HOST
# KARAPACE_REST_REGISTRY_PORT

start_karapace_registry(){
  echo "starting karapace schema registry"

  cat >/opt/karapace/registry.config.json <<- EOF
{
    "advertised_hostname": "${KARAPACE_REGISTRY_ADVERTISED_HOSTNAME}",
    "bootstrap_uri": "${KARAPACE_REGISTRY_BOOTSTRAP_URI}",
    "host": "${KARAPACE_REGISTRY_HOST:-$KARAPACE_REGISTRY_HOST_DEFAULT}",
    "port": ${KARAPACE_REGISTRY_PORT:-$KARAPACE_REGISTRY_PORT_DEFAULT},
    "client_id": "${KARAPACE_REGISTRY_CLIENT_ID:-$KARAPACE_REGISTRY_CLIENT_ID_DEFAULT}",
    "group_id": "${KARAPACE_REGISTRY_GROUP_ID:-$KARAPACE_REGISTRY_GROUP_ID_DEFAULT}",
    "master_eligibility": ${KARAPACE_REGISTRY_MASTER_ELIGIBITY:-$KARAPACE_REGISTRY_MASTER_ELIGIBITY_DEFAULT},
    "topic_name": "${KARAPACE_REGISTRY_TOPIC_NAME:-$KARAPACE_REGISTRY_TOPIC_NAME_DEFAULT}",
    "compatibility": "FULL",
    "log_level": "${KARAPACE_REGISTRY_LOG_LEVEL:-$KARAPACE_REGISTRY_LOG_LEVEL_DEFAULT}",
    "replication_factor": 1,
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": null,
    "ssl_certfile": null,
    "ssl_keyfile": null
}
EOF
  exec python3 -m karapace.schema_registry_apis /opt/karapace/registry.config.json
}

start_karapace_rest(){
  echo "starting karapace rest api"

  # in theory we dont need to advertise the internal hostname since this should always be accessible from the outside
  cat >/opt/karapace/rest.config.json <<- EOF
{
    "advertised_hostname": "${KARAPACE_REST_ADVERTISED_HOSTNAME}",
    "bootstrap_uri": "${KARAPACE_REST_BOOTSTRAP_URI}",
    "registry_host": "${KARAPACE_REST_REGISTRY_HOST}",
    "registry_port": ${KARAPACE_REST_REGISTRY_PORT},
    "host": "${KARAPACE_REST_HOST:-$KARAPACE_REST_HOST_DEFAULT}",
    "port": ${KARAPACE_REST_PORT:-$KARAPACE_REST_PORT_DEFAULT},
    "admin_metadata_max_age": ${KARAPACE_REST_ADMIN_METADATA_MAX_AGE:-$KARAPACE_REST_ADMIN_METADATA_MAX_AGE_DEFAULT},
    "log_level": "${KARAPACE_REST_LOG_LEVEL:-$KARAPACE_REST_LOG_LEVEL_DEFAULT}",
    "security_protocol": "PLAINTEXT",
    "ssl_cafile": null,
    "ssl_certfile": null,
    "ssl_keyfile": null
}
EOF
  exec python3 -m karapace.kafka_rest_apis /opt/karapace/rest.config.json
}

case $1 in
  rest)
    start_karapace_rest &
  ;;
  registry)
    start_karapace_registry &
  ;;
  *)
    echo "usage: start-karapace.sh <registry|rest>"
    exit 0
  ;;
esac

wait
