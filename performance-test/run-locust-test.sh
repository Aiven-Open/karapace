#!/bin/env bash
set -eo pipefail

# The REST proxy address params
BASE_URL=${BASE_URL:-http://localhost:8082}
TOPIC=${TOPIC:-test-topic}

DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-50}
LOCUST_FILE=${LOCUST_FILE:-"rest-proxy-post-topic-test.py"}

GUI_PARAMS="--headless"
if [ ! -z $LOCUST_GUI ]; then
    GUI_PARAMS="--autostart"
fi

TOPIC="${TOPIC}" locust "${GUI_PARAMS}" \
    --run-time "${DURATION}" \
    --users "${CONCURRENCY}" \
    -H "${BASE_URL}" \
    --spawn-rate 0.50 \
    --stop-timeout 5 \
    --locustfile "${LOCUST_FILE}"
