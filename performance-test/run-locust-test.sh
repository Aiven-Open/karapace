#!/usr/bin/env bash
set -Eeuo pipefail

# The REST proxy address params
BASE_URL=${BASE_URL:-http://localhost:8082}
TOPIC=${TOPIC:-test-topic}

DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-50}
LOCUST_FILE=${LOCUST_FILE:-"rest-proxy-produce-consume-test.py"}

GUI_PARAMS=""
if [[ -n ${LOCUST_GUI-} ]]; then
    GUI_PARAMS="--autostart"
fi

locust ${GUI_PARAMS:+${GUI_PARAMS}} \
    --run-time "${DURATION}" \
    --users "${CONCURRENCY}" \
    -H "${BASE_URL}" \
    --spawn-rate 0.50 \
    --stop-timeout 5 \
    --locustfile "${LOCUST_FILE}"
