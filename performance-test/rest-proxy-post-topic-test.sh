#!/bin/env bash
set -eo pipefail

# The REST proxy address params
BASE_URL=${BASE_URL:-http://localhost:8082}
TOPIC=${TOPIC:-test-topic}

DURATION=${DURATION:-1m}
CONCURRENCY=${CONCURRENCY:-50}

GUI_PARAMS="--headless"
if [ ! -z $LOCUST_GUI ]; then
    GUI_PARAMS="--autostart"
fi

TOPIC="${TOPIC}" locust "${GUI_PARAMS}" \
     --run-time "${DURATION}" \
     --users "${CONCURRENCY}" \
     -H "${BASE_URL}" \
     --spawn-rate 4 \
     --stop-timeout 5 \
     --locustfile rest-proxy-post-topic-test.py
