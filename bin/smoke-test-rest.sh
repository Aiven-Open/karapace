#!/usr/bin/env bash

retries=5

for ((i = 0; i <= retries; i++)); do
    response=$(curl --silent --verbose --fail http://localhost:8082/topics)

    if [[ $response == '["_schemas","__consumer_offsets"]' ]]; then
        echo "Ok!"
        break
    fi

    if ((i == retries)); then
        echo "Still failing after $i retries, giving up."
        exit 1
    fi

    echo "Smoke test failed, retrying in 5 seconds ..."
    sleep 5
done
