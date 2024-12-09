#!/usr/bin/env bash

retries=5

for ((i = 0; i <= retries; i++)); do
    response=$(
        curl --silent --verbose --fail --request POST \
            --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
            --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
            "http://localhost:$KARAPACE_PORT/subjects/test-key/versions"
    )

    if [[ $response == '{"id":1}' ]]; then
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
