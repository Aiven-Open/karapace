#!/usr/bin/env bash

set -uo pipefail

retries=5
last_oidc_error=""
chmod +x /opt/karapace/bin/get_oidc_token.py

for ((i = 0; i <= retries; i++)); do
    if ! token=$(/opt/karapace/bin/get_oidc_token.py 2>&1); then
        last_oidc_error="$token"
        if ((i == retries)); then
            echo "Still failing to fetch OIDC token after $i retries, giving up."
            if [[ -n ${last_oidc_error} ]]; then
                printf '%s\n' "${last_oidc_error}" >&2
            fi
            exit 1
        fi

        echo "Fetching OIDC token failed, retrying in 5 seconds ..."
        sleep 5
        continue
    fi

    if curl --silent --show-error --fail --request POST \
        --cacert "$CAROOT/rootCA.pem" \
        --header "Authorization: Bearer $token" \
        --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
        --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
        "https://karapace-schema-registry:8081/subjects/test-key/versions"; then
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
