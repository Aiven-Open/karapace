#!/usr/bin/env bash

retries=5
chmod +x /opt/karapace/bin/get_oidc_token.py
token=$(python3 /opt/karapace/bin/get_oidc_token.py)

for ((i = 0; i <= retries; i++)); do
    response=$(
        curl --silent --verbose --fail --request POST \
            --cacert "$CAROOT/rootCA.pem" \
            --header "Authorization: Bearer $token" \
            --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
            --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
            "https://karapace-schema-registry:8081/subjects/test-key/versions"
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
