---
title: API examples
---

# API examples

These examples assume the Schema Registry is reachable at `http://localhost:8081` and
the REST proxy at `http://localhost:8082`. Adjust the hosts to match your deployment.

## Schema Registry

Register the first version of a schema under the subject `test-key` using an Avro schema:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
  http://localhost:8081/subjects/test-key/versions
# Response:
# {"id":1}
```

Register a version of a schema using JSON Schema — set the `schemaType` property:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schemaType": "JSON", "schema": "{\"type\": \"object\",\"properties\":{\"age\":{\"type\": \"number\"}},\"additionalProperties\":true}"}' \
  http://localhost:8081/subjects/test-key-json-schema/versions
# Response:
# {"id":2}
```

List all subjects:

```bash
curl -X GET http://localhost:8081/subjects
# Response:
# ["test-key"]
```

List all versions of a given subject:

```bash
curl -X GET http://localhost:8081/subjects/test-key/versions
# Response:
# [1]
```

Fetch the schema whose global id is 1:

```bash
curl -X GET http://localhost:8081/schemas/ids/1
# Response:
# {"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}
```

Get version 1 of the schema:

```bash
curl -X GET http://localhost:8081/subjects/test-key/versions/1
```

Get the latest version of the schema under subject `test-key`:

```bash
curl -X GET http://localhost:8081/subjects/test-key/versions/latest
```

Delete version 10 of the schema registered under subject `test-key` (if it exists):

```bash
curl -X DELETE http://localhost:8081/subjects/test-key/versions/10
# Response:
# 10
```

Delete all versions of the schema registered under subject `test-key`:

```bash
curl -X DELETE http://localhost:8081/subjects/test-key
# Response:
# [1]
```

Test the compatibility of a schema with the latest schema under subject `test-key`:

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"int\"}"}' \
  http://localhost:8081/compatibility/subjects/test-key/versions/latest
# Response:
# {"is_compatible":true}
```

:::note
If the subject's compatibility mode is transitive (`BACKWARD_TRANSITIVE`,
`FORWARD_TRANSITIVE` or `FULL_TRANSITIVE`), compatibility is checked not only against the
latest schema but also against all previous schemas.
:::

Get the current global backwards compatibility setting:

```bash
curl -X GET http://localhost:8081/config
# Response:
# {"compatibilityLevel":"BACKWARD"}
```

Change compatibility requirements for all subjects where it is not otherwise defined:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "NONE"}' http://localhost:8081/config
# Response:
# {"compatibility":"NONE"}
```

Change compatibility requirement to `FULL` for the `test-key` subject:

```bash
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"compatibility": "FULL"}' http://localhost:8081/config/test-key
# Response:
# {"compatibility":"FULL"}
```

## REST Proxy

List topics:

```bash
curl "http://localhost:8082/topics"
```

Get info for one particular topic:

```bash
curl "http://localhost:8082/topics/my_topic"
```

Produce a message backed by the schema registry:

```bash
curl -H "Content-Type: application/vnd.kafka.avro.v2+json" -X POST -d \
  '{"value_schema": "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"simple\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "name0"}}]}' \
  http://localhost:8082/topics/my_topic
```

A record may carry optional Kafka message `headers`, a list of `{"name": <string>, "value": <base64 string | null>}` objects (header values are raw bytes, so they are base64-encoded):

```bash
curl -H "Content-Type: application/vnd.kafka.json.v2+json" -X POST -d \
  '{"records": [{"value": {"name": "name0"}, "headers": [{"name": "traceId", "value": "YWJjMTIz"}]}]}' \
  http://localhost:8082/topics/my_topic
```

Create a consumer with consumer group `avro_consumers` and instance `my_consumer`:

```bash
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
  --data '{"name": "my_consumer", "format": "avro", "auto.offset.reset": "earliest"}' \
  http://localhost:8082/consumers/avro_consumers
```

Subscribe to the topic:

```bash
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["my_topic"]}' \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer/subscription
```

Consume previously produced messages:

```bash
curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer/records?timeout=1000
```

When a consumed record has headers, they are returned in a `headers` field using the same `{"name": ..., "value": <base64 string | null>}` form (the field is omitted for records without headers).

Commit offsets for a topic partition:

```bash
curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{}' \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer/offsets
```

Delete the consumer:

```bash
curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
  http://localhost:8082/consumers/avro_consumers/instances/my_consumer
```
