Quickstart examples
===================

Schema Hub
----------

To register the first version of a schema under the subject "test" using Avro schema::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
    $KARAPACE_REGISTRY_URI/subjects/test-key/versions
  {"id":1}

To register a version of a schema using JSON Schema, one needs to use `schemaType` property::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schemaType": "JSON", "schema": "{\"type\": \"object\",\"properties\":{\"age\":{\"type\": \"number\"}},\"additionalProperties\":true}"}' \
    $KARAPACE_REGISTRY_URI/subjects/test-key-json-schema/versions
  {"id":2}

To list all subjects (including the one created just above)::

  $ curl -X GET $KARAPACE_REGISTRY_URI/subjects
  ["test-key"]

To list all the versions of a given schema (including the one just created above)::

  $ curl -X GET $KARAPACE_REGISTRY_URI/subjects/test-key/versions
  [1]

To fetch back the schema whose global id is 1 (i.e. the one registered above)::

  $ curl -X GET $KARAPACE_REGISTRY_URI/schemas/ids/1
  {"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

To get the specific version 1 of the schema just registered run::

  $ curl -X GET $KARAPACE_REGISTRY_URI/subjects/test-key/versions/1
  {"subject":"test-key","version":1,"id":1,"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

To get the latest version of the schema under subject test-key run::

  $ curl -X GET $KARAPACE_REGISTRY_URI/subjects/test-key/versions/latest
  {"subject":"test-key","version":1,"id":1,"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

In order to delete version 10 of the schema registered under subject "test-key" (if it exists)::

  $ curl -X DELETE $KARAPACE_REGISTRY_URI/subjects/test-key/versions/10
   10

To Delete all versions of the schema registered under subject "test-key"::

  $ curl -X DELETE $KARAPACE_REGISTRY_URI/subjects/test-key
  [1]

Test the compatibility of a schema with the latest schema under subject "test-key"::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"int\"}"}' \
    $KARAPACE_REGISTRY_URI/compatibility/subjects/test-key/versions/latest
  {"is_compatible":true}

Get current global backwards compatibility setting value::

  $ curl -X GET $KARAPACE_REGISTRY_URI/config
  {"compatibilityLevel":"BACKWARD"}

Change compatibility requirements for all subjects where it's not
specifically defined otherwise::

  $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}' $KARAPACE_REGISTRY_URI/config
  {"compatibility":"NONE"}

Change compatibility requirement to FULL for the test-key subject::

  $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "FULL"}' $KARAPACE_REGISTRY_URI/config/test-key
  {"compatibility":"FULL"}


REST API 
---------

List topics::

  $ curl "$KARAPACE_REST_URI/topics"

Get info for one particular topic::

  $ curl "$KARAPACE_REST_URI/topics/my_topic"

Produce a message backed up by schema registry::

  $ curl -H "Content-Type: application/vnd.kafka.avro.v2+json" -X POST -d \
    '{"value_schema": "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"simple\", \"fields\": \
    [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "name0"}}]}' $KARAPACE_REST_URI/topics/my_topic

Create a consumer::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
    --data '{"name": "my_consumer", "format": "avro", "auto.offset.reset": "earliest"}' \
    $KARAPACE_REST_URI/consumers/avro_consumers

Subscribe to the topic we previously published to::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["my_topic"]}' \
    $KARAPACE_REST_URI/consumers/avro_consumers/instances/my_consumer/subscription

Consume previously published message::

  $ curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
    $KARAPACE_REST_URI/consumers/avro_consumers/instances/my_consumer/records?timeout=1000

Commit offsets for a particular topic partition::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{}' \
    $KARAPACE_REST_URI/consumers/avro_consumers/instances/my_consumer/offsets

Delete consumer::

  $ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
    $KARAPACE_REST_URI/consumers/avro_consumers/instances/my_consumer

