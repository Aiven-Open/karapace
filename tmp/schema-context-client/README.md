# Schema context test bench

Test bench for the [schema linking design](../Karapace_schema-linking-design_v2_2026-11-11.md). It checks, against a registry that supports contexts, that a Confluent Java client with a standard configuration (`schema.registry.url` only, no context option) decodes two topics whose messages carry the same schema id, from two different contexts. The same scenario runs against released Karapace, which has no contexts, and against Karapace built from the current branch.

## Components

| Component | Where | Role |
| --- | --- | --- |
| `kafka` | podman, `apache/kafka:4.4.0` (KRaft, single node), `localhost:9092` | Topics `orders`, `A.orders` and `_schemas` |
| `karapace` (profile `karapace`) | podman, `aiven-open/karapace:6.2.4`, `localhost:18081` | Released Karapace, no contexts |
| `karapace-dev` (profile `karapace-dev`) | podman, image built from the repository sources, `localhost:18081` | Karapace of the current branch, with contexts |
| `schema-registry` (profile `confluent`) | podman, `confluentinc/cp-schema-registry:8.4.1` (Community), `localhost:18081` | Reference registry with contexts |
| `client/` | local (Java 21, Maven) | Scenario with `kafka-avro-serializer` 8.4.1 |

Versions can be overridden: `KAFKA_IMAGE_VERSION`, `KARAPACE_IMAGE_VERSION` and `SCHEMA_REGISTRY_IMAGE_VERSION` for the images, `-Dconfluent.version=...` for the client. Align the client with the version used in production: sending `?subject=` on `GET /schemas/ids/{id}` is to be confirmed version by version.

## Choosing the registry

All registries are in `compose.yml`, one profile each, on the same port: the client does not change. The active profile is set by `COMPOSE_PROFILES` in `.env` (`karapace`, `karapace-dev` or `confluent`; `karapace` by default), or for a single command:

```bash
COMPOSE_PROFILES=confluent podman compose up -d
```

`karapace-dev` builds its image from the repository root (`container/Dockerfile`) on the first `up`. After a code change: `COMPOSE_PROFILES=karapace-dev podman compose build karapace-dev`.

Run `podman compose down` before switching registry: it releases port 18081 and starts again from an empty registry. When the variable is set on the command line, pass it to `down` too.

## Running

```bash
cd tmp/schema-context-client

# Empty registry on every run: the id collision is only reproduced on a fresh registry.
podman compose down
podman compose up -d                             # or COMPOSE_PROFILES=confluent podman compose up -d
until curl -sf localhost:18081/subjects >/dev/null; do sleep 2; done

cd client
mvn -q package                                   # or -Dconfluent.version=<production version>
java -jar target/schema-context-client.jar
```

The exit code is 0 on success, 1 otherwise. The main class is `poc.schemalinking.Main` (scenario in `ContextScenario`), which can also be run from the IDE. On Java 25, Guava prints `sun.misc.Unsafe` warnings: run with a JDK 21 or add the JVM option `--sun-misc-unsafe-memory-access=allow`.

## Checking a registry restart

The registry rebuilds its state by replaying `_schemas`. To check that contexts and their counters survive a restart:

```bash
cd client
java -jar target/schema-context-client.jar                                 # 1. full scenario, fresh registry
cd .. && podman compose restart karapace-dev && cd client                  # 2. restart, Kafka and _schemas kept
until curl -sf localhost:18081/subjects >/dev/null; do sleep 2; done
java -Dscenario.consume-only=true -jar target/schema-context-client.jar    # 3. read back only
```

In read-only mode (`scenario.consume-only: true` in `client.yaml`, or `-Dscenario.consume-only=true` without editing the file), the scenario creates no topic, registers no schema and produces nothing. It reads back the last message of `orders` and `A.orders`, then runs the same checks: same ids in both contexts, both topics decoded, same registry responses. Do not run `podman compose down` between steps 1 and 3: it would delete `_schemas`.

## Client configuration

The whole configuration is in `client/src/main/resources/client.yaml`:

| Section | Role |
| --- | --- |
| `scenario` | `consume-only`: read-only mode (see above) |
| `logging` | slf4j-simple system properties (`org.slf4j.simpleLogger.defaultLogLevel: warn`), applied before the first logger |
| `kafka` | Shared values (`bootstrap.servers`), merged into the client sections with `<<: *kafka` |
| `producer-b` | Local producer of B, topic `orders`: `KafkaAvroSerializer`, `schema.registry.url: http://localhost:18081` |
| `producer-a` | Producer of A, topic `A.orders`: `KafkaAvroSerializer`, `schema.registry.url: http://localhost:18081/contexts/.A` |
| `consumer` | Consumer and deserializer: `KafkaAvroDeserializer`, `schema.registry.url`; no context option |

Each client section is passed as is to the Kafka client, with standard keys. A single value is replaced at runtime: the consumer `schema.registry.url` points to the recording proxy, which forwards to the configured URL. The Kafka admin and the id lookups use `bootstrap.servers` and `schema.registry.url` of the `consumer` section.

There is a single registry: `producer-a` puts context `.A` in the URL. Its schemas land where the exporter would copy them on B, with an id allocated by the counter of `.A`, as a separate cluster A would allocate it in its default context.

To try another configuration without rebuilding, copy `client.yaml` and pass it as a JVM option:

```bash
java -Dscenario.config=/path/to/client.yaml -jar target/schema-context-client.jar
```

## What the scenario does

1. Produces one message per topic with `KafkaAvroSerializer` (`auto.register.schemas=true`). Schema Y (`com.example.b.Order`) is registered under `orders-value`, schema X (`com.example.a.Order`) under `:.A:A.orders-value`.
2. Checks the collision: each context has its own counter, so both schemas get the same id.
3. Reads both topics with a single `KafkaAvroDeserializer` configured with `schema.registry.url` only. With `TopicNameStrategy` (the default), the client sends `GET /schemas/ids/1?subject=<topic>-value`, an unqualified subject.
4. The registry finds the context: when an unqualified subject does not hold the id in the default context, the same name is tried in every known context. `A.orders-value` only exists in `.A`, so id 1 is resolved there.
5. A local HTTP proxy between the deserializer and the registry records the requests actually sent.

This relies on the registry, not on the client. It requires subject names that are unique across contexts (replicated topics prefixed with their origin).

## Success criteria

| Criterion | Check |
| --- | --- |
| Standard configuration, unpatched client | The deserializer only has `schema.registry.url` |
| Subject sent on `GET /schemas/ids/{id}` | The proxy sees `subject=orders-value` and `subject=A.orders-value` |
| Two topics, a single deserializer, resolved in the right context | `orders` decoded as `com.example.b.Order`, `A.orders` as `com.example.a.Order`, with the produced values |

A failed step does not stop the scenario: it is reported KO and the scenario goes on with what could be produced. The last section prints the registry responses (`/schemas/ids/{id}` with and without `?subject=`, `/contexts/.A/schemas/ids/{id}`, `/contexts`), to compare between profiles.

## Observed behaviour

| Check | `confluent` (8.4.1) | `karapace` (6.2.4) | `karapace-dev` |
| --- | --- | --- | --- |
| Registration through `…/contexts/.A` | OK, id 1 in `.A` | Fails, no `/contexts` route | OK, id 1 in `.A` |
| Same id 1 in two contexts | OK | Not reproducible | OK |
| `orders` decoded | OK | OK | OK |
| `A.orders` decoded, standard configuration | OK | Not testable, no message | OK |
| `GET /schemas/ids/{id}?subject=` | Resolves the context | Parameter ignored | Resolves the context |

On `karapace-dev`, a context is carried by the qualified subject (`:.A:A.orders-value`), stored as is in `_schemas`: each context has its own id table and counter. `/contexts/{ctx}/subjects|config|mode/{subject}` and `/contexts/{ctx}/schemas/ids/{id}` are rewritten onto the existing routes, and `GET /contexts` lists the contexts.

## Inspecting by hand

```bash
curl -s localhost:18081/contexts
curl -s 'localhost:18081/subjects?subjectPrefix=:*:'
curl -s 'localhost:18081/schemas/ids/1?subject=:.A:A.orders-value'
curl -s 'localhost:18081/schemas/ids/1?subject=A.orders-value'   # lookup by name in other contexts
curl -s localhost:18081/schemas/ids/1
```
