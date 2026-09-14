---
slug: /
title: Overview
---

# Karapace

Karapace is a free and Open Source implementation of the Kafka
[Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
and [Kafka REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html).
It is your perfect companion to Apache Kafka®, adding schema handling and an HTTP
interface to your Kafka services.

## What it does

- **Schema Registry** — stores schemas in a central repository that clients use to
  serialize and deserialize messages. Schemas keep their own version histories and can
  be checked for compatibility between versions. Avro, JSON Schema and Protobuf are
  supported.
- **REST Proxy** — a RESTful interface to your Apache Kafka cluster for producing and
  consuming messages and performing administrative work over HTTP.

## Highlights

- Drop-in replacement on both the client and server side of Schema Registry and Kafka
  REST Proxy.
- Implements the Confluent Schema Registry API and is Confluent REST Proxy v2 compatible.
  See [Compatibility](./compatibility.md).
- Leader/replica architecture for high availability and load balancing.
- Asynchronous architecture based on aiohttp; Schema Registry is built on FastAPI.
- Observability with metrics and OpenTelemetry.

## Next steps

- [Install Karapace](./install.md) — Docker or source.
- [API examples](./api-examples.md) — Schema Registry and REST Proxy endpoints.
- [Configuration](./configuration.md) — every configuration key.
- [Authentication & authorization](./authentication.md) — basic auth and OAuth2/OIDC.
