---
title: Compatibility
---

# Compatibility

Karapace is a drop-in replacement for Confluent Schema Registry and the Kafka REST Proxy,
on both the client and server sides.

## Schema Registry API compatibility

Karapace implements the Confluent Schema Registry API and covers the core schema
registration, retrieval, versioning and compatibility workflows. Content negotiation is
compatible: Karapace accepts the same `application/vnd.schemaregistry.v1+json`,
`application/vnd.schemaregistry+json`, `application/json` and `application/octet-stream`
content types, and, like Confluent, versions the API through headers rather than URL
paths.

Common workflows that are supported include:

- Registering, retrieving and listing subjects, versions and schema IDs.
- All compatibility levels (`BACKWARD`, `BACKWARD_TRANSITIVE`, `FORWARD`,
  `FORWARD_TRANSITIVE`, `FULL`, `FULL_TRANSITIVE`, `NONE`), set globally or per subject.
- Soft and hard deletes of subjects and versions, listing deleted subjects
  (`?deleted=true`), and reference-protection on delete.
- HTTP basic auth and OAuth2 / OIDC bearer tokens (see
  [Authentication](./authentication.md)).

The transitive modes (`BACKWARD_TRANSITIVE`, `FORWARD_TRANSITIVE`, `FULL_TRANSITIVE`) check
a new schema not only against the latest schema but also against all previous versions. See
the [API examples](./api-examples.md) for setting compatibility.

Some of the more advanced or newer Confluent Schema Registry features are not covered. If
you are migrating an existing deployment, review the features you depend on against your
Karapace version.

## REST Proxy API compatibility

The Karapace REST proxy implements Confluent's REST Proxy API **v1 and v2**, with **v2 as
the default** — Karapace is Confluent REST Proxy **v2 compatible**.

## Supported schema formats

- **Avro** — full support.
- **JSON Schema** — Draft 7.
- **Protobuf** — full support.

## Schema normalization

When requested with `?normalize=true` on the register and compatibility endpoints,
Karapace stores schemas in a canonical form so that semantically equivalent schemas are
treated as equal. Normalization is currently supported for **Protobuf** schemas only and
covers:

- Ordering of optional fields in the schema.
- Restoring `map<K, V>` shorthand for binary-registered schemas that contain the expanded
  entry-message form.

Karapace does not implement every normalization feature of Confluent Schema Registry.
Treat normalization as a feature that will be extended over time.

## Migrating from Confluent Schema Registry

Karapace supports two key formats for the internal `_schemas` topic: `CANONICAL`, which
matches Confluent's format, and a legacy `DEPRECATED_KARAPACE` format. Use the `CANONICAL`
format when migrating so the topic lines up with Confluent's.

When planning a migration, review the schema formats and features your applications depend
on — including any reliance on specific schema IDs — against your Karapace version.
