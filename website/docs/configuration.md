---
title: Configuration
---

# Configuration

Each configuration key can be overridden with an environment variable prefixed with
`KARAPACE_`, the exception being keys that already start with the `karapace` string. For
example, to override the `bootstrap_uri` value, use the environment variable
`KARAPACE_BOOTSTRAP_URI`.

An [example configuration file](https://github.com/Aiven-Open/karapace/blob/main/karapace.config.json)
in the repository gives you an idea of what you can change.

At least one of `karapace_registry` and `karapace_rest` must be enabled for the service to
start.

## Common keys

| Parameter            | Default           | Description                                                                                |
| -------------------- | ----------------- | ------------------------------------------------------------------------------------------ |
| `bootstrap_uri`      | `localhost:9092`  | The Kafka service where schemas are stored and coordination among Karapace instances runs. |
| `host`               | `127.0.0.1`       | Listening host. Use an empty string to listen on all networks.                             |
| `port`               | `8081`            | Listening port for the Karapace server.                                                    |
| `topic_name`         | `_schemas`        | The Kafka topic where schemas are stored.                                                  |
| `group_id`           | `schema-registry` | Kafka group name used to elect a master for storing schemas.                               |
| `client_id`          | `sr-1`            | The client id used when coordinating with other Karapace instances.                        |
| `replication_factor` | `1`               | Replication factor for the schema topic.                                                   |
| `karapace_registry`  | `true`            | Include the registry part of the app in the starting process.                              |
| `karapace_rest`      | `true`            | Include the REST part of the app in the starting process.                                  |
| `log_level`          | `DEBUG`           | Logging level.                                                                             |
| `log_handler`        | `stdout`          | Log handler: `stdout` or `systemd`.                                                        |

## High availability / master election

| Parameter                                 | Default                | Description                                                                                                                         |
| ----------------------------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `advertised_hostname`                     | `socket.gethostname()` | Hostname advertised to other Karapace instances in the same Kafka group.                                                            |
| `advertised_port`                         | `None`                 | Port advertised to other instances. Falls back to `port`.                                                                           |
| `advertised_protocol`                     | `http`                 | Protocol advertised to other instances.                                                                                             |
| `master_eligibility`                      | `true`                 | Whether the instance can be promoted to master.                                                                                     |
| `master_election_strategy`                | `lowest`               | Basis on which the cluster master is chosen.                                                                                        |
| `waiting_time_before_acting_as_master_ms` | `5000`                 | Time a master waits before becoming active. Should be an upper bound of the time to write and consume a message in the Kafka topic. |

## REST proxy

| Parameter                     | Default      | Description                                                                             |
| ----------------------------- | ------------ | --------------------------------------------------------------------------------------- |
| `consumer_enable_autocommit`  | `true`       | Enable auto commit on REST proxy consumers.                                             |
| `consumer_request_max_bytes`  | `67108864`   | Maximum bytes fetched per consumer request.                                             |
| `consumer_request_timeout_ms` | `11000`      | Timeout for consumer reads without their own timeout.                                   |
| `fetch_min_bytes`             | `1`          | Minimum bytes fetched per consumer request.                                             |
| `producer_acks`               | `1`          | Consistency level for each produced message.                                            |
| `producer_compression_type`   | `None`       | Compression used by REST proxy producers.                                               |
| `producer_max_request_size`   | `1048576`    | Maximum size of a request in bytes.                                                     |
| `name_strategy`               | `topic_name` | Name strategy for storing schemas: `topic_name`, `record_name`, or `topic_record_name`. |
| `rest_authorization`          | `false`      | Delegate REST proxy authorization to Kafka over SASL using the caller's credentials.    |
| `rest_base_uri`               | `None`       | Publicly available URI advertised to clients for stateful operations.                   |

## TLS / security

| Parameter                                                          | Default     | Description                                                                                    |
| ------------------------------------------------------------------ | ----------- | ---------------------------------------------------------------------------------------------- |
| `security_protocol`                                                | `PLAINTEXT` | Kafka security protocol: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.                  |
| `sasl_mechanism`                                                   | `None`      | SASL mechanism (`PLAIN`, `SCRAM-SHA-512`, `OAUTHBEARER`).                                      |
| `ssl_cafile` / `ssl_certfile` / `ssl_keyfile`                      | `None`      | SSL CA / cert / key used when `security_protocol` is `SSL`.                                    |
| `server_tls_cafile` / `server_tls_certfile` / `server_tls_keyfile` | `None`      | TLS files for the Karapace server in HTTPS mode.                                               |
| `server_tls_client_auth`                                           | `none`      | Client certificate requirement: `none`, `optional`, or `required`.                             |
| `registry_authfile`                                                | `None`      | Users and access-control rules for HTTP basic auth. See [Authentication](./authentication.md). |

## Protobuf

| Parameter                    | Default   | Description                                                   |
| ---------------------------- | --------- | ------------------------------------------------------------- |
| `protobuf_runtime_directory` | `runtime` | Runtime directory for the `protoc` parser and code generator. |
| `use_protobuf_formatter`     | `false`   | Normalize and persist Protobuf schemas in a formatted state.  |

For the full list of keys, see the
[README](https://github.com/Aiven-Open/karapace/blob/main/README.rst#configuration-keys).
