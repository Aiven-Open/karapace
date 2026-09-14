---
title: Install Karapace
---

# Install Karapace

Karapace requires **Python 3.12+**.

## Using Docker

To get up and running with the latest build of Karapace, a Docker image is available:

```bash
# Fetch the latest build from the main branch
docker pull ghcr.io/aiven-open/karapace:develop

# Fetch the latest release
docker pull ghcr.io/aiven-open/karapace:latest
```

Versions `3.7.1` and earlier are available from the `ghcr.io/aiven` registry:

```bash
docker pull ghcr.io/aiven/karapace:3.7.1
```

An example setup including configuration and a Kafka connection is available as a
compose example:

```bash
docker compose -f ./container/compose.yml up -d
```

You should then be able to reach two sets of endpoints:

- Karapace Schema Registry on `http://localhost:8081`
- Karapace REST on `http://localhost:8082`

## Source install

You can do a source install using:

```bash
pip install .
```

:::note Troubleshooting
An updated version of [wheel](https://pypi.org/project/wheel/) is required, along with
updated versions of `go` and `rust`. Create and activate a virtual environment (venv) to
manage dependencies.
:::

## Run

Make sure Kafka is running first.

Start the Schema Registry. This starts Karapace on `http://localhost:8081`:

```bash
python -m karapace
```

Verify in your browser — `http://localhost:8081/subjects` returns an array of subjects
if any exist, or an empty array. Or with curl:

```bash
curl -X GET http://localhost:8081/subjects
```

Start the REST proxy. This starts Karapace on `http://localhost:8082`:

```bash
python -m karapace.kafka_rest_apis
```

Verify by listing topics:

```bash
curl "http://localhost:8082/topics"
```

## Next steps

- [Configure Karapace](./configuration.md) with environment variables or a config file.
- Enable [authentication and authorization](./authentication.md).
- Try the [API examples](./api-examples.md).
