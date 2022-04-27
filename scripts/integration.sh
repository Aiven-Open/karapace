#!/bin/bash

set -ex

# TODO: figure out a smart way to build this only on demand
# podman build --no-cache --tag karapace:integrationtest --file Containerfile.tests
  
POD=$(podman pod create --hostname=test)
trap "podman pod stop $POD && podman pod rm $POD --force" EXIT

podman run --pull missing --pod $POD -d --env-file scripts/zookeeper.env docker.io/confluentinc/cp-zookeeper
podman run --pull missing --pod $POD -d --env-file scripts/kafka.env docker.io/confluentinc/cp-kafka
podman run --pull missing --pod $POD docker.io/jwilder/dockerize dockerize -wait "tcp://0.0.0.0:9092" -timeout 2m
podman run --pod $POD --tty karapace:integrationtest python3 -m pytest -sx -vvv -n 2 tests/integration --kafka-bootstrap-servers=test:9092

