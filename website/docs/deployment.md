---
title: Deployment
---

# Deployment

## Docker images

Docker images and the compose example are covered on the [Install](./install.md) page.

## High availability

Karapace uses a leader/replica architecture for high availability and load balancing.
Instances coordinate through a Kafka consumer group (`group_id`) and elect a master that
handles writes; the others serve reads and forward writes.

Relevant configuration:

- `advertised_hostname`, `advertised_port`, `advertised_protocol` — how an instance is
  reached by its peers. All nodes must set these so they can reach each other.
- `master_eligibility` — set to `false` to keep an instance from being promoted to master
  (for example, a standby in another location).
- `master_election_strategy` — how the master is chosen.
- `waiting_time_before_acting_as_master_ms` — how long a newly elected master waits before
  acting.

## Running as a service

Once installed, the `karapace` program is on your path. It is the main daemon process and
should be run under a service manager such as `systemd`. Set `log_handler` to `systemd`
when running that way.

## Uninstall

Docker:

```bash
docker ps | grep karapace
docker stop <CONTAINER_ID>
docker rm <CONTAINER_ID>
```

Source install:

```bash
pip uninstall karapace
```
