Performance tests
=================

Install development requirements per instructions from `README.rst <../README.rst>`_

Requires Kafka and Zookeeper running in the containers::
  cd ../container
  docker-compose start zookeeper kafka

Run Karapace from repository root::
  cd ..
  python -m karapace.karapace_all karapace.config.json

Performance test is run from the shell script::
  ./rest-proxy-post-topic-test.sh

Script supports some environment variables:
 * `BASE_URL` for setting Karapace address and port.
 * `TOPIC` for setting the topic name where data is produced.
 * `DURATION` for setting how long the test runs.
 * `CONCURRENCY` for setting how many concurrent users are emulated.
 * `LOCUST_GUI` for enabling the Locust web user interface.
