Karapace
========

``karapace`` Your Kafka essentials in one tool


Features
========

* Schema Registry and Rest Proxy that are 1:1 Compatible with the pre-existing proprietary
  Confluent Schema Registry and Kafka Rest Proxy
* Drop in replacement both on pre-existing Schema Registry / Kafka Rest Proxy client and
  server-sides
* Moderate memory consumption
* Asynchronous architecture based on aiohttp


Overview
========

Karapace supports the storing of schemas in a central repository, which
clients can access to serialize and deserialize messages.  The schemas also
maintain their own version histories and can be checked for compatibility
between their different respective versions.

Karapace rest provides a RESTful interface to your Kafka cluster, allowing you to perform
tasks such as producing and consuming messages and perform administrative cluster work,
all the while using the language of the WEB.

Setup
=====

Karapace is a Python project, and requires Kafka for its backend storage. There is also a `Docker setup for development`_.

Requirements
------------

Karapace requires Python 3.6 or later and some additional components in
order to operate:

* aiohttp_ for serving schemas over HTTP in an asynchronous fashion
* avro-python3_ for Avro serialization
* kafka-python_ to read, write and coordinate Karapace's persistence in Kafka
* raven-python_ (optional) to report exceptions to sentry
* aiokafka_ for some components of the rest proxy

.. _`aiohttp`: https://github.com/aio-libs/aiohttp
.. _`aiokafka`: https://github.com/aio-libs/aiokafka
.. _`avro-python3`: https://github.com/apache/avro
.. _`kafka-python`: https://github.com/dpkp/kafka-python
.. _`raven-python`: https://github.com/getsentry/raven-python

Developing and testing Karapace also requires the following utilities:
requests_, flake8_, pylint_ and pytest_.

.. _`flake8`: https://flake8.readthedocs.io/
.. _`requests`: http://www.python-requests.org/en/latest/
.. _`pylint`: https://www.pylint.org/
.. _`pytest`: http://pytest.org/

Karapace has been developed and tested on modern Linux x86-64 systems, but
should work on other platforms that provide the required modules.


Building
--------

To build an installation package for your distribution, go to the root
directory of a Karapace Git checkout and run::

  python3 setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.

Installation
------------

Python/Other::

  easy_install dist/karapace-0.1.0-py3.6.egg

On Linux systems it is recommended to simply run ``karapace`` under
``systemd``::

  systemctl enable karapace.service

and eventually after the setup section, you can just run::

  systemctl start karapace.service

Configuration
-------------

After this you need to create a suitable JSON configuration file for your
installation.  Keys to take special care are the ones needed to configure
Kafka and advertised_hostname.

Each configuration key can be overridden with an environment variable prefixed with
``KARAPACE_``, exception being configuration keys that actually start with the ``karapace`` string.
For example, to override the ``bootstrap_uri`` config value, one would use the environment variable
``KARAPACE_BOOTSTRAP_URI``


To see descriptions of configuration keys see section ``config``.  Here's an
example configuration file to give you an idea what you need to change::

  {
      "advertised_hostname": "localhost",
      "bootstrap_uri": "127.0.0.1:9092",
      "client_id": "sr-1",
      "compatibility": "FULL",
      "group_id": "schema-registry",
      "host": "127.0.0.1",
      "log_level": "DEBUG",
      "port": 8081,
      "master_eligibility": true,
      "replication_factor": 1,
      "security_protocol": "PLAINTEXT",
      "ssl_cafile": null,
      "ssl_certfile": null,
      "ssl_keyfile": null,
      "topic_name": "_schemas"
  }

Local Development
-----------------

Currently Karapace runs on the Python major versions 3.7, 3.8 and 3.9. You can use any of these for development.
Naturally, independently of Python version you use, the code needs to run on all the supported versions.
The CI pipeline in GitHub actions will run the tests on all these Python versions to ensure this.

To run Karapace locally, or develop it, first install the dependencies.
If you only need the runtime, i.e. you're not running tests or committing to Git,
it's enough to install the runtime dependencies::

    # Runtime dependencies
    python3 -m pip install -r requirements.txt

If you are developing and e.g. running tests, install the development dependencies.
This will install also the runtime dependencies::

    # Development dependencies, contains runtime dependencies
    python3 -m pip install -r requirements-dev.txt

To run the local/current version of the code, set up the configuration file in ``karapace.config.json`` to include connection details for Kafka and any other config you want to change, then run::

    python3 -m karapace.karapace_all karapace.config.json

There are two flavors of tests, unit tests and integration tests. The unit tests are standalone,
i.e. can be run without anything outside of the test running. The integration tests in turn need
a running ZooKeeper and Kafka, but take internally care of starting and stopping them.

The tests can be run from the command line using :code:`make`::

    # Running unit tests
    make unittest

    # Running integration tests
    make integrationtest

To run the tests in an IDE, you need once download and untar Kafka
by :code:`make fetch-kafka`. Additionally ensure that the working directory
when running tests, is set to Git root, e.g. in PyCharm you can
create a configuration template with the correct working directory.

The integration tests are run in parallel e.g. in the CI-pipeline.
The tests need to be engineered taking this in mind.

There are several coding style checks in `GitHub Actions <https://github.com/aiven/karapace/actions>`_.
Your code changes need to pass these tests. To run the checks locally,
you can run them manually::

    # Runs all coding style checks
    make pre-commit

Alternatively,you can use `pre-commit <https://pre-commit.com/>`_ to automatically run the checks on commit time::

    pre-commit install

Docker setup for development
----------------------------

To get you up and running with a development copy of Karapace, a docker setup
is available. You can find everything you need for this in the ``container/``
folder.

Get the containers running::

    docker-compose up

Then you should be able to reach two sets of endpoints:

* Karapace schema registry on http://localhost:8081
* Karapace REST on http://localhost:8082

Quickstart
==========

To Register the first version of a schema under the subject "test"::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
    http://localhost:8081/subjects/test-key/versions
  {"id":1}


To list all subjects (including the one created just above)::

  $ curl -X GET http://localhost:8081/subjects
  ["test-key"]

To list all the versions of a given schema (including the one just created above)::

  $ curl -X GET http://localhost:8081/subjects/test-key/versions
  [1]

To fetch back the schema whose global id is 1 (i.e. the one registered above)::

  $ curl -X GET http://localhost:8081/schemas/ids/1
  {"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

To get the specific version 1 of the schema just registered run::

  $ curl -X GET http://localhost:8081/subjects/test-key/versions/1
    {"subject":"test-key","version":1,"id":1,"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

To get the latest version of the schema under subject test-key run::

  $ curl -X GET http://localhost:8081/subjects/Kafka-value/versions/latest
    {"subject":"test-key","version":1,"id":1,"schema":"{\"fields\":[{\"name\":\"age\",\"type\":\"int\"}],\"name\":\"Obj\",\"type\":\"record\"}"}

In order to delete version 10 of the schema registered under subject "test-key" (if it exists)::

  $ curl -X DELETE http://localhost:8081/subjects/test-key/versions/10
    10

To Delete all versions of the schema registered under subject "test-key"::

  $ curl -X DELETE http://localhost:8081/subjects/test-key
    [1]

Test the compatibility of a schema with the latest schema under subject "test-key"::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data '{"schema": "{\"type\": \"int\"}"}' \
      http://localhost:8081/compatibility/subjects/test-key/versions/latest
    {"is_compatible":true}

Get current global backwards compatibility setting value::

  $ curl -X GET http://localhost:8081/config
    {"compatibilityLevel":"BACKWARD"}

Change compatibility requirements for all subjects where it's not
specifically defined otherwise::

  $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}' http://localhost:8081/config
    {"compatibility":"NONE"}

Change compatibility requirement to FULL for the test-key subject::

  $ curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data '{"compatibility": "FULL"}' http://localhost:8081/config/test-key
    {"compatibility":"FULL"}

List topics::

  $ curl "http://localhost:8081/topics"

Get info for one particular topic::

  $ curl "http://localhost:8081/topics/my_topic"

Produce a message backed up by schema registry::

  $ curl -H "Content-Type: application/vnd.kafka.avro.v2+json" -X POST -d \
  '{"value_schema": "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"simple\", \"fields\": \
  [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "name0"}}]}' http://localhost:8081/topics/my_topic

Create a consumer::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
    --data '{"name": "my_consumer", "format": "avro", "auto.offset.reset": "earliest"}' \
    http://localhost:8081/consumers/avro_consumers

Subscribe to the topic we previously published to::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["my_topic"]}' \
    http://localhost:8081/consumers/avro_consumers/instances/my_consumer/subscription

Consume previously published message::

  $ curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
  http://localhost:8081/consumers/avro_consumers/instances/my_consumer/records?timeout=1000

Commit offsets for a particular topic partition:

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{}' \
    http://localhost:8081/consumers/avro_consumers/instances/my_consumer/offsets

Delete consumer::

  $ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
  http://localhost:8081/consumers/avro_consumers/instances/my_consumer
Backing up your Karapace
========================

Karapace natively stores its data in a Kafka topic the name of which you can
configure freely but which by default is called _schemas.

Karapace includes a tool to backing up and restoring data. To back up, run::

  karapace_schema_backup get --config karapace.config.json --location schemas.log

You can also back up the data simply by using Kafka's Java console
consumer::

  ./kafka-console-consumer.sh --bootstrap-server brokerhostname:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 1000 1> schemas.log


Restoring Karapace from backup
==============================

Your backup can be restored with Karapace by running::

  karapace_schema_backup restore --config karapace.config.json --location schemas.log

Or Kafka's Java console producer can be used to restore the data
to a new Kafka cluster.

You can restore the data from the previous step by running::

  ./kafka-console-producer.sh --broker-list brokerhostname:9092 --topic _schemas --property parse.key=true < schemas.log


Performance comparison to Confluent stack
==========================================
Latency
-------

* 50 concurrent connections, 50.000 requests

====== ========== ===========
Format  Karapace   Confluent
====== ========== ===========
Avro    80.95      7.22
Binary  66.32      46.99
Json    60.36      53.7
====== ========== ===========

* 15 concurrent connections, 50.000 requests

====== =========== ===========
Format   Karapace   Confluent
====== =========== ===========
Avro     25.05      18.14
Binary   21.35      15.85
Json     21.38      14.83
====== =========== ===========

* 4 concurrent connections, 50.000 requests

====== =========== ===========
Format  Karapace   Confluent
====== =========== ===========
Avro     6.54        5.67
Binary   6.51        4.56
Json     6.86        5.32
====== =========== ===========


Also, it appears there is quite a bit of variation on subsequent runs, especially for the lower numbers, so once
more exact measurements are required, it's advised we increase the total req count to something like 500K

We'll focus on avro serialization only after this round, as it's the more expensive one, plus it tests the entire stack

Consuming RAM
-------------

A basic push pull test , with 12 connections on the publisher process and 3 connections on the subscriber process, with a
10 minute duration. The publisher has the 100 ms timeout and 100 max_bytes parameters set on each request so both processes have work to do
Heap size limit is set to 256M on Rest proxy

Ram consumption, different consumer count, over 300s

=========== =================== ================
 Consumers   Karapace combined   Confluent rest
=========== =================== ================
    1            47                  200
    10           55                  400
    20           83                  530
=========== =================== ================


Commands
========

Once installed, the ``karapace`` program should be in your path.  It is the
main daemon process that should be run under a service manager such as
``systemd`` to serve clients.


Configuration keys
==================


.. list-table::
   :header-rows: 1

   * - Parameter
     - Default Value
     - Description
   * - ``advertised_hostname``
     - ``socket.gethostname()``
     - The hostname being advertised to other instances of Karapace that are attached to the same Kafka group.  All nodes within the cluster need to have their ``advertised_hostname``'s set so that they can all reach each other.
   * - ``bootstrap_uri``
     - ``localhost:9092``
     - The URI to the Kafka service where to store the schemas and to run
       coordination among the Karapace instances.
   * - ``client_id``
     - ``sr-1``
     - The client_id name by which the Karapace will use when coordinating with
       other Karapaces who is master.  The one with the name that sorts as the
       first alphabetically is chosen as master from among the services with
       master_eligibility set to true.
   * - ``consumer_enable_autocommit``
     - ``True``
     - Enable auto commit on rest proxy consumers
   * - ``consumer_enable_autocommit``
     - ``True``
     - Enable auto commit on rest proxy consumers
   * - ``consumer_request_timeout_ms``
     - ``11000``
     - Rest proxy consumers timeout for reads that do not limit the max bytes or provide their own timeout
   * - ``consumer_request_max_bytes``
     - ``67108864``
     - Rest proxy consumers maximum bytes to be fetched per request
   * - ``fetch_min_bytes``
     - ``-1``
     - Rest proxy consumers minimum bytes to be fetched per request. ``-1`` means no limit
   * - ``group_id``
     - ``schema-registry``
     - The Kafka group name used for selecting a master service to coordinate the storing of Schemas.
   * - ``master_eligibility``
     - ``true``
     - Should the service instance be considered for promotion to be the master
       service.  Reason to turn this off would be to have an instances of Karapace
       running somewhere else for HA purposes but which you wouldn't want to
       automatically promote to master if the primary instances were to become
       unavailable.
   * - ``producer_compression_type``
     - ``None``
     - Type of compression to be used by rest proxy producers
   * - ``producer_acks``
     - ``1``
     - Level of consistency desired by each producer message sent on the rest proxy
       More on
   * - ``producer_linger_ms``
     - ``0``
     - Time to wait for grouping together requests
       More on `Kafka Producer <https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html>`_
   * - ``security_protocol``
     - ``PLAINTEXT``
     - Default Kafka security protocol needed to communicate with the Kafka
       cluster.  Other options is to use SSL for SSL client certificate
       authentication.
   * - ``sentry``
     - ``None``
     - Used to configure parameters for sentry integration (dsn, tags, ...). Setting the
       environment variable ``SENTRY_DSN`` will also enable sentry integration.
   * - ``ssl_cafile``
     - ``/path/to/cafile``
     - Used when security_protocol is set to SSL, the path to the SSL CA certificate.
   * - ``ssl_certfile``
     - ``/path/to/certfile``
     - Used when security_protocol is set to SSL, the path to the SSL certfile.
   * - ``ssl_keyfile``
     - ``/path/to/keyfile``
     - Used when security_protocol is set to SSL, the path to the SSL keyfile.
   * - ``topic_name``
     - ``_schemas``
     - The name of the Kafka topic where to store the schemas.
   * - ``replication_factor``
     - ``1``
     - The replication factor to be used with the schema topic.
   * - ``host``
     - ``"127.0.0.1"``
     - Address to bind the Karapace HTTP server to.  Set to an empty string to
       listen to all available addresses.
   * - ``registry_host``
     - ``"127.0.0.1"``
     - Kafka Registry host, used by Kafka Rest for avro related requests.
       If running both in the same process, it should be left to its default value
   * - ``port``
     - ``8081``
     - HTTP webserver port to bind the Karapace to.
   * - ``registry_port``
     - ``8081``
     - Kafka Registry port, used by Kafka Rest for avro related requests.
       If running both in the same process, it should be left to its default value
   * - ``metadata_max_age_ms``
     - ``60000``
     - Period of time in milliseconds after Kafka metadata is force refreshed.
   * - ``karapace_rest``
     - ``true``
     - If the rest part of the app should be included in the starting process
       At least one of this and ``karapace_registry`` options need to be enabled in order
       for the service to start
   * - ``karapace_registry``
     - ``true``
     - If the registry part of the app should be included in the starting process
       At least one of this and ``karapace_rest`` options need to be enabled in order
       for the service to start
   * - ``name_strategy``
     - ``subject_name``
     - Name strategy to use when storing schemas from the kafka rest proxy service
   * - ``master_election_strategy``
     - ``lowest``
     - Decides on what basis the Karapace cluster master is chosen (only relevant in a multi node setup)



License
=======

Karapace is licensed under the Apache license, version 2.0.  Full license text is
available in the ``LICENSE`` file.

Please note that the project explicitly does not require a CLA (Contributor
License Agreement) from its contributors.


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/karapace .  Any possible
vulnerabilities or other serious issues should be reported directly to the
maintainers <opensource@aiven.io>.


Credits
=======

Karapace was created by, and is maintained by, Aiven_ cloud data hub
developers.

The schema storing part of Karapace loans heavily from the ideas of the
earlier Schema Registry implementation by Confluent and thanks are in order
to them for pioneering the concept.

.. _`Aiven`: https://aiven.io/

Recent contributors are listed on the GitHub project page,
https://github.com/aiven/karapace/graphs/contributors

Copyright ⓒ 2019 Aiven Ltd.
