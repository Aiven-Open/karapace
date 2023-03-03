Karapace
========

``karapace``. Your Apache Kafka® essentials in one tool.

An `open-source <https://github.com/aiven/karapace/blob/master/LICENSE>`_ implementation
of `Kafka REST <https://docs.confluent.io/platform/current/kafka-rest/index.html#features>`_ and
`Schema Registry <https://docs.confluent.io/platform/current/schema-registry/index.html>`_.

|Tests| |Contributor Covenant|

.. |Tests| image:: https://github.com/aiven/karapace/actions/workflows/tests.yml/badge.svg

.. |Contributor Covenant| image:: https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg
    :target: CODE_OF_CONDUCT.md

Overview
========

Karapace supports the storing of schemas in a central repository, which clients can access to
serialize and deserialize messages. The schemas also maintain their own version histories and can be
checked for compatibility between their different respective versions.

Karapace rest provides a RESTful interface to your Apache Kafka cluster, allowing you to perform tasks such
as producing and consuming messages and perform administrative cluster work, all the while using the
language of the WEB.

Features
========

* Drop-in replacement both on pre-existing Schema Registry / Kafka Rest Proxy client and
  server-sides
* Moderate memory consumption
* Asynchronous architecture based on aiohttp
* Supports Avro, JSON Schema, and Protobuf
* Leader/Replica architecture for HA and load balancing

Compatibility details
---------------------

Karapace is compatible with Schema Registry 6.1.1 on API level. When a new version of SR is released, the goal is
to support it in a reasonable time. Karapace supports all operations in the API.

There are some caveats regarding the schema normalization, and the error messages being the same as in Schema Registry, which
cannot be always fully guaranteed.

Setup
=====

Using Docker
------------

To get you up and running with the latest build of Karapace, a docker image is available::

  # Fetch the latest build from main branch
  docker pull ghcr.io/aiven/karapace:develop

  # Fetch the latest release
  docker pull ghcr.io/aiven/karapace:latest

An example setup including configuration and Kafka connection is available as docker-compose example::

    docker-compose -f ./container/docker-compose.yml up -d

Then you should be able to reach two sets of endpoints:

* Karapace schema registry on http://localhost:8081
* Karapace REST on http://localhost:8082

Configuration
^^^^^^^^^^^^^

Each configuration key can be overridden with an environment variable prefixed with ``KARAPACE_``,
exception being configuration keys that actually start with the ``karapace`` string. For example, to
override the ``bootstrap_uri`` config value, one would use the environment variable
``KARAPACE_BOOTSTRAP_URI``. Here_ you can find an example configuration file to give you an idea
what you need to change.

.. _`Here`: https://github.com/aiven/karapace/blob/master/karapace.config.json

Source install
--------------

Alternatively you can do a source install using::

  python setup.py install

Quickstart
==========

To register the first version of a schema under the subject "test" using Avro schema::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{\"type\": \"record\", \"name\": \"Obj\", \"fields\":[{\"name\": \"age\", \"type\": \"int\"}]}"}' \
    http://localhost:8081/subjects/test-key/versions
  {"id":1}

To register a version of a schema using JSON Schema, one needs to use `schemaType` property::

  $ curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schemaType": "JSON", "schema": "{\"type\": \"object\",\"properties\":{\"age\":{\"type\": \"number\"}},\"additionalProperties\":true}"}' \
    http://localhost:8081/subjects/test-key-json-schema/versions
  {"id":2}

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

  $ curl -X GET http://localhost:8081/subjects/test-key/versions/latest
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

  $ curl "http://localhost:8082/topics"

Get info for one particular topic::

  $ curl "http://localhost:8082/topics/my_topic"

Produce a message backed up by schema registry::

  $ curl -H "Content-Type: application/vnd.kafka.avro.v2+json" -X POST -d \
    '{"value_schema": "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"simple\", \"fields\": \
    [{\"name\": \"name\", \"type\": \"string\"}]}", "records": [{"value": {"name": "name0"}}]}' http://localhost:8082/topics/my_topic

Create a consumer::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
    --data '{"name": "my_consumer", "format": "avro", "auto.offset.reset": "earliest"}' \
    http://localhost:8082/consumers/avro_consumers

Subscribe to the topic we previously published to::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"topics":["my_topic"]}' \
    http://localhost:8082/consumers/avro_consumers/instances/my_consumer/subscription

Consume previously published message::

  $ curl -X GET -H "Accept: application/vnd.kafka.avro.v2+json" \
    http://localhost:8082/consumers/avro_consumers/instances/my_consumer/records?timeout=1000

Commit offsets for a particular topic partition::

  $ curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{}' \
    http://localhost:8082/consumers/avro_consumers/instances/my_consumer/offsets

Delete consumer::

  $ curl -X DELETE -H "Accept: application/vnd.kafka.v2+json" \
    http://localhost:8082/consumers/avro_consumers/instances/my_consumer

Backing up your Karapace
========================

Karapace natively stores its data in a Kafka topic the name of which you can
configure freely but which by default is called _schemas.

Karapace includes a tool to backing up and restoring data. To back up, run::

  karapace_schema_backup get --config karapace.config.json --location schemas.log

You can also back up the data by using Kafka's Java console
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

We'll focus on Avro serialization only after this round, as it's the more expensive one, plus it tests the entire stack

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

Keys to take special care are the ones needed to configure Kafka and advertised_hostname.

.. list-table::
   :header-rows: 1

   * - Parameter
     - Default Value
     - Description
   * - ``advertised_protocol``
     - ``http``
     - The protocol being advertised to other instances of Karapace that are attached to the same Kafka group.
   * - ``advertised_hostname``
     - ``socket.gethostname()``
     - The hostname being advertised to other instances of Karapace that are attached to the same Kafka group.  All nodes within the cluster need to have their ``advertised_hostname``'s set so that they can all reach each other.
   * - ``advertised_port``
     - ``None``
     - The port being advertised to other instances of Karapace that are attached to the same Kafka group.  Fallbacks to ``port`` if not set.
   * - ``bootstrap_uri``
     - ``localhost:9092``
     - The URI to the Kafka service where to store the schemas and to run
       coordination among the Karapace instances.
   * - ``sasl_bootstrap_uri``
     - ``None``
     - The URI to the Kafka service to use with the Kafka REST API when SASL authorization with REST is used.
   * - ``client_id``
     - ``sr-1``
     - The ``client_id`` Karapace will use when coordinating with
       other Karapace instances. The instance with the ID that sorts
       first alphabetically is chosen as master from the services with
       master_eligibility set to true.
   * - ``consumer_enable_autocommit``
     - ``True``
     - Enable auto commit on rest proxy consumers
   * - ``consumer_request_timeout_ms``
     - ``11000``
     - Rest proxy consumers timeout for reads that do not limit the max bytes or provide their own timeout
   * - ``consumer_request_max_bytes``
     - ``67108864``
     - Rest proxy consumers maximum bytes to be fetched per request
   * - ``consumer_idle_disconnect_timeout``
     - ``0``
     - Disconnect idle consumers after timeout seconds if not used.  Inactivity leads to consumer leaving consumer group and consumer state.  0 (default) means no auto-disconnect.
   * - ``fetch_min_bytes``
     - ``1``
     - Rest proxy consumers minimum bytes to be fetched per request.
   * - ``group_id``
     - ``schema-registry``
     - The Kafka group name used for selecting a master service to coordinate the storing of Schemas.
   * - ``master_eligibility``
     - ``true``
     - Should the service instance be considered for promotion to the master
       service. One reason to turn this off would be to have an instance of Karapace
       running somewhere else for HA purposes but which you wouldn't want to
       automatically promote to master if the primary instances become
       unavailable.
   * - ``producer_compression_type``
     - ``None``
     - Type of compression to be used by rest proxy producers
   * - ``producer_acks``
     - ``1``
     - Level of consistency desired by each producer message sent on the rest proxy.
       More on `Kafka Producer <https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html>`_
   * - ``producer_linger_ms``
     - ``0``
     - Time to wait for grouping together requests.
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
     - Used when ``security_protocol`` is set to SSL, the path to the SSL CA certificate.
   * - ``ssl_certfile``
     - ``/path/to/certfile``
     - Used when ``security_protocol`` is set to SSL, the path to the SSL certfile.
   * - ``ssl_keyfile``
     - ``/path/to/keyfile``
     - Used when ``security_protocol`` is set to SSL, the path to the SSL keyfile.
   * - ``topic_name``
     - ``_schemas``
     - The name of the Kafka topic where to store the schemas.
   * - ``replication_factor``
     - ``1``
     - The replication factor to be used with the schema topic.
   * - ``host``
     - ``127.0.0.1``
     - Listening host for the Karapace server.  Use an empty string to
       listen to all available networks.
   * - ``port``
     - ``8081``
     - Listening port for the Karapace server.
   * - ``server_tls_certfile``
     - ``/path/to/certfile``
     - Filename to a certificate chain for the Karapace server in HTTPS mode.
   * - ``server_tls_keyfile``
     - ``/path/to/keyfile``
     - Filename to a private key for the Karapace server in HTTPS mode.
   * - ``registry_host``
     - ``127.0.0.1``
     - Schema Registry host, used by Kafka Rest for schema related requests.
       If running both in the same process, it should be left to its default value
   * - ``registry_port``
     - ``8081``
     - Schema Registry port, used by Kafka Rest for schema related requests.
       If running both in the same process, it should be left to its default value
   * - ``registry_user``
     - ``None``
     - Schema Registry user for authentication, used by Kafka Rest for schema related requests.
   * - ``registry_password``
     - ``None``
     - Schema Registry password for authentication, used by Kafka Rest for schema related requests.
   * - ``registry_ca``
     - ``/path/to/cafile``
     - Kafka Registry CA certificate, used by Kafka Rest for Avro related requests.
       If this is set, Kafka Rest will use HTTPS to connect to the registry.
       If running both in the same process, it should be left to its default value
   * - ``registry_authfile``
     - ``/path/to/authfile.json``
     - Filename to specify users and access control rules for Karapace Schema Registry.
       If this is set, Schema Segistry requires authentication for most of the endpoints and applies per endpoint authorization rules.
   * - ``rest_authorization``
     - ``false``
     - Use REST API's calling authorization credentials to invoke Kafka operations over SASL authentication of ``sasl_bootstrap_uri`` to delegate REST proxy authorization to Kafka.  If false, then use configured common credentials for all Kafka connections of REST proxy operations.
   * - ``rest_base_uri``
     - ``None``
     - Publicly available URI of this instance advertised to the clients using stateful operations such as creating consumers.  If not set, then construct URI using ``advertised_protocol``, ``advertised_hostname``, and ``advertised_port``.
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
   * - ``protobuf_runtime_directory``
     - ``runtime``
     - Runtime directory for the ``protoc`` protobuf schema parser and code generator
   * - ``name_strategy``
     - ``subject_name``
     - Name strategy to use when storing schemas from the kafka rest proxy service
   * - ``master_election_strategy``
     - ``lowest``
     - Decides on what basis the Karapace cluster master is chosen (only relevant in a multi node setup)


Authentication and authorization of Karapace Schema Registry REST API
=====================================================================

To enable HTTP Basic Authentication and user authorization the authorization configuration file is set in the main configuration key ``registry_authfile`` of the Karapace.

Karapace Schema Registry authorization file is an optional JSON configuration, which contains a list of authorized users in ``users`` and a list of access control rules in ``permissions``.

Each user entry contains following attributes:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``username``
     - A string
   * - ``algorithm``
     - One of supported hashing algorithms, ``scrypt``, ``sha1``, ``sha256``, or ``sha512``
   * - ``salt``
     - Salt used for hashing the password
   * - ``password_hash``
     - Hash string of the password calculated using given algorithm and salt.

Password hashing can be done using ``karapace_mkpasswd`` tool, if installed, or by invoking directly with ``python -m karapace.auth``. The tool generates JSON entry with these fields. ::

  $ karapace_mkpasswd -u user -a sha512 secret
  {
      "username": "user",
      "algorithm": "sha512",
      "salt": "iuLouaExTeg9ypqTxqP-dw",
      "password_hash": "R6ghYSXdLGsq6hkQcg8wT4xkD4QToxBhlp7NerTnyB077M+mD2qiN7ZxXCDb4aE+5lExu5P11UpMPYAcVYxSQA=="
  }

Each access control rule contains following attributes:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Description
   * - ``username``
     - A string to match against authenticated user
   * - ``operation``
     - Exact value of ``Read`` or ``Write``. Write implies also read permissions. Write includes all mutable operations, e.g. deleting schema versions
   * - ``resource``
     - A regular expression used to match against accessed resource.

Supported resource authorization:

.. list-table::
   :header-rows: 1

   * - Resource
     - Description
   * - ``Config:``
     - Controls authorization to global schema registry configuration.
   * - ``Subject:<subject_name>``
     - Controls authorization to subject. The ``<subject_name>`` is a regular expression to match against the accessed subject.

Example of complete authorization file
--------------------------------------

::

    {
        "users": [
            {
                "username": "admin",
                "algorithm": "scrypt",
                "salt": "<put salt for randomized hashing here>",
                "password_hash": "<put hashed password here>"
            },
            {
                "username": "plainuser",
                "algorithm": "sha256",
                "salt": "<put salt for randomized hashing here>",
                "password_hash": "<put hashed password here>"
            }
        ],
        "permissions": [
            {
                "username": "admin",
                "operation": "Write",
                "resource": ".*"
            },
            {
                "username": "plainuser",
                "operation": "Read",
                "resource": "Subject:general.*"
            },
            {
                "username": "plainuser",
                "operation": "Read",
                "resource": "Config:"
            }
        ]
    }

Uninstall
=========

To unistall Karapace from the system you can follow the instructions described below. We would love to hear your reasons for uninstalling though. Please file an issue if you experience any problems or email us_ with feedback

.. _`us`: mailto:opensource@aiven.io


Installed via Docker
--------------------

If you installed Karapace via Docker, you would need to first stop and remove the images like described:

First obtain the container IDs related to Karapace, you should have one for the registry itself and another one for the rest interface::

    docker ps | grep karapace

After this, you can stop each of the containers with::

    docker stop <CONTAINER_ID>

If you don't need or want to have the Karapace images around you can now proceed to delete them using::

    docker rm <CONTAINER_ID>

Installed from Sources
----------------------

If you installed Karapace from the sources via ``python setup.py install``, it can be uninstalled with the following ``pip`` command::

    pip uninstall karapace

Development
===========

Execute ``make`` (GNU, usually ``gmake`` on BSD and Mac) to set up a ``venv``
and install the required software for development. Use ``make unit-tests`` and
``make integration-tests`` to execute the respective test suite, or simply
``make test`` to execute both. You can set ``PYTEST_ARGS`` to customize the
execution (e.g. ``PYTEST_ARGS=--maxfail=1 make test``).

By default ``pyenv`` is expected to be installed and in ``PATH``. This ensures
on all platforms that arbitrary Python versions can be used for development. It
is possible to overwrite this by setting ``PYENV`` to something else (e.g.
``PYENV=python3 make venv`` to simply use the global Python executable). The
default Python version is defined in ``.python-version``.

Karapace currently depends on various system software to be installed. The
installation of these is automated for some operation systems, but not all. At
the time of writing Java, the Protobuf Compiler, and the Snappy shared library
are required to work with Karapace. You need to install them manually if your
operating system is not supported by the automatic installation scripts. Note
that the scripts are going to ask before installing any of these on your system.

Note that Karapace requires a Protobuf Compiler older than 3.20.0, because
3.20.0 introduces various breaking changes. The tests are going to fail if the
Protobuf Compiler is newer than that. However, you can work around this locally
by running ``pip install --upgrade protobuf`` in your venv. We are going to fix
this soon.

Note that the integration tests are currently not working on Mac. You can use
Docker, just be sure to set ``VENV_DIR`` to a directory outside the working
directory so that the container is not overwriting files from the host (e.g.
``docker run --env VENV_DIR=/tmp/venv ...``).

Note that the ``runtime`` directory **MUST** exist and that Karapace is going to
fail if it does not. The ``runtime`` directory is also not cleaned between test
runs, and left over data might result in failing tests. Use the ``make`` test
targets that correctly clean the ``runtime`` directory without deleting it, but
keep this in mind whenever you are not using ``make`` (e.g. running tests from
your IDE).

Note that the pre-commit checks are currently not working with the default
Python version. This is because isort dropped Python 3.7 support. You have to
use at least Python 3.8 for the pre-commit checks. Use ``pipx`` or ``brew`` or
… to install pre-commit and use the global installation, there is also no
dependency on it.

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

Trademark
=========
Apache Kafka is either a registered trademark or trademark of the Apache Software Foundation in the United States and/or other countries. Kafka Rest and Schema Registry are trademarks and property of their respective owners. All product and service names used in this page are for identification purposes only and do not imply endorsement.

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

Copyright ⓒ 2021 Aiven Ltd.
