Karapace |BuildStatus|_
=======================

.. |BuildStatus| image:: https://travis-ci.org/aiven/karapace.png?branch=master
.. _BuildStatus: https://travis-ci.org/aiven/karapace

``karapace`` Your Kafka essentials in one tool


Features
========

* Schema Registry that is 1:1 Compatible with the pre-existing proprietary
  Confluent Schema Registry
* Drop in replacement both on pre-existing Schema Registry client and
  server-sides
* Moderate memory consumption
* Asynchronous architecture based on aiohttp


Overview
========

Karapace supports the storing of schemas in a central repository, which
clients can access to serialize and deserialize messages.  The schemas also
maintain their own version histories and can be checked for compatibility
between their different respective versions.


Requirements
============

Karapace requires Python 3.6 or later and some additional components in
order to operate:

* aiohttp_ for serving schemas over HTTP in an asynchronous fashion
* avro-python3_ for Avro serialization
* kafka-python_ to read, write and coordinate Karapace's persistence in Kafka

.. _`aiohttp`: https://github.com/aio-libs/aiohttp
.. _`avro-python3`: https://github.com/apache/avro
.. _`kafka-python`: https://github.com/dpkp/kafka-python

Developing and testing Karapace also requires the following utilities:
requests_, flake8_, pylint_ and pytest_.

.. _`flake8`: https://flake8.readthedocs.io/
.. _`requests`: http://www.python-requests.org/en/latest/
.. _`pylint`: https://www.pylint.org/
.. _`pytest`: http://pytest.org/

Karapace has been developed and tested on modern Linux x86-64 systems, but
should work on other platforms that provide the required modules.


Building
========

To build an installation package for your distribution, go to the root
directory of a Karapace Git checkout and run:

Fedora::

  make rpm

This will produce a ``.rpm`` package usually into ``rpm/RPMS/noarch/``.

Python/Other::

  python3 setup.py bdist_egg

This will produce an egg file into a dist directory within the same folder.


Installation
============

To install it run as root:

Fedora::

  dnf install rpm/RPMS/noarch/*

On Linux systems it is recommended to simply run ``karapace`` under
``systemd``::

  systemctl enable karapace.service

and eventually after the setup section, you can just run::

  systemctl start karapace.service

Python/Other::

  easy_install dist/karapace-0.1.0-py3.6.egg


Setup
=====

After this you need to create a suitable JSON configuration file for your
installation.  Keys to take special care are the ones needed to configure
Kafka and advertised_hostname.

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


Backing up your Karapace
========================

Karapace natively stores its data in a Kafka topic the name of which you can
configure freely but which by default is called _schemas.

To easily back up the data in the topic you can run Kafka's Java console
consumer::

  ./kafka-console-consumer.sh --bootstrap-server brokerhostname:9092 --topic _schemas --from-beginning --property print.key=true --timeout-ms 1000 1> schemas.log


Restoring Karapace from backup
==============================

Kafka's Java console producer can then in turn be used to restore the data
to a new Kafka cluster.

You can restore the data from the previous step by running::

  ./kafka-console-producer.sh --broker-list brokerhostname:9092 --topic _schemas --property parse.key=true < schemas.log


Commands
========

Once installed, the ``karapace`` program should be in your path.  It is the
main daemon process that should be run under a service manager such as
``systemd`` to serve clients.


Configuration keys
==================

``advertised_hostname`` (default ``socket.gethostname()``)

The hostname being advertised to other instances of Karapace that are
attached to the same Kafka group.  All nodes within the cluster need to have
their advertised_hostname's set so that they can all reach each other.

``bootstrap_uri`` (default ``localhost:9092``)

The URI to the Kafka service where to store the schemas and to run
coordination among the Karapace instances.

``client_id`` (default ``sr-1``)

The client_id name by which the Karapace will use when coordinating with
other Karapaces who is master.  The one with the name that sorts as the
first alphabetically is chosen as master from among the services with
master_eligibility set to true.

``group_id`` (default ``schema-registry``)

The Kafka group name used for selecting a master service to coordinate the
storing of Schemas.

``master_eligibility`` (``true``)

Should the service instance be considered for promotion to be the master
service.  Reason to turn this off would be to have an instances of Karapace
running somewhere else for HA purposes but which you wouldn't want to
automatically promote to master if the primary instances were to become
unavailable.

``security_protocol`` (default ``PLAINTEXT``)

Default Kafka security protocol needed to communicate with the Kafka
cluster.  Other options is to use SSL for SSL client certificate
authentication.

``ssl_cafile`` (default ``Path to CA certificate``)

Used when security_protocol is set to SSL, the path to the SSL CA certificate.

``ssl_certfile`` (default ``/path/to/certfile``)

Used when security_protocol is set to SSL, the path to the SSL certfile.

``ssl_keyfile`` (default ``/path/to/keyfile``)

Used when security_protocol is set to SSL, the path to the SSL keyfile.

``topic_name`` (default ``_schemas``)

The name of the Kafka topic where to store the schemas.

``replication_factor`` (default ``1``)

The replication factor to be used with the schema topic.

``host`` (default ``"127.0.0.1"``)

Address to bind the Karapace HTTP server to.  Set to an empty string to
listen to all available addresses.

``port`` (default ``8081``)

HTTP webserver port to bind the Karapace to.


License
=======

Karapace is licensed under the AGPL, version 3.  Full license text is
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
