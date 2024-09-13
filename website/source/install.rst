Install Karapace
================

Using Docker
------------

To get you up and running with the latest build of Karapace, a docker image is available::

  # Fetch the latest build from main branch
  docker pull ghcr.io/aiven-open/karapace:develop

  # Fetch the latest release
  docker pull ghcr.io/aiven-open/karapace:latest

An example setup including configuration and Kafka connection is available as compose example::

    docker compose -f ./container/compose.yml up -d

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

.. _`Here`: https://github.com/Aiven-Open/karapace/blob/main/karapace.config.json

Source install
--------------

Alternatively you can do a source install using::

  pip install .
