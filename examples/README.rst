OAUTHBEARER Authentication
==========================

Karapace supports pluggable OAUTHBEARER token providers for authenticating to
Kafka brokers. This is useful for deployments using AWS MSK with IAM
authentication, or any Kafka cluster that uses the OAUTHBEARER SASL mechanism.

How it works
------------

Set the ``sasl_oauth_token_provider_class`` config option to a Python import
path pointing to your token provider class. Karapace will import and
instantiate the class, then pass it to every Kafka client (admin, consumer,
producer) it creates — including the schema reader's internal clients.

Your class must implement a single method::

    def token_with_expiry(self, config: str | None = None) -> tuple[str, int | None]:
        """Return (token_string, expiry_epoch_timestamp_or_None)."""

This matches the ``TokenWithExpiryProvider`` protocol defined in
``karapace.core.kafka.common``.

Configuration
-------------

Three config values are needed:

.. code-block:: bash

    # The Kafka security protocol
    KARAPACE_SECURITY_PROTOCOL=SASL_SSL

    # The SASL mechanism
    KARAPACE_SASL_MECHANISM=OAUTHBEARER

    # Python import path to your token provider class
    KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=my_module:MyTokenProvider

The import path format is ``dotted.module.path:ClassName``. Karapace uses
pydantic's ``ImportString`` to resolve it, so standard Python import rules
apply.

Loading a provider that is not in the Karapace package
------------------------------------------------------

Your token provider class does **not** need to live inside the Karapace
repository. It just needs to be importable by Python at runtime. Here are
common approaches:

**Option 1: Install as a Python package**

Package your provider and install it into the same environment as Karapace:

.. code-block:: bash

    pip install my-msk-auth
    # or for local development:
    pip install -e /path/to/my-msk-auth

Then reference it by its package name:

.. code-block:: bash

    KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=my_msk_auth.provider:MSKIAMTokenProvider

**Option 2: Add to PYTHONPATH**

Place your provider file anywhere on disk and add its parent directory to
``PYTHONPATH``:

.. code-block:: bash

    export PYTHONPATH=/opt/custom:$PYTHONPATH
    export KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=msk_iam_token_provider:MSKIAMTokenProvider

Where ``/opt/custom/msk_iam_token_provider.py`` contains your class.

**Option 3: Docker — mount and extend the path**

.. code-block:: dockerfile

    COPY msk_iam_token_provider.py /opt/custom/
    ENV PYTHONPATH=/opt/custom
    ENV KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=msk_iam_token_provider:MSKIAMTokenProvider

AWS MSK IAM example
-------------------

An example provider for AWS MSK IAM authentication is included at
``examples/msk_iam_token_provider.py``. It uses the
``aws-msk-iam-sasl-signer-python`` library:

.. code-block:: bash

    pip install aws-msk-iam-sasl-signer-python

    export KARAPACE_SECURITY_PROTOCOL=SASL_SSL
    export KARAPACE_SASL_MECHANISM=OAUTHBEARER
    export KARAPACE_SASL_OAUTH_TOKEN_PROVIDER_CLASS=examples.msk_iam_token_provider:MSKIAMTokenProvider
    export AWS_DEFAULT_REGION=us-east-1

The provider uses the default boto3 credential chain (environment variables,
instance profile, ECS task role, etc.).
