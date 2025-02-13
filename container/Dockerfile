ARG PYTHON_VERSION=3.10.11

# Current versions of avro and zstandard don't yet have wheels for 3.11.
FROM python:${PYTHON_VERSION}-bullseye AS builder
ARG KARAPACE_VERSION

# Create, activate, and enforce usage of virtualenv.
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"
ENV PIP_REQUIRE_VIRTUALENV=true

# Install golang needed by extensions
ENV GO_VERSION=1.21.0
ENV PATH="/usr/local/go/bin:${PATH}"
RUN wget --progress=dot:giga "https://go.dev/dl/go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
 && tar -C /usr/local -xzf "go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
 && rm "go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz"

# Copy the requirements.txt and install dependencies in venv. Using a separate
# command to use layer caching.
#
# Note: the requirements.txt is pinned, if any of the dependencies is updated
# the cache will be invalidated and the image regenerated, which is the
# intended behavior.
COPY ./requirements/requirements.txt /build/
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install -r /build/requirements.txt

COPY . /build/karapace-repo
WORKDIR /build/karapace-repo
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ -z "${KARAPACE_VERSION}" ]; then \
        PRETEND_VERSION="$(python -c 'from src.karapace import version; print(version.__version__)')"; \
    else \
        PRETEND_VERSION=$KARAPACE_VERSION; \
    fi; \
    SETUPTOOLS_SCM_PRETEND_VERSION=$PRETEND_VERSION python3 -m pip install --no-deps .


# Karapace image, i.e. production.
FROM builder AS karapace
# Install protobuf compiler.
ARG PROTOBUF_COMPILER_VERSION="3.12.4-1+deb11u1"
RUN apt-get update \
 && apt-get install --assume-yes --no-install-recommends \
    protobuf-compiler=$PROTOBUF_COMPILER_VERSION \
 && rm -rf /var/lib/apt/lists/*

# Copy virtualenv from builder and activate it.
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"


FROM karapace AS cli
ARG RUNNER_UID
ARG RUNNER_GID

# Setup files and directories.
RUN mkdir /opt/karapace /opt/karapace/runtime /var/log/karapace /opt/karapace/coverage \
&& touch /opt/karapace/coverage/.coverage.3.10 /opt/karapace/coverage/.coverage.3.11 /opt/karapace/coverage/.coverage.3.12 \
&& chown --recursive "$RUNNER_UID:$RUNNER_GID" /opt/karapace /opt/karapace/coverage /var/log/karapace

# Install Java via openjdk-11
COPY --from=openjdk:11 /usr/local/openjdk-11 /usr/local/openjdk-11
ENV JAVA_HOME /usr/local/openjdk-11
RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-11/bin/java 1

WORKDIR /opt/karapace

COPY ./requirements /opt/karapace/requirements
RUN python3 -m pip install -r /opt/karapace/requirements/requirements-dev.txt -r /opt/karapace/requirements/requirements-typing.txt

COPY . .
RUN SETUPTOOLS_SCM_PRETEND_VERSION=$KARAPACE_VERSION python3 -m pip install .
ENV PYTHONPATH="/opt/karapace/src:$PYTHONPATH"


FROM karapace AS production
# Setup user and directories.
# https://docs.redhat.com/en/documentation/openshift_container_platform/4.17/html/images/creating-images#use-uid_create-images
RUN useradd --system --gid 0 karapace \
 && mkdir /opt/karapace /opt/karapace/runtime /var/log/karapace \
 && chgrp -R 0  /opt/karapace /opt/karapace/runtime /var/log/karapace \
 && chmod -R g+rwX /opt/karapace


COPY ./container/healthcheck.py /opt/karapace

WORKDIR /opt/karapace
USER karapace

HEALTHCHECK --interval=10s --timeout=30s --retries=3 --start-period=60s \
    CMD python3 healthcheck.py http://localhost:$KARAPACE_PORT/_health || exit 1
