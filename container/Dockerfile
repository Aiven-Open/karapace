# Current versions of avro and zstandard don't yet have wheels for 3.11.
FROM python:3.10.11-bullseye AS builder

ARG KARAPACE_VERSION

# Create, activate, and enforce usage of virtualenv.
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"
ENV PIP_REQUIRE_VIRTUALENV=true

# Install golang needed by extensions
ENV GO_VERSION=1.21.0
ENV PATH="/usr/local/go/bin:${PATH}"
RUN  wget --progress=dot:giga "https://go.dev/dl/go${GO_VERSION}.linux-$(dpkg --print-architecture).tar.gz" \
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
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install /build/karapace-repo

# Karapace image, i.e. production.
FROM python:3.10.11-slim-bullseye AS karapace

# Setup user and directories.
RUN groupadd --system karapace \
 && useradd --system --gid karapace karapace \
 && mkdir /opt/karapace /opt/karapace/runtime /var/log/karapace \
 && chown --recursive karapace:karapace /opt/karapace /var/log/karapace

# Install protobuf compiler.
ARG PROTOBUF_COMPILER_VERSION="3.12.4-1+deb11u1"
RUN apt-get update \
 && apt-get install --assume-yes --no-install-recommends \
    protobuf-compiler=$PROTOBUF_COMPILER_VERSION \
 && rm -rf /var/lib/apt/lists/*

# Copy virtualenv from builder and activate it.
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"

COPY ./container/start.sh /opt/karapace
RUN chmod 500 /opt/karapace/start.sh \
 && chown karapace:karapace /opt/karapace/start.sh

COPY ./container/healthcheck.py /opt/karapace

WORKDIR /opt/karapace
USER karapace

HEALTHCHECK --interval=10s --timeout=30s --retries=3 --start-period=60s \
    CMD python3 healthcheck.py http://localhost:$KARAPACE_PORT/_health || exit 1
