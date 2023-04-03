# Builder image contains header files and additional dependencies necessary to
# generate wheel files.
FROM debian:stable-slim AS builder

LABEL org.opencontainers.image.authors=opensource@aiven.io \
      org.opencontainers.image.url=https://karapace.io \
      org.opencontainers.image.documentation=https://github.com/aiven/karapace/ \
      org.opencontainers.image.source=https://github.com/aiven/karapace/ \
      org.opencontainers.image.vendor=Aiven \
      org.opencontainers.image.licenses=Apache-2.0

# Build dependencies that need to be installed:
# - git: Used to install dependencies directly from their public repos (release
#   not on PyPI).
# - python3-devel: Python .h files, used to compile C extensions (e.g. multidict)
#
# Build dependencies that need to be installed because of `--no-install-recommends`:
# - gcc: g++ and gcc to compile C extensions
# - python3-wheel: Library to generate .whl files
# - python3-setuptools: Packaging library
#
RUN apt-get update && \
    apt-get -y install --no-install-recommends git python3-dev python3-pip python3-setuptools python3-wheel gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt and generate wheels for each dependency. Using a
# separate command to use layer caching.
#
# Note: the requirements.txt is pinned, if any of the dependencies is updated
# the cache will be invalidated and the image regenerated, which is the
# intended behavior.
#
COPY ./requirements/requirements.txt /build/
RUN pip3 wheel --requirement /build/requirements.txt --wheel-dir /build/dependencies-wheels

COPY . /build/karapace-repo
RUN pip3 wheel --no-deps /build/karapace-repo --wheel-dir /build/karapace-wheel

# Karapace image.
FROM debian:stable-slim AS karapace

# Labels must be redefined beucase the base image is debian
LABEL org.opencontainers.image.authors=opensource@aiven.io \
      org.opencontainers.image.url=https://karapace.io \
      org.opencontainers.image.documentation=https://github.com/aiven/karapace/ \
      org.opencontainers.image.source=https://github.com/aiven/karapace/ \
      org.opencontainers.image.vendor=Aiven \
      org.opencontainers.image.licenses=Apache-2.0

RUN groupadd --system karapace && \
    useradd --system --gid karapace karapace && \
    mkdir /opt/karapace /opt/karapace/runtime /var/log/karapace && \
    chown --recursive karapace:karapace /opt/karapace /var/log/karapace

RUN apt-get update && \
    apt-get -y install --no-install-recommends python3-pip protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/dependencies-wheels/*.whl /build/dependencies-wheels/
RUN pip3 install --no-deps /build/dependencies-wheels/*.whl && rm -rf /build/dependencies-wheels/

COPY --from=builder /build/karapace-wheel/*.whl /build/karapace-wheel/
RUN pip3 install --no-deps /build/karapace-wheel/*.whl && rm -rf /build/karapace-wheel/

COPY ./container/start.sh /opt/karapace
RUN chmod 500 /opt/karapace/start.sh && chown karapace:karapace /opt/karapace/start.sh

WORKDIR /opt/karapace
USER karapace

ARG CREATED
ARG VERSION
ARG COMMIT
LABEL org.opencontainers.image.created=$CREATED \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT
