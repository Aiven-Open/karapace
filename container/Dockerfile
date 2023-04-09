# Builder image contains header files and additional dependencies necessary to
# generate wheel files.
FROM debian:stable-slim AS builder

ARG KARAPACE_VERSION

ARG KARAPACE_VERSION
ARG PYTHON_VERSION="3.9.2-3"
ARG PIP_VERSION="20.3.4-4+deb11u1"
ARG SETUPTOOLS_VERSION="52.0.0-4"
ARG WHEEL_VERSION="0.34.2-1"
ARG GCC_VERSION="4:10.2.1-1"

# Build dependencies that need to be installed:
# - python3-devel: Python .h files, used to compile C extensions (e.g. multidict)
#
# Build dependencies that need to be installed because of `--no-install-recommends`:
# - gcc: g++ and gcc to compile C extensions
# - python3-wheel: Library to generate .whl files
# - python3-setuptools: Packaging library
#
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
 && apt-get -y install --no-install-recommends \
      python3-dev=$PYTHON_VERSION \
      python3-pip=$PIP_VERSION \
      python3-setuptools=$SETUPTOOLS_VERSION \
      python3-wheel=$WHEEL_VERSION \
      gcc=$GCC_VERSION

# Copy the requirements.txt and generate wheels for each dependency. Using a
# separate command to use layer caching.
#
# Note: the requirements.txt is pinned, if any of the dependencies is updated
# the cache will be invalidated and the image regenerated, which is the
# intended behavior.
#
COPY ./requirements/requirements.txt /build/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 wheel --requirement /build/requirements.txt --wheel-dir /build/dependencies-wheels

COPY . /build/karapace-repo
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 wheel --no-deps /build/karapace-repo --wheel-dir /build/karapace-wheel

# Karapace image.
FROM debian:stable-slim AS karapace

RUN groupadd --system karapace \
 && useradd --system --gid karapace karapace \
 && mkdir /opt/karapace /opt/karapace/runtime /var/log/karapace \
 && chown --recursive karapace:karapace /opt/karapace /var/log/karapace

ARG PROTOBUF_COMPILER_VERSION="3.12.4-1"
ARG PIP_VERSION="20.3.4-4+deb11u1"

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
 && apt-get -y install --no-install-recommends \
       python3-pip=$PIP_VERSION \
       protobuf-compiler=$PROTOBUF_COMPILER_VERSION

COPY --from=builder /build/dependencies-wheels/*.whl /build/dependencies-wheels/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install --no-deps /build/dependencies-wheels/*.whl \
 && rm -rf /build/dependencies-wheels/

COPY --from=builder /build/karapace-wheel/*.whl /build/karapace-wheel/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install --no-deps /build/karapace-wheel/*.whl \
 && rm -rf /build/karapace-wheel/

COPY ./container/start.sh /opt/karapace
RUN chmod 500 /opt/karapace/start.sh \
 && chown karapace:karapace /opt/karapace/start.sh

COPY ./container/healthcheck.py /opt/karapace

WORKDIR /opt/karapace
USER karapace

HEALTHCHECK --interval=10s --timeout=30s --retries=3 --start-period=60s \
    CMD python3 healthcheck.py http://localhost:$KARAPACE_PORT/_health || exit 1
