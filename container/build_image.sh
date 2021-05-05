#!/usr/bin/bash
# Helper script to generate a karapace image.
#
# Notes:
# - The script will always create a fresh temporary directory to run from. This
# has a few benefits:
#  - Ensures a clean state for copying files into the docker image. This is
#  important specially important for the generate version.py file.
#  - Makes sure the code in the container is not from a dirty working tree, so
#  the commit in the project's version is the real code.
#  - Prevents errors with concurrent changes, e.g. if you're generating a image
#  while coding, and unintentionally save a file.
#  - Effective uses of the caching layers.
#
# If for some reason you want to include the dirty state in the container just
# call docker build directly.

ARG_COMMIT=$1
ARG_TAG_NAME=${2:-$1}

COMMIT=$(git rev-parse -q --verify "${ARG_COMMIT}^{commit}")
# replaces every occurence of / with -
# this is useful if the commit is qualified with the repo name, e.g. aiven/master
TAG_NAME=${ARG_TAG_NAME////-}

if [[ -z "${COMMIT}" ]]; then
    echo "Invalid commit provided '${ARG_COMMIT}'"
    echo ""
    echo "$0 <commitish_object> [:tag]"
    exit 1
fi

code_checkout=$(mktemp --directory --suffix=-karapace-image)
trap "rm -rf ${code_checkout}" EXIT

git clone $(dirname $0)/.. "${code_checkout}"

pushd ${code_checkout}
git checkout $COMMIT

sudo docker build \
    --build-arg "CREATED=$(date --rfc-3339=seconds)" \
    --build-arg "VERSION=$(git describe --always)" \
    --build-arg "COMMIT=$COMMIT" \
    --tag "karapace:${TAG_NAME}" \
    --file container/Dockerfile \
    ${code_checkout}

# The TAG_NAME has to be explicitly provided, otherwise `latest` is assume,
# which may not be the version we are currently building.
sudo docker build \
    --build-arg "TAG_NAME=${TAG_NAME}" \
    --tag "karapace-registry:${TAG_NAME}" \
    --file container/Dockerfile.registry \
    ${code_checkout}

sudo docker build \
    --build-arg "TAG_NAME=${TAG_NAME}" \
    --tag "karapace-rest:${TAG_NAME}" \
    --file container/Dockerfile.rest \
    ${code_checkout}
