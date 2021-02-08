#!/usr/bin/bash
# Helper script to generate a karapace image.
#
# Notes:
# - The script will always createa fresh temporary directory to run from. This
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
COMMIT=$(git rev-parse -q --verify "${ARG_COMMIT}^{commit}")

if [[ -z "${COMMIT}" ]]; then
    echo "Invalid commit provided '${COMMIT}'"
    echo ""
    echo "$0 <commitish_object>"
    exit 1
fi

code_checkout=$(mktemp --directory --suffix=-karapace-image)
trap "rm -rf ${code_checkout}" EXIT

git clone $(dirname $0)/.. "${code_checkout}"

pushd ${code_checkout}
git checkout $COMMIT

# HACK: Delete files from the `.git` which change on every operation.
#
# - .git/logs/HEAD - command history
# - .git/index - binary file for the current index, very important for a
#    working repository, not interesting for our image
#
# Being extra cautious here, to make sure only the temporary copy is modified.
rm ${code_checkout}/.git/logs/HEAD
rm ${code_checkout}/.git/index

# replaces every occurence of / with -
# this is useful if the commit is qualified with the repo name, e.g. aiven/master
tag_name=${ARG_COMMIT////-}

podman build \
    --build-arg "CREATED=$(date --rfc-3339=seconds)" \
    --build-arg "VERSION=$(git describe --always)" \
    --build-arg "COMMIT=$COMMIT" \
    --target karapace \
    --tag "aiven/karapace:${tag_name}" \
    --file container/Dockerfile \
    ${code_checkout}

podman build \
    --build-arg "CREATED=$(date --rfc-3339=seconds)" \
    --build-arg "VERSION=$(git describe --always)" \
    --build-arg "COMMIT=$COMMIT" \
    --target karapace-registry \
    --tag "aiven/karapace-registry:${tag_name}" \
    --file container/Dockerfile \
    ${code_checkout}

podman build \
    --build-arg "CREATED=$(date --rfc-3339=seconds)" \
    --build-arg "VERSION=$(git describe --always)" \
    --build-arg "COMMIT=$COMMIT" \
    --target karapace-rest \
    --tag "aiven/karapace-rest:${tag_name}" \
    --file container/Dockerfile \
    ${code_checkout}
