#!/usr/bin/env bash
set -Eeuo pipefail
#
# Helper script to publish an existing image to hub.docker.com
#

COLOR_RED=$(tput setaf 1)
COLOR_RESET=$(tput setaf reset)

function die() {
    msg=$1
    echo "${COLOR_RED}${msg}${COLOR_RESET}"
    exit 1
}

function error() {
    msg=$1
    echo "${COLOR_RED}${msg}${COLOR_RESET}"
}

function usage() {
    echo "${0} <commit|tag>"
    exit 1
}

if [[ $# -ne 1 ]]; then
    usage
fi

IMAGE_TAG=$1
IMAGE_NAME=karapace
HUB_USER=aivenoy
HUB_REPO=karapace
DRY_RUN_FLAG=0

if [[ $DRY_RUN_FLAG -eq 1 ]]; then
    DRY_RUN='echo'
else
    DRY_RUN=sudo
fi

object_type=$(git cat-file -t "$IMAGE_TAG")
if [[ ${object_type} == commit ]]; then
    # make sure we are using the full commit
    IMAGE_TAG=$(git rev-parse --verify "${IMAGE_TAG}")
elif [[ ${object_type} == tag ]]; then
    git verify-tag "${IMAGE_TAG}" || die "The git tag '${IMAGE_TAG}' is not signed or the signature could not be validated"
else
    # Only release from commit or tags
    error "Input should be a commit or tag"
    echo
    usage
fi

IMAGE_NAME_TAG="${IMAGE_NAME}:${IMAGE_TAG}"
HUB_USER_REPO="${HUB_USER}/${HUB_REPO}"
HUB_IMAGE_LATEST="${HUB_USER_REPO}:latest"
HUB_IMAGE="${HUB_USER_REPO}:${IMAGE_TAG}"

sudo docker pull ${HUB_USER_REPO}

sudo docker image inspect "${IMAGE_NAME_TAG}" &>/dev/null || die "image '${IMAGE_NAME_TAG}' doesn't exist."
$DRY_RUN docker tag "${IMAGE_NAME_TAG}" "${HUB_IMAGE}" || die "retagging '${IMAGE_NAME_TAG}' to '${HUB_IMAGE}' failed"

FIRST_IMAGE=0
sudo docker image inspect "${HUB_IMAGE_LATEST}" &>/dev/null || FIRST_IMAGE=1

if [[ $FIRST_IMAGE -eq 1 ]]; then
    PUSH_LATEST=1
    $DRY_RUN docker tag "${IMAGE_NAME_TAG}" "${HUB_IMAGE_LATEST}" || die "retagging '${IMAGE_NAME_TAG}' to '${HUB_IMAGE_LATEST}' failed"
else
    # Check if the new image should be tagged as latest too
    # - If for some reason an older commit should be tagged as latest, it
    # will need to be done manually.
    # - This is useful to publish missing releases
    latest_published_commit=$(sudo docker image inspect "${HUB_IMAGE_LATEST}" | jq -r '.[].ContainerConfig.Labels["org.opencontainers.image.version"]')

    # List the commits that are in $IMAGE_TAG but not in $latest_published_commit
    extra_commits_local=$(git rev-list "${latest_published_commit}..${IMAGE_TAG}" | wc --lines)
    # the opposite
    extra_commits_remote=$(git rev-list "${IMAGE_TAG}..${latest_published_commit}" | wc --lines)

    if [[ $extra_commits_local -gt 0 && $extra_commits_remote -gt 0 ]]; then
        error "The published '${HUB_IMAGE_LATEST}' image and the new image are from different branches."
        echo
        error "'${HUB_IMAGE_LATEST}' is based on '${latest_published_commit}'"
        error "it has ${extra_commits_remote} additional commits"
        echo
        error "'${TAG_NAME}' is based on '${latest_published_commit}'"
        error "it has ${extra_commits_local} additional commits"
        echo
        die "aborting"
    fi

    # The local image has new commits, so it is the latest
    if [[ $extra_commits_local -gt 0 && $extra_commits_remote -eq 0 ]]; then
        PUSH_LATEST=1
    fi
fi

$DRY_RUN docker push "${HUB_IMAGE}"
if [[ $PUSH_LATEST -eq 1 ]]; then
    $DRY_RUN docker push "${HUB_IMAGE_LATEST}"
fi
