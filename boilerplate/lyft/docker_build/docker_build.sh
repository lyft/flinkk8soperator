#!/usr/bin/env bash

# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'LYFT/BOILERPLATE' REPOSITORY:
# 
# TO OPT OUT OF UPDATES, SEE https://github.com/lyft/boilerplate/blob/master/Readme.rst

set -e

echo ""
echo "------------------------------------"
echo "           DOCKER BUILD"
echo "------------------------------------"
echo ""

# If you have a special id_rsa file, you can pass it here.
: ${RSA_FILE=~/.ssh/id_rsa}

if [ -n "$REGISTRY" ]; then
  # Do not push if there are unstaged git changes
  CHANGED=$(git status --porcelain)
  if [ -n "$CHANGED" ]; then
    echo "Please commit git changes before pushing to a registry"
    exit 1
  fi
fi


GIT_SHA=$(git rev-parse HEAD)
RELEASE_SEMVER=$(git describe --tags --exact-match "$GIT_SHA" 2>/dev/null) || true

IMAGE_TAG_WITH_SHA="${IMAGE_NAME}:${GIT_SHA}"
BUILDER_IMAGE_TAG_WITH_SHA="${IMAGE_NAME}:${GIT_SHA}-builder"

if [ -n "$RELEASE_SEMVER" ]; then
  IMAGE_TAG_WITH_SEMVER="${IMAGE_NAME}:${RELEASE_SEMVER}"
  BUILDER_IMAGE_TAG_WITH_SEMVER="${IMAGE_NAME}:${RELEASE_SEMVER}-builder"
fi

# build both the builder image and the final image
docker build --target builder -t "$BUILDER_IMAGE_TAG_WITH_SHA" --build-arg=SSH_PRIVATE_KEY="$(cat ${RSA_FILE})" .
echo "${IMAGE_TAG_WITH_SHA} built locally."
docker build -t "$IMAGE_TAG_WITH_SHA" --build-arg=SSH_PRIVATE_KEY="$(cat ${RSA_FILE})" .
echo "${BUILDER_IMAGE_TAG_WITH_SHA} built locally."

# if REGISTRY specified, push the images to the remote registy
if [ -n "$REGISTRY" ]; then

  if [ -n "${DOCKER_REGISTRY_PASSWORD}" ]; then
    docker login --username="$DOCKER_REGISTRY_USERNAME" --password="$DOCKER_REGISTRY_PASSWORD"
  fi

  docker tag "$IMAGE_TAG_WITH_SHA" "${REGISTRY}/${IMAGE_TAG_WITH_SHA}"
  docker tag "$BUILDER_IMAGE_TAG_WITH_SHA" "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SHA}"

  docker push "${REGISTRY}/${IMAGE_TAG_WITH_SHA}"
  echo "${REGISTRY}/${IMAGE_TAG_WITH_SHA} pushed to remote."
  docker push "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SHA}"
  echo "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SHA} pushed to remote."

  # If the current commit has a semver tag, also push the images with the semver tag
  if [ -n "$RELEASE_SEMVER" ]; then

    docker tag "$IMAGE_TAG_WITH_SHA" "${REGISTRY}/${IMAGE_TAG_WITH_SEMVER}"
    docker tag "$BUILDER_IMAGE_TAG_WITH_SHA" "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SEMVER}"

    docker push "${REGISTRY}/${IMAGE_TAG_WITH_SEMVER}"
    echo "${REGISTRY}/${IMAGE_TAG_WITH_SEMVER} pushed to remote."
    docker push "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SEMVER}"
    echo "${REGISTRY}/${BUILDER_IMAGE_TAG_WITH_SEMVER} pushed to remote."

  fi
fi
