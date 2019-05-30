#!/usr/bin/env sh

set -e

docker login -u "$DOCKER_REGISTRY_USERNAME" -p "$DOCKER_REGISTRY_PASSWORD"

sudo snap install microk8s --classic --channel=1.12/stable

sh boilerplate/lyft/golang_test_targets/dep_install.sh

dep ensure
