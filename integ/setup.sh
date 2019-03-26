#!/usr/bin/env bash

sed -i "s/\${sha}/$(git rev-parse HEAD)/" integ/flinkk8soperator_integ.yaml

microk8s.start
microk8s.status --wait-ready
microk8s.enable dns

microk8s.kubectl proxy --port 8001 &

# Enable our private docker registry
# TODO: remove for open source
microk8s.kubectl create secret docker-registry dockerhub \
  --docker-server=docker.io \
  --docker-username=$DOCKER_REGISTRY_USERNAME \
  --docker-password=$DOCKER_REGISTRY_PASSWORD \
  --docker-email=none

# Start the operator
microk8s.kubectl create -f deploy/crd.yaml

microk8s.kubectl create -f integ/flinkk8soperator_integ.yaml
