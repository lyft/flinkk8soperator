#!/usr/bin/env bash

export NAMESPACE=default
export OPERATOR_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export IMAGE=localhost:32000/flinkk8soperator:local

docker tag $OPERATOR_IMAGE $IMAGE

microk8s.start
microk8s.status --wait-ready
microk8s.enable dns registry

microk8s.kubectl proxy --port 8001 &
microk8s.kubectl config view > ~/.kube/config
docker push localhost:32000/flinkk8soperator
