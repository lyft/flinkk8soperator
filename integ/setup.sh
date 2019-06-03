#!/usr/bin/env bash

export DOCKER_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export OPERATOR_IMAGE=127.0.0.1:32000/flinkk8soperator:local

microk8s.docker build -t $DOCKER_IMAGE .
microk8s.docker tag $DOCKER_IMAGE $OPERATOR_IMAGE
microk8s.docker push 127.0.0.1:32000/flinkk8soperator

microk8s.start
microk8s.status --wait-ready

microk8s.kubectl proxy --port 8001 &
microk8s.kubectl config view > ~/.kube/config
