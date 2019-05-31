#!/usr/bin/env bash

export NAMESPACE=default
export OPERATOR_IMAGE=flinkk8soperator:$(git rev-parse HEAD)

microk8s.start
microk8s.status --wait-ready
microk8s.enable dns

microk8s.ctr -n $NAMESPACE flinkk8soperator import flinkk8soperator.tar

microk8s.kubectl proxy --port 8001 &
microk8s.kubectl config view > ~/.kube/config
