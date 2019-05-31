#!/usr/bin/env bash

set -e

microk8s.kubectl apply -f integ/abc.yaml
watch -n 5 microk8s.kubectl get pods

export INTEGRATION=true

# needed to create the checkpoints directory with world-writable permissions
umask 000

cd $(dirname "$0")
go test -timeout 20m -check.vv IntegSuite

