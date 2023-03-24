#!/usr/bin/env bash

set -e

export INTEGRATION=true
export OPERATOR_IMAGE=flinkk8soperator:local

minikube ssh 'mkdir /tmp/checkpoints'
minikube ssh 'sudo chmod -R 0777 /tmp/checkpoints'

cd $(dirname "$0")
go test -p 1 -timeout 10m -check.vv IntegSuite
