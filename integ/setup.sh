#!/usr/bin/env bash

cd integ/operator-test-app
export TEST_APP_IMAGE=operator-test-app:$(git rev-parse HEAD)
docker build -t ${TEST_APP_IMAGE} .
microk8s.docker tag $TEST_APP_IMAGE 127.0.0.1:3200/flink-test-app:local.1
microk8s.docker tag $TEST_APP_IMAGE 127.0.0.1:3200/flink-test-app:local.2
cd ../../

export DOCKER_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export OPERATOR_IMAGE=127.0.0.1:32000/flinkk8soperator:local

microk8s.docker build -t $DOCKER_IMAGE .
microk8s.docker tag $DOCKER_IMAGE $OPERATOR_IMAGE
microk8s.docker push 127.0.0.1:32000/flinkk8soperator

microk8s.start
microk8s.status --wait-ready

microk8s.kubectl proxy --port 8001 &
microk8s.kubectl config view > ~/.kube/config
