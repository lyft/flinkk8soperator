#!/usr/bin/env bash

# Test App Setup

# TODO: upgrade flink test app from 1.8
cd integ/operator-test-app
export TEST_APP_IMAGE=operator-test-app:$(git rev-parse HEAD)
docker build -t $TEST_APP_IMAGE .
docker tag $TEST_APP_IMAGE operator-test-app:local.1
docker tag $TEST_APP_IMAGE operator-test-app:local.2
minikube image load operator-test-app:local.1
minikube image load operator-test-app:local.2

cd ../../

# Operator Setup

export DOCKER_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export OPERATOR_IMAGE=flinkk8soperator:local

docker build -t $DOCKER_IMAGE .
docker tag $DOCKER_IMAGE $OPERATOR_IMAGE
minikube image load $OPERATOR_IMAGE

kubectl proxy --port 8001 &
