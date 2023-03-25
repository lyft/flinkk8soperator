#!/usr/bin/env bash

# Test App Setup

#cd integ/operator-test-app
#export TEST_APP_IMAGE=operator-test-app:$(git rev-parse HEAD)
#docker build -t $TEST_APP_IMAGE .
#docker tag $TEST_APP_IMAGE flink-test-app:local.1
#docker tag $TEST_APP_IMAGE flink-test-app:local.2
#minikube image load flink-test-app:local.1
#minikube image load flink-test-app:local.2
#
#cd ../../

docker pull lyft/operator-test-app:b1b3cb8e8f98bd41f44f9c89f8462ce255e0d13f.1
docker pull lyft/operator-test-app:b1b3cb8e8f98bd41f44f9c89f8462ce255e0d13f.2
minikube image load lyft/operator-test-app:b1b3cb8e8f98bd41f44f9c89f8462ce255e0d13f.1
minikube image load lyft/operator-test-app:b1b3cb8e8f98bd41f44f9c89f8462ce255e0d13f.2


# Operator Setup

export DOCKER_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export OPERATOR_IMAGE=flinkk8soperator:local

docker build -t $DOCKER_IMAGE .
docker tag $DOCKER_IMAGE $OPERATOR_IMAGE
minikube image load $OPERATOR_IMAGE

kubectl proxy --port 8001 &
