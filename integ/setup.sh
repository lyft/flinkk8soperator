#!/usr/bin/env bash

export NAMESPACE=default
export OPERATOR_IMAGE=flinkk8soperator:$(git rev-parse HEAD)
export IMAGE=127.0.0.1:32000/flinkk8soperator:local

docker tag $OPERATOR_IMAGE $IMAGE

microk8s.start
microk8s.status --wait-ready
microk8s.enable dns registry

docker images 127.0.0.1:32000/flinkk8soperator
docker images 127.0.0.1:32000
docker push 127.0.0.1:32000/flinkk8soperator

images=$(docker images 127.0.0.1:32000/flinkk8soperator -q | xargs)
echo $images
while [ "$images" == "" ]
do
  docker push 127.0.0.1:32000/flinkk8soperator
  images=$(docker images 127.0.0.1:32000/flinkk8soperator -q | xargs)
  sleep 5
done

microk8s.kubectl proxy --port 8001 &
microk8s.kubectl config view > ~/.kube/config
