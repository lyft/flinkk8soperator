#!/usr/bin/env sh

set -e

curl -LO -s https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

minikube config set memory 7000
minikube start --kubernetes-version=v1.20.15
minikube ssh 'sudo cat /etc/kubernetes/manifests/kube-apiserver.yaml | sed -r "s/--authorization-mode=.+/--authorization-mode=AlwaysAllow/g" | sudo tee /etc/kubernetes/manifests/kube-apiserver.yaml'

sh boilerplate/lyft/golang_test_targets/dep_install.sh

dep ensure
