#!/usr/bin/env sh

set -e

sudo snap install microk8s --classic --channel=1.13/stable
microk8s.status --wait-ready
microk8s.enable dns
microk8s.enable registry

sh boilerplate/lyft/golang_test_targets/dep_install.sh

dep ensure
