#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

vendor/k8s.io/code-generator/generate-groups.sh \
deepcopy,client \
github.com/lyft/flinkk8soperator/pkg/client \
github.com/lyft/flinkk8soperator/pkg/apis \
app:v1beta1 \
--go-header-file "./tmp/codegen/boilerplate.go.txt"
