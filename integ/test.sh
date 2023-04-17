#!/usr/bin/env bash

set -e

export INTEGRATION=true
export OPERATOR_IMAGE=flinkk8soperator:local

cd $(dirname "$0")
go test -p 1 -timeout 40m -check.vv IntegSuite
