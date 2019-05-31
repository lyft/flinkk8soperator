#!/usr/bin/env bash

set -e

export INTEGRATION=true
export IMAGE=127.0.0.1:32000/flinkk8soperator:local

# needed to create the checkpoints directory with world-writable permissions
umask 000

cd $(dirname "$0")
go test -timeout 20m -check.vv IntegSuite

