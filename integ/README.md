# Integration Tests

This directory contains integration tests for the operator. These
tests involve running the operator against a real Kubernetes system to
validate its behavior.

## Running the integration tests

You will need a few things to run these tests. Firstly, you will need
a Kubernetes cluster and a kubeconfig file to talk to it. The easiest
way to get this is probably to install Docker for Mac (if on Mac) or
Minikube/microk8s on Linux. You will also need `kube proxy` running on
port 8001.

The tests can run in two modes: direct and image. In direct mode, the
operator is run from the current source code from within the test. In
image mode the operator is submitted to Kubernetes as a deployment and
run from there.

By default the tests create, use, and clean up the namespace
`flinkoperatortest`.

These tests use a sample Flink job called operator-test-app. Currently
this is hosted out-of-repo at
https://github.com/lyft/streamperfbench/tree/master/operator-test-app. The
tests currently use two images built from here:

* `lyft/operator-test-app:fce52d22e06f7747ae4aed1028ae09d96ba68812`
* `lyft/operator-test-app:cccd70864965fb2c24bf05bfcb0c95711e505270`

Those images are available on our private Dockerhub registry, and you
will either need to pull them locally or give Kubernetes access to the
registry.

### Setup

These tests create and mount a directory located at `/tmp/checkpoints`
into containers. You may need to configure this directory as a bind
mount. The tests also need to create this directory with
world-writable permissions. On linux this may require that you
run `umask 000` before running the tests.

```
$ kubectl proxy &
$ dep ensure
```

### Running in Direct mode

(from within this directory)

```
$ INTEGRATION=true RUN_DIRECT=true go test
```

### Running in Image mode

```
$ INTEGRATION=true IMAGE={operator image} go test
```

Note that you will need to either build an image with tag flinkk8soperator:latest specify the operator image using the
`IMAGE` environment

### Options

The behavior of the tests are controlled via environment
variables. Supported options include:

* `INTEGRATION` If not set, all integration tests will be skipped
* `KUBERNETES_CONFIG` Should point to your Kubernetes config file
  (defaults to `~/.kube/config`)
* `NAMESPACE` The namespace to use for all Kubernetes resources
  created by the tests. If set to default, the test framework will not
  create or delete the namespace.
* `RUN_DIRECT` If set, will run the operator directly; otherwise will
  submit run it via a deployment inside Kubernetes
* `IMAGE` The image to use for the operator when running in image
  mode. By default, `lyft/flinkk8soperator:latest`

You can also pass [gocheck](http://labix.org/gocheck) options to the
test runner. Particularly useful is `-check.vv` which will output logs
from the operator and Flink pods to help debugging test failures.
