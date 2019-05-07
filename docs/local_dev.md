# Flink Operator local development

This guide will describe how to get set up for local development of
the Flink Operator. This is most likely useful for people actually
developing the operator, but may also be useful for developers looking
to develop their applications locally.

## Run the operator

### Install [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)

Once installed and running, enabled Kuberenetes in settings (from the
docker icon in the menu bar, click Preferences -> Kubernetes -> Enable
Kubernetes).

### (Optional) Setup kubernetes dashboard

This can be a handy complement to the CLI, especially for new users

```bash
$ kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v1.10.0/src/deploy/recommended/kubernetes-dashboard.yaml
$ kubectl proxy &
$ open http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/#!/overview
```

### Set up your Go environment

```bash
$ export GOPATH=~/src/go
```

(should probably go into your shell's profile)

### Checkout the code

```bash
$ mkdir -p $GOPATH/src/github.com/lyft
$ cd $GOPATH/src/github.com/lyft
$ git clone git@github.com:lyft/flinkk8soperator.git
```

### Install the custom resource definition

```bash
$ cd flinkk8soperator
$ kubectl create -f deploy/crd.yaml
```

### Start the operator

#### Option 1: run outside the kubernetes cluster

In this mode, we run the operator locally (on our mac) or inside the
IDE and configure it to talk to the docker-for-mac kubernetes
cluster. This is very convinient for development, as we can iterate
quickly, use a debugger, etc.

```bash
$ dep ensure
$ KUBERNETES_CONFIG="$HOME/.kube/config" go run ./cmd/flinkk8soperator/main.go  --config=local_config.yaml
```

(you may need to accept a firewall prompt and `brew install dep` if you don't have it installed)

#### Option 2: run inside the kubernetes cluster

This mode more realistically emulates how the operator will run in
production, however the turn-around time for changes is much longer.

First we need to build the docker container for the operator:

```bash
$ docker build -t flinkk8soperator .
```

Then create the operator cluster resources:

```bash
$ kubectl create -f deploy/flinkk8soperator_local.yaml
```

## Run an application

```bash
$ kubectl create -f examples/wordcount/flink-operator-custom-resource.yaml
```

Now you should be able to see two pods (one for the jobmanager and one
for the taskmanager) starting:

```bash
$ kubectl get pods
```

You should also be able to access the jobmanager UI at:

```bash
http://localhost:8001/api/v1/namespaces/default/services/{APP_NAME}-jm:8081/proxy/#/overview
```

(note you will need to be running `kubectl proxy` for this to work)

You can tail the logs for the jobmanager (which may be useful for
debugging failures) via:

```bash
$ kubectl logs -f service/{APP_NAME}-jm
```

You can SSH into the jobmanager by running

```bash
$ kubectl exec -it $(kubectl get pods -o=custom-columns=NAME:.metadata.name | grep "\-jm\-") -- /bin/bash
```
