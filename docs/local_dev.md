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

On the auth screen, just click "Skip"

### Log into ECR

```bash
$ $(aws ecr get-login --no-include-email)
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

### Create a development config:

flink-development-config.yaml:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-development-config
data:
  APPLICATION_ENV: development
  JOB_MANAGER_HEAP_MB: "200"
  TASK_MANAGER_SLOTS: "2"
  TASK_MANAGER_HEAP_MB: "200"
```

apply it to the cluster:

``` bash
$ kubectl create -f flink-development-config.yaml
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

First we need to update the registry for the go image to reference a 
real registry so we can use standard docker instead of control. This 
can be done with the following one-liner:

```bash
perl -pi -e "s,FROM lyft,FROM 173840052742.dkr.ecr.us-east-1.amazonaws.com,g" Dockerfile
``` 

(make sure not to commit this change)

First we need to build the docker container for the operator:

```bash
$ docker build -t flinkk8soperator .
```

Then create the operator cluster resources:

```bash
$ kubectl create -f deploy/flinkk8soperator_local.yaml
```

## Run an application

1. Pull the builder into local docker

```bash
$ docker pull 173840052742.dkr.ecr.us-east-1.amazonaws.com/s2istreamingplatformflink:$SHA
```

(for now, use the latest SHA from
[here](https://github.com/lyft/streamingplatform/commits/master/s2i-streamingplatform-flink)).

2. Build the application image

(`brew install source-to-image` if you don't have it installed)

```bash
$ cd ~/src/$APP_NAME
$ s2i build --copy -e "CODE_ROOT=/code" . 173840052742.dkr.ecr.us-east-1.amazonaws.com/s2istreamingplatformflink:$SHA $APP_NAME
```

(for rebuilds, use `--incremental` to skip downloading the dependencies)

3. Deploy the app


Create a yaml file for your deployment. It should look something like
this:

app-development.yaml:

```yaml
apiVersion: flink.k8s.io/v1alpha1
kind: FlinkApplication
metadata:
  name: $APP_NAME
  annotations:
  labels:
    environment: development
spec:
  deploymentMode: Single
  jarName: "$JAR"
  parallelism: 1
  entryClass: "$MAIN_CLASS"
  flinkVersion: "$FLINK_VERSION"
  image: $APP_NAME:latest
  imagePullPolicy: Never
  jobManagerConfig:
    envConfig:
      envFrom:
      - configMapRef:
          name: flink-development-config
    replicas: 1
    resources:
      requests:
        cpu: "1"
        memory: "200Mi"
  taskManagerConfig:
    envConfig:
      envFrom:
      - configMapRef:
          name: flink-development-config
    resources:
      requests:
        cpu: "1"
        memory: "200Mi"
    taskSlots: 2

```

```bash
$ kubectl create -f app-development.yaml
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
