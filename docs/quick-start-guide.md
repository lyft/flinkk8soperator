# Quick Start Guide

If you are looking to develop and test operator in your local machine, refer to [Local development guide](local_dev.md)

Follow the steps below if you have Kubernetes cluster up and running.

## Setup kubectl
Follow the instructions [here](https://kubernetes.io/docs/tasks/tools/install-kubectl/) to install and setup kubectl

## Operator installation

* Let's first create the custom resource definition, namespace, and roles for running the flink operator.

```bash
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/crd.yaml
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/namespace.yaml
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/role.yaml
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/role-binding.yaml
```

* Before creating the flink operator deployment, edit/update the operator config:

``` bash
$ curl https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/config.yaml
```

Replace the `{ingress_suffix}` to indicate your cluster's ingress url.

```yaml
data:
  config: |-
    operator:
      ingressUrlFormat: "{{$jobCluster}}.{ingress_suffix}"
    logger:
      level: 4
```

Note: If `ingressUrlFormat` is not set, then no ingress is created for the application.

Then create the ConfigMap on the cluster:
```bash
$ kubectl create -f config.yaml
```

Finally, create the operator Deployment:
```
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/deploy/flinkk8soperator.yaml
```

* Ensure that the flink operator pod is *RUNNING*, and check operator logs if needed.

```bash
$ kubectl get pods -n flink-operator
$ kubectl logs {pod-name} -n flink-operator
```

## Running the example

You can find sample application to run with the flink operator [here](/examples/wordcount/).
Make sure to edit the value of `sha` with the most recently pushed tag found [here](https://hub.docker.com/r/lyft/wordcount-operator-example/tags)
```yaml
  image: docker.io/lyft/wordcount-operator-example:{sha}
```

To run a flink application, run the following command:

```bash
$ kubectl create -f https://raw.githubusercontent.com/lyft/flinkk8soperator/v0.4.0/examples/wordcount/flink-operator-custom-resource.yaml
```

The above command will create the flink application custom resource in kubernetes. The operator will observe the custom resource, and will create a flink cluster in kubernetes.

Command below should show deployments created for the application
```bash
$ kubectl get deployments -n flink-operator
```

Check the phase and other status attributes in the custom resource
```bash
$ kubectl get flinkapplication.flink.k8s.io -n flink-operator wordcount-operator-example -o yaml
```

The output should be something like this
```yaml
apiVersion: v1beta1
kind: FlinkApplication
metadata:
  clusterName: ""
  creationTimestamp: 2019-07-30T07:35:42Z
  finalizers:
  - job.finalizers.flink.k8s.io
  generation: 1
  labels:
    environment: development
  name: wordcount-operator-example
  namespace: flink-operator
  resourceVersion: "1025774"
  selfLink: v1beta1
  uid: a2855178-b29c-11e9-9a3b-025000000001
spec:
  entryClass: org.apache.flink.WordCount
  flinkConfig:
    state.backend.fs.checkpointdir: file:///checkpoints/flink/checkpoints
    state.checkpoints.dir: file:///checkpoints/flink/externalized-checkpoints
    state.savepoints.dir: file:///checkpoints/flink/savepoints
    taskmanager.heap.size: 200
  flinkVersion: "1.8"
  image: docker.io/lyft/wordcount-operator-example:3b0347b2cdc1bda817e72b3099dac1c1b1363311
  jarName: wordcount-operator-example-1.0.0-SNAPSHOT.jar
  jobManagerConfig:
    envConfig: {}
    replicas: 1
    resources:
      requests:
        cpu: 200m
        memory: 200Mi
  parallelism: 3
  restartNonce: ""
  savepointInfo: {}
  taskManagerConfig:
    envConfig: {}
    resources:
      requests:
        cpu: 200m
        memory: 200Mi
    taskSlots: 2
status:
  clusterStatus:
    availableTaskSlots: 4
    health: Green
    healthyTaskManagers: 2
    numberOfTaskManagers: 2
    numberOfTaskSlots: 4
  deployHash: d9f8a6a8
  failedDeployHash: ""
  jobStatus:
    completedCheckpointCount: 0
    entryClass: org.apache.flink.WordCount
    failedCheckpointCount: 0
    health: Green
    jarName: wordcount-operator-example-1.0.0-SNAPSHOT.jar
    jobID: acd232a002dd5204669d1041736b8fa0
    jobRestartCount: 0
    lastCheckpointTime: null
    lastFailingTime: null
    parallelism: 3
    restorePath: ""
    restoreTime: null
    startTime: 2019-07-30T07:35:59Z
    state: FINISHED
  lastSeenError: null
  lastUpdatedAt: 2019-07-30T07:36:09Z
  phase: Running
  retryCount: 0
```

To check events for the `FlinkApplication` object, run the following command:

```bash
$ kubectl describe flinkapplication.flink.k8s.io -n flink-operator wordcount-operator-example
```

This will show the events similarly to the following:

```
Events:
  Type    Reason           Age   From              Message
  ----    ------           ----  ----              -------
  Normal  CreatingCluster  4m    flinkK8sOperator  Creating Flink cluster for deploy d9f8a6a8
  Normal  JobSubmitted     3m    flinkK8sOperator  Flink job submitted to cluster with id acd232a002dd5204669d1041736b8fa0
```
