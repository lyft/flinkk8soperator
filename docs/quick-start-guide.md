# Quick Start Guide

If you are looking to develop and test operator in your local machine, refer to [Local development guide](local_dev.md)

Follow the steps below if you have Kubernetes cluster up and running.

## Setup kubectl
Follow the instructions [here](https://kubernetes.io/docs/tasks/tools/install-kubectl/) to install and setup kubectl

## Operator installation

* Let's first create the custom resource definition, namespace, and roles for running the flink operator.

```bash
$ kubectl create -f deploy/crd.yaml
$ kubectl create -f deploy/namespace.yaml
$ kubectl create -f deploy/role.yaml
$ kubectl create -f deploy/role-binding.yaml
```

* Before creating the flink operator deployment, edit/update the config in [config.yaml](/deploy/config.yaml)

```yaml
Replace the {ingress_suffix} to indicate your cluster's ingress url.
data:
  config: |-
    operator:
      ingressUrlFormat: "{{$jobCluster}}.{ingress_suffix}"
    logger:
      level: 4
```

Then create the ConfigMap containing the configurations:
```bash
$ kubectl create -f deploy/config.yaml
```

Also edit the resource requirements for the operator deployment if needed. The default are:
```yaml
  requests:
    memory: "4Gi"
    cpu: "4"
  limits:
    memory: "8G"
    cpu: "8"
```

Then create the operator Deployment:
```
$ kubectl create -f deploy/flinkk8soperator.yaml
```

* Ensure that the flink operator pod is *RUNNING*, and check operator logs if needed.

```bash
$ kubectl get pods -n flink-operator
$ kubectl logs {pod-name} -n flink-operator
```

## Running the example

You can find sample application to run with the flink operator [here](/examples/). To run a flink application, run the following command:

```bash
$ kubectl create -f examples/wordcount/flink-operator-custom-resource.yaml
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
apiVersion: flink.k8s.io/v1alpha1
kind: FlinkApplication
metadata:
  clusterName: ""
  creationTimestamp: "2019-05-03T01:29:27Z"
  generation: 1
  labels:
    environment: development
  name: wordcount-operator-example
  namespace: flink-operator
  resourceVersion: "51383673"
  selfLink: /apis/flink.k8s.io/v1alpha1/namespaces/flink-operator/flinkapplications/wordcount-operator-example
  uid: e415a43d-6d42-11e9-bf89-0acd1b812506
spec:
  deploymentMode: Single
  entryClass: org.apache.flink.WordCount
  flinkConfig:
    state.backend.fs.checkpointdir: file:///checkpoints/flink/checkpoints
    state.checkpoints.dir: file:///checkpoints/flink/externalized-checkpoints
    state.savepoints.dir: file:///checkpoints/flink/savepoints
    taskmanager.heap.size: 200
  flinkVersion: "1.8"
  image: docker.io/lyft/wordcount-operator-example:latest
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
  jobId: 8bda8bbe03946a690cc0f28f9a6f307f
  lastUpdatedAt: "2019-05-03T01:30:28Z"
  phase: Running
```

To check events for the `FlinkApplication` object, run the following command:

```bash
$ kubectl describe flinkapplication.flink.k8s.io -n flink-operator wordcount-operator-example
```

This will show the events similarly to the following:

```
Events:
  Type    Reason  Age   From              Message
  ----    ------  ----  ----              -------
  Normal  Update  35s   flinkk8soperator  Flink cluster created
  Normal  Update  5s    flinkk8soperator  Flink cluster is ready
  Normal  Update  4s    flinkk8soperator  Flink job submitted to cluster
```
