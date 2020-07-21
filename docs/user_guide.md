# User Guide

For a quick introduction on how to build and install the Kubernetes Operator for Apache Flink, and how to run some sample applications, please refer to the [Quick Start Guide](quick-start-guide.md). For a complete reference of the custom resource definition of the `FlinkApplication`, please refer to the [API Specification](crd.md).

## Working with FlinkApplications

### Building a new Flink application
The Flink operator brings up Jobmanager and Taskmanager for an application in Kubernetes. It does this by creating [deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) objects based on the *image* field in the FlinkApplication custom resource object. For more information about building images, please refer to this [documentation](/examples/README.md) and [examples](/examples/wordcount/).

### Creating a New FlinkApplication

A `FlinkApplication` can be created from a YAML file storing the `FlinkApplication` specification using the `kubectl apply -f <YAML file path>` command. Once a `FlinkApplication` is successfully created, the operator will receive it and creates a Flink cluster as configured in the specification to run on the Kubernetes cluster.

#### Mounting Volumes in the Flink Pods

The Flink operator supports mounting user-defined [volumes](https://kubernetes.io/docs/concepts/storage/volumes/) in the Flink job manager and task manager pods. The volume can be of various types (e.g. configMap, secret, hostPath and persistentVolumeClaim). To specify the volume to be mounted, in the FlinkApp YAML, include `volumes` and `volumeMounts` under `spec` section.

For example, the following YAML specifies a volume named `config-vol` populated by a ConfigMap named `dummy-cm`. The volume `config-vol` will be mounted at path `/opt/flink/mycm` in the job manager and task manager pods.

```yaml
volumes:
  - name: config-vol
    configMap:
      name: dummy-cm
volumeMounts:
  - name: config-vol
    mountPath: /opt/flink/mycm
```

### Deleting a FlinkApplication

A `FlinkApplication` can be deleted using the `kubectl delete <name>` command. Deleting a `Flinkapplication` deletes the Flink application custom resource and the Flink cluster associated with it. If the Flink job is running when the deletion happens, the Flink job is cancelled with savepoint before the cluster is deleted.

### Updating an existing FlinkApplication

A `FlinkApplication` can be updated using the `kubectl apply -f <updated YAML file>` command. When a `FlinkApplication` is successfully updated, the operator observes that the resource has changed. The operator before deleting the existing deployment, will cancel the Flink job with a savepoint. After the savepoint succeeds, the operator deletes the existing deployment and submits a new Flink job from the savepoint in the new Flink cluster.

### Checking a FlinkApplication

A `FlinkApplication` can be checked using the `kubectl describe flinkapplication.flink.k8s.io <name>` command. The output of the command shows the specification and status of the `FlinkApplication` as well as events associated with it.

## Customizing the flink operator

To customize the Flink operator, set/update these [configurations](https://github.com/lyft/flinkk8soperator/blob/master/pkg/controller/config/config.go). The values for config can be set either through a [ConfigMap](/deploy/config.yaml) or through command line.
