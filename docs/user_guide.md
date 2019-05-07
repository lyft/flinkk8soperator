# User Guide

For a quick introduction on how to build and install the Kubernetes Operator for Apache Flink, and how to run some sample applications, please refer to the [Quick Start Guide](quick-start-guide.md). For a complete reference of the custom resource definition of the `FlinkApplication`, please refer to the [API Specification](crd.md).

## Working with FlinkApplications

### Creating a New FlinkApplication

A `FlinkApplication` can be created from a YAML file storing the `FlinkApplication` specification using either the `kubectl apply -f <YAML file path>` command. Once a `FlinkApplication` is successfully created, the operator will receive it and creates a flink cluster as configured in the specification to run on the Kubernetes cluster.

### Deleting a FlinkApplication

A `FlinkApplication` can be deleted using either the `kubectl delete <name>` command. Deleting a `Flinkapplication` deletes the Flink application custom resource and flink cluster associated with it. If the flink job is running when the deletion happens, the flink job is cancelled with savepoint before the cluster is deleted.

### Updating an existing FlinkApplication

A `FlinkApplication` can be updated using the `kubectl apply -f <updated YAML file>` command. When a `FlinkApplication` is successfully updated, the operator observes that the resource has changed. The operator before deleting the existing deployment, will cancel the flink job with savepoint. After the savepoint succeeds, the operator deletes the existing deployment and submits a new flink job from the savepoint in the new flink cluster.

### Checking a FlinkApplication

A `FlinkApplication` can be checked using the `kubectl describe flinkapplication.flink.k8s.io <name>` command. The output of the command shows the specification and status of the `FlinkApplication` as well as events associated with it.

## Customizing the flink operator

To customize the flink operator, set/update these [configurations](https://github.com/lyft/flinkk8soperator/blob/master/pkg/controller/config/config.go). The values for config can be set either through [configmap](/deploy/config.yaml) or through command line.
