# Application examples

This contains examples showing applications that produce a docker image which is compatible with the Flink operator. Please use these examples as a reference while building applications to be be executed by the Flink operator.

* The Flink operator custom resource contains **image** field, and expects the image to have both flink and application code to be packaged in it.
* The operator starts up Jobmanager and Taskmanager pods using [Container Args](https://godoc.org/k8s.io/api/core/v1#Container).
* The operator injects Flink configuration through [environment variables](https://github.com/lyft/flinkk8soperator/blob/master/pkg/controller/flink/container_utils.go#L84). The image should have the support to integrate these into the existing configuration as illustrated in example [here](https://github.com/lyft/flinkk8soperator/blob/master/examples/wordcount-sessioncluster/docker-entrypoint.sh#L26).
* If there are issues in the **image** that causes either pods to restart, or Flink cluster to not respond to REST API requests, the state machine will not transition beyond the **READY** state.

#### wordcount-sessioncluster

The operator submits the flink job through the [REST API](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/rest_api.html#jars-jarid-run) in the Jobmanager. For this to work, the jar file of the application needs to be present in the folder as indicated by the config value [web.upload.dir](https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html#web-upload-dir).  In the FlinkApplication CR YAML file, the user needs to specify the name of the application jar under `spec.jarName`.

#### wordcount-jobcluster

The user can place the application jar anywhere on the Flink classpath (e.g. under `$FLINK_HOME/lib`). Only the main class needs to be specified in the FlinkApplication YAML file. No jar name is required. Flink runtime will automatically find the jar corresponding to the main class.

