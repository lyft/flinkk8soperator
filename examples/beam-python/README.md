# Beam Python Application example

This example shows how to build a Docker image for a Beam Python application that is compatible with the Flink operator, from Flink and Beam base containers.

The Python SDK workers run within the task manager container and the pipeline is submitted through the native Flink entry point (no Beam job server required). For more information about the Beam deployment see this [document](https://docs.google.com/document/d/1z3LNrRtr8kkiFHonZ5JJM_L4NWNBBNcqRc_yAf6G0VI/edit#heading=h.fh2f571kms4d).

To deploy the example locally: `kubectl create -f flink-operator-custom-resource.yaml`

Flink UI (after running `kubectl proxy`): `http://localhost:8001/api/v1/namespaces/flink-operator/services/beam-python-flinkk8soperator-example:8081/proxy/#/overview`
