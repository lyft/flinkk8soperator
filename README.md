[![Current Release](https://img.shields.io/github/release/lyft/flinkk8soperator.svg)](https://github.com/lyft/flinkk8soperator/releases/latest)
[![Build Status](https://travis-ci.org/lyft/flinkk8soperator.svg?branch=master)](https://travis-ci.org/lyft/flinkk8soperator)
[![GoDoc](https://godoc.org/github.com/lyft/flinkk8soperator?status.svg)](https://godoc.org/github.com/lyft/flinkk8soperator)
[![License](https://img.shields.io/badge/LICENSE-Apache2.0-ff69b4.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![CodeCoverage](https://img.shields.io/codecov/c/github/lyft/flinkk8soperator.svg)](https://codecov.io/gh/lyft/flinkk8soperator)
[![Go Report Card](https://goreportcard.com/badge/github.com/lyft/flinkk8soperator)](https://goreportcard.com/report/github.com/lyft/flinkk8soperator)
![Commit activity](https://img.shields.io/github/commit-activity/w/lyft/flinkk8soperator.svg?style=plastic)
![Commit since last release](https://img.shields.io/github/commits-since/lyft/flinkk8soperator/latest.svg?style=plastic)


# Flinkk8soperator
FlinkK8sOperator is a [Kubernetes operator](https://coreos.com/operators/) that manages [Flink](https://flink.apache.org/) applications on Kubernetes. The operator acts as control plane to manage the complete deployment lifecycle of the application.


## Project Status

*Alpha*

The FlinkK8sOperator is still under active development and has not been extensively tested in production environment. Backward compatibility of the APIs is not guaranteed for alpha releases.

## Prerequisites
* Version >= 1.9 of Kubernetes.
* Version >= 1.7 of Apache Flink.

## Overview

![Flink operator overview](docs/flink-operator-overview.svg)

The goal of running Flink on Kubernetes is to enable more flexible, lighter-weight deployment of streaming applications, without needing to manage infrastructure. The Flink operator aims to abstract out the complexity of hosting, configuring, managing and operating Flink clusters from application developers. It achieves this by extending any kubernetes cluster using a [custom resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources).

The Operator creates flink clusters dynamically using the specified custom resource. Flink clusters in kubernetes consist of the following:
* JobManager [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
* TaskManager [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
* JobManager [Service](https://kubernetes.io/docs/concepts/services-networking/service/)
* JobManager [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) for the UI

Deploying and managing Flink applications in Kubernetes involves two steps:

* **Building Flink application packaged as a docker image:** A docker image is built containing the application source code with the necessary dependencies built in. This is required to bootstrap the Jobmanager and Taskmanager pods. At Lyft we use Source-To-Image [S2I](https://docs.openshift.com/enterprise/3.0/using_images/s2i_images/index.html) as the image build tool that provides a common builder image with Apache Flink pre-installed. The docker image could be built using any pre-existing workflows at an organization.

* **Creating the Flink application custom resource:** The custom resource for Flink application provides the spec for configuring and managing flink clusters in Kubernetes. The FlinkK8sOperator, deployed on Kubernetes, continuously monitors the resource and the corresponding flink cluster, and performs action based on the diff.

## Documentation

* [Quick start guide](/docs/quick-start-guide.md)
* [User guide](/docs/user_guide.md)
* [Flink application custom resource](/docs/crd.md)
* [Operator state machine](/docs/state_machine.md)
