package flink

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/lyft/flytestdlib/logger"

	"github.com/benlaurie/objecthash/go/objecthash"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/json"
)

const (
	AppName                          = "APP_NAME"
	AwsMetadataServiceTimeoutKey     = "AWS_METADATA_SERVICE_TIMEOUT"
	AwsMetadataServiceNumAttemptsKey = "AWS_METADATA_SERVICE_NUM_ATTEMPTS"
	AwsMetadataServiceTimeout        = "5"
	AwsMetadataServiceNumAttempts    = "20"
	OperatorFlinkConfig              = "OPERATOR_FLINK_CONFIG"
	HostName                         = "HOST_NAME"
	HostIP                           = "HOST_IP"
	FlinkDeploymentTypeEnv           = "FLINK_DEPLOYMENT_TYPE"
	FlinkDeploymentType              = "flink-deployment-type"
	FlinkDeploymentTypeJobmanager    = "jobmanager"
	FlinkDeploymentTypeTaskmanager   = "taskmanager"
	FlinkAppHash                     = "flink-app-hash"
	FlinkJobProperties               = "flink-job-properties"
	RestartNonce                     = "restart-nonce"
)

func getFlinkContainerName(containerName string) string {
	cfg := config.GetConfig()
	containerNameFormat := cfg.ContainerNameFormat
	if containerNameFormat != "" {
		return fmt.Sprintf(containerNameFormat, containerName)
	}
	return containerName
}

func getCommonAppLabels(app *v1beta1.FlinkApplication) map[string]string {
	return k8.GetAppLabel(app.Name)
}

func getCommonAnnotations(app *v1beta1.FlinkApplication) map[string]string {
	annotations := common.DuplicateMap(app.Annotations)
	annotations[FlinkJobProperties] = fmt.Sprintf(
		"jarName: %s\nparallelism: %d\nentryClass:%s\nprogramArgs:\"%s\"",
		app.Spec.JarName, app.Spec.Parallelism, app.Spec.EntryClass, app.Spec.ProgramArgs)
	if app.Spec.RestartNonce != "" {
		annotations[RestartNonce] = app.Spec.RestartNonce
	}
	return annotations
}

func GetAWSServiceEnv() []v1.EnvVar {
	return []v1.EnvVar{
		{
			Name:  AwsMetadataServiceTimeoutKey,
			Value: AwsMetadataServiceTimeout,
		},
		{
			Name:  AwsMetadataServiceNumAttemptsKey,
			Value: AwsMetadataServiceNumAttempts,
		},
	}
}

func getFlinkEnv(app *v1beta1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	appName := app.Name

	flinkConfig, err := renderFlinkConfig(app)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to serialize flink configuration")
	}

	env = append(env, []v1.EnvVar{
		{
			Name:  AppName,
			Value: appName,
		},
		{
			Name:  OperatorFlinkConfig,
			Value: flinkConfig,
		},
		{
			Name: HostName,
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: HostIP,
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
	}...)
	return env, nil
}

func GetFlinkContainerEnv(app *v1beta1.FlinkApplication) []v1.EnvVar {
	env := []v1.EnvVar{}
	env = append(env, GetAWSServiceEnv()...)
	flinkEnv, err := getFlinkEnv(app)
	if err == nil {
		env = append(env, flinkEnv...)
	}
	return env
}

func ImagePullPolicy(app *v1beta1.FlinkApplication) v1.PullPolicy {
	if app.Spec.ImagePullPolicy == "" {
		return v1.PullIfNotPresent
	}
	return app.Spec.ImagePullPolicy
}

func fromHashToByteArray(input [32]byte) []byte {
	output := make([]byte, 32)
	for idx, val := range input {
		output[idx] = val
	}
	return output
}

// Generate a deterministic hash in bytes for the pb object
func ComputeDeploymentHash(deployment appsv1.Deployment) ([]byte, error) {
	// json marshalling includes:
	// - omitting empty values which supports backwards compatibility of old protobuf definitions
	jsonObj, err := json.Marshal(deployment)
	if err != nil {
		return nil, err
	}

	// Deterministically hash the JSON object to a byte array. The library will sort the map keys of the JSON object
	// so that we do not run into the issues from json marshalling.
	hash, err := objecthash.CommonJSONHash(string(jsonObj))
	if err != nil {
		return nil, err
	}

	return fromHashToByteArray(hash), err
}

// Returns an 8 character hash sensitive to the application name, labels, annotations, and spec.
// TODO: we may need to add collision-avoidance to this
func HashForApplication(app *v1beta1.FlinkApplication) string {
	// we round-trip through json to normalize the deployment objects
	jmDeployment := jobmanagerTemplate(app)
	jmDeployment.OwnerReferences = make([]metav1.OwnerReference, 0)

	tmDeployment := taskmanagerTemplate(app)
	tmDeployment.OwnerReferences = make([]metav1.OwnerReference, 0)

	jmHashBytes, err := ComputeDeploymentHash(*jmDeployment)
	if err != nil {
		// the hasher cannot actually throw an error on write
		panic(fmt.Sprintf("got error trying when computing hash %v", err))
	}

	tmHashBytes, err := ComputeDeploymentHash(*tmDeployment)
	if err != nil {
		// the hasher cannot actually throw an error on write
		panic(fmt.Sprintf("got error trying when computing hash %v", err))
	}

	hasher := fnv.New32a()
	_, err = hasher.Write(jmHashBytes)
	if err != nil {
		// the hasher cannot actually throw an error on write
		panic(fmt.Sprintf("got error trying when writing to hasher %v", err))
	}
	_, err = hasher.Write(tmHashBytes)
	if err != nil {
		// the hasher cannot actually throw an error on write
		panic(fmt.Sprintf("got error trying when writing to hasher %v", err))
	}

	return fmt.Sprintf("%08x", hasher.Sum32())
}

func InjectOperatorCustomizedConfig(deployment *appsv1.Deployment, app *v1beta1.FlinkApplication, hash string, deploymentType string) {
	var newContainers []v1.Container
	for _, container := range deployment.Spec.Template.Spec.Containers {
		var newEnv []v1.EnvVar
		for _, env := range container.Env {
			if env.Name == OperatorFlinkConfig {
				if isHAEnabled(app.Spec.FlinkConfig) {
					env.Value = fmt.Sprintf("%s\nhigh-availability.cluster-id: %s-%s\n", env.Value, app.Name, hash)
					if deploymentType == FlinkDeploymentTypeJobmanager {
						env.Value = fmt.Sprintf("%sjobmanager.rpc.address: $HOST_IP\n", env.Value)
					}
				} else {
					env.Value = fmt.Sprintf("%s\njobmanager.rpc.address: %s\n", env.Value, VersionedJobManagerServiceName(app, hash))
				}
				if deploymentType == FlinkDeploymentTypeTaskmanager {
					env.Value = fmt.Sprintf("%staskmanager.host: $HOST_IP\n", env.Value)
				}
			}
			newEnv = append(newEnv, env)
		}
		container.Env = newEnv
		newContainers = append(newContainers, container)
	}
	deployment.Spec.Template.Spec.Containers = newContainers
}

func envsEqual(a []v1.EnvVar, b []v1.EnvVar) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i].Name != b[i].Name || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func containersEqual(a *v1.Container, b *v1.Container) bool {
	if !(a.Image == b.Image &&
		a.ImagePullPolicy == b.ImagePullPolicy &&
		apiequality.Semantic.DeepEqual(a.Args, b.Args) &&
		apiequality.Semantic.DeepEqual(a.Resources, b.Resources) &&
		envsEqual(a.Env, b.Env) &&
		apiequality.Semantic.DeepEqual(a.EnvFrom, b.EnvFrom) &&
		apiequality.Semantic.DeepEqual(a.VolumeMounts, b.VolumeMounts)) {
		return false
	}

	if len(a.Ports) != len(b.Ports) {
		return false
	}

	for i := 0; i < len(a.Ports); i++ {
		if a.Ports[i].Name != b.Ports[i].Name ||
			a.Ports[i].ContainerPort != b.Ports[i].ContainerPort {
			return false
		}
	}

	return true
}

// Returns true if there are no relevant differences between the deployments. This should be used only to determine
// that two deployments correspond to the same FlinkApplication, not as a general notion of equality.
func DeploymentsEqual(a *appsv1.Deployment, b *appsv1.Deployment) bool {
	if !apiequality.Semantic.DeepEqual(a.Spec.Template.Spec.Volumes, b.Spec.Template.Spec.Volumes) {
		logger.Infof(context.Background(), "%v %v", a.Spec.Template.Spec.Volumes, b.Spec.Template.Spec.Volumes)
		return false
	}
	if len(a.Spec.Template.Spec.Containers) == 0 ||
		len(b.Spec.Template.Spec.Containers) == 0 ||
		!containersEqual(&a.Spec.Template.Spec.Containers[0], &b.Spec.Template.Spec.Containers[0]) {
		return false
	}
	if *a.Spec.Replicas != *b.Spec.Replicas {
		return false
	}
	if a.Annotations[FlinkJobProperties] != b.Annotations[FlinkJobProperties] {
		return false
	}
	if a.Annotations[RestartNonce] != b.Annotations[RestartNonce] {
		return false
	}
	return true
}
