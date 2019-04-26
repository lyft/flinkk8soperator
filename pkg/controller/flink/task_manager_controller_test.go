package flink

import (
	"testing"

	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"

	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func getTMControllerForTest() TaskManagerController {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	return TaskManagerController{
		metrics:   newTaskManagerMetrics(testScope),
		k8Cluster: &k8mock.K8Cluster{},
	}
}

func TestComputeTaskManagerReplicas(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	taskSlots := int32(4)
	app.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	app.Spec.Parallelism = 9
	app.Spec.FlinkVersion = "1.7"

	assert.Equal(t, int32(3), computeTaskManagerReplicas(&app))
}

func TestGetTaskManagerName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-"+testAppHash+"-tm", getTaskManagerName(&app, testAppHash))
}

func TestGetTaskManagerPodName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-"+testAppHash+"-tm-pod", getTaskManagerPodName(&app, testAppHash))
}

func TestGetTaskManagerDeployment(t *testing.T) {
	app := getFlinkTestApp()
	deployment := v1.Deployment{}
	deployment.Name = getTaskManagerName(&app, testAppHash)
	deployments := []v1.Deployment{
		deployment,
	}
	assert.Equal(t, deployment, *getTaskManagerDeployment(deployments, &app))
}

func TestGetTaskManagerReplicaCount(t *testing.T) {
	app := getFlinkTestApp()
	deployment := v1.Deployment{}
	deployment.Name = getTaskManagerName(&app, testAppHash)
	replicaCount := int32(2)
	deployment.Spec.Replicas = &replicaCount
	deployments := []v1.Deployment{
		deployment,
	}
	assert.Equal(t, int32(2), getTaskManagerCount(deployments, &app))
}

func TestTaskManagerCreateSuccess(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	hash := "2c193a3b"
	annotations := map[string]string{
		"key":                   "annotation",
		"flink-app-parallelism": "8",
	}
	app.Annotations = annotations
	expectedLabels := map[string]string{
		"app":                   "app-name",
		"flink-app-hash":        hash,
		"flink-deployment-type": "taskmanager",
	}
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		deployment := object.(*v1.Deployment)
		assert.Equal(t, getTaskManagerName(&app, hash), deployment.Name)
		assert.Equal(t, app.Namespace, deployment.Namespace)
		assert.Equal(t, getTaskManagerPodName(&app, hash), deployment.Spec.Template.Name)
		assert.Equal(t, annotations, deployment.Annotations)
		assert.Equal(t, annotations, deployment.Spec.Template.Annotations)
		assert.Equal(t, app.Namespace, deployment.Spec.Template.Namespace)
		assert.Equal(t, expectedLabels, deployment.Labels)

		assert.Equal(t, "blob.server.port: 6125\njobmanager.heap.size: 1536\n"+
			"jobmanager.rpc.address: app-name-jm\njobmanager.rpc.port: 6123\n"+
			"jobmanager.web.port: 8081\nmetrics.internal.query-service.port: 50101\n"+
			"query.server.port: 6124\ntaskmanager.heap.size: 512\n"+
			"taskmanager.numberOfTaskSlots: 16\n\n"+
			"high-availability.cluster-id: app-name-"+hash+"\n",
			common.GetEnvVar(deployment.Spec.Template.Spec.Containers[0].Env,
				"OPERATOR_FLINK_CONFIG").Value)

		return nil
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
}

func TestTaskManagerCreateErr(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		return errors.New("create error")
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.EqualError(t, err, "create error")
}

func TestTaskManagerCreateAlreadyExists(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		return k8sErrors.NewAlreadyExists(schema.GroupResource{}, "")
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
}
