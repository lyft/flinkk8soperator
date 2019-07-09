package flink

import (
	"testing"

	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"

	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestTaskManagerCreateSuccess(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	app.Spec.JarName = "test.jar"
	app.Spec.EntryClass = "com.test.MainClass"
	app.Spec.ProgramArgs = "--test"
	annotations := map[string]string{
		"key":                  "annotation",
		"flink-job-properties": "jarName: test.jar\nparallelism: 8\nentryClass:com.test.MainClass\nprogramArgs:\"--test\"",
	}

	hash := "334c7c5d"

	app.Annotations = annotations
	expectedLabels := map[string]string{
		"flink-app":             "app-name",
		"flink-app-hash":        hash,
		"flink-deployment-type": "taskmanager",
	}
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		deployment := object.(*v1.Deployment)
		assert.Equal(t, getTaskManagerName(&app, hash), deployment.Name)
		assert.Equal(t, app.Namespace, deployment.Namespace)
		assert.Equal(t, getTaskManagerPodName(&app, hash), deployment.Spec.Template.Name)
		assert.Equal(t, annotations, deployment.Annotations)
		assert.Equal(t, annotations, deployment.Spec.Template.Annotations)
		assert.Equal(t, app.Namespace, deployment.Spec.Template.Namespace)
		assert.Equal(t, expectedLabels, deployment.Labels)

		assert.Equal(t, "blob.server.port: 6125\njobmanager.heap.size: 1536\n"+
			"jobmanager.rpc.port: 6123\n"+
			"jobmanager.web.port: 8081\nmetrics.internal.query-service.port: 50101\n"+
			"query.server.port: 6124\ntaskmanager.heap.size: 512\n"+
			"taskmanager.numberOfTaskSlots: 16\n\n"+
			"high-availability.cluster-id: app-name-"+hash+"\n"+
			"jobmanager.rpc.address: app-name-"+hash+"\n",
			common.GetEnvVar(deployment.Spec.Template.Spec.Containers[0].Env,
				"OPERATOR_FLINK_CONFIG").Value)

		return nil
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
	assert.True(t, newlyCreated)
}

func TestTaskManagerCreateErr(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		return errors.New("create error")
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.EqualError(t, err, "create error")
	assert.False(t, newlyCreated)
}

func TestTaskManagerCreateAlreadyExists(t *testing.T) {
	testController := getTMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		return k8sErrors.NewAlreadyExists(schema.GroupResource{}, "")
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
	assert.False(t, newlyCreated)
}
