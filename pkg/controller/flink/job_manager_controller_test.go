package flink

import (
	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"testing"

	"context"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func getJMControllerForTest() FlinkJobManagerController {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	return FlinkJobManagerController{
		metrics:   newFlinkJobManagerMetrics(testScope),
		k8Cluster: &k8mock.MockK8Cluster{},
	}
}

func TestGetJobManagerName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-11ae1-jm", getJobManagerName(&app))
}

func TestGetJobManagerPodName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-11ae1-jm-pod", getJobManagerPodName(&app))
}

func TestGetJobManagerDeployment(t *testing.T) {
	app := getFlinkTestApp()
	deployment := v1.Deployment{}
	deployment.Name = getJobManagerName(&app)
	deployments := []v1.Deployment{
		deployment,
	}
	assert.Equal(t, deployment, *getJobManagerDeployment(deployments, &app))
}

func TestGetJobManagerReplicaCount(t *testing.T) {
	app := getFlinkTestApp()
	deployment := v1.Deployment{}
	deployment.Name = getJobManagerName(&app)
	replicaCount := int32(2)
	deployment.Spec.Replicas = &replicaCount
	deployments := []v1.Deployment{
		deployment,
	}
	assert.Equal(t, int32(2), getJobManagerReplicaCount(deployments, &app))
}

func TestJobManagerCreateSuccess(t *testing.T) {
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	annotations := map[string]string{
		"key": "annotation",
	}
	app.Annotations = annotations
	expectedLabels := map[string]string{
		"app":      "app-name",
		"imageKey": "11ae1",
	}
	ctr := 0
	mockK8Cluster := testController.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		ctr += 1
		switch ctr {
		case 1:
			deployment := object.(*v1.Deployment)
			assert.Equal(t, getJobManagerName(&app), deployment.Name)
			assert.Equal(t, app.Namespace, deployment.Namespace)
			assert.Equal(t, getJobManagerPodName(&app), deployment.Spec.Template.Name)
			assert.Equal(t, annotations, deployment.Annotations)
			assert.Equal(t, annotations, deployment.Spec.Template.Annotations)
			assert.Equal(t, app.Namespace, deployment.Spec.Template.Namespace)
			assert.Equal(t, expectedLabels, deployment.Labels)
		case 2:
			service := object.(*coreV1.Service)
			assert.Equal(t, getJobManagerServiceName(&app), service.Name)
			assert.Equal(t, app.Namespace, service.Namespace)
			assert.Equal(t, map[string]string{"frontend": "app-name-jm"}, service.Spec.Selector)
		case 3:
			labels := map[string]string{
				"app": "app-name",
			}
			ingress := object.(*v1beta1.Ingress)
			assert.Equal(t, app.Name, ingress.Name)
			assert.Equal(t, app.Namespace, ingress.Namespace)
			assert.Equal(t, labels, ingress.Labels)
		}
		return nil
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
}

func TestJobManagerCreateErr(t *testing.T) {
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		return errors.New("create error")
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.EqualError(t, err, "create error")
}

func TestJobManagerCreateAlreadyExists(t *testing.T) {
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.MockK8Cluster)
	ctr := 0
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		ctr += 1
		return k8sErrors.NewAlreadyExists(schema.GroupResource{}, "")
	}
	err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Equal(t, ctr, 3)
	assert.Nil(t, err)
}
