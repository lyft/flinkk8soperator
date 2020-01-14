package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"

	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"

	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func getJMControllerForTest() JobManagerController {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	return JobManagerController{
		metrics:   newJobManagerMetrics(testScope),
		k8Cluster: &k8mock.K8Cluster{},
	}
}

func TestGetJobManagerName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-"+testAppHash+"-jm", getJobManagerName(&app, testAppHash))
}

func TestGetJobManagerPodName(t *testing.T) {
	app := getFlinkTestApp()
	assert.Equal(t, "app-name-"+testAppHash+"-jm-pod", getJobManagerPodName(&app, testAppHash))
}

func TestJobManagerCreateSuccess(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	app.Spec.JarName = testJarName
	app.Spec.EntryClass = testEntryClass
	app.Spec.ProgramArgs = testProgramArgs
	annotations := map[string]string{
		"key":                  "annotation",
		"flink-job-properties": "jarName: " + testJarName + "\nparallelism: 8\nentryClass:" + testEntryClass + "\nprogramArgs:\"" + testProgramArgs + "\"",
	}
	app.Annotations = annotations
	hash := "c3c0af0b"
	expectedLabels := map[string]string{
		"flink-app":             "app-name",
		"flink-app-hash":        hash,
		"flink-deployment-type": "jobmanager",
	}
	ctr := 0
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		switch ctr {
		case 1:
			deployment := object.(*v1.Deployment)
			assert.Equal(t, getJobManagerName(&app, hash), deployment.Name)
			assert.Equal(t, app.Namespace, deployment.Namespace)
			assert.Equal(t, getJobManagerPodName(&app, hash), deployment.Spec.Template.Name)
			assert.Equal(t, annotations, deployment.Annotations)
			assert.Equal(t, annotations, deployment.Spec.Template.Annotations)
			assert.Equal(t, app.Namespace, deployment.Spec.Template.Namespace)
			assert.Equal(t, expectedLabels, deployment.Labels)
			assert.Equal(t, int32(1), *deployment.Spec.Replicas)
			assert.Equal(t, "app-name", deployment.OwnerReferences[0].Name)
			assert.Equal(t, "flink.k8s.io/v1beta1", deployment.OwnerReferences[0].APIVersion)
			assert.Equal(t, "FlinkApplication", deployment.OwnerReferences[0].Kind)

			assert.Equal(t, "blob.server.port: 6125\njobmanager.heap.size: 1572864k\n"+
				"jobmanager.rpc.port: 6123\n"+
				"jobmanager.web.port: 8081\nmetrics.internal.query-service.port: 50101\n"+
				"query.server.port: 6124\ntaskmanager.heap.size: 524288k\n"+
				"taskmanager.numberOfTaskSlots: 16\n\n"+
				"jobmanager.rpc.address: app-name-"+hash+"\n",
				common.GetEnvVar(deployment.Spec.Template.Spec.Containers[0].Env,
					"FLINK_PROPERTIES").Value)
		case 2:
			service := object.(*coreV1.Service)
			assert.Equal(t, app.Name, service.Name)
			assert.Equal(t, app.Namespace, service.Namespace)
			assert.Equal(t, map[string]string{"flink-app": "app-name", "flink-app-hash": hash, "flink-deployment-type": "jobmanager"}, service.Spec.Selector)
		case 3:
			service := object.(*coreV1.Service)
			assert.Equal(t, app.Name+"-"+hash, service.Name)
			assert.Equal(t, "app-name", service.OwnerReferences[0].Name)
			assert.Equal(t, app.Namespace, service.Namespace)
			assert.Equal(t, map[string]string{"flink-app": "app-name", "flink-app-hash": hash, "flink-deployment-type": "jobmanager"}, service.Spec.Selector)
		case 4:
			labels := map[string]string{
				"flink-app": "app-name",
			}
			ingress := object.(*v1beta1.Ingress)
			assert.Equal(t, app.Name, ingress.Name)
			assert.Equal(t, app.Namespace, ingress.Namespace)
			assert.Equal(t, labels, ingress.Labels)
		}
		return nil
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
	assert.True(t, newlyCreated)
	assert.Equal(t, 4, ctr)
}

func TestJobManagerHACreateSuccess(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	app.Spec.JarName = testJarName
	app.Spec.EntryClass = testEntryClass
	app.Spec.ProgramArgs = testProgramArgs
	annotations := map[string]string{
		"key":                  "annotation",
		"flink-job-properties": "jarName: " + testJarName + "\nparallelism: 8\nentryClass:" + testEntryClass + "\nprogramArgs:\"" + testProgramArgs + "\"",
	}
	app.Annotations = annotations
	app.Spec.FlinkConfig = map[string]interface{}{
		"high-availability": "zookeeper",
	}
	hash := "52623ded"
	expectedLabels := map[string]string{
		"flink-app":             "app-name",
		"flink-app-hash":        hash,
		"flink-deployment-type": "jobmanager",
	}
	ctr := 0
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		switch ctr {
		case 1:
			deployment := object.(*v1.Deployment)
			assert.Equal(t, getJobManagerName(&app, hash), deployment.Name)
			assert.Equal(t, app.Namespace, deployment.Namespace)
			assert.Equal(t, getJobManagerPodName(&app, hash), deployment.Spec.Template.Name)
			assert.Equal(t, annotations, deployment.Annotations)
			assert.Equal(t, annotations, deployment.Spec.Template.Annotations)
			assert.Equal(t, app.Namespace, deployment.Spec.Template.Namespace)
			assert.Equal(t, expectedLabels, deployment.Labels)
			assert.Equal(t, int32(1), *deployment.Spec.Replicas)
			assert.Equal(t, "app-name", deployment.OwnerReferences[0].Name)
			assert.Equal(t, "flink.k8s.io/v1beta1", deployment.OwnerReferences[0].APIVersion)
			assert.Equal(t, "FlinkApplication", deployment.OwnerReferences[0].Kind)

			assert.Equal(t, "blob.server.port: 6125\nhigh-availability: zookeeper\njobmanager.heap.size: 1572864k\n"+
				"jobmanager.rpc.port: 6123\n"+
				"jobmanager.web.port: 8081\nmetrics.internal.query-service.port: 50101\n"+
				"query.server.port: 6124\ntaskmanager.heap.size: 524288k\n"+
				"taskmanager.numberOfTaskSlots: 16\n\n"+
				"high-availability.cluster-id: app-name-"+hash+"\n"+
				"jobmanager.rpc.address: $HOST_IP\n",
				common.GetEnvVar(deployment.Spec.Template.Spec.Containers[0].Env,
					"FLINK_PROPERTIES").Value)
		case 2:
			service := object.(*coreV1.Service)
			assert.Equal(t, app.Name, service.Name)
			assert.Equal(t, app.Namespace, service.Namespace)
			assert.Equal(t, map[string]string{"flink-app": "app-name", "flink-app-hash": hash, "flink-deployment-type": "jobmanager"}, service.Spec.Selector)
		case 3:
			service := object.(*coreV1.Service)
			assert.Equal(t, app.Name+"-"+hash, service.Name)
			assert.Equal(t, "app-name", service.OwnerReferences[0].Name)
			assert.Equal(t, app.Namespace, service.Namespace)
			assert.Equal(t, map[string]string{"flink-app": "app-name", "flink-app-hash": hash, "flink-deployment-type": "jobmanager"}, service.Spec.Selector)
		case 4:
			labels := map[string]string{
				"flink-app": "app-name",
			}
			ingress := object.(*v1beta1.Ingress)
			assert.Equal(t, app.Name, ingress.Name)
			assert.Equal(t, app.Namespace, ingress.Namespace)
			assert.Equal(t, labels, ingress.Labels)
		}
		return nil
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
	assert.True(t, newlyCreated)
}

func TestJobManagerSecurityContextAssignment(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	app.Spec.JarName = testJarName
	app.Spec.EntryClass = testEntryClass
	app.Spec.ProgramArgs = testProgramArgs

	fsGroup := int64(2000)
	runAsUser := int64(1000)
	runAsGroup := int64(3000)
	runAsNonRoot := bool(true)

	app.Spec.SecurityContext = &coreV1.PodSecurityContext{
		FSGroup:      &fsGroup,
		RunAsUser:    &runAsUser,
		RunAsGroup:   &runAsGroup,
		RunAsNonRoot: &runAsNonRoot,
	}

	hash := "c06b960b"

	ctr := 0
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		switch ctr {
		case 1:
			deployment := object.(*v1.Deployment)
			assert.Equal(t, getJobManagerName(&app, hash), deployment.Name)

			appSc := app.Spec.SecurityContext
			depSc := deployment.Spec.Template.Spec.SecurityContext

			assert.Equal(t, *appSc.FSGroup, *depSc.FSGroup)
			assert.Equal(t, *appSc.RunAsUser, *depSc.RunAsUser)
			assert.Equal(t, *appSc.RunAsGroup, *depSc.RunAsGroup)
			assert.Equal(t, *appSc.RunAsNonRoot, *depSc.RunAsNonRoot)
		}
		return nil
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Nil(t, err)
	assert.True(t, newlyCreated)
}

func TestJobManagerCreateErr(t *testing.T) {
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		return errors.New("create error")
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.EqualError(t, err, "create error")
	assert.False(t, newlyCreated)
}

func TestJobManagerCreateAlreadyExists(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	ctr := 0
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		return k8sErrors.NewAlreadyExists(schema.GroupResource{}, "")
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Equal(t, ctr, 4)
	assert.Nil(t, err)
	assert.False(t, newlyCreated)
}

func TestJobManagerCreateNoIngress(t *testing.T) {
	err := config.ConfigSection.SetConfig(&config.Config{
		FlinkIngressURLFormat: "",
	})
	assert.Nil(t, err)
	testController := getJMControllerForTest()
	app := getFlinkTestApp()
	mockK8Cluster := testController.k8Cluster.(*k8mock.K8Cluster)
	ctr := 0
	mockK8Cluster.CreateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		return k8sErrors.NewAlreadyExists(schema.GroupResource{}, "")
	}
	newlyCreated, err := testController.CreateIfNotExist(context.Background(), &app)
	assert.Equal(t, ctr, 3)
	assert.Nil(t, err)
	assert.False(t, newlyCreated)
}
