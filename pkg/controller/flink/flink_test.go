package flink

import (
	"context"
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"

	"github.com/lyft/flinkk8soperator/pkg/client/clientset/versioned/scheme"
	"k8s.io/client-go/tools/record"

	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	clientMock "github.com/lyft/flinkk8soperator/pkg/controller/flink/client/mock"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/mock"
	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
)

const testImage = "123.xyz.com/xx:11ae1218924428faabd9b64423fa0c332efba6b2"

// Note: if you find yourself changing this to fix a test, that should be treated as a breaking API change
const testAppHash = "752c76d3"
const testAppName = "app-name"
const testNamespace = "ns"
const testJobID = "j1"
const testFlinkVersion = "1.7"
const testJarName = "test.jar"
const testEntryClass = "com.test.MainClass"
const testProgramArgs = "--test"

func getTestFlinkController() Controller {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	recorderProvider := record.NewBroadcaster()

	return Controller{
		jobManager:    &mock.JobManagerController{},
		taskManager:   &mock.TaskManagerController{},
		k8Cluster:     &k8mock.K8Cluster{},
		flinkClient:   &clientMock.JobManagerClient{},
		metrics:       newControllerMetrics(testScope),
		eventRecorder: recorderProvider.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "test"}),
	}
}

func getFlinkTestApp() v1beta1.FlinkApplication {
	app := v1beta1.FlinkApplication{
		TypeMeta: metaV1.TypeMeta{
			Kind:       v1beta1.FlinkApplicationKind,
			APIVersion: v1beta1.SchemeGroupVersion.String(),
		},
	}
	app.Spec.Parallelism = 8
	app.Name = testAppName
	app.Namespace = testNamespace
	app.Status.JobStatus.JobID = testJobID
	app.Spec.Image = testImage
	app.Spec.FlinkVersion = testFlinkVersion

	return app
}

func TestFlinkIsClusterReady(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	labelMapVal := map[string]string{
		"flink-app":      testAppName,
		"flink-app-hash": testAppHash,
	}
	flinkApp := getFlinkTestApp()

	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		assert.Equal(t, testNamespace, namespace)
		assert.Equal(t, labelMapVal, labelMap)
		jmDeployment := FetchJobMangerDeploymentCreateObj(&flinkApp, testAppHash)
		jmDeployment.Status.AvailableReplicas = 1

		tmDeployment := FetchTaskMangerDeploymentCreateObj(&flinkApp, testAppHash)
		tmDeployment.Status.AvailableReplicas = *tmDeployment.Spec.Replicas
		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*jmDeployment,
				*tmDeployment,
			},
		}, nil
	}

	result, err := flinkControllerForTest.IsClusterReady(
		context.Background(), &flinkApp,
	)
	assert.True(t, result)
	assert.Nil(t, err)
}

func TestFlinkApplicationNotChanged(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	labelMapVal := map[string]string{
		"flink-app":      testAppName,
		"flink-app-hash": testAppHash,
	}
	flinkApp := getFlinkTestApp()
	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		assert.Equal(t, testNamespace, namespace)
		assert.Equal(t, labelMapVal, labelMap)
		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*FetchTaskMangerDeploymentCreateObj(&flinkApp, testAppHash),
				*FetchJobMangerDeploymentCreateObj(&flinkApp, testAppHash),
			},
		}, nil
	}
	cur, err := flinkControllerForTest.GetCurrentDeploymentsForApp(
		context.Background(), &flinkApp,
	)
	assert.Nil(t, err)
	assert.False(t, cur == nil)
}

func TestFlinkApplicationChanged(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	labelMapVal := map[string]string{
		"flink-app":      testAppName,
		"flink-app-hash": testAppHash,
	}
	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		assert.Equal(t, testNamespace, namespace)
		assert.Equal(t, labelMapVal, labelMap)
		return &v1.DeploymentList{}, nil
	}
	flinkApp := getFlinkTestApp()
	cur, err := flinkControllerForTest.GetCurrentDeploymentsForApp(
		context.Background(), &flinkApp,
	)
	assert.True(t, cur == nil)
	assert.Nil(t, err)
}

func testJobPropTriggersChange(t *testing.T, changeFun func(application *v1beta1.FlinkApplication)) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		assert.Equal(t, testNamespace, namespace)
		if val, ok := labelMap["flink-app"]; ok {
			assert.Equal(t, testAppName, val)
		}

		hash := HashForApplication(&flinkApp)
		if hash == labelMap[FlinkAppHash] {
			tm := FetchTaskMangerDeploymentCreateObj(&flinkApp, hash)
			jm := FetchJobMangerDeploymentCreateObj(&flinkApp, hash)
			return &v1.DeploymentList{
				Items: []v1.Deployment{
					*tm, *jm,
				},
			}, nil
		}

		return &v1.DeploymentList{}, nil
	}

	newApp := flinkApp.DeepCopy()
	changeFun(newApp)
	cur, err := flinkControllerForTest.GetCurrentDeploymentsForApp(
		context.Background(), newApp,
	)
	assert.True(t, cur == nil)
	assert.Nil(t, err)
}

func TestFlinkApplicationChangedJobProps(t *testing.T) {
	testJobPropTriggersChange(t, func(app *v1beta1.FlinkApplication) {
		app.Spec.Parallelism = 3
	})

	testJobPropTriggersChange(t, func(app *v1beta1.FlinkApplication) {
		app.Spec.JarName = "another.jar"
	})

	testJobPropTriggersChange(t, func(app *v1beta1.FlinkApplication) {
		app.Spec.ProgramArgs = "--test-change"
	})

	testJobPropTriggersChange(t, func(app *v1beta1.FlinkApplication) {
		app.Spec.EntryClass = "com.another.Class"
	})
}

func TestFlinkIsServiceReady(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetClusterOverviewFunc = func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.ClusterOverviewResponse{
			TaskManagerCount:  3,
			NumberOfTaskSlots: flinkApp.Spec.Parallelism + 6,
			SlotsAvailable:    flinkApp.Spec.Parallelism + 6,
		}, nil
	}
	isReady, err := flinkControllerForTest.IsServiceReady(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.True(t, isReady)
}

func TestFlinkIsServiceReadyErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetClusterOverviewFunc = func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return nil, errors.New("Get cluster failed")
	}
	isReady, err := flinkControllerForTest.IsServiceReady(context.Background(), &flinkApp, "hash")
	assert.EqualError(t, err, "Get cluster failed")
	assert.False(t, isReady)
}

func TestFlinkGetSavepointStatus(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	flinkApp.Status.SavepointTriggerID = "t1"

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.CheckSavepointStatusFunc = func(ctx context.Context, url string, jobID, triggerID string) (*client.SavepointResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		assert.Equal(t, jobID, testJobID)
		assert.Equal(t, triggerID, "t1")
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointInProgress,
			},
		}, nil
	}
	status, err := flinkControllerForTest.GetSavepointStatus(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.NotNil(t, status)

	assert.Equal(t, client.SavePointInProgress, status.SavepointStatus.Status)
}

func TestFlinkGetSavepointStatusErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.CheckSavepointStatusFunc = func(ctx context.Context, url string, jobID, triggerID string) (*client.SavepointResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		assert.Equal(t, jobID, testJobID)
		return nil, errors.New("Savepoint error")
	}
	status, err := flinkControllerForTest.GetSavepointStatus(context.Background(), &flinkApp, "hash")
	assert.Nil(t, status)
	assert.NotNil(t, err)

	assert.EqualError(t, err, "Savepoint error")
}

func TestGetActiveJob(t *testing.T) {
	job := client.FlinkJob{
		Status: client.Running,
		JobID:  "j1",
	}
	jobs := []client.FlinkJob{
		job,
	}
	activeJob := GetActiveFlinkJobs(jobs)
	assert.Equal(t, 1, len(activeJob))
	assert.Equal(t, job, activeJob[0])
}

func TestGetActiveJobFinished(t *testing.T) {
	job := client.FlinkJob{
		Status: client.Finished,
		JobID:  "j1",
	}
	jobs := []client.FlinkJob{
		job,
	}
	activeJob := GetActiveFlinkJobs(jobs)
	assert.Equal(t, 1, len(activeJob))
	assert.Equal(t, job, activeJob[0])
}

func TestGetActiveJobNil(t *testing.T) {
	job := client.FlinkJob{
		Status: client.Canceled,
		JobID:  "j1",
	}
	jobs := []client.FlinkJob{
		job,
	}
	activeJob := GetActiveFlinkJobs(jobs)
	assert.Equal(t, 0, len(activeJob))
}

func TestGetActiveJobEmpty(t *testing.T) {
	jobs := []client.FlinkJob{}
	activeJob := GetActiveFlinkJobs(jobs)
	assert.Equal(t, 0, len(activeJob))
}

func TestDeleteOldResources(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	app := getFlinkTestApp()

	jmDeployment := FetchTaskMangerDeploymentCreateObj(&app, "oldhash")
	tmDeployment := FetchJobMangerDeploymentCreateObj(&app, "oldhash")
	service := FetchJobManagerServiceCreateObj(&app, "oldhash")
	service.Labels[FlinkAppHash] = "oldhash"
	service.Name = VersionedJobManagerServiceName(&app, "oldhash")

	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)

	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		curJobmanager := FetchJobMangerDeploymentCreateObj(&app, testAppHash)
		curTaskmanager := FetchTaskMangerDeploymentCreateObj(&app, testAppHash)
		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*jmDeployment,
				*tmDeployment,
				*curJobmanager,
				*curTaskmanager,
			},
		}, nil
	}

	mockK8Cluster.GetServicesWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*corev1.ServiceList, error) {
		curService := FetchJobManagerServiceCreateObj(&app, testAppHash)
		curService.Labels[FlinkAppHash] = testAppHash
		curService.Name = VersionedJobManagerServiceName(&app, testAppHash)

		generic := FetchJobManagerServiceCreateObj(&app, testAppHash)
		return &corev1.ServiceList{
			Items: []corev1.Service{
				*service,
				*curService,
				*generic,
			},
		}, nil
	}

	ctr := 0
	mockK8Cluster.DeleteK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		ctr++
		switch ctr {
		case 1:
			assert.Equal(t, jmDeployment, object)
		case 2:
			assert.Equal(t, tmDeployment, object)
		case 3:
			assert.Equal(t, service, object)
		}
		return nil
	}

	err := flinkControllerForTest.DeleteOldResourcesForApp(context.Background(), &app)
	assert.Equal(t, 3, ctr)
	assert.Nil(t, err)
}

func TestCreateCluster(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	mockJobManager := flinkControllerForTest.jobManager.(*mock.JobManagerController)
	mockTaskManager := flinkControllerForTest.taskManager.(*mock.TaskManagerController)

	mockJobManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockTaskManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return true, nil
	}
	err := flinkControllerForTest.CreateCluster(context.Background(), &flinkApp)
	assert.Nil(t, err)
}

func TestCreateClusterJmErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	mockJobManager := flinkControllerForTest.jobManager.(*mock.JobManagerController)
	mockTaskManager := flinkControllerForTest.taskManager.(*mock.TaskManagerController)

	mockJobManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return false, errors.New("jm failed")
	}
	mockTaskManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		assert.False(t, true)
		return false, nil
	}
	err := flinkControllerForTest.CreateCluster(context.Background(), &flinkApp)
	assert.EqualError(t, err, "jm failed")
}

func TestCreateClusterTmErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	mockJobManager := flinkControllerForTest.jobManager.(*mock.JobManagerController)
	mockTaskManager := flinkControllerForTest.taskManager.(*mock.TaskManagerController)

	mockJobManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockTaskManager.CreateIfNotExistFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return false, errors.New("tm failed")
	}
	err := flinkControllerForTest.CreateCluster(context.Background(), &flinkApp)
	assert.EqualError(t, err, "tm failed")
}

func TestStartFlinkJob(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	flinkApp.Spec.Parallelism = 4
	flinkApp.Spec.ProgramArgs = "args"
	flinkApp.Spec.EntryClass = "class"
	flinkApp.Spec.JarName = "jar-name"
	flinkApp.Spec.SavepointPath = "location//"
	flinkApp.Spec.FlinkVersion = "1.7"

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.SubmitJobFunc = func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		assert.Equal(t, jarID, "jar-name")
		assert.Equal(t, submitJobRequest.Parallelism, int32(4))
		assert.Equal(t, submitJobRequest.ProgramArgs, "args")
		assert.Equal(t, submitJobRequest.EntryClass, "class")
		assert.Equal(t, submitJobRequest.SavepointPath, "location//")
		assert.Equal(t, submitJobRequest.AllowNonRestoredState, false)

		return &client.SubmitJobResponse{
			JobID: testJobID,
		}, nil
	}
	jobID, err := flinkControllerForTest.StartFlinkJob(context.Background(), &flinkApp, "hash",
		flinkApp.Spec.JarName, flinkApp.Spec.Parallelism, flinkApp.Spec.EntryClass, flinkApp.Spec.ProgramArgs,
		flinkApp.Spec.AllowNonRestoredState, "location//")
	assert.Nil(t, err)
	assert.Equal(t, jobID, testJobID)
}

func TestStartFlinkJobAllowNonRestoredState(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	flinkApp.Spec.AllowNonRestoredState = true

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.SubmitJobFunc = func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
		assert.Equal(t, submitJobRequest.AllowNonRestoredState, true)

		return &client.SubmitJobResponse{
			JobID: testJobID,
		}, nil
	}
	jobID, err := flinkControllerForTest.StartFlinkJob(context.Background(), &flinkApp, "hash",
		flinkApp.Spec.JarName, flinkApp.Spec.Parallelism, flinkApp.Spec.EntryClass, flinkApp.Spec.ProgramArgs,
		flinkApp.Spec.AllowNonRestoredState, "")
	assert.Nil(t, err)
	assert.Equal(t, jobID, testJobID)
}

func TestStartFlinkJobEmptyJobID(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.SubmitJobFunc = func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {

		return &client.SubmitJobResponse{}, nil
	}
	jobID, err := flinkControllerForTest.StartFlinkJob(context.Background(), &flinkApp, "hash",
		flinkApp.Spec.JarName, flinkApp.Spec.Parallelism, flinkApp.Spec.EntryClass, flinkApp.Spec.ProgramArgs,
		flinkApp.Spec.AllowNonRestoredState, "")
	assert.EqualError(t, err, "unable to submit job: invalid job id")
	assert.Empty(t, jobID)
}

func TestStartFlinkJobErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.SubmitJobFunc = func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
		return nil, errors.New("submit error")
	}
	jobID, err := flinkControllerForTest.StartFlinkJob(context.Background(), &flinkApp, "hash",
		flinkApp.Spec.JarName, flinkApp.Spec.Parallelism, flinkApp.Spec.EntryClass, flinkApp.Spec.ProgramArgs,
		flinkApp.Spec.AllowNonRestoredState, "")
	assert.EqualError(t, err, "submit error")
	assert.Empty(t, jobID)
}

func TestCancelWithSavepoint(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.CancelJobWithSavepointFunc = func(ctx context.Context, url string, jobID string) (string, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		assert.Equal(t, jobID, testJobID)
		return "t1", nil
	}
	triggerID, err := flinkControllerForTest.CancelWithSavepoint(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.Equal(t, triggerID, "t1")
}

func TestCancelWithSavepointErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.CancelJobWithSavepointFunc = func(ctx context.Context, url string, jobID string) (string, error) {
		return "", errors.New("cancel error")
	}
	triggerID, err := flinkControllerForTest.CancelWithSavepoint(context.Background(), &flinkApp, "hash")
	assert.EqualError(t, err, "cancel error")
	assert.Empty(t, triggerID)
}

func TestGetJobsForApplication(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetJobsFunc = func(ctx context.Context, url string) (*client.GetJobsResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.GetJobsResponse{
			Jobs: []client.FlinkJob{
				{
					JobID: testJobID,
				},
			},
		}, nil
	}
	jobs, err := flinkControllerForTest.GetJobsForApplication(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, jobs[0].JobID, testJobID)
}

func TestGetJobsForApplicationErr(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetJobsFunc = func(ctx context.Context, url string) (*client.GetJobsResponse, error) {
		return nil, errors.New("get jobs error")
	}
	jobs, err := flinkControllerForTest.GetJobsForApplication(context.Background(), &flinkApp, "hash")
	assert.EqualError(t, err, "get jobs error")
	assert.Nil(t, jobs)
}

func TestFindExternalizedCheckpoint(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	flinkApp.Status.JobStatus.JobID = "jobid"

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetLatestCheckpointFunc = func(ctx context.Context, url string, jobId string) (*client.CheckpointStatistics, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		assert.Equal(t, "jobid", jobId)
		return &client.CheckpointStatistics{
			TriggerTimestamp: time.Now().Unix(),
			ExternalPath:     "/tmp/checkpoint",
		}, nil
	}

	checkpoint, err := flinkControllerForTest.FindExternalizedCheckpoint(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.Equal(t, "/tmp/checkpoint", checkpoint)
}

func TestClusterStatusUpdated(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	labelMapVal := map[string]string{
		"flink-app":      testAppName,
		"flink-app-hash": testAppHash,
	}
	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		assert.Equal(t, testNamespace, namespace)
		assert.Equal(t, labelMapVal, labelMap)

		tmDeployment := FetchTaskMangerDeploymentCreateObj(&flinkApp, testAppHash)
		tmDeployment.Status.AvailableReplicas = *tmDeployment.Spec.Replicas
		tmDeployment.Status.Replicas = *tmDeployment.Spec.Replicas

		jmDeployment := FetchJobMangerDeploymentCreateObj(&flinkApp, testAppHash)

		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*tmDeployment,
				*jmDeployment,
			},
		}, nil
	}

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetClusterOverviewFunc = func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.ClusterOverviewResponse{
			NumberOfTaskSlots: 1,
			SlotsAvailable:    0,
			TaskManagerCount:  1,
		}, nil
	}

	mockJmClient.GetTaskManagersFunc = func(ctx context.Context, url string) (*client.TaskManagersResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.TaskManagersResponse{
			TaskManagers: []client.TaskManagerStats{
				{
					TimeSinceLastHeartbeat: time.Now().UnixNano() / int64(time.Millisecond),
					SlotsNumber:            3,
					FreeSlots:              0,
				},
			},
		}, nil
	}

	_, err = flinkControllerForTest.CompareAndUpdateClusterStatus(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.Equal(t, int32(1), flinkApp.Status.ClusterStatus.NumberOfTaskSlots)
	assert.Equal(t, int32(0), flinkApp.Status.ClusterStatus.AvailableTaskSlots)
	assert.Equal(t, int32(1), flinkApp.Status.ClusterStatus.HealthyTaskManagers)
	assert.Equal(t, v1beta1.Green, flinkApp.Status.ClusterStatus.Health)
	assert.Equal(t, "app-name.lyft.xyz/#/overview", flinkApp.Status.ClusterStatus.ClusterOverviewURL)

}

func TestNoClusterStatusChange(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	flinkApp.Status.ClusterStatus.NumberOfTaskSlots = int32(1)
	flinkApp.Status.ClusterStatus.AvailableTaskSlots = int32(0)
	flinkApp.Status.ClusterStatus.HealthyTaskManagers = int32(1)
	flinkApp.Status.ClusterStatus.Health = v1beta1.Green
	flinkApp.Status.ClusterStatus.NumberOfTaskManagers = int32(1)
	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		tmDeployment := FetchTaskMangerDeploymentCreateObj(&flinkApp, testAppHash)
		tmDeployment.Status.AvailableReplicas = 1
		tmDeployment.Status.Replicas = 1

		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*tmDeployment,
			},
		}, nil
	}

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetClusterOverviewFunc = func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.ClusterOverviewResponse{
			NumberOfTaskSlots: 1,
			SlotsAvailable:    0,
			TaskManagerCount:  1,
		}, nil
	}

	mockJmClient.GetTaskManagersFunc = func(ctx context.Context, url string) (*client.TaskManagersResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.TaskManagersResponse{
			TaskManagers: []client.TaskManagerStats{
				{
					TimeSinceLastHeartbeat: time.Now().UnixNano() / int64(time.Millisecond),
					SlotsNumber:            3,
					FreeSlots:              0,
				},
			},
		}, nil
	}
	hasClusterStatusChanged, err := flinkControllerForTest.CompareAndUpdateClusterStatus(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)
	assert.False(t, hasClusterStatusChanged)
}

func TestHealthyTaskmanagers(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()

	hash := getCurrentHash(&flinkApp)
	mockK8Cluster := flinkControllerForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetDeploymentsWithLabelFunc = func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
		tmDeployment := FetchTaskMangerDeploymentCreateObj(&flinkApp, hash)
		tmDeployment.Status.AvailableReplicas = 1
		tmDeployment.Status.Replicas = 1

		jmDeployment := FetchJobMangerDeploymentCreateObj(&flinkApp, hash)

		return &v1.DeploymentList{
			Items: []v1.Deployment{
				*tmDeployment,
				*jmDeployment,
			},
		}, nil
	}

	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)

	mockJmClient.GetClusterOverviewFunc = func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
		assert.Equal(t, url, "http://app-name-"+hash+".ns:8081")
		return &client.ClusterOverviewResponse{
			NumberOfTaskSlots: 1,
			SlotsAvailable:    0,
			TaskManagerCount:  1,
		}, nil
	}

	mockJmClient.GetTaskManagersFunc = func(ctx context.Context, url string) (*client.TaskManagersResponse, error) {
		assert.Equal(t, url, "http://app-name-"+hash+".ns:8081")
		return &client.TaskManagersResponse{
			TaskManagers: []client.TaskManagerStats{
				{
					// 1 day old
					TimeSinceLastHeartbeat: time.Now().AddDate(0, 0, -1).UnixNano() / int64(time.Millisecond),
					SlotsNumber:            3,
					FreeSlots:              0,
				},
			},
		}, nil
	}

	_, err := flinkControllerForTest.CompareAndUpdateClusterStatus(context.Background(), &flinkApp, hash)
	assert.Nil(t, err)
	assert.Equal(t, int32(1), flinkApp.Status.ClusterStatus.NumberOfTaskSlots)
	assert.Equal(t, int32(0), flinkApp.Status.ClusterStatus.AvailableTaskSlots)
	assert.Equal(t, int32(0), flinkApp.Status.ClusterStatus.HealthyTaskManagers)
	assert.Equal(t, v1beta1.Yellow, flinkApp.Status.ClusterStatus.Health)

}

func TestJobStatusUpdated(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	flinkControllerForTest := getTestFlinkController()
	flinkApp := getFlinkTestApp()
	startTime := metaV1.Now().UnixNano() / int64(time.Millisecond)
	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)
	mockJmClient.GetJobOverviewFunc = func(ctx context.Context, url string, jobID string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.FlinkJobOverview{
			JobID:     "abc",
			State:     client.Running,
			StartTime: startTime,
			Vertices: []client.FlinkJobVertex{
				{
					Status: "RUNNING",
					Tasks: map[string]int64{
						"SCHEDULED":   4,
						"FINISHED":    0,
						"CANCELED":    0,
						"CANCELING":   0,
						"DEPLOYING":   1,
						"RUNNING":     2,
						"RECONCILING": 0,
						"FAILED":      0,
						"CREATED":     0,
					},
				},
			},
		}, nil
	}

	mockJmClient.GetCheckpointCountsFunc = func(ctx context.Context, url string, jobID string) (*client.CheckpointResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.CheckpointResponse{
			Counts: map[string]int32{
				"restored":  1,
				"completed": 4,
				"failed":    0,
			},
			Latest: client.LatestCheckpoints{
				Restored: &client.CheckpointStatistics{
					RestoredTimeStamp: startTime,
					ExternalPath:      "/test/externalpath",
				},

				Completed: &client.CheckpointStatistics{
					LatestAckTimestamp: startTime,
				},
			},
		}, nil
	}

	flinkApp.Status.JobStatus.JobID = "abc"
	expectedTime := metaV1.NewTime(time.Unix(startTime/1000, 0))
	_, err = flinkControllerForTest.CompareAndUpdateJobStatus(context.Background(), &flinkApp, "hash")
	assert.Nil(t, err)

	assert.Equal(t, v1beta1.Running, flinkApp.Status.JobStatus.State)
	assert.Equal(t, &expectedTime, flinkApp.Status.JobStatus.StartTime)
	assert.Equal(t, v1beta1.Yellow, flinkApp.Status.JobStatus.Health)

	assert.Equal(t, int32(0), flinkApp.Status.JobStatus.FailedCheckpointCount)
	assert.Equal(t, int32(4), flinkApp.Status.JobStatus.CompletedCheckpointCount)
	assert.Equal(t, int32(1), flinkApp.Status.JobStatus.JobRestartCount)
	assert.Equal(t, &expectedTime, flinkApp.Status.JobStatus.RestoreTime)

	assert.Equal(t, "/test/externalpath", flinkApp.Status.JobStatus.RestorePath)
	assert.Equal(t, &expectedTime, flinkApp.Status.JobStatus.LastCheckpointTime)
	assert.Equal(t, "app-name.lyft.xyz/#/jobs/abc", flinkApp.Status.JobStatus.JobOverviewURL)

	assert.Equal(t, int32(2), flinkApp.Status.JobStatus.RunningTasks)
	assert.Equal(t, int32(7), flinkApp.Status.JobStatus.TotalTasks)

}

func TestNoJobStatusChange(t *testing.T) {
	err := config.ConfigSection.SetConfig(&config.Config{
		FlinkIngressURLFormat: "",
	})
	assert.Nil(t, err)
	flinkControllerForTest := getTestFlinkController()
	constTime := time.Now().UnixNano() / int64(time.Millisecond)
	metaTime := metaV1.NewTime(time.Unix(constTime/1000, 0))
	app1 := getFlinkTestApp()
	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)

	app1.Status.JobStatus.State = v1beta1.Running
	app1.Status.JobStatus.StartTime = &metaTime
	app1.Status.JobStatus.LastCheckpointTime = &metaTime
	app1.Status.JobStatus.CompletedCheckpointCount = int32(4)
	app1.Status.JobStatus.JobRestartCount = int32(1)
	app1.Status.JobStatus.FailedCheckpointCount = int32(0)
	app1.Status.JobStatus.Health = v1beta1.Green
	app1.Status.JobStatus.RestoreTime = &metaTime
	app1.Status.JobStatus.RestorePath = "/test/externalpath"
	app1.Status.JobStatus.JobOverviewURL = ""

	mockJmClient.GetJobOverviewFunc = func(ctx context.Context, url string, jobID string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.FlinkJobOverview{
			JobID:     "j1",
			State:     client.Running,
			StartTime: constTime,
		}, nil
	}

	mockJmClient.GetCheckpointCountsFunc = func(ctx context.Context, url string, jobID string) (*client.CheckpointResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.CheckpointResponse{
			Counts: map[string]int32{
				"restored":  1,
				"completed": 4,
				"failed":    0,
			},
			Latest: client.LatestCheckpoints{
				Restored: &client.CheckpointStatistics{
					RestoredTimeStamp: constTime,
					ExternalPath:      "/test/externalpath",
				},

				Completed: &client.CheckpointStatistics{
					LatestAckTimestamp: constTime,
				},
			},
		}, nil
	}
	hasJobStatusChanged, err := flinkControllerForTest.CompareAndUpdateJobStatus(context.Background(), &app1, "hash")
	assert.Nil(t, err)
	assert.False(t, hasJobStatusChanged)

}

func TestGetAndUpdateJobStatusHealth(t *testing.T) {
	flinkControllerForTest := getTestFlinkController()
	lastFailedTime := metaV1.NewTime(time.Now().Add(-10 * time.Second))
	app1 := getFlinkTestApp()
	mockJmClient := flinkControllerForTest.flinkClient.(*clientMock.JobManagerClient)

	app1.Status.JobStatus.State = v1beta1.Failing
	app1.Status.JobStatus.LastFailingTime = &lastFailedTime

	mockJmClient.GetJobOverviewFunc = func(ctx context.Context, url string, jobID string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.FlinkJobOverview{
			JobID:     "abc",
			State:     client.Running,
			StartTime: metaV1.Now().UnixNano() / int64(time.Millisecond),
		}, nil
	}

	mockJmClient.GetCheckpointCountsFunc = func(ctx context.Context, url string, jobID string) (*client.CheckpointResponse, error) {
		assert.Equal(t, url, "http://app-name-hash.ns:8081")
		return &client.CheckpointResponse{
			Counts: map[string]int32{
				"restored":  1,
				"completed": 4,
				"failed":    0,
			},
		}, nil
	}
	_, err := flinkControllerForTest.CompareAndUpdateJobStatus(context.Background(), &app1, "hash")
	assert.Nil(t, err)
	// Job is in a RUNNING state but was in a FAILING state in the last 1 minute, so we expect
	// JobStatus.Health to be Red
	assert.Equal(t, app1.Status.JobStatus.Health, v1beta1.Red)

}

func TestMaxCheckpointRestoreAge(t *testing.T) {
	// Test invalid checkpoint that cannot be recovered from. Recovery age is 10 minutes
	assert.True(t, isCheckpointOldToRecover(time.Now().Unix()-700, 600))

	// Test valid checkpoint that can be recovered. Recovery age is 10 minutes
	assert.False(t, isCheckpointOldToRecover(time.Now().Unix()-100, 600))
}
