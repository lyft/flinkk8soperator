package flinkapplication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/mock"
	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
)

const testSavepointLocation = "location"

func getTestStateMachine() FlinkStateMachine {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	return FlinkStateMachine{
		flinkController: &mock.FlinkController{},
		k8Cluster:       &k8mock.K8Cluster{},
		clock:           &clock.FakeClock{},
		metrics:         newStateMachineMetrics(testScope),
		retryHandler:    &mock.RetryHandler{},
	}
}

func testFlinkDeployment(app *v1beta1.FlinkApplication) common.FlinkDeployment {
	hash := flink.HashForApplication(app)
	return common.FlinkDeployment{
		Jobmanager:  flink.FetchJobMangerDeploymentCreateObj(app, hash),
		Taskmanager: flink.FetchTaskMangerDeploymentCreateObj(app, hash),
		Hash:        hash,
	}
}

func TestHandleNewOrCreate(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationClusterStarting, application.Status.Phase)
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{},
	})
	assert.Nil(t, err)
}

func TestHandleStartingClusterStarting(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationClusterStarting,
		},
	})
	assert.Nil(t, err)
}

func TestHandleNewOrCreateWithSavepointDisabled(t *testing.T) {
	updateInvoked := false
	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{
			SavepointDisabled: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationClusterStarting,
			DeployHash: "old-hash",
		},
	}

	stateMachineForTest := getTestStateMachine()

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (b bool, e error) {
		return true, nil
	}
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		fd := testFlinkDeployment(application)
		fd.Taskmanager.Status.AvailableReplicas = 2
		fd.Jobmanager.Status.AvailableReplicas = 1
		return &fd, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		return nil
	}

	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationCancelling, application.Status.Phase)
		updateInvoked = true
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.True(t, updateInvoked)
}

func TestHandleApplicationCancel(t *testing.T) {
	jobID := "j1"
	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{
			SavepointDisabled: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationCancelling,
			DeployHash: "old-hash",
		},
	}

	cancelInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, "old-hash", hash)
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: client.Running,
		}, nil
	}

	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (e error) {
		assert.Equal(t, "old-hash", hash)
		cancelInvoked = true

		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, application.Status.Phase)
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.True(t, cancelInvoked)
}

func TestHandleApplicationCancelFailedWithMaxRetries(t *testing.T) {

	retryableErr := client.GetRetryableError(errors.New("blah"), "ForceCancelJob", "FAILED", 5)
	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{
			SavepointDisabled: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationCancelling,
			DeployHash:    "old-hash",
			LastSeenError: retryableErr.(*v1beta1.FlinkApplicationError),
		},
	}

	app.Status.LastSeenError.LastErrorUpdateTime = nil
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error {
		// given we maxed out on retries, we should never have come here
		assert.False(t, true)
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		updateInvoked = true
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationRollingBackJob, application.Status.Phase)
		return nil
	}

	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		return true
	}
	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		return false
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.True(t, updateInvoked)
}

func TestHandleStartingDual(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (b bool, e error) {
		return true, nil
	}

	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		fd := testFlinkDeployment(application)
		fd.Taskmanager.Status.AvailableReplicas = 2
		fd.Jobmanager.Status.AvailableReplicas = 1
		return &fd, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationSavepointing, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationClusterStarting,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingInitialDeploy(t *testing.T) {
	// on the initial deploy we should skip savepointing and go straight to SubmittingJob
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (s string, e error) {
		// should not be called
		assert.False(t, true)
		return "", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, application.Status.Phase)
		updateInvoked = true
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationSavepointing,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingDual(t *testing.T) {
	app := v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSavepointing,
			DeployHash: "old-hash",
		},
	}

	cancelInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (s string, e error) {
		assert.Equal(t, "old-hash", hash)
		cancelInvoked = true

		return "trigger", nil
	}

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		assert.Equal(t, "old-hash", hash)
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: testSavepointLocation,
			},
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 0
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		if updateCount == 0 {
			assert.Equal(t, "trigger", application.Status.SavepointTriggerID)
		} else {
			assert.Equal(t, testSavepointLocation, application.Status.SavepointPath)
			assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, application.Status.Phase)
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.Equal(t, updateCount, 2)
	assert.True(t, cancelInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingFailed(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:              v1beta1.FlinkApplicationSavepointing,
			DeployHash:         "blah",
			SavepointTriggerID: "trigger",
		},
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Empty(t, application.Status.SavepointPath)
		assert.Equal(t, v1beta1.FlinkApplicationRecovering, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestRestoreFromExternalizedCheckpoint(t *testing.T) {
	updateInvoked := false

	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:              v1beta1.FlinkApplicationRecovering,
			DeployHash:         "blah",
			SavepointTriggerID: "trigger",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.FindExternalizedCheckpointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
		return "/tmp/checkpoint", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, "/tmp/checkpoint", application.Status.SavepointPath)
		assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestSubmittingToRunning(t *testing.T) {
	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSubmittingJob,
			DeployHash: "old-hash",
		},
	}
	appHash := flink.HashForApplication(&app)

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		return true, nil
	}

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, appHash, hash)
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: client.Running,
		}, nil
	}

	startCount := 0
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error) {

		assert.Equal(t, appHash, hash)
		assert.Equal(t, app.Spec.JarName, jarName)
		assert.Equal(t, app.Spec.Parallelism, parallelism)
		assert.Equal(t, app.Spec.EntryClass, entryClass)
		assert.Equal(t, app.Spec.ProgramArgs, programArgs)
		assert.Equal(t, app.Spec.AllowNonRestoredState, allowNonRestoredState)
		assert.Equal(t, app.Status.SavepointPath, savepointPath)

		startCount++
		return jobID, nil
	}

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		assert.Equal(t, appHash, hash)
		if startCount > 0 {
			return []client.FlinkJob{
				{
					JobID:  jobID,
					Status: client.Running,
				},
			}, nil
		}
		return nil, nil
	}

	podSelector := "wc7ydhul"

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {
		assert.Equal(t, "flink", namespace)
		assert.Equal(t, "test-app", name)

		getServiceCount++
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"pod-deployment-selector": "blah",
				},
			},
		}, nil
	}

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		if updateCount == 0 {
			// update to the service
			service := object.(*v1.Service)
			assert.Equal(t, podSelector, service.Spec.Selector["pod-deployment-selector"])
		} else if updateCount == 1 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		}

		updateCount++
		return nil
	}

	statusUpdateCount := 0
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		if statusUpdateCount == 0 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobID, mockFlinkController.GetLatestJobID(ctx, application))
		} else if statusUpdateCount == 1 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, appHash, application.Status.DeployHash)
			jobStatus := mockFlinkController.GetLatestJobStatus(ctx, application)
			assert.Equal(t, app.Spec.JarName, jobStatus.JarName)
			assert.Equal(t, app.Spec.Parallelism, jobStatus.Parallelism)
			assert.Equal(t, app.Spec.EntryClass, jobStatus.EntryClass)
			assert.Equal(t, app.Spec.ProgramArgs, jobStatus.ProgramArgs)
			assert.Equal(t, v1beta1.FlinkApplicationRunning, application.Status.Phase)
		}
		statusUpdateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.Equal(t, 1, startCount)
	assert.Equal(t, 3, updateCount)
	assert.Equal(t, 2, statusUpdateCount)
}

func TestHandleNilDeployments(t *testing.T) {
	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSubmittingJob,
			DeployHash: "old-hash",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		return true, nil
	}

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: client.Running,
		}, nil
	}

	startCount := 0
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error) {
		startCount++
		return jobID, nil
	}

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		if startCount > 0 {
			return []client.FlinkJob{
				{
					JobID:  jobID,
					Status: client.Running,
				},
			}, nil
		}
		return nil, nil
	}

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		return nil, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {
		assert.Equal(t, "flink", namespace)
		assert.Equal(t, "test-app", name)

		getServiceCount++
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"pod-deployment-selector": "blah",
				},
			},
		}, nil
	}

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		updateCount++
		return nil
	}

	statusUpdateCount := 0
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		statusUpdateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Error(t, err)
}

func TestHandleApplicationRunning(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		fd := testFlinkDeployment(application)
		return &fd, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		assert.True(t, false)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationRunning,
		},
	})
	assert.Nil(t, err)
}

func TestRunningToClusterStarting(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		return nil, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationUpdating, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationRunning,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestRollingBack(t *testing.T) {
	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationRollingBackJob,
			DeployHash:    "old-hash",
			SavepointPath: "file:///savepoint",
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				v1beta1.FlinkApplicationVersionStatus{
					JobStatus: v1beta1.FlinkJobStatus{
						JarName:     "old-job.jar",
						Parallelism: 10,
						EntryClass:  "com.my.OldClass",
						ProgramArgs: "--no-test",
					},
				},
			},
		},
	}
	appHash := flink.HashForApplication(&app)

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		assert.Equal(t, "old-hash", hash)
		return true, nil
	}

	startCalled := false
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error) {

		startCalled = true
		assert.Equal(t, "old-hash", hash)
		jobStatus := mockFlinkController.GetLatestJobStatus(ctx, application)
		assert.Equal(t, jobStatus.JarName, jarName)
		assert.Equal(t, jobStatus.Parallelism, parallelism)
		assert.Equal(t, jobStatus.EntryClass, entryClass)
		assert.Equal(t, jobStatus.ProgramArgs, programArgs)
		assert.Equal(t, jobStatus.AllowNonRestoredState, allowNonRestoredState)
		assert.Equal(t, app.Status.SavepointPath, savepointPath)
		return jobID, nil
	}

	getCount := 0
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		assert.Equal(t, "old-hash", hash)
		var res []client.FlinkJob
		if getCount == 1 {
			res = []client.FlinkJob{
				{
					JobID:  jobID,
					Status: client.Running,
				}}
		}
		getCount++
		return res, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {
		assert.Equal(t, "flink", namespace)
		assert.Equal(t, "test-app", name)

		hash := appHash
		if getServiceCount > 0 {
			hash = "old-hash"
		}

		getServiceCount++
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": hash,
				},
			},
		}, nil
	}

	podSelector := "a7ldf128"

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		if updateCount == 0 {
			// update to the service
			service := object.(*v1.Service)
			assert.Equal(t, podSelector, service.Spec.Selector["pod-deployment-selector"])
		} else if updateCount == 1 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		}

		updateCount++
		return nil
	}

	statusUpdated := false
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		if !statusUpdated {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, appHash, application.Status.FailedDeployHash)
			assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, application.Status.Phase)
			statusUpdated = true
		}
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.True(t, startCalled)
	assert.True(t, statusUpdated)
	assert.Equal(t, 2, updateCount)
}

func TestIsApplicationStuck(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	stateMachineForTest.clock.(*clock.FakeClock).SetTime(time.Now())
	retryableErr := client.GetRetryableError(errors.New("blah"), "GetClusterOverview", "FAILED", 3)
	failFastError := client.GetNonRetryableError(errors.New("blah"), "SubmitJob", "400BadRequest")

	app := &v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationClusterStarting,
			DeployHash:    "prevhash",
			LastSeenError: retryableErr.(*v1beta1.FlinkApplicationError),
		},
	}
	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return ferr.IsRetryable
	}

	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return retryCount <= ferr.MaxRetries
	}

	mockRetryHandler.IsErrorFailFastFunc = func(err error) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return ferr.IsFailFast
	}
	// Retryable error
	shouldRollback, _ := stateMachineForTest.shouldRollback(context.Background(), app)
	assert.False(t, shouldRollback)
	// Retryable errors don't get reset until all retries are exhausted
	assert.NotNil(t, app.Status.LastSeenError)
	// the rollback loop does not update retry counts.
	assert.Equal(t, int32(0), app.Status.RetryCount)

	// Retryable error with retries exhausted
	app.Status.RetryCount = 100
	app.Status.LastSeenError = retryableErr.(*v1beta1.FlinkApplicationError)
	shouldRollback, _ = stateMachineForTest.shouldRollback(context.Background(), app)
	assert.True(t, shouldRollback, app)
	assert.Nil(t, app.Status.LastSeenError)
	assert.Equal(t, int32(100), app.Status.RetryCount)

	// Fail fast error
	app.Status.RetryCount = 0
	app.Status.LastSeenError = failFastError.(*v1beta1.FlinkApplicationError)
	shouldRollback, _ = stateMachineForTest.shouldRollback(context.Background(), app)
	assert.True(t, shouldRollback)
	assert.Nil(t, app.Status.LastSeenError)
	assert.Equal(t, int32(0), app.Status.RetryCount)

}

func TestDeleteWithSavepoint(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationDeleting,
			DeployHash: "deployhash",
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				v1beta1.FlinkApplicationVersionStatus{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: jobID,
					},
				},
			},
		},
	}

	triggerID := "t1"
	savepointPath := "s3:///path/to/savepoint"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (string, error) {
		return triggerID, nil
	}

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs *client.FlinkJobOverview, err error) {
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: "RUNNING",
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateStatusCount := 0
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationDeleting, application.Status.Phase)

		if updateStatusCount == 0 {
			assert.Equal(t, triggerID, application.Status.SavepointTriggerID)
		} else if updateStatusCount == 1 {
			assert.NotNil(t, application.Status.LastSeenError)
		} else if updateStatusCount == 2 {
			assert.Equal(t, savepointPath, application.Status.SavepointPath)
		}

		updateStatusCount++
		return nil
	}

	updated := false
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		assert.Equal(t, 0, len(app.Finalizers))
		updated = true
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	savepointStatusCount := 0
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		savepointStatusCount++

		if savepointStatusCount == 1 {
			return &client.SavepointResponse{
				SavepointStatus: client.SavepointStatusResponse{
					Status: client.SavePointCompleted,
				},
				Operation: client.SavepointOperationResponse{
					FailureCause: client.FailureCause{
						Class:      "java.util.concurrent.CompletionException",
						StackTrace: "Exception",
					},
				},
			}, nil
		}
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: "s3:///path/to/savepoint",
			},
		}, nil
	}

	// the first time we return an error from the savepointing status
	err = stateMachineForTest.Handle(context.Background(), app.DeepCopy())
	assert.Error(t, err)

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	assert.Equal(t, 3, updateStatusCount)

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs *client.FlinkJobOverview, err error) {
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: "CANCELED",
		}, nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	assert.True(t, updated)

}

func TestDeleteWithSavepointAndFinishedJob(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationDeleting,
			DeployHash:    "deployhash",
			SavepointPath: "file:///savepoint",
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				v1beta1.FlinkApplicationVersionStatus{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: jobID,
					},
				},
			},
		},
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "FINISHED",
			},
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationDeleting, application.Status.Phase)

		assert.Equal(t, 0, len(app.Finalizers))

		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
}

func TestDeleteWithForceCancel(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	jobID := "j1"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: v1beta1.FlinkApplicationSpec{
			DeleteMode: v1beta1.DeleteModeForceCancel,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationDeleting,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				v1beta1.FlinkApplicationVersionStatus{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: jobID,
					},
				},
			},

			DeployHash: "deployhash",
		},
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: "RUNNING",
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 1
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationDeleting, application.Status.Phase)

		if updateCount == 1 {
			assert.Equal(t, 0, len(app.Finalizers))
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
	assert.Equal(t, 1, updateCount)
	assert.True(t, cancelled)

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return &client.FlinkJobOverview{
			JobID: jobID,
			State: "CANCELED",
		}, nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
	assert.Equal(t, 2, updateCount)
}

func TestDeleteModeNone(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: v1beta1.FlinkApplicationSpec{
			DeleteMode: v1beta1.DeleteModeNone,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationDeleting,
		},
	}

	jobID := "j1"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 1
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationDeleting, application.Status.Phase)

		if updateCount == 1 {
			assert.Equal(t, 0, len(app.Finalizers))
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
	assert.Equal(t, 2, updateCount)
	assert.False(t, cancelled)
}

func TestHandleInvalidPhase(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	err := stateMachineForTest.Handle(context.Background(), &v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: "asd",
		},
	})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Invalid state asd for the application")
}

func TestRollbackWithRetryableError(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSavepointing,
			DeployHash: "old-hash-retry",
		},
	}

	retryableErr := client.GetRetryableError(errors.New("blah"), "GetClusterOverview", "FAILED", 3)
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.SavepointFunc = func(ctx context.Context, app *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (savepoint string, err error) {
		return "", retryableErr
	}

	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return ferr.IsRetryable
	}

	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return retryCount <= ferr.MaxRetries
	}

	mockRetryHandler.GetRetryDelayFunc = func(retryCount int32) time.Duration {
		return time.Minute * 5
	}

	mockRetryHandler.IsTimeToRetryFunc = func(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool {
		return true
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	updateErrCount := 0
	mockK8Cluster.UpdateStatusFunc = func(ctx context.Context, object runtime.Object) error {
		updateErrCount++
		return nil
	}

	retries := 0
	for ; app.Status.Phase != v1beta1.FlinkApplicationRecovering; retries++ {
		assert.Equal(t, v1beta1.FlinkApplicationSavepointing, app.Status.Phase)
		err := stateMachineForTest.Handle(context.Background(), &app)

		// First attempt does not rollback
		if retries > 0 && retries < 4 {
			assert.Equal(t, int32(retries), app.Status.RetryCount)
			assert.NotNil(t, err)
			assert.NotNil(t, app.Status.LastSeenError)
		}
	}

	assert.Equal(t, 5, retries)
	assert.Equal(t, 5, updateErrCount)
	// Retries should have been exhausted and errors and retry counts reset
	assert.Equal(t, v1beta1.FlinkApplicationRecovering, app.Status.Phase)
	assert.Equal(t, int32(0), app.Status.RetryCount)
	assert.Nil(t, app.Status.LastSeenError)
}

func TestRollbackWithFailFastError(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSubmittingJob,
			DeployHash: "old-hash-retry-err",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	getCount := 0
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		var res []client.FlinkJob
		if getCount == 2 {
			res = []client.FlinkJob{
				{
					JobID:  "jid1",
					Status: client.Running,
				}}
		}
		getCount++
		return res, nil
	}

	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		return true, nil
	}
	failFastError := client.GetNonRetryableError(errors.New("blah"), "SubmitJob", "400BadRequest")
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error) {
		return "", failFastError
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	appHash := flink.HashForApplication(&app)

	podSelector := "81u2312"

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {
		hash := "old-hash-retry-err"
		if getServiceCount > 0 {
			hash = appHash
		}

		getServiceCount++
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": hash,
				},
			},
		}, nil
	}
	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorFailFastFunc = func(err error) bool {
		ferr, ok := err.(*v1beta1.FlinkApplicationError)
		assert.True(t, ok)
		return ferr.IsFailFast
	}
	retries := 0
	var err error
	for ; app.Status.Phase == v1beta1.FlinkApplicationSubmittingJob; retries++ {
		err = stateMachineForTest.Handle(context.Background(), &app)
		if app.Status.Phase == v1beta1.FlinkApplicationSubmittingJob {
			assert.NotNil(t, err)
			assert.Equal(t, int32(0), app.Status.RetryCount)
			assert.NotNil(t, app.Status.LastSeenError)
		}

	}

	assert.Equal(t, 2, retries)
	// once in rollingback phase, errors no longer exist
	assert.Equal(t, v1beta1.FlinkApplicationRollingBackJob, app.Status.Phase)
	assert.Equal(t, int32(0), app.Status.RetryCount)
	assert.Nil(t, app.Status.LastSeenError)
}

func TestRollbackAfterJobSubmission(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",

			// force a rollback
			ForceRollback: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSubmittingJob,
			DeployHash: "old-hash-retry-err",
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobid",
					},
				},
			},
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.Equal(t, v1beta1.FlinkApplicationRollingBackJob, app.Status.Phase)
	assert.Equal(t, "", mockFlinkController.GetLatestJobID(context.Background(), &app))
}

func TestErrorHandlingInRunningPhase(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationRunning,
			DeployHash: "old-hash-retry-err",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, app *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		return &common.FlinkDeployment{
			Jobmanager:  nil,
			Taskmanager: nil,
			Hash:        "",
		}, nil
	}

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return nil, client.GetNonRetryableError(errors.New("running phase error"), "TestError", "400")
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NotNil(t, err)
	// In the running phase, we don't want to invoke any of the error handling logic
	assert.Equal(t, int32(0), app.Status.RetryCount)
	assert.Nil(t, app.Status.LastSeenError)

}

func TestForceRollback(t *testing.T) {
	oldHash := "old-hash-force-rollback"
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:       "job.jar",
			Parallelism:   5,
			EntryClass:    "com.my.Class",
			ProgramArgs:   "--test",
			ForceRollback: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSubmittingJob,
			DeployHash: oldHash,
		},
	}

	stateMachineForTest := getTestStateMachine()
	stateMachineForTest.clock.(*clock.FakeClock).SetTime(time.Now())

	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.WaitOnErrorFunc = func(clock clock.Clock, lastUpdatedTime time.Time) (duration time.Duration, b bool) {
		return time.Millisecond, true
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {
		hash := oldHash
		if getServiceCount > 0 {
			hash = oldHash
		}

		getServiceCount++
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": hash,
				},
			},
		}, nil
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		return true, nil
	}

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": "aasdf"},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": "aasdf"},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	// rolled deploy while cluster is starting
	assert.Equal(t, v1beta1.FlinkApplicationRollingBackJob, app.Status.Phase)
	assert.True(t, app.Spec.ForceRollback)

	err = stateMachineForTest.Handle(context.Background(), &app)
	// Check if rollback hash is set
	assert.Nil(t, err)
	assert.Equal(t, oldHash, app.Status.RollbackHash)
}

func TestLastSeenErrTimeIsNil(t *testing.T) {
	oldHash := "old-hash-force-nil"
	retryableErr := client.GetRetryableError(errors.New("blah"), "GetClusterOverview", "FAILED", 3)
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationClusterStarting,
			DeployHash:    oldHash,
			LastSeenError: retryableErr.(*v1beta1.FlinkApplicationError),
		},
	}
	app.Status.LastSeenError.LastErrorUpdateTime = nil

	stateMachineForTest := getTestStateMachine()

	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		return true
	}

	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		return true
	}
	stateMachineForTest.clock.(*clock.FakeClock).SetTime(time.Now())
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

}

func TestCheckSavepointStatusFailing(t *testing.T) {
	oldHash := "old-hash-fail"
	maxRetries := int32(1)
	retryableErr := client.GetRetryableError(errors.New("blah"), "CheckSavepointStatus", "FAILED", 1)
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:              v1beta1.FlinkApplicationSavepointing,
			DeployHash:         oldHash,
			LastSeenError:      retryableErr.(*v1beta1.FlinkApplicationError),
			SavepointTriggerID: "trigger",
		},
	}
	app.Status.LastSeenError.LastErrorUpdateTime = nil

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		return nil, retryableErr.(*v1beta1.FlinkApplicationError)
	}

	mockFlinkController.FindExternalizedCheckpointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
		return "/tmp/checkpoint", nil
	}
	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		return true
	}
	mockRetryHandler.IsTimeToRetryFunc = func(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool {
		return true
	}
	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		return retryCount < maxRetries
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	// 1 retry left
	assert.NotNil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationSavepointing, app.Status.Phase)

	// No retries left for CheckSavepointStatus
	// The app should hence try to recover from an externalized checkpoint
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationRecovering, app.Status.Phase)
}

func TestDeleteWhenCheckSavepointStatusFailing(t *testing.T) {
	retryableErr := client.GetRetryableError(errors.New("blah"), "CheckSavepointStatus", "FAILED", 1)
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:              v1beta1.FlinkApplicationSavepointing,
			DeployHash:         "appHash",
			LastSeenError:      retryableErr.(*v1beta1.FlinkApplicationError),
			SavepointTriggerID: "trigger",
		},
	}
	app.Status.LastSeenError.LastErrorUpdateTime = nil

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		return nil, retryableErr.(*v1beta1.FlinkApplicationError)
	}
	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (s string, e error) {
		return "triggerId", nil
	}
	mockRetryHandler := stateMachineForTest.retryHandler.(*mock.RetryHandler)
	mockRetryHandler.IsErrorRetryableFunc = func(err error) bool {
		return true
	}
	mockRetryHandler.IsRetryRemainingFunc = func(err error, retryCount int32) bool {
		return true
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NotNil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationSavepointing, app.Status.Phase)
	assert.NotNil(t, app.Status.LastSeenError)
	// Try to force delete the app while it's in a savepointing state (with errors)
	// We should handle the delete here
	app.Status.Phase = v1beta1.FlinkApplicationDeleting
	app.Spec.DeleteMode = v1beta1.DeleteModeForceCancel

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, "appHash", hash)
		return &client.FlinkJobOverview{
			JobID: "jobID",
			State: client.Failing,
		}, nil
	}

	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error {
		return nil
	}
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Nil(t, app.Status.LastSeenError)
	assert.Equal(t, int32(0), app.Status.RetryCount)
	assert.Nil(t, app.GetFinalizers())

}

func TestRunningToDualRunning(t *testing.T) {
	deployHash := "appHash"
	updatingHash := "b1b084ee"
	triggerID := "trigger"
	savepointPath := "savepointPath"
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:        "job.jar",
			Parallelism:    5,
			EntryClass:     "com.my.Class",
			ProgramArgs:    "--test",
			DeploymentMode: v1beta1.DeploymentModeBlueGreen,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationRunning,
			DeployHash:    deployHash,
			DeployVersion: v1beta1.GreenFlinkApplication,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.GreenFlinkApplication,
				},
				{},
			},
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return &client.FlinkJobOverview{
			JobID: "jobID2",
			State: client.Running,
		}, nil

	}

	podSelector := "1231kjh2"

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (b bool, err error) {
		return true, nil
	}

	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (b bool, err error) {
		return true, nil
	}

	// Handle Running and move to Updating
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationUpdating, app.Status.Phase)
	assert.Equal(t, 2, len(app.Status.VersionStatuses))
	assert.Equal(t, "", app.Status.UpdatingHash)

	// Handle Updating and move to ClusterStarting
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Equal(t, v1beta1.FlinkApplicationClusterStarting, app.Status.Phase)
	assert.Equal(t, v1beta1.BlueFlinkApplication, app.Status.UpdatingVersion)
	assert.Nil(t, err)

	// Handle ClusterStarting and move to Savepointing
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Equal(t, v1beta1.FlinkApplicationSavepointing, app.Status.Phase)
	assert.Equal(t, updatingHash, app.Status.VersionStatuses[1].VersionHash)
	assert.Equal(t, v1beta1.BlueFlinkApplication, app.Status.VersionStatuses[1].Version)
	assert.Equal(t, updatingHash, app.Status.UpdatingHash)
	assert.Nil(t, err)

	// Handle Savepointing and move to SubmittingJob
	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (s string, err error) {
		assert.False(t, isCancel)
		return triggerID, nil
	}
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (response *client.SavepointResponse, err error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location:     savepointPath,
				FailureCause: client.FailureCause{},
			},
		}, nil
	}
	assert.Equal(t, app.Status.SavepointTriggerID, triggerID)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, app.Status.SavepointPath, savepointPath)
	assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, app.Status.Phase)

	// Handle SubmittingJob and move to DualRunning
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {

		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": updatingHash,
				},
			},
		}, nil
	}

	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (s string, err error) {
		return "jobID2", nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, "jobID2", app.Status.VersionStatuses[1].JobStatus.JobID)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, "jobID2", app.Status.VersionStatuses[1].JobStatus.JobID)
	assert.Equal(t, app.Spec.JarName, app.Status.VersionStatuses[1].JobStatus.JarName)
	assert.Equal(t, app.Spec.Parallelism, app.Status.VersionStatuses[1].JobStatus.Parallelism)
	assert.Equal(t, app.Spec.EntryClass, app.Status.VersionStatuses[1].JobStatus.EntryClass)
	assert.Equal(t, app.Spec.ProgramArgs, app.Status.VersionStatuses[1].JobStatus.ProgramArgs)

	assert.Equal(t, v1beta1.FlinkApplicationDualRunning, app.Status.Phase)
	assert.Equal(t, deployHash, app.Status.DeployHash)
	assert.Equal(t, updatingHash, app.Status.UpdatingHash)
	assert.Equal(t, v1beta1.GreenFlinkApplication, app.Status.DeployVersion)
	assert.Equal(t, v1beta1.BlueFlinkApplication, app.Status.UpdatingVersion)

	// Set an obsolete tearDownVersionHash and ensure that the application continues to run in DualRunning
	// And updates all status fields
	mockFlinkController.GetVersionAndJobIDForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (version string, s string, err error) {
		return "", "", errors.New("no version found")
	}
	mockFlinkController.CompareAndUpdateJobStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		assert.Equal(t, deployHash, application.Status.VersionStatuses[0].VersionHash)
		assert.Equal(t, updatingHash, application.Status.VersionStatuses[1].VersionHash)
		return true, nil
	}

	app.Spec.TearDownVersionHash = "obsoleteHash"
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationDualRunning, app.Status.Phase)
	assert.Equal(t, "jobId", app.Status.VersionStatuses[0].JobStatus.JobID)
	assert.Equal(t, "jobID2", app.Status.VersionStatuses[1].JobStatus.JobID)

}

func TestDualRunningToRunning(t *testing.T) {
	deployHash := "appHash"
	updatingHash := "2845d780"
	teardownHash := "6c87fe8f"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:        "job.jar",
			Parallelism:    5,
			EntryClass:     "com.my.Class",
			ProgramArgs:    "--test",
			DeploymentMode: v1beta1.DeploymentModeBlueGreen,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:           v1beta1.FlinkApplicationDualRunning,
			DeployHash:      deployHash,
			UpdatingHash:    updatingHash,
			DeployVersion:   v1beta1.GreenFlinkApplication,
			UpdatingVersion: v1beta1.BlueFlinkApplication,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.GreenFlinkApplication,
				},
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobId2",
						State: v1beta1.Running,
					},
					VersionHash: updatingHash,
					Version:     v1beta1.BlueFlinkApplication,
				},
			},
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.DeleteResourcesForAppWithHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error {
		assert.Equal(t, deployHash, hash)
		return nil
	}
	mockFlinkController.GetVersionAndJobIDForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, string, error) {
		assert.Equal(t, deployHash, hash)
		return string(v1beta1.GreenFlinkApplication), "jobId", nil
	}
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (e error) {
		assert.Equal(t, "jobId", jobID)
		return nil
	}
	app.Spec.TearDownVersionHash = deployHash
	expectedVersionStatus := app.Status.VersionStatuses[1]
	// Handle DualRunning and move to Running
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationRunning, app.Status.Phase)
	assert.Empty(t, app.Status.VersionStatuses[1])
	assert.Equal(t, teardownHash, app.Status.TeardownHash)
	assert.Equal(t, expectedVersionStatus, app.Status.VersionStatuses[0])
	assert.Equal(t, "", app.Status.UpdatingHash)
	assert.Equal(t, updatingHash, app.Status.DeployHash)
	assert.Equal(t, "", string(app.Status.UpdatingVersion))
	assert.Equal(t, v1beta1.BlueFlinkApplication, app.Status.DeployVersion)
}

func TestBlueGreenUpdateWithError(t *testing.T) {
	deployHash := "deployHash"
	updatingHash := "updateHash"

	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:        "job.jar",
			Parallelism:    5,
			EntryClass:     "com.my.Class",
			ProgramArgs:    "--test",
			DeploymentMode: v1beta1.DeploymentModeBlueGreen,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationSubmittingJob,
			DeployHash:    deployHash,
			DeployVersion: v1beta1.GreenFlinkApplication,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.GreenFlinkApplication,
				},
				{},
			},
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (s string, err error) {
		return "", client.GetNonRetryableError(errors.New("bad submission"), "SubmitJob", "500")
	}

	podSelector := "1231kjh2"

	mockFlinkController.GetDeploymentsForHashFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (deployment *common.FlinkDeployment, err error) {
		jm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		tm := appsv1.Deployment{
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"pod-deployment-selector": podSelector},
				},
			},
		}

		return &common.FlinkDeployment{
			Jobmanager:  &jm,
			Taskmanager: &tm,
			Hash:        hash,
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string, version string) (*v1.Service, error) {

		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": updatingHash,
				},
			},
		}, nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NotNil(t, err)
	assert.NotNil(t, app.Status.LastSeenError)
	assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, app.Status.Phase)

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationRollingBackJob, app.Status.Phase)
	assert.Equal(t, "", app.Status.VersionStatuses[1].JobStatus.JobID)

	// We should have moved to DeployFailed without affecting the existing job
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, app.Status.Phase)
	assert.Equal(t, "jobId", app.Status.VersionStatuses[0].JobStatus.JobID)

}

func TestBlueGreenDeployWithSavepointDisabled(t *testing.T) {
	deployHash := "appHashTest"
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:           "job.jar",
			Parallelism:       5,
			EntryClass:        "com.my.Class",
			ProgramArgs:       "--test",
			DeploymentMode:    v1beta1.DeploymentModeBlueGreen,
			SavepointDisabled: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:         v1beta1.FlinkApplicationClusterStarting,
			DeployHash:    deployHash,
			DeployVersion: v1beta1.GreenFlinkApplication,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "jobId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.GreenFlinkApplication,
				},
				{},
			},
		},
	}
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		return &client.FlinkJobOverview{
			JobID: "jobID2",
			State: client.Running,
		}, nil

	}

	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (b bool, err error) {
		return true, nil
	}

	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (b bool, err error) {
		return true, nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationSubmittingJob, app.Status.Phase)
}

func TestDeleteBlueGreenDeployment(t *testing.T) {
	deployHash := "deployHashDelete"
	updateHash := "updateHashDelete"
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:           "job.jar",
			Parallelism:       5,
			EntryClass:        "com.my.Class",
			ProgramArgs:       "--test",
			DeploymentMode:    v1beta1.DeploymentModeBlueGreen,
			SavepointDisabled: true,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:           v1beta1.FlinkApplicationDeleting,
			DeployHash:      deployHash,
			DeployVersion:   v1beta1.GreenFlinkApplication,
			UpdatingHash:    updateHash,
			UpdatingVersion: v1beta1.BlueFlinkApplication,
			VersionStatuses: []v1beta1.FlinkApplicationVersionStatus{
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "greenId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.GreenFlinkApplication,
				},
				{
					JobStatus: v1beta1.FlinkJobStatus{
						JobID: "blueId",
						State: v1beta1.Running,
					},
					VersionHash: deployHash,
					Version:     v1beta1.BlueFlinkApplication,
				},
			},
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	jobCtr1 := 0
	jobCtr2 := 0
	mockFlinkController.GetJobToDeleteForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		if hash == deployHash {
			jobCtr1++
			if jobCtr1 <= 2 {
				return &client.FlinkJobOverview{
					JobID: "greenId",
					State: client.Running,
				}, nil
			}
			return &client.FlinkJobOverview{
				JobID: "greenId",
				State: client.Canceled,
			}, nil
		}

		jobCtr2++
		if jobCtr2 <= 2 {
			return &client.FlinkJobOverview{
				JobID: "blueId",
				State: client.Running,
			}, nil
		}

		return &client.FlinkJobOverview{
			JobID: "blueId",
			State: client.Canceled,
		}, nil

	}
	triggerID := "t1"
	savepointCtr := 0
	mockFlinkController.SavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (string, error) {
		return triggerID, nil
	}

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
		if savepointCtr == 0 {
			assert.Equal(t, deployHash, hash)
			assert.Equal(t, "greenId", jobID)
		} else {
			assert.Equal(t, updateHash, hash)
			assert.Equal(t, "blueId", jobID)
		}
		savepointCtr++
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: testSavepointLocation + hash,
			},
		}, nil
	}
	// Go through deletes until both applications are deleted
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, 2, savepointCtr)
	assert.Empty(t, app.Finalizers)
	assert.Equal(t, testSavepointLocation+updateHash, app.Status.SavepointPath)
}

func TestIncompatibleDeploymentModeSwitch(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:        "job.jar",
			Parallelism:    5,
			EntryClass:     "com.my.Class",
			ProgramArgs:    "--test",
			DeploymentMode: v1beta1.DeploymentModeDual,
		},
	}

	app.Status.Phase = v1beta1.FlinkApplicationRunning
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
		return nil, nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.DeploymentModeDual, app.Status.DeploymentMode)

	// Try to switch from Dual to BlueGreen
	app.Status.Phase = v1beta1.FlinkApplicationRunning
	app.Spec.DeploymentMode = v1beta1.DeploymentModeBlueGreen
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, app.Status.Phase)

	app.Spec.DeploymentMode = v1beta1.DeploymentModeBlueGreen
	app.Status.Phase = v1beta1.FlinkApplicationRunning
	app.Status.DeploymentMode = ""
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.DeploymentModeBlueGreen, app.Status.DeploymentMode)

	// Try to switch from BlueGreen to Dual
	app.Status.Phase = v1beta1.FlinkApplicationRunning
	app.Spec.DeploymentMode = v1beta1.DeploymentModeDual
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, app.Status.Phase)
}

func TestIsScaleUp(t *testing.T) {
	app := v1beta1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			JarName:        "job.jar",
			Parallelism:    100,
			EntryClass:     "com.my.Class",
			ProgramArgs:    "--test",
			DeploymentMode: v1beta1.DeploymentModeDual,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: "Running",
			JobStatus: v1beta1.FlinkJobStatus{
				Parallelism: 100,
			},
		},
	}

	hash := flink.HashForApplication(&app)
	app.Status.DeployHash = hash

	app.Spec.Parallelism = 150
	assert.True(t, isScaleUp(&app))

	app.Spec.Parallelism = 90
	assert.False(t, isScaleUp(&app))

	app.Spec.Parallelism = 150
	app.Spec.RestartNonce = "blah"
	assert.False(t, isScaleUp(&app))
}
