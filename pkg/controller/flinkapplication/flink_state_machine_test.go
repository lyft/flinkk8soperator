package flinkapplication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
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

func TestHandleStartingDual(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
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
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (s string, e error) {
		// should not be called
		assert.False(t, true)
		return "", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
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

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (s string, e error) {
		assert.Equal(t, "old-hash", hash)
		cancelInvoked = true

		return "trigger", nil
	}

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
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
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		if updateCount == 0 {
			assert.Equal(t, "trigger", application.Spec.SavepointInfo.TriggerID)
		} else {
			assert.Equal(t, testSavepointLocation, application.Spec.SavepointInfo.SavepointLocation)
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
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	app := v1beta1.FlinkApplication{
		Spec: v1beta1.FlinkApplicationSpec{
			SavepointInfo: v1beta1.SavepointInfo{
				TriggerID: "trigger",
			},
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSavepointing,
			DeployHash: "blah",
		},
	}
	hash := flink.HashForApplication(&app)

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Empty(t, application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, hash, application.Status.FailedDeployHash)
		assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, application.Status.Phase)
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
		Spec: v1beta1.FlinkApplicationSpec{
			SavepointInfo: v1beta1.SavepointInfo{
				TriggerID: "trigger",
			},
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase:      v1beta1.FlinkApplicationSavepointing,
			DeployHash: "blah",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	mockFlinkController.FindExternalizedCheckpointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
		return "/tmp/checkpoint", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, "/tmp/checkpoint", application.Spec.SavepointInfo.SavepointLocation)
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

	getCount := 0
	mockFlinkController.GetJobForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
		assert.Equal(t, appHash, hash)
		var res *client.FlinkJobOverview
		if getCount == 1 {
			res = &client.FlinkJobOverview{
				JobID:  jobID,
				State: client.Running,
			}
		}
		getCount++
		return res, nil
	}

	startCount := 0
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool) (string, error) {

		assert.Equal(t, appHash, hash)
		assert.Equal(t, app.Spec.JarName, jarName)
		assert.Equal(t, app.Spec.Parallelism, parallelism)
		assert.Equal(t, app.Spec.EntryClass, entryClass)
		assert.Equal(t, app.Spec.ProgramArgs, programArgs)
		assert.Equal(t, app.Spec.AllowNonRestoredState, allowNonRestoredState)

		startCount++
		return jobID, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string) (*v1.Service, error) {
		assert.Equal(t, "flink", namespace)
		assert.Equal(t, "test-app", name)

		hash := "old-hash"
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

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		if updateCount == 0 {
			// update to the service
			service := object.(*v1.Service)
			assert.Equal(t, appHash, service.Spec.Selector["flink-app-hash"])
		} else if updateCount == 1 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		} else if updateCount == 2 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobID, application.Status.JobStatus.JobID)
			assert.Equal(t, appHash, application.Status.DeployHash)
			assert.Equal(t, app.Spec.JarName, app.Status.JobStatus.JarName)
			assert.Equal(t, app.Spec.Parallelism, app.Status.JobStatus.Parallelism)
			assert.Equal(t, app.Spec.EntryClass, app.Status.JobStatus.EntryClass)
			assert.Equal(t, app.Spec.ProgramArgs, app.Status.JobStatus.ProgramArgs)
			assert.Equal(t, v1beta1.FlinkApplicationRunning, application.Status.Phase)
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.Equal(t, 2, getCount)
	assert.Equal(t, 1, startCount)
	assert.Equal(t, 3, updateCount)
}

func TestHandleApplicationNotReady(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
		return false, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		assert.False(t, true)
		return nil, nil
	}
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool) (string, error) {
		assert.False(t, true)
		return "", nil
	}

	app := v1beta1.FlinkApplication{
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationSubmittingJob,
		},
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string) (*v1.Service, error) {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "flink",
			},
			Spec: v1.ServiceSpec{
				Selector: map[string]string{
					"flink-app-hash": flink.HashForApplication(&app),
				},
			},
		}, nil
	}

	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
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
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
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
			Phase:      v1beta1.FlinkApplicationRollingBackJob,
			DeployHash: "old-hash",
			JobStatus: v1beta1.FlinkJobStatus{
				JarName:     "old-job.jar",
				Parallelism: 10,
				EntryClass:  "com.my.OldClass",
				ProgramArgs: "--no-test",
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
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool) (string, error) {

		startCalled = true
		assert.Equal(t, "old-hash", hash)
		assert.Equal(t, app.Status.JobStatus.JarName, jarName)
		assert.Equal(t, app.Status.JobStatus.Parallelism, parallelism)
		assert.Equal(t, app.Status.JobStatus.EntryClass, entryClass)
		assert.Equal(t, app.Status.JobStatus.ProgramArgs, programArgs)
		assert.Equal(t, app.Status.JobStatus.AllowNonRestoredState, allowNonRestoredState)
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
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string) (*v1.Service, error) {
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

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		if updateCount == 0 {
			// update to the service
			service := object.(*v1.Service)
			assert.Equal(t, "old-hash", service.Spec.Selector["flink-app-hash"])
		} else if updateCount == 1 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		} else if updateCount == 2 {
			application := object.(*v1beta1.FlinkApplication)
			assert.Equal(t, appHash, application.Status.FailedDeployHash)
			assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, application.Status.Phase)
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)
	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.Nil(t, err)

	assert.True(t, startCalled)
	assert.Equal(t, 3, updateCount)
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
			JobStatus: v1beta1.FlinkJobStatus{
				JobID: jobID,
			},
		},
	}

	triggerID := "t1"
	savepointPath := "s3:///path/to/savepoint"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
		return triggerID, nil
	}

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 1
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1beta1.FlinkApplication)
		assert.Equal(t, v1beta1.FlinkApplicationDeleting, application.Status.Phase)

		if updateCount == 1 {
			assert.Equal(t, triggerID, application.Spec.SavepointInfo.TriggerID)
		} else if updateCount == 2 {
			assert.NotNil(t, application.Status.LastSeenError)
		} else if updateCount == 3 {
			assert.Equal(t, savepointPath, application.Spec.SavepointInfo.SavepointLocation)
		} else if updateCount == 4 {
			assert.Equal(t, 0, len(app.Finalizers))
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
	assert.Equal(t, 2, updateCount)

	savepointStatusCount := 0
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
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

	assert.Equal(t, 4, updateCount)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "CANCELED",
			},
		}, nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	assert.Equal(t, 5, updateCount)

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
			Phase:      v1beta1.FlinkApplicationDeleting,
			DeployHash: "deployhash",
			JobStatus: v1beta1.FlinkJobStatus{
				JobID: jobID,
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
			JobStatus: v1beta1.FlinkJobStatus{
				JobID: jobID,
			},
			DeployHash: "deployhash",
		},
	}

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
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error {
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

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "CANCELED",
			},
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
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error {
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
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (savepoint string, err error) {
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
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		updateErrCount++
		return nil
	}

	retries := 0
	for ; app.Status.Phase != v1beta1.FlinkApplicationDeployFailed; retries++ {
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
	assert.Equal(t, v1beta1.FlinkApplicationDeployFailed, app.Status.Phase)
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
		if getCount == 1 {
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
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool) (string, error) {
		return "", failFastError
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	appHash := flink.HashForApplication(&app)
	getServiceCount := 0
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string) (*v1.Service, error) {
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

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
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
	mockK8Cluster.GetServiceFunc = func(ctx context.Context, namespace string, name string) (*v1.Service, error) {
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
