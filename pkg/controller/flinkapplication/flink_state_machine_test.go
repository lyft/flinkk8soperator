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

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
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
	}
}

func testFlinkDeployment(app *v1alpha1.FlinkApplication) common.FlinkDeployment {
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
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{},
	})
	assert.Nil(t, err)
}

func TestHandleStartingClusterStarting(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationClusterStarting,
		},
	})
	assert.Nil(t, err)
}

func TestHandleStartingDual(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, error) {
		fd := testFlinkDeployment(application)
		fd.Taskmanager.Status.AvailableReplicas = 2
		fd.Jobmanager.Status.AvailableReplicas = 1
		return &fd, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationSavepointing, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationClusterStarting,
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
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (s string, e error) {
		// should not be called
		assert.False(t, true)
		return "", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationSubmittingJob, application.Status.Phase)
		updateInvoked = true
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSavepointing,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingDual(t *testing.T) {
	app := v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationSavepointing,
			DeployHash: "old-hash",
		},
	}

	cancelInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (s string, e error) {
		assert.Equal(t, "old-hash", hash)
		cancelInvoked = true

		return "trigger", nil
	}

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
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
		application := object.(*v1alpha1.FlinkApplication)
		if updateCount == 0 {
			assert.Equal(t, "trigger", application.Spec.SavepointInfo.TriggerID)
		} else {
			assert.Equal(t, testSavepointLocation, application.Spec.SavepointInfo.SavepointLocation)
			assert.Equal(t, v1alpha1.FlinkApplicationSubmittingJob, application.Status.Phase)
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
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	app := v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{
			SavepointInfo: v1alpha1.SavepointInfo{
				TriggerID: "trigger",
			},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationSavepointing,
			DeployHash: "blah",
		},
	}
	hash := flink.HashForApplication(&app)

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Empty(t, application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, hash, application.Status.FailedDeployHash)
		assert.Equal(t, v1alpha1.FlinkApplicationDeployFailed, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestRestoreFromExternalizedCheckpoint(t *testing.T) {
	updateInvoked := false

	app := v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{
			SavepointInfo: v1alpha1.SavepointInfo{
				TriggerID: "trigger",
			},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationSavepointing,
			DeployHash: "blah",
		},
	}

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	mockFlinkController.FindExternalizedCheckpointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
		return "/tmp/checkpoint", nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, "/tmp/checkpoint", application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationSubmittingJob, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestSubmittingToRunning(t *testing.T) {
	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationSubmittingJob,
			DeployHash: "old-hash",
		},
	}
	appHash := flink.HashForApplication(&app)

	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
		return true, nil
	}

	getCount := 0
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		assert.Equal(t, appHash, hash)
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

	startCount := 0
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string) (string, error) {

		assert.Equal(t, appHash, hash)
		assert.Equal(t, app.Spec.JarName, jarName)
		assert.Equal(t, app.Spec.Parallelism, parallelism)
		assert.Equal(t, app.Spec.EntryClass, entryClass)
		assert.Equal(t, app.Spec.ProgramArgs, programArgs)

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
			application := object.(*v1alpha1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		} else if updateCount == 2 {
			application := object.(*v1alpha1.FlinkApplication)
			assert.Equal(t, jobID, application.Status.JobStatus.JobID)
			assert.Equal(t, appHash, application.Status.DeployHash)
			assert.Equal(t, app.Spec.JarName, app.Status.JobStatus.JarName)
			assert.Equal(t, app.Spec.Parallelism, app.Status.JobStatus.Parallelism)
			assert.Equal(t, app.Spec.EntryClass, app.Status.JobStatus.EntryClass)
			assert.Equal(t, app.Spec.ProgramArgs, app.Status.JobStatus.ProgramArgs)
			assert.Equal(t, v1alpha1.FlinkApplicationRunning, application.Status.Phase)
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
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
		return false, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
		assert.False(t, true)
		return nil, nil
	}
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string) (string, error) {
		assert.False(t, true)
		return "", nil
	}

	app := v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSubmittingJob,
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
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, error) {
		fd := testFlinkDeployment(application)
		return &fd, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		assert.True(t, false)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationRunning,
		},
	})
	assert.Nil(t, err)
}

func TestRunningToClusterStarting(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetCurrentDeploymentsForAppFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, error) {
		return nil, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationUpdating, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationRunning,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestRollingBack(t *testing.T) {
	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "flink",
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			JarName:     "job.jar",
			Parallelism: 5,
			EntryClass:  "com.my.Class",
			ProgramArgs: "--test",
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationRollingBackJob,
			DeployHash: "old-hash",
			JobStatus: v1alpha1.FlinkJobStatus{
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
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
		assert.Equal(t, "old-hash", hash)
		return true, nil
	}

	startCalled := false
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string) (string, error) {

		startCalled = true
		assert.Equal(t, "old-hash", hash)
		assert.Equal(t, app.Status.JobStatus.JarName, jarName)
		assert.Equal(t, app.Status.JobStatus.Parallelism, parallelism)
		assert.Equal(t, app.Status.JobStatus.EntryClass, entryClass)
		assert.Equal(t, app.Status.JobStatus.ProgramArgs, programArgs)
		return jobID, nil
	}

	getCount := 0
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
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
			application := object.(*v1alpha1.FlinkApplication)
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		} else if updateCount == 2 {
			application := object.(*v1alpha1.FlinkApplication)
			assert.Equal(t, appHash, application.Status.FailedDeployHash)
			assert.Equal(t, v1alpha1.FlinkApplicationDeployFailed, application.Status.Phase)
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
	app := &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:         v1alpha1.FlinkApplicationClusterStarting,
			DeployHash:    "prevhash",
			LastSeenError: client.GetErrorKey(client.GetError(errors.New("blah"), "GetClusterOverview", "FAILED")),
		},
	}
	// Retryable error
	assert.False(t, stateMachineForTest.shouldRollback(context.Background(), app))
	assert.Empty(t, app.Status.LastSeenError)
	assert.Equal(t, int32(1), app.Status.RetryCount)

	// Retryable error with retries exhausted
	app.Status.RetryCount = 100
	app.Status.LastSeenError = client.GetErrorKey(client.GetError(errors.New("blah"), "GetClusterOverview", "FAILED"))
	assert.True(t, stateMachineForTest.shouldRollback(context.Background(), app))
	assert.NotEmpty(t, app.Status.LastSeenError)
	assert.Equal(t, int32(100), app.Status.RetryCount)

	// Non retryable error
	app.Status.LastSeenError = client.GetErrorKey(client.GetError(errors.New("blah"), "SubmitJob", "FAILED"))
	assert.True(t, stateMachineForTest.shouldRollback(context.Background(), app))
	assert.NotEmpty(t, app.Status.LastSeenError)
}

func TestDeleteWithSavepoint(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationDeleting,
			DeployHash: "deployhash",
			JobStatus: v1alpha1.FlinkJobStatus{
				JobID: jobID,
			},
		},
	}

	triggerID := "t1"
	savepointPath := "s3:///path/to/savepoint"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
		return triggerID, nil
	}

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
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
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationDeleting, application.Status.Phase)

		if updateCount == 1 {
			assert.Equal(t, triggerID, application.Spec.SavepointInfo.TriggerID)
		} else if updateCount == 2 {
			assert.Equal(t, savepointPath, application.Spec.SavepointInfo.SavepointLocation)
		} else if updateCount == 3 {
			assert.Equal(t, 0, len(app.Finalizers))
		}

		updateCount++
		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
	assert.Equal(t, 2, updateCount)

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: "s3:///path/to/savepoint",
			},
		}, nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	assert.Equal(t, 3, updateCount)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "CANCELED",
			},
		}, nil
	}

	err = stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)

	assert.Equal(t, 4, updateCount)

}

func TestDeleteWithSavepointAndFinishedJob(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:      v1alpha1.FlinkApplicationDeleting,
			DeployHash: "deployhash",
			JobStatus: v1alpha1.FlinkJobStatus{
				JobID: jobID,
			},
		},
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "FINISHED",
			},
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationDeleting, application.Status.Phase)

		assert.Equal(t, 0, len(app.Finalizers))

		return nil
	}

	err := stateMachineForTest.Handle(context.Background(), &app)
	assert.NoError(t, err)
}

func TestDeleteWithForceCancel(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			DeleteMode: v1alpha1.DeleteModeForceCancel,
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationDeleting,
			JobStatus: v1alpha1.FlinkJobStatus{
				JobID: jobID,
			},
			DeployHash: "deployhash",
		},
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 1
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationDeleting, application.Status.Phase)

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

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
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

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			DeleteMode: v1alpha1.DeleteModeNone,
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationDeleting,
		},
	}

	jobID := "j1"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 1
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object runtime.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationDeleting, application.Status.Phase)

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

	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: "asd",
		},
	})
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Invalid state asd for the application")
}
