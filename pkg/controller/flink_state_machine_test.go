package controller

import (
	"testing"

	"context"

	"errors"

	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	controller_config "github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/mock"
	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	"github.com/lyft/flytestdlib/config"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestHandleNewOrCreate(t *testing.T) {
	jobJarName := "ExampleJar"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.Equal(t, jobJarName, application.Spec.JarName)
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, jobJarName, application.Spec.JarName)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{
			JarName: jobJarName,
		},
	})
	assert.Nil(t, err)
}

func TestHandleNewOrCreateError(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		return errors.New("random")
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{})
	assert.NotNil(t, err)
}

func TestHandleStartingSingleClusterReady(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetCurrentAndOldDeploymentsForAppFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error) {
		dummyDep := []v1.Deployment{
			{
				Status: v1.DeploymentStatus{
					AvailableReplicas: 2,
				},
			},
		}
		return dummyDep, nil, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationReady, application.Status.Phase)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationClusterStarting,
		},
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
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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

func TestHandleStartingMultiClusterReady(t *testing.T) {
	triggerID := "trigger"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetCurrentAndOldDeploymentsForAppFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error) {
		dummyDep := []v1.Deployment{
			{
				Status: v1.DeploymentStatus{
					AvailableReplicas: 2,
				},
			},
		}
		return dummyDep, dummyDep, nil
	}

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerID, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, triggerID, application.Spec.SavepointInfo.TriggerID)
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

func TestHandleApplicationReadyAndRunning(t *testing.T) {
	jobID := "j1"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: client.Running,
			},
		}, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		if updateCount == 0 {
			assert.Equal(t, jobFinalizer, application.Finalizers[0])
		} else if updateCount == 1 {
			assert.Equal(t, jobID, application.Status.JobStatus.JobID)
			assert.Equal(t, v1alpha1.FlinkApplicationRunning, application.Status.Phase)
		}

		updateCount++
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationReady,
		},
	})
	assert.Equal(t, 2, updateCount)
	assert.Nil(t, err)
}

func TestHandleApplicationReadyNotRunning(t *testing.T) {
	updateInvoked := false
	jobID := "j1"
	activeJobID := "j2"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: client.Canceled,
			},
		}, nil
	}
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return activeJobID, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)

	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, activeJobID, application.Status.JobStatus.JobID)
		assert.Empty(t, application.Spec.SavepointInfo.SavepointLocation)
		assert.Empty(t, application.Spec.SavepointInfo.TriggerID)
		assert.Equal(t, v1alpha1.FlinkApplicationRunning, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers: []string{jobFinalizer},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationReady,
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			SavepointInfo: v1alpha1.SavepointInfo{
				SavepointLocation: testSavepointLocation,
			},
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationNotReady(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
		assert.False(t, true)
		return nil, nil
	}
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		assert.False(t, true)
		return "", nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationReady,
		},
	})
	assert.Nil(t, err)
}

func TestHandleApplicationRunning(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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

func TestHandleApplicationRunningUpdated(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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

func TestHandleApplicationSavepointingSingleMode(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: testSavepointLocation,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, testSavepointLocation, application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationNew, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSavepointing,
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			DeploymentMode: v1alpha1.DeploymentModeSingle,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingCompleted(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: testSavepointLocation,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, testSavepointLocation, application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationReady, application.Status.Phase)
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

func TestHandleApplicationSavepointingInProgress(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointInProgress,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSavepointing,
		},
	})
	assert.Nil(t, err)
}

func TestHandleApplicationSavepointingFailed(t *testing.T) {
	updateInvoked := false
	deleteInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		deleteInvoked = true
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Empty(t, application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationFailed, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSavepointing,
		},
	})
	assert.True(t, updateInvoked)
	assert.False(t, deleteInvoked)
	assert.Nil(t, err)
}

func TestRestoreFromExternalizedCheckpoint(t *testing.T) {
	updateInvoked := false
	deleteInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	mockFlinkController.FindExternalizedCheckpointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return "/tmp/checkpoint", nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		deleteInvoked = true
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, "/tmp/checkpoint", application.Spec.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationReady, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationSavepointing,
		},
	})
	assert.True(t, updateInvoked)
	assert.True(t, deleteInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationUpdatingParallelismChanged(t *testing.T) {
	updateInvoked := false
	triggerID := "t1"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.False(t, true)
		return nil
	}

	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerID, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, triggerID, application.Spec.SavepointInfo.TriggerID)
		assert.Equal(t, v1alpha1.FlinkApplicationSavepointing, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationUpdating,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationUpdatingTakManagerUpdate(t *testing.T) {
	triggerID := "t1"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerID, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, triggerID, application.Spec.SavepointInfo.TriggerID)
		assert.Equal(t, v1alpha1.FlinkApplicationSavepointing, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationUpdating,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestIsApplicationStuck(t *testing.T) {
	testDuration := config.Duration{}
	testDuration.Duration = 5 * time.Minute
	err := controller_config.ConfigSection.SetConfig(&controller_config.Config{
		StatemachineStalenessDuration: testDuration,
	})
	assert.Nil(t, err)
	stateMachineForTest := getTestStateMachine()
	lastUpdated := v12.NewTime(time.Now().Add(time.Duration(-8) * time.Minute))
	stateMachineForTest.clock.(*clock.FakeClock).SetTime(time.Now())
	app := &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:         v1alpha1.FlinkApplicationUpdating,
			LastUpdatedAt: &lastUpdated,
		},
	}
	isStuck := stateMachineForTest.isApplicationStuck(context.Background(), app)
	assert.True(t, isStuck)
	updateInvoked := false
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationFailed, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err = stateMachineForTest.Handle(context.Background(), app)
	assert.Nil(t, err)
	assert.True(t, updateInvoked)
}

func TestIsApplicationNotStuck(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	lastUpdated := v12.NewTime(time.Now().Add(-1 * time.Duration(1) * time.Minute))
	stateMachineForTest.clock.(*clock.FakeClock).SetTime(time.Now())
	isStuck := stateMachineForTest.isApplicationStuck(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase:         v1alpha1.FlinkApplicationUpdating,
			LastUpdatedAt: &lastUpdated,
		},
	})
	assert.False(t, isStuck)
}

func TestDeleteWithSavepoint(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &v12.Time{Time: time.Now()},
		},
		Status: v1alpha1.FlinkApplicationStatus{
			JobStatus: v1alpha1.FlinkJobStatus{
				JobID: jobID,
			},
		},
	}

	triggerID := "t1"
	savepointPath := "s3:///path/to/savepoint"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)
	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerID, nil
	}

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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

	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
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

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (jobs []client.FlinkJob, err error) {
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

func TestDeleteWithForceCancel(t *testing.T) {
	stateMachineForTest := getTestStateMachine()

	jobID := "j1"

	app := v1alpha1.FlinkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Finalizers:        []string{jobFinalizer},
			DeletionTimestamp: &v12.Time{Time: time.Now()},
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			DeleteMode: v1alpha1.DeleteModeForceCancel,
		},
		Status: v1alpha1.FlinkApplicationStatus{
			JobStatus: v1alpha1.FlinkJobStatus{
				JobID: jobID,
			},
		},
	}

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (jobs []client.FlinkJob, err error) {
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
			DeletionTimestamp: &v12.Time{Time: time.Now()},
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			DeleteMode: v1alpha1.DeleteModeNone,
		},
	}

	jobID := "j1"

	mockFlinkController := stateMachineForTest.flinkController.(*mock.FlinkController)

	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (jobs []client.FlinkJob, err error) {
		return []client.FlinkJob{
			{
				JobID:  jobID,
				Status: "RUNNING",
			},
		}, nil
	}

	cancelled := false
	mockFlinkController.ForceCancelFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		cancelled = true
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.K8Cluster)
	updateCount := 0
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
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
