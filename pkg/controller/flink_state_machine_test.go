package controller

import (
	"testing"

	"context"

	"errors"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/mock"
	k8mock "github.com/lyft/flinkk8soperator/pkg/controller/k8/mock"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/stretchr/testify/assert"
)

func getTestStateMachine() FlinkStateMachine {
	return FlinkStateMachine{
		flinkController: &mock.MockFlinkController{},
		k8Cluster:       &k8mock.MockK8Cluster{},
	}
}

func TestHandleNewOrCreate(t *testing.T) {
	jobJarName := "ExampleJar"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.Equal(t, jobJarName, application.Spec.FlinkJob.JarName)
		return nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, jobJarName, application.Spec.FlinkJob.JarName)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Spec: v1alpha1.FlinkApplicationSpec{
			FlinkJob: v1alpha1.FlinkJobInfo{
				JarName: jobJarName,
			},
		},
	})
	assert.Nil(t, err)
}

func TestHandleNewOrCreateError(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		return errors.New("random")
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		assert.False(t, true)
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{})
	assert.NotNil(t, err)
}

func TestHandleStartingSingleClusterReady(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.IsMultipleClusterPresentFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
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
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
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
	triggerId := "trigger"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.IsMultipleClusterPresentFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerId, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, triggerId, application.Spec.FlinkJob.SavepointInfo.TriggerId)
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
	jobId := "j1"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
		return []client.FlinkJob{
			{
				JobId:  jobId,
				Status: client.FlinkJobRunning,
			},
		}, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, jobId, application.Status.ActiveJobId)
		assert.Equal(t, v1alpha1.FlinkApplicationRunning, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationReady,
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationReadyNotRunning(t *testing.T) {
	updateInvoked := false
	jobId := "j1"
	activeJobId := "j2"
	savePointLoc := "location"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsServiceReadyFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockFlinkController.GetJobsForApplicationFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
		return []client.FlinkJob{
			{
				JobId:  jobId,
				Status: client.FlinkJobCanceled,
			},
		}, nil
	}
	mockFlinkController.StartFlinkJobFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return activeJobId, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, activeJobId, application.Status.ActiveJobId)
		assert.Empty(t, application.Spec.FlinkJob.SavepointInfo.SavepointLocation)
		assert.Empty(t, application.Spec.FlinkJob.SavepointInfo.TriggerId)
		assert.Equal(t, v1alpha1.FlinkApplicationRunning, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationReady,
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			FlinkJob: v1alpha1.FlinkJobInfo{
				SavepointInfo: v1alpha1.SavepointInfo{
					SavepointLocation: savePointLoc,
				},
			},
		},
	})
	assert.True(t, updateInvoked)
	assert.Nil(t, err)
}

func TestHandleApplicationNotReady(t *testing.T) {
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
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
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
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
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		assert.False(t, true)
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
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.HasApplicationChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
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
	savePointLoc := "location"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: savePointLoc,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error {
		assert.False(t, deleteFrontEnd)
		return nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, savePointLoc, application.Spec.FlinkJob.SavepointInfo.SavepointLocation)
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
	savePointLoc := "location"
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
			Operation: client.SavepointOperationResponse{
				Location: savePointLoc,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error {
		assert.False(t, deleteFrontEnd)
		return nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, savePointLoc, application.Spec.FlinkJob.SavepointInfo.SavepointLocation)
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
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointInProgress,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error {
		assert.False(t, true)
		return nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
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
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.GetSavepointStatusFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
		return &client.SavepointResponse{
			SavepointStatus: client.SavepointStatusResponse{
				Status: client.SavePointCompleted,
			},
		}, nil
	}

	mockFlinkController.DeleteOldClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error {
		assert.False(t, true)
		return nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Empty(t, application.Spec.FlinkJob.SavepointInfo.SavepointLocation)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
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

func TestHandleApplicationUpdatingImageChanged(t *testing.T) {
	updateInvoked := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterChangeNeededFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.Equal(t, v1alpha1.FlinkApplicationUpdating, application.Status.Phase)
		return nil
	}
	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
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

func TestHandleApplicationUpdatingParallelismChanged(t *testing.T) {
	updateInvoked := false
	triggerId := "t1"
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterChangeNeededFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.False(t, true)
		return nil
	}

	mockFlinkController.CheckAndUpdateClusterResourcesFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockFlinkController.HasApplicationJobChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return true, nil
	}

	mockFlinkController.CancelWithSavepointFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
		return triggerId, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, triggerId, application.Spec.FlinkJob.SavepointInfo.TriggerId)
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
	updateInvoked := false
	taskManagerUpdated := false
	stateMachineForTest := getTestStateMachine()
	mockFlinkController := stateMachineForTest.flinkController.(*mock.MockFlinkController)
	mockFlinkController.IsClusterChangeNeededFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockFlinkController.CreateClusterFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) error {
		assert.False(t, true)
		return nil
	}

	mockFlinkController.CheckAndUpdateClusterResourcesFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		taskManagerUpdated = true
		return false, nil
	}

	mockFlinkController.HasApplicationJobChangedFunc = func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
		return false, nil
	}

	mockK8Cluster := stateMachineForTest.k8Cluster.(*k8mock.MockK8Cluster)
	mockK8Cluster.UpdateK8ObjectFunc = func(ctx context.Context, object sdk.Object) error {
		application := object.(*v1alpha1.FlinkApplication)
		assert.Equal(t, v1alpha1.FlinkApplicationClusterStarting, application.Status.Phase)
		updateInvoked = true
		return nil
	}
	err := stateMachineForTest.Handle(context.Background(), &v1alpha1.FlinkApplication{
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationUpdating,
		},
	})
	assert.True(t, updateInvoked)
	assert.True(t, taskManagerUpdated)
	assert.Nil(t, err)
}
