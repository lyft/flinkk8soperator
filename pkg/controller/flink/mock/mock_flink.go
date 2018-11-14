package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
)

type CreateClusterFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) error
type DeleteOldClusterFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error
type CancelWithSavepointFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)
type StartFlinkJobFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)
type GetSavepointStatusFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error)
type IsClusterReadyFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type IsServiceReadyFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type HasApplicationChangedFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type IsClusterChangeNeededFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type IsClusterUpdateNeededFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type CheckAndUpdateClusterResourcesFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type HasApplicationJobChangedFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type IsMultipleClusterPresentFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type GetJobsForApplicationFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error)

type MockFlinkController struct {
	CreateClusterFunc                  CreateClusterFunc
	DeleteOldClusterFunc               DeleteOldClusterFunc
	CancelWithSavepointFunc            CancelWithSavepointFunc
	StartFlinkJobFunc                  StartFlinkJobFunc
	GetSavepointStatusFunc             GetSavepointStatusFunc
	IsClusterReadyFunc                 IsClusterReadyFunc
	IsServiceReadyFunc                 IsServiceReadyFunc
	HasApplicationChangedFunc          HasApplicationChangedFunc
	IsClusterChangeNeededFunc          IsClusterChangeNeededFunc
	IsClusterUpdateNeededFunc          IsClusterUpdateNeededFunc
	CheckAndUpdateClusterResourcesFunc CheckAndUpdateClusterResourcesFunc
	HasApplicationJobChangedFunc       HasApplicationJobChangedFunc
	IsMultipleClusterPresentFunc       IsMultipleClusterPresentFunc
	GetJobsForApplicationFunc          GetJobsForApplicationFunc
}

func (m *MockFlinkController) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if m.CreateClusterFunc != nil {
		return m.CreateClusterFunc(ctx, application)
	}
	return nil
}

func (m *MockFlinkController) DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication, deleteFrontEnd bool) error {
	if m.DeleteOldClusterFunc != nil {
		return m.DeleteOldClusterFunc(ctx, application, deleteFrontEnd)
	}
	return nil
}

func (m *MockFlinkController) CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	if m.CancelWithSavepointFunc != nil {
		return m.CancelWithSavepointFunc(ctx, application)
	}
	return "", nil
}

func (m *MockFlinkController) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	if m.StartFlinkJobFunc != nil {
		return m.StartFlinkJobFunc(ctx, application)
	}
	return "", nil
}

func (m *MockFlinkController) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
	if m.GetSavepointStatusFunc != nil {
		return m.GetSavepointStatusFunc(ctx, application)
	}
	return nil, nil
}

func (m *MockFlinkController) IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsClusterReadyFunc != nil {
		return m.IsClusterReadyFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsServiceReadyFunc != nil {
		return m.IsServiceReadyFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) HasApplicationChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.HasApplicationChangedFunc != nil {
		return m.HasApplicationChangedFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) IsClusterChangeNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsClusterChangeNeededFunc != nil {
		return m.IsClusterChangeNeededFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) IsClusterUpdateNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsClusterUpdateNeededFunc != nil {
		return m.IsClusterUpdateNeededFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) CheckAndUpdateClusterResources(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.CheckAndUpdateClusterResourcesFunc != nil {
		return m.CheckAndUpdateClusterResourcesFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) HasApplicationJobChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.HasApplicationJobChangedFunc != nil {
		return m.HasApplicationJobChangedFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) IsMultipleClusterPresent(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsMultipleClusterPresentFunc != nil {
		return m.IsMultipleClusterPresentFunc(ctx, application)
	}
	return false, nil
}

func (m *MockFlinkController) GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
	if m.GetJobsForApplicationFunc != nil {
		return m.GetJobsForApplicationFunc(ctx, application)
	}
	return nil, nil
}
