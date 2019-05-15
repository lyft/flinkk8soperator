package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	corev1 "k8s.io/api/core/v1"
)

type CreateClusterFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) error
type DeleteClusterFunc func(ctx context.Context, deployment *common.FlinkDeployment) error
type CancelWithSavepointFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error)
type ForceCancelFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error
type StartFlinkJobFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string) (string, error)
type GetSavepointStatusFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error)
type IsClusterReadyFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)
type IsServiceReadyFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error)
type GetJobsForApplicationFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error)
type GetCurrentAndOldDeploymentsForAppFunc func(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, []common.FlinkDeployment, error)
type FindExternalizedCheckpointFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error)
type CompareAndUpdateClusterStatusFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error)
type CompareAndUpdateJobStatusFunc func(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error)

type FlinkController struct {
	CreateClusterFunc                     CreateClusterFunc
	DeleteClusterFunc                     DeleteClusterFunc
	CancelWithSavepointFunc               CancelWithSavepointFunc
	ForceCancelFunc                       ForceCancelFunc
	StartFlinkJobFunc                     StartFlinkJobFunc
	GetSavepointStatusFunc                GetSavepointStatusFunc
	IsClusterReadyFunc                    IsClusterReadyFunc
	IsServiceReadyFunc                    IsServiceReadyFunc
	GetJobsForApplicationFunc             GetJobsForApplicationFunc
	GetCurrentAndOldDeploymentsForAppFunc GetCurrentAndOldDeploymentsForAppFunc
	FindExternalizedCheckpointFunc        FindExternalizedCheckpointFunc
	Events                                []corev1.Event
	CompareAndUpdateClusterStatusFunc     CompareAndUpdateClusterStatusFunc
	CompareAndUpdateJobStatusFunc         CompareAndUpdateJobStatusFunc
}

func (m *FlinkController) GetCurrentAndOldDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, []common.FlinkDeployment, error) {
	if m.GetCurrentAndOldDeploymentsForAppFunc != nil {
		return m.GetCurrentAndOldDeploymentsForAppFunc(ctx, application)
	}
	return nil, nil, nil
}

func (m *FlinkController) DeleteCluster(ctx context.Context, deployment *common.FlinkDeployment) error {
	if m.DeleteClusterFunc != nil {
		return m.DeleteClusterFunc(ctx, deployment)
	}
	return nil
}

func (m *FlinkController) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if m.CreateClusterFunc != nil {
		return m.CreateClusterFunc(ctx, application)
	}
	return nil
}

func (m *FlinkController) CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
	if m.CancelWithSavepointFunc != nil {
		return m.CancelWithSavepointFunc(ctx, application, hash)
	}
	return "", nil
}

func (m *FlinkController) ForceCancel(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error {
	if m.ForceCancelFunc != nil {
		return m.ForceCancelFunc(ctx, application, hash)
	}
	return nil
}

func (m *FlinkController) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string) (string, error) {
	if m.StartFlinkJobFunc != nil {
		return m.StartFlinkJobFunc(ctx, application, hash, jarName, parallelism, entryClass, programArgs)
	}
	return "", nil
}

func (m *FlinkController) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
	if m.GetSavepointStatusFunc != nil {
		return m.GetSavepointStatusFunc(ctx, application, hash)
	}
	return nil, nil
}

func (m *FlinkController) IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	if m.IsClusterReadyFunc != nil {
		return m.IsClusterReadyFunc(ctx, application)
	}
	return false, nil
}

func (m *FlinkController) IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
	if m.IsServiceReadyFunc != nil {
		return m.IsServiceReadyFunc(ctx, application, hash)
	}
	return false, nil
}

func (m *FlinkController) GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
	if m.GetJobsForApplicationFunc != nil {
		return m.GetJobsForApplicationFunc(ctx, application, hash)
	}
	return nil, nil
}

func (m *FlinkController) FindExternalizedCheckpoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
	if m.FindExternalizedCheckpointFunc != nil {
		return m.FindExternalizedCheckpointFunc(ctx, application, hash)
	}
	return "", nil
}

func (m *FlinkController) LogEvent(ctx context.Context, app *v1alpha1.FlinkApplication, fieldPath string, eventType string, message string) {
	m.Events = append(m.Events, k8.CreateEvent(app, fieldPath, eventType, "Test", message))
}

func (m *FlinkController) CompareAndUpdateClusterStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
	if m.CompareAndUpdateClusterStatusFunc != nil {
		return m.CompareAndUpdateClusterStatusFunc(ctx, application, hash)
	}

	return false, nil
}

func (m *FlinkController) CompareAndUpdateJobStatus(ctx context.Context, app *v1alpha1.FlinkApplication, hash string) (bool, error) {
	if m.CompareAndUpdateJobStatusFunc != nil {
		return m.CompareAndUpdateJobStatusFunc(ctx, app, hash)
	}

	return false, nil
}
