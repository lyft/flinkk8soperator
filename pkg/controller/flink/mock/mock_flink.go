package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	corev1 "k8s.io/api/core/v1"
)

type CreateClusterFunc func(ctx context.Context, application *v1beta1.FlinkApplication) error
type DeleteOldResourcesForApp func(ctx context.Context, application *v1beta1.FlinkApplication) error
type CancelWithSavepointFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error)
type ForceCancelFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error
type StartFlinkJobFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error)
type GetSavepointStatusFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error)
type IsClusterReadyFunc func(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error)
type IsServiceReadyFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error)
type GetJobsForApplicationFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error)
type GetJobForApplicationFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error)
type GetCurrentDeploymentsForAppFunc func(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error)
type FindExternalizedCheckpointFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error)
type CompareAndUpdateClusterStatusFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error)
type CompareAndUpdateJobStatusFunc func(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error)
type GetLatestClusterStatusFunc func(ctx context.Context, app *v1beta1.FlinkApplication) v1beta1.FlinkClusterStatus
type GetLatestJobStatusFunc func(ctx context.Context, app *v1beta1.FlinkApplication) v1beta1.FlinkJobStatus
type GetLatestJobIDFunc func(ctx context.Context, app *v1beta1.FlinkApplication) string
type UpdateLatestJobIDFunc func(ctx context.Context, app *v1beta1.FlinkApplication, jobID string)
type UpdateLatestJobStatusFunc func(ctx context.Context, app *v1beta1.FlinkApplication, jobStatus v1beta1.FlinkJobStatus)
type UpdateLatestClusterStatusFunc func(ctx context.Context, app *v1beta1.FlinkApplication, clusterStatus v1beta1.FlinkClusterStatus)

type FlinkController struct {
	CreateClusterFunc                 CreateClusterFunc
	DeleteOldResourcesForAppFunc      DeleteOldResourcesForApp
	CancelWithSavepointFunc           CancelWithSavepointFunc
	ForceCancelFunc                   ForceCancelFunc
	StartFlinkJobFunc                 StartFlinkJobFunc
	GetSavepointStatusFunc            GetSavepointStatusFunc
	IsClusterReadyFunc                IsClusterReadyFunc
	IsServiceReadyFunc                IsServiceReadyFunc
	GetJobsForApplicationFunc         GetJobsForApplicationFunc
	GetJobForApplicationFunc          GetJobForApplicationFunc
	GetCurrentDeploymentsForAppFunc   GetCurrentDeploymentsForAppFunc
	FindExternalizedCheckpointFunc    FindExternalizedCheckpointFunc
	Events                            []corev1.Event
	CompareAndUpdateClusterStatusFunc CompareAndUpdateClusterStatusFunc
	CompareAndUpdateJobStatusFunc     CompareAndUpdateJobStatusFunc
	GetLatestClusterStatusFunc        GetLatestClusterStatusFunc
	GetLatestJobStatusFunc            GetLatestJobStatusFunc
	GetLatestJobIDFunc                GetLatestJobIDFunc
	UpdateLatestJobIDFunc             UpdateLatestJobIDFunc
	UpdateLatestJobStatusFunc         UpdateLatestJobStatusFunc
	UpdateLatestClusterStatusFunc     UpdateLatestClusterStatusFunc
}

func (m *FlinkController) GetCurrentDeploymentsForApp(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
	if m.GetCurrentDeploymentsForAppFunc != nil {
		return m.GetCurrentDeploymentsForAppFunc(ctx, application)
	}
	return nil, nil
}

func (m *FlinkController) DeleteOldResourcesForApp(ctx context.Context, application *v1beta1.FlinkApplication) error {
	if m.DeleteOldResourcesForAppFunc != nil {
		return m.DeleteOldResourcesForAppFunc(ctx, application)
	}
	return nil
}

func (m *FlinkController) CreateCluster(ctx context.Context, application *v1beta1.FlinkApplication) error {
	if m.CreateClusterFunc != nil {
		return m.CreateClusterFunc(ctx, application)
	}
	return nil
}

func (m *FlinkController) CancelWithSavepoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
	if m.CancelWithSavepointFunc != nil {
		return m.CancelWithSavepointFunc(ctx, application, hash)
	}
	return "", nil
}

func (m *FlinkController) ForceCancel(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error {
	if m.ForceCancelFunc != nil {
		return m.ForceCancelFunc(ctx, application, hash)
	}
	return nil
}

func (m *FlinkController) StartFlinkJob(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool, savepointPath string) (string, error) {
	if m.StartFlinkJobFunc != nil {
		return m.StartFlinkJobFunc(ctx, application, hash, jarName, parallelism, entryClass, programArgs, allowNonRestoredState, savepointPath)
	}
	return "", nil
}

func (m *FlinkController) GetSavepointStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
	if m.GetSavepointStatusFunc != nil {
		return m.GetSavepointStatusFunc(ctx, application, hash)
	}
	return nil, nil
}

func (m *FlinkController) IsClusterReady(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	if m.IsClusterReadyFunc != nil {
		return m.IsClusterReadyFunc(ctx, application)
	}
	return false, nil
}

func (m *FlinkController) IsServiceReady(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
	if m.IsServiceReadyFunc != nil {
		return m.IsServiceReadyFunc(ctx, application, hash)
	}
	return false, nil
}

func (m *FlinkController) GetJobsForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
	if m.GetJobsForApplicationFunc != nil {
		return m.GetJobsForApplicationFunc(ctx, application, hash)
	}
	return nil, nil
}

func (m *FlinkController) GetJobForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
	if m.GetJobForApplicationFunc != nil {
		return m.GetJobForApplicationFunc(ctx, application, hash)
	}
	return nil, nil
}

func (m *FlinkController) FindExternalizedCheckpoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
	if m.FindExternalizedCheckpointFunc != nil {
		return m.FindExternalizedCheckpointFunc(ctx, application, hash)
	}
	return "", nil
}

func (m *FlinkController) LogEvent(ctx context.Context, app *v1beta1.FlinkApplication, eventType string, reason string, message string) {
	m.Events = append(m.Events, corev1.Event{
		InvolvedObject: corev1.ObjectReference{
			Kind:      app.Kind,
			Name:      app.Name,
			Namespace: app.Namespace,
		},
		Type:    eventType,
		Reason:  reason,
		Message: message,
	})
}

func (m *FlinkController) CompareAndUpdateClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
	if m.CompareAndUpdateClusterStatusFunc != nil {
		return m.CompareAndUpdateClusterStatusFunc(ctx, application, hash)
	}

	return false, nil
}

func (m *FlinkController) CompareAndUpdateJobStatus(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (bool, error) {
	if m.CompareAndUpdateJobStatusFunc != nil {
		return m.CompareAndUpdateJobStatusFunc(ctx, app, hash)
	}

	return false, nil
}

func (m *FlinkController) GetLatestClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication) v1beta1.FlinkClusterStatus {
	if m.GetLatestClusterStatusFunc != nil {
		return m.GetLatestClusterStatusFunc(ctx, application)
	}

	return application.Status.VersionStatuses[getCurrentStatusIndex(application)].ClusterStatus
}

func (m *FlinkController) GetLatestJobStatus(ctx context.Context, application *v1beta1.FlinkApplication) v1beta1.FlinkJobStatus {
	if m.GetLatestClusterStatusFunc != nil {
		return m.GetLatestJobStatusFunc(ctx, application)
	}

	return application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus
}

func (m *FlinkController) GetLatestJobID(ctx context.Context, application *v1beta1.FlinkApplication) string {
	if m.GetLatestClusterStatusFunc != nil {
		return m.GetLatestJobIDFunc(ctx, application)
	}

	return application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus.JobID
}

func (m *FlinkController) UpdateLatestJobID(ctx context.Context, application *v1beta1.FlinkApplication, jobID string) {
	if m.UpdateLatestJobIDFunc != nil {
		m.UpdateLatestJobIDFunc(ctx, application, jobID)
	}

	application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus.JobID = jobID
}

func (m *FlinkController) UpdateLatestJobStatus(ctx context.Context, application *v1beta1.FlinkApplication, jobStatus v1beta1.FlinkJobStatus) {
	if m.UpdateLatestJobStatusFunc != nil {
		m.UpdateLatestJobStatusFunc(ctx, application, jobStatus)
	}

	application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus = jobStatus
}

func (m *FlinkController) UpdateLatestClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication, clusterStatus v1beta1.FlinkClusterStatus) {
	if m.UpdateLatestClusterStatusFunc != nil {
		m.UpdateLatestClusterStatusFunc(ctx, application, clusterStatus)
	}

	application.Status.VersionStatuses[getCurrentStatusIndex(application)].ClusterStatus = clusterStatus
}

func getCurrentStatusIndex(app *v1beta1.FlinkApplication) int32 {
	desiredCount := v1beta1.GetMaxRunningJobs(app.Spec.DeploymentMode)
	if v1beta1.IsRunningPhase(app.Status.Phase) {
		return 0
	}

	return desiredCount - 1
}
