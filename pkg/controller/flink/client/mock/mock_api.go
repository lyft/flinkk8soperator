package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
)

type CancelJobWithSavepointFunc func(ctx context.Context, url string, jobID string) (string, error)
type ForceCancelJobFunc func(ctx context.Context, url string, jobID string) error
type SubmitJobFunc func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error)
type CheckSavepointStatusFunc func(ctx context.Context, url string, jobID, triggerID string) (*client.SavepointResponse, error)
type GetJobsFunc func(ctx context.Context, url string) (*client.GetJobsResponse, error)
type GetClusterOverviewFunc func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error)
type GetLatestCheckpointFunc func(ctx context.Context, url string, jobID string) (*client.CheckpointStatistics, error)
type GetJobConfigFunc func(ctx context.Context, url string, jobID string) (*client.JobConfigResponse, error)
type GetTaskManagersFunc func(ctx context.Context, url string) (*client.TaskManagersResponse, error)
type GetCheckpointCountsFunc func(ctx context.Context, url string, jobID string) (*client.CheckpointResponse, error)
type GetJobOverviewFunc func(ctx context.Context, url string, jobID string) (*client.FlinkJobOverview, error)
type SavepointJobFunc func(ctx context.Context, url string, jobID string) (string, error)
type JobManagerClient struct {
	CancelJobWithSavepointFunc CancelJobWithSavepointFunc
	ForceCancelJobFunc         ForceCancelJobFunc
	SubmitJobFunc              SubmitJobFunc
	CheckSavepointStatusFunc   CheckSavepointStatusFunc
	GetJobsFunc                GetJobsFunc
	GetClusterOverviewFunc     GetClusterOverviewFunc
	GetJobConfigFunc           GetJobConfigFunc
	GetLatestCheckpointFunc    GetLatestCheckpointFunc
	GetTaskManagersFunc        GetTaskManagersFunc
	GetCheckpointCountsFunc    GetCheckpointCountsFunc
	GetJobOverviewFunc         GetJobOverviewFunc
	SavepointJobFunc           SavepointJobFunc
}

func (m *JobManagerClient) SubmitJob(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
	if m.SubmitJobFunc != nil {
		return m.SubmitJobFunc(ctx, url, jarID, submitJobRequest)
	}
	return nil, nil
}

func (m *JobManagerClient) CancelJobWithSavepoint(ctx context.Context, url string, jobID string) (string, error) {
	if m.CancelJobWithSavepointFunc != nil {
		return m.CancelJobWithSavepointFunc(ctx, url, jobID)
	}
	return "", nil
}

func (m *JobManagerClient) ForceCancelJob(ctx context.Context, url string, jobID string) error {
	if m.ForceCancelJobFunc != nil {
		return m.ForceCancelJobFunc(ctx, url, jobID)
	}
	return nil
}

func (m *JobManagerClient) CheckSavepointStatus(ctx context.Context, url string, jobID, triggerID string) (*client.SavepointResponse, error) {
	if m.CheckSavepointStatusFunc != nil {
		return m.CheckSavepointStatusFunc(ctx, url, jobID, triggerID)
	}
	return nil, nil
}

func (m *JobManagerClient) GetJobs(ctx context.Context, url string) (*client.GetJobsResponse, error) {
	if m.GetJobsFunc != nil {
		return m.GetJobsFunc(ctx, url)
	}
	return nil, nil
}

func (m *JobManagerClient) GetClusterOverview(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
	if m.GetClusterOverviewFunc != nil {
		return m.GetClusterOverviewFunc(ctx, url)
	}
	return nil, nil
}

func (m *JobManagerClient) GetJobConfig(ctx context.Context, url string, jobID string) (*client.JobConfigResponse, error) {
	if m.GetJobConfigFunc != nil {
		return m.GetJobConfigFunc(ctx, url, jobID)
	}
	return nil, nil
}

func (m *JobManagerClient) GetLatestCheckpoint(ctx context.Context, url string, jobID string) (*client.CheckpointStatistics, error) {
	if m.GetLatestCheckpointFunc != nil {
		return m.GetLatestCheckpointFunc(ctx, url, jobID)
	}
	return nil, nil
}

func (m *JobManagerClient) GetTaskManagers(ctx context.Context, url string) (*client.TaskManagersResponse, error) {
	if m.GetTaskManagersFunc != nil {
		return m.GetTaskManagersFunc(ctx, url)
	}
	return nil, nil
}

func (m *JobManagerClient) GetCheckpointCounts(ctx context.Context, url string, jobID string) (*client.CheckpointResponse, error) {
	if m.GetCheckpointCountsFunc != nil {
		return m.GetCheckpointCountsFunc(ctx, url, jobID)
	}
	return nil, nil
}

func (m *JobManagerClient) GetJobOverview(ctx context.Context, url string, jobID string) (*client.FlinkJobOverview, error) {
	if m.GetJobOverviewFunc != nil {
		return m.GetJobOverviewFunc(ctx, url, jobID)
	}
	return nil, nil
}

func (m *JobManagerClient) SavepointJob(ctx context.Context, url string, jobID string) (string, error) {
	if m.SavepointJobFunc != nil {
		return m.SavepointJobFunc(ctx, url, jobID)
	}

	return "", nil
}
