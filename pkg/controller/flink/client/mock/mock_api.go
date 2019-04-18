package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
)

type CancelJobWithSavepointFunc func(ctx context.Context, url string, jobID string) (string, error)
type SubmitJobFunc func(ctx context.Context, url string, jarID string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error)
type CheckSavepointStatusFunc func(ctx context.Context, url string, jobID, triggerID string) (*client.SavepointResponse, error)
type GetJobsFunc func(ctx context.Context, url string) (*client.GetJobsResponse, error)
type GetClusterOverviewFunc func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error)
type GetLatestCheckpointFunc func(ctx context.Context, url string, jobID string) (*client.CheckpointStatistics, error)
type GetJobConfigFunc func(ctx context.Context, url string, jobID string) (*client.JobConfigResponse, error)

type JobManagerClient struct {
	CancelJobWithSavepointFunc CancelJobWithSavepointFunc
	SubmitJobFunc              SubmitJobFunc
	CheckSavepointStatusFunc   CheckSavepointStatusFunc
	GetJobsFunc                GetJobsFunc
	GetClusterOverviewFunc     GetClusterOverviewFunc
	GetJobConfigFunc           GetJobConfigFunc
	GetLatestCheckpointFunc    GetLatestCheckpointFunc
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
