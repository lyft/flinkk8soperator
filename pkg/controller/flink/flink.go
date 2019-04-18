package flink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"k8s.io/api/apps/v1"
)

const proxyUrl = "http://localhost:%d/api/v1/namespaces/%s/services/%s-jm:8081/proxy"
const port = 8081

// Maximum age of an externalized checkpoint that we will attempt to restore
const maxRestoreCheckpointAge = 24 * time.Hour

// Interface to manage Flink Application in Kubernetes
type FlinkInterface interface {
	// Creates a Flink cluster with necessary Job Manager, Task Managers and services for UI
	CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error

	// Deletes a Flink cluster that does not match the spec of the Application if there is one
	DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Cancels the running/active jobs in the Cluster for the Application after savepoint is created
	CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)

	// Starts the Job in the Flink Cluster based on values in the Application
	StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)

	// Savepoint creation is asynchronous.
	// Polls the status of the Savepoint, using the triggerId
	GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error)

	// Check if the Flink Kubernetes Cluster is Ready.
	// Checks if all the pods of task and job managers are ready.
	IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Checks to see if the Flink Cluster is ready to handle API requests
	IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Compares the Application Spec with the underlying Flink Cluster.
	// Return true if the spec does not match the underlying Flink cluster.
	HasApplicationChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Returns the list of Jobs running on the Flink Cluster for the Application
	GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error)

	// For the application, a deployment corresponds to an image. This returns the current and older deployments for the app.
	GetCurrentAndOldDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error)

	FindExternalizedCheckpoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)
}

func NewFlinkController(scope promutils.Scope) FlinkInterface {
	metrics := newFlinkControllerMetrics(scope)
	return &FlinkController{
		k8Cluster:        k8.NewK8Cluster(),
		flinkJobManager:  NewFlinkJobManagerController(scope),
		FlinkTaskManager: NewFlinkTaskManagerController(scope),
		flinkClient:      client.NewFlinkJobManagerClient(scope),
		metrics:          metrics,
	}
}

func newFlinkControllerMetrics(scope promutils.Scope) *flinkControllerMetrics {
	flinkControllerScope := scope.NewSubScope("flink_controller")
	return &flinkControllerMetrics{
		scope:                       scope,
		deleteClusterSuccessCounter: labeled.NewCounter("delete_cluster_success", "Flink cluster deleted successfully", flinkControllerScope),
		deleteClusterFailedCounter:  labeled.NewCounter("delete_cluster_failure", "Flink cluster deletion failed", flinkControllerScope),
		applicationChangedCounter:   labeled.NewCounter("app_changed_counter", "Flink application has changed", flinkControllerScope),
	}
}

type flinkControllerMetrics struct {
	scope                       promutils.Scope
	deleteClusterSuccessCounter labeled.Counter
	deleteClusterFailedCounter  labeled.Counter
	applicationChangedCounter   labeled.Counter
}

type FlinkController struct {
	k8Cluster        k8.K8ClusterInterface
	flinkJobManager  FlinkJobManagerControllerInterface
	FlinkTaskManager FlinkTaskManagerControllerInterface
	flinkClient      client.FlinkAPIInterface
	metrics          *flinkControllerMetrics
}

func getUrlFromApp(application *v1alpha1.FlinkApplication) string {
	cfg := config.GetConfig()
	if cfg.UseProxy {
		return fmt.Sprintf(proxyUrl, cfg.ProxyPort.Port, application.Namespace, application.Name)
	} else {
		return fmt.Sprintf("http://%s:%d", GetJobManagerExternalServiceName(application), port)
	}
}

func GetActiveFlinkJob(jobs []client.FlinkJob) *client.FlinkJob {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		if job.Status == client.FlinkJobRunning ||
			job.Status == client.FlinkJobCreated {
			return &job
		}
	}
	return nil
}

// returns true iff the deployment exactly matches the flink application
func (f *FlinkController) deploymentMatches(ctx context.Context, deployment *v1.Deployment, application *v1alpha1.FlinkApplication) bool {
	if DeploymentIsTaskmanager(deployment) {
		return TaskManagerDeploymentMatches(deployment, application)
	}
	if DeploymentIsJobmanager(deployment) {
		return JobManagerDeploymentMatches(deployment, application)
	}

	logger.Warnf(ctx, "Found deployment that is not a TaskManager or JobManager: %s", deployment.Name)
	return false
}

func (f *FlinkController) GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
	jobResponse, err := f.flinkClient.GetJobs(ctx, getUrlFromApp(application))
	if err != nil {
		return nil, err
	}
	return jobResponse.Jobs, nil
}

// The operator for now assumes and is intended to run single application per Flink Cluster.
// Once we move to run multiple applications, this has to be removed/updated
func (f *FlinkController) getJobIdForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	if application.Status.JobId != "" {
		return application.Status.JobId, nil
	}

	return "", errors.New("active job id not available")
}

func (f *FlinkController) CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return "", err
	}
	return f.flinkClient.CancelJobWithSavepoint(ctx, getUrlFromApp(application), jobId)
}

func (f *FlinkController) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	err := f.flinkJobManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Job manager cluster creation did not succeed %v", err)
		return err
	}
	err = f.FlinkTaskManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Task manager cluster creation did not succeed %v", err)
		return err
	}
	return nil
}

func (f *FlinkController) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	response, err := f.flinkClient.SubmitJob(
		ctx,
		getUrlFromApp(application),
		application.Spec.JarName,
		client.SubmitJobRequest{
			Parallelism:   application.Spec.Parallelism,
			SavepointPath: application.Spec.SavepointInfo.SavepointLocation,
			EntryClass:    application.Spec.EntryClass,
			ProgramArgs:   application.Spec.ProgramArgs,
		})
	if err != nil {
		return "", err
	}
	if response.JobId == "" {
		logger.Errorf(ctx, "Job id in the submit job response was empty")
		return "", errors.New("unable to submit job: invalid job id")
	}
	return response.JobId, nil
}

func (f *FlinkController) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return nil, err
	}
	return f.flinkClient.CheckSavepointStatus(ctx, getUrlFromApp(application), jobId, application.Spec.SavepointInfo.TriggerId)
}

func (f *FlinkController) DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	_, oldDeployments, err := f.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	if len(oldDeployments) == 0 {
		logger.Infof(ctx, "No old deployments found for the cluster to delete")
		return true, nil
	}
	err = f.k8Cluster.DeleteDeployments(ctx, v1.DeploymentList{
		Items: oldDeployments,
	})
	if err != nil {
		f.metrics.deleteClusterFailedCounter.Inc(ctx)
		return false, err
	}
	f.metrics.deleteClusterSuccessCounter.Inc(ctx)
	return true, nil
}

func (f *FlinkController) IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	appHashLabel := GetAppHashSelector(application)
	return f.k8Cluster.AreAllPodsRunning(ctx, application.Namespace, appHashLabel)
}

func (f *FlinkController) IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	_, err := f.flinkClient.GetClusterOverview(ctx, getUrlFromApp(application))
	if err != nil {
		logger.Infof(ctx, "Error response indicating flink API is not ready to handle request %v", err)
		return false, err
	}
	return true, nil
}

func (f *FlinkController) HasApplicationChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	cur, _, err := f.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}

	return len(cur) == 0, nil
}

func (f *FlinkController) GetCurrentAndOldDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error) {
	appLabels := k8.GetAppLabel(application.Name)
	deployments, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, appLabels)
	if err != nil {
		return nil, nil, err
	}

	cur := make([]v1.Deployment, 0)
	old := make([]v1.Deployment, 0)
	appHash := HashForApplication(application)
	for _, deployment := range deployments.Items {
		if f.deploymentMatches(ctx, &deployment, application) {
			cur = append(cur, deployment)
		} else {
			if deployment.Labels[FlinkAppHash] == appHash {
				// we had a hash collision (i.e., the previous application has the same hash as the new one)
				// this is *very* unlikely to occur (1/2^32)
				return nil, nil, errors.New("found hash collision for deployment, you must do a clean deploy")
			}
			old = append(old, deployment)
		}
	}

	return cur, old, nil
}

// Attempts to find an externalized checkpoint for the job. This can be used to recover an application that is not
// able to savepoint for some reason.
func (f *FlinkController) FindExternalizedCheckpoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	checkpoint, err := f.flinkClient.GetLatestCheckpoint(ctx, getUrlFromApp(application), application.Status.JobId)
	if err != nil {
		return "", err
	}
	if checkpoint == nil {
		return "", nil
	}

	if time.Now().Sub(time.Unix(checkpoint.TriggerTimestamp, 0)) > maxRestoreCheckpointAge {
		logger.Info(ctx, "Found checkpoint to restore from, but was too old")
		return "", nil
	}

	return checkpoint.ExternalPath, nil
}
