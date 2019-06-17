package flink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const proxyURL = "http://localhost:%d/api/v1/namespaces/%s/services/%s:8081/proxy"
const port = 8081

// Maximum age of an externalized checkpoint that we will attempt to restore
const maxRestoreCheckpointAge = 24 * time.Hour

// If the last hearbeat from a taskmanager was more than taskManagerHeartbeatThreshold, the task
// manager is considered unhealthy.
const taskManagerHeartbeatThreshold = 2 * time.Minute

// Maximum allowable number of checkpoint failures before job health status is Red
const maxCheckpointTime = 10 * time.Minute

// If the job has been in and out of a FAILING state within failingIntervalThreshold, we consider
// the JobStatus.Health to be "Red"
const failingIntervalThreshold = 1 * time.Minute

// Interface to manage Flink Application in Kubernetes
type ControllerInterface interface {
	// Creates a Flink cluster with necessary Job Manager, Task Managers and services for UI
	CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error

	// Cancels the running/active jobs in the Cluster for the Application after savepoint is created
	CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error)

	// Force cancels the running/active job without taking a savepoint
	ForceCancel(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error

	// Starts the Job in the Flink Cluster
	StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string) (string, error)

	// Savepoint creation is asynchronous.
	// Polls the status of the Savepoint, using the triggerID
	GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error)

	// Check if the Flink Kubernetes Cluster is Ready.
	// Checks if all the pods of task and job managers are ready.
	IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Checks to see if the Flink Cluster is ready to handle API requests
	IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error)

	// Returns the list of Jobs running on the Flink Cluster for the Application
	GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error)

	// Returns the pair of deployments (tm/jm) for the current version of the application
	GetCurrentDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, error)

	// Deletes all old resources (deployments and services) for the app
	DeleteOldResourcesForApp(ctx context.Context, app *v1alpha1.FlinkApplication) error

	// Attempts to find an externalized checkpoint for the job. This can be used to recover an application that is not
	// able to savepoint for some reason.
	FindExternalizedCheckpoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error)

	// Logs an event to the FlinkApplication resource and to the operator log
	LogEvent(ctx context.Context, app *v1alpha1.FlinkApplication, fieldPath string, eventType string, message string)

	// Compares and updates new cluster status with current cluster status
	// Returns true if there is a change in ClusterStatus
	CompareAndUpdateClusterStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error)

	// Compares and updates new job status with current job status
	// Returns true if there is a change in JobStatus
	CompareAndUpdateJobStatus(ctx context.Context, app *v1alpha1.FlinkApplication, hash string) (bool, error)
}

func NewController(k8sCluster k8.ClusterInterface, config config.RuntimeConfig) ControllerInterface {
	metrics := newControllerMetrics(config.MetricsScope)
	return &Controller{
		k8Cluster:   k8sCluster,
		jobManager:  NewJobManagerController(k8sCluster, config),
		taskManager: NewTaskManagerController(k8sCluster, config),
		flinkClient: client.NewFlinkJobManagerClient(config),
		metrics:     metrics,
	}
}

func newControllerMetrics(scope promutils.Scope) *controllerMetrics {
	flinkControllerScope := scope.NewSubScope("flink_controller")
	return &controllerMetrics{
		scope:                        scope,
		deleteResourceSuccessCounter: labeled.NewCounter("delete_resource_success", "Flink resource deleted successfully", flinkControllerScope),
		deleteResourceFailedCounter:  labeled.NewCounter("delete_resource_failure", "Flink resource deletion failed", flinkControllerScope),
		applicationChangedCounter:    labeled.NewCounter("app_changed_counter", "Flink application has changed", flinkControllerScope),
	}
}

type controllerMetrics struct {
	scope                        promutils.Scope
	deleteResourceSuccessCounter labeled.Counter
	deleteResourceFailedCounter  labeled.Counter
	applicationChangedCounter    labeled.Counter
}

type Controller struct {
	k8Cluster   k8.ClusterInterface
	jobManager  JobManagerControllerInterface
	taskManager TaskManagerControllerInterface
	flinkClient client.FlinkAPIInterface
	metrics     *controllerMetrics
}

func getURLFromApp(application *v1alpha1.FlinkApplication, hash string) string {
	service := VersionedJobManagerServiceName(application, hash)
	cfg := config.GetConfig()
	if cfg.UseProxy {
		return fmt.Sprintf(proxyURL, cfg.ProxyPort.Port, application.Namespace, service)
	}
	return fmt.Sprintf("http://%s.%s:%d", service, application.Namespace, port)
}

func GetActiveFlinkJob(jobs []client.FlinkJob) *client.FlinkJob {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		if job.Status == client.Running ||
			job.Status == client.Created ||
			job.Status == client.Finished {
			return &job
		}
	}
	return nil
}

// returns true iff the deployment exactly matches the flink application
func (f *Controller) deploymentMatches(ctx context.Context, deployment *v1.Deployment, application *v1alpha1.FlinkApplication) bool {
	if DeploymentIsTaskmanager(deployment) {
		return TaskManagerDeploymentMatches(deployment, application)
	}
	if DeploymentIsJobmanager(deployment) {
		return JobManagerDeploymentMatches(deployment, application)
	}

	logger.Warnf(ctx, "Found deployment that is not a TaskManager or JobManager: %s", deployment.Name)
	return false
}

func (f *Controller) GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
	jobResponse, err := f.flinkClient.GetJobs(ctx, getURLFromApp(application, hash))
	if err != nil {
		return nil, err
	}

	return jobResponse.Jobs, nil
}

// The operator for now assumes and is intended to run single application per Flink Cluster.
// Once we move to run multiple applications, this has to be removed/updated
func (f *Controller) getJobIDForApplication(application *v1alpha1.FlinkApplication) (string, error) {
	if application.Status.JobStatus.JobID != "" {
		return application.Status.JobStatus.JobID, nil
	}

	return "", errors.New("active job id not available")
}

func (f *Controller) CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
	jobID, err := f.getJobIDForApplication(application)
	if err != nil {
		return "", err
	}
	return f.flinkClient.CancelJobWithSavepoint(ctx, getURLFromApp(application, hash), jobID)
}

func (f *Controller) ForceCancel(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) error {
	jobID, err := f.getJobIDForApplication(application)
	if err != nil {
		return err
	}
	return f.flinkClient.ForceCancelJob(ctx, getURLFromApp(application, hash), jobID)
}

func (f *Controller) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	newlyCreatedJm, err := f.jobManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Job manager cluster creation did not succeed %v", err)
		f.LogEvent(ctx, application, "", corev1.EventTypeWarning,
			fmt.Sprintf("Failed to create job managers: %v", err))

		return err
	}
	newlyCreatedTm, err := f.taskManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Task manager cluster creation did not succeed %v", err)
		f.LogEvent(ctx, application, "", corev1.EventTypeWarning,
			fmt.Sprintf("Failed to create task managers: %v", err))
		return err
	}

	if newlyCreatedJm || newlyCreatedTm {
		f.LogEvent(ctx, application, "", corev1.EventTypeNormal, "Flink cluster created")
	}
	return nil
}

func (f *Controller) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string) (string, error) {
	response, err := f.flinkClient.SubmitJob(
		ctx,
		getURLFromApp(application, hash),
		jarName,
		client.SubmitJobRequest{
			Parallelism:   parallelism,
			SavepointPath: application.Spec.SavepointInfo.SavepointLocation,
			EntryClass:    entryClass,
			ProgramArgs:   programArgs,
		})
	if err != nil {
		return "", err
	}
	if response.JobID == "" {
		logger.Errorf(ctx, "Job id in the submit job response was empty")
		return "", errors.New("unable to submit job: invalid job id")
	}
	return response.JobID, nil
}

func (f *Controller) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (*client.SavepointResponse, error) {
	jobID, err := f.getJobIDForApplication(application)
	if err != nil {
		return nil, err
	}
	return f.flinkClient.CheckSavepointStatus(ctx, getURLFromApp(application, hash), jobID, application.Spec.SavepointInfo.TriggerID)
}

func (f *Controller) IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	labelMap := GetAppHashSelector(application)

	deploymentList, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, labelMap)
	if err != nil {
		logger.Warnf(ctx, "Failed to get deployments for label map %v", labelMap)
		return false, err
	}
	if deploymentList == nil || len(deploymentList.Items) == 0 {
		logger.Infof(ctx, "No deployments present for label map %v", labelMap)
		return false, nil
	}

	// TODO: Find if any events can be populated, that are useful to users
	for _, deployment := range deploymentList.Items {
		// For Jobmanager we only need on replica to be available
		if deployment.Labels[FlinkDeploymentType] == FlinkDeploymentTypeJobmanager {
			if deployment.Status.AvailableReplicas == 0 {
				return false, nil
			}
		} else {
			if deployment.Spec.Replicas != nil &&
				deployment.Status.AvailableReplicas < *deployment.Spec.Replicas {
				return false, nil
			}
		}
	}
	return true, nil
}

func (f *Controller) IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
	_, err := f.flinkClient.GetClusterOverview(ctx, getURLFromApp(application, hash))
	if err != nil {
		logger.Infof(ctx, "Error response indicating flink API is not ready to handle request %v", err)
		return false, err
	}
	return true, nil
}

func listToFlinkDeployment(ds []v1.Deployment, hash string) *common.FlinkDeployment {
	if len(ds) != 2 {
		return nil
	}

	fd := common.FlinkDeployment{
		Hash: hash,
	}

	l0 := ds[0].Labels[FlinkDeploymentType]
	l1 := ds[1].Labels[FlinkDeploymentType]

	if l0 == FlinkDeploymentTypeJobmanager && l1 == FlinkDeploymentTypeTaskmanager {
		fd.Jobmanager = &ds[0]
		fd.Taskmanager = &ds[1]
	} else if l0 == FlinkDeploymentTypeTaskmanager && l1 == FlinkDeploymentTypeJobmanager {
		fd.Jobmanager = &ds[1]
		fd.Taskmanager = &ds[0]
	} else {
		return nil
	}

	return &fd
}

func getCurrentHash(app *v1alpha1.FlinkApplication) string {
	appHash := HashForApplication(app)

	if appHash == app.Status.FailedDeployHash {
		return app.Status.DeployHash
	}
	return appHash
}

// Gets the current deployment and any other deployments for the application. The current deployment will be the one
// that matches the FlinkApplication, unless the FailedDeployHash is set, in which case it will be the one with that
// hash.
func (f *Controller) GetCurrentDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) (*common.FlinkDeployment, error) {
	labels := k8.GetAppLabel(application.Name)
	curHash := getCurrentHash(application)
	labels[FlinkAppHash] = curHash

	deployments, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, labels)
	if err != nil {
		return nil, err
	}

	cur := listToFlinkDeployment(deployments.Items, curHash)
	if cur != nil && application.Status.FailedDeployHash == "" &&
		(!f.deploymentMatches(ctx, cur.Jobmanager, application) || !f.deploymentMatches(ctx, cur.Taskmanager, application)) {
		// we had a hash collision (i.e., the previous application has the same hash as the new one)
		// this is *very* unlikely to occur (1/2^32)
		return nil, errors.New("found hash collision for deployment, you must do a clean deploy")
	}

	return cur, nil
}

func (f *Controller) DeleteOldResourcesForApp(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	curHash := getCurrentHash(app)

	appLabel := k8.GetAppLabel(app.Name)
	deployments, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, app.Namespace, appLabel)
	if err != nil {
		return err
	}

	oldObjects := make([]metav1.Object, 0)

	for _, d := range deployments.Items {
		if d.Labels[FlinkAppHash] != "" &&
			d.Labels[FlinkAppHash] != curHash &&
			// verify that this deployment matches the jobmanager or taskmanager naming format
			(d.Name == fmt.Sprintf(JobManagerNameFormat, app.Name, d.Labels[FlinkAppHash]) ||
				d.Name == fmt.Sprintf(TaskManagerNameFormat, app.Name, d.Labels[FlinkAppHash])) {
			oldObjects = append(oldObjects, d.DeepCopy())
		}
	}

	services, err := f.k8Cluster.GetServicesWithLabel(ctx, app.Namespace, appLabel)
	if err != nil {
		return err
	}

	for _, d := range services.Items {
		if d.Labels[FlinkAppHash] != "" &&
			d.Labels[FlinkAppHash] != curHash &&
			d.Name == VersionedJobManagerServiceName(app, d.Labels[FlinkAppHash]) {
			oldObjects = append(oldObjects, d.DeepCopy())
		}
	}

	deletedHashes := make(map[string]bool)

	for _, resource := range oldObjects {
		err := f.k8Cluster.DeleteK8Object(ctx, resource.(runtime.Object))
		if err != nil {
			f.metrics.deleteResourceFailedCounter.Inc(ctx)
			return err
		}
		f.metrics.deleteResourceSuccessCounter.Inc(ctx)
		deletedHashes[resource.GetLabels()[FlinkAppHash]] = true
	}

	for k := range deletedHashes {
		f.LogEvent(ctx, app, "", corev1.EventTypeNormal, fmt.Sprintf("Deleted old cluster with hash %s", k))
	}

	return nil
}

func (f *Controller) FindExternalizedCheckpoint(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (string, error) {
	checkpoint, err := f.flinkClient.GetLatestCheckpoint(ctx, getURLFromApp(application, hash), application.Status.JobStatus.JobID)
	if err != nil {
		return "", err
	}
	if checkpoint == nil {
		return "", nil
	}

	if time.Since(time.Unix(checkpoint.TriggerTimestamp, 0)) > maxRestoreCheckpointAge {
		logger.Info(ctx, "Found checkpoint to restore from, but was too old")
		return "", nil
	}

	return checkpoint.ExternalPath, nil
}

func (f *Controller) LogEvent(ctx context.Context, app *v1alpha1.FlinkApplication, fieldPath string, eventType string, message string) {
	reason := "Create"
	if app.Status.DeployHash != "" {
		// this is not the first deploy
		reason = "Update"
	}
	if app.DeletionTimestamp != nil {
		reason = "Delete"
	}

	event := k8.CreateEvent(app, fieldPath, eventType, reason, message)
	logger.Infof(ctx, "Logged %s event: %s: %s", eventType, reason, message)

	// TODO: switch to using EventRecorder once we switch to controller runtime
	if err := f.k8Cluster.CreateK8Object(ctx, &event); err != nil {
		b, _ := json.Marshal(event)
		logger.Errorf(ctx, "Failed to log event %v: %v", string(b), err)
	}
}

// Gets and updates the cluster status
func (f *Controller) CompareAndUpdateClusterStatus(ctx context.Context, application *v1alpha1.FlinkApplication, hash string) (bool, error) {
	oldClusterStatus := application.Status.ClusterStatus
	clusterErrors := ""
	// Get Cluster overview
	response, err := f.flinkClient.GetClusterOverview(ctx, getURLFromApp(application, hash))

	if err != nil {
		clusterErrors = err.Error()
	} else {
		// Update cluster overview
		application.Status.ClusterStatus.NumberOfTaskManagers = response.TaskManagerCount
		application.Status.ClusterStatus.AvailableTaskSlots = response.SlotsAvailable
		application.Status.ClusterStatus.NumberOfTaskSlots = response.NumberOfTaskSlots
	}

	// Get Healthy Taskmanagers
	tmResponse, tmErr := f.flinkClient.GetTaskManagers(ctx, getURLFromApp(application, hash))
	if tmErr != nil {
		clusterErrors += tmErr.Error()
	} else {
		application.Status.ClusterStatus.HealthyTaskManagers = getHealthyTaskManagerCount(tmResponse)
	}
	// Determine Health of the cluster.
	// Error retrieving cluster / taskmanagers overview (after startup/readiness) --> Red
	// Healthy TaskManagers == Number of taskmanagers --> Green
	// Else --> Yellow
	if clusterErrors != "" && (application.Status.Phase != v1alpha1.FlinkApplicationClusterStarting &&
		application.Status.Phase != v1alpha1.FlinkApplicationSubmittingJob) {
		application.Status.ClusterStatus.Health = v1alpha1.Red
		return false, errors.New(clusterErrors)
	} else if application.Status.ClusterStatus.HealthyTaskManagers == application.Status.ClusterStatus.NumberOfTaskManagers {
		application.Status.ClusterStatus.Health = v1alpha1.Green
	} else {
		application.Status.ClusterStatus.Health = v1alpha1.Yellow
	}

	return !apiequality.Semantic.DeepEqual(oldClusterStatus, application.Status.ClusterStatus), nil
}

func getHealthyTaskManagerCount(response *client.TaskManagersResponse) int32 {
	healthyTMCount := 0
	for index := range response.TaskManagers {
		// A taskmanager is considered healthy if its last heartbeat was within taskManagerHeartbeatThreshold
		if time.Since(time.Unix(response.TaskManagers[index].TimeSinceLastHeartbeat/1000, 0)) <= taskManagerHeartbeatThreshold {
			healthyTMCount++
		}
	}

	return int32(healthyTMCount)

}

func (f *Controller) CompareAndUpdateJobStatus(ctx context.Context, app *v1alpha1.FlinkApplication, hash string) (bool, error) {
	// Initialize the last failing time to beginning of time if it's never been set
	if app.Status.JobStatus.LastFailingTime == nil {
		initTime := metav1.NewTime(time.Time{})
		app.Status.JobStatus.LastFailingTime = &initTime
	}

	oldJobStatus := app.Status.JobStatus

	app.Status.JobStatus.JobID = oldJobStatus.JobID
	jobResponse, err := f.flinkClient.GetJobOverview(ctx, getURLFromApp(app, hash), app.Status.JobStatus.JobID)
	if err != nil {
		return false, err
	}
	checkpoints, err := f.flinkClient.GetCheckpointCounts(ctx, getURLFromApp(app, hash), app.Status.JobStatus.JobID)
	if err != nil {
		return false, err
	}

	// Job status
	app.Status.JobStatus.State = v1alpha1.JobState(jobResponse.State)
	jobStartTime := metav1.NewTime(time.Unix(jobResponse.StartTime/1000, 0))
	app.Status.JobStatus.StartTime = &jobStartTime

	// Checkpoints status
	app.Status.JobStatus.FailedCheckpointCount = checkpoints.Counts["failed"]
	app.Status.JobStatus.CompletedCheckpointCount = checkpoints.Counts["completed"]
	app.Status.JobStatus.JobRestartCount = checkpoints.Counts["restored"]

	latestCheckpoint := checkpoints.Latest.Completed
	var lastCheckpointAgeSeconds int
	if latestCheckpoint != nil {
		lastCheckpointTimeMillis := metav1.NewTime(time.Unix(latestCheckpoint.LatestAckTimestamp/1000, 0))
		app.Status.JobStatus.LastCheckpointTime = &lastCheckpointTimeMillis
		lastCheckpointAgeSeconds = app.Status.JobStatus.LastCheckpointTime.Second()
	}

	if checkpoints.Latest.Restored != nil {
		app.Status.JobStatus.RestorePath = checkpoints.Latest.Restored.ExternalPath
		restoreTime := metav1.NewTime(time.Unix(checkpoints.Latest.Restored.RestoredTimeStamp/1000, 0))
		app.Status.JobStatus.RestoreTime = &restoreTime

	}

	// Health Status for job
	// Job is in FAILING state --> RED
	// Time since last successful checkpoint > maxCheckpointTime --> YELLOW
	// Else --> Green

	if app.Status.JobStatus.State == v1alpha1.Failing || time.Since(app.Status.JobStatus.LastFailingTime.Time) <
		failingIntervalThreshold {
		app.Status.JobStatus.Health = v1alpha1.Red
	} else if time.Since(time.Unix(int64(lastCheckpointAgeSeconds), 0)) < maxCheckpointTime {
		app.Status.JobStatus.Health = v1alpha1.Yellow
	} else {
		app.Status.JobStatus.Health = v1alpha1.Green
	}
	// Update LastFailingTime
	if app.Status.JobStatus.State == v1alpha1.Failing {
		currTime := metav1.Now()
		app.Status.JobStatus.LastFailingTime = &currTime
	}

	return !apiequality.Semantic.DeepEqual(oldJobStatus, app.Status.JobStatus), err
}
