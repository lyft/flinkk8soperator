package flink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"

	controllerConfig "github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
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
const proxyVersionURL = "http://localhost:%d/api/v1/namespaces/%s/services/%s-%s:8081/proxy"
const externalVersionURL = "%s-%s"
const port = 8081
const indexOffset = 1

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
	CreateCluster(ctx context.Context, application *v1beta1.FlinkApplication) error

	// Cancels the running/active jobs in the Cluster for the Application after savepoint is created
	Savepoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (string, error)

	// Force cancels the running/active job without taking a savepoint
	ForceCancel(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error

	// Starts the Job in the Flink Cluster
	StartFlinkJob(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
		jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool,
		savepointPath string) (string, error)

	// Savepoint creation is asynchronous.
	// Polls the status of the Savepoint, using the triggerID
	GetSavepointStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error)

	// Check if the Flink Kubernetes Cluster is Ready.
	// Checks if all the pods of task and job managers are ready.
	IsClusterReady(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error)

	// Checks to see if the Flink Cluster is ready to handle API requests
	IsServiceReady(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error)

	// Returns the list of Jobs running on the Flink Cluster for the Application
	GetJobsForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error)

	// Returns the current job for the application, if one exists in the cluster
	GetJobForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error)

	// Returns the pair of deployments (tm/jm) for the current version of the application
	GetCurrentDeploymentsForApp(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error)

	// Returns the pair of deployments (tm/jm) for a given version of the application
	GetDeploymentsForHash(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*common.FlinkDeployment, error)

	// Deletes all old resources (deployments and services) for the app
	DeleteOldResourcesForApp(ctx context.Context, app *v1beta1.FlinkApplication) error

	// Attempts to find an externalized checkpoint for the job. This can be used to recover an application that is not
	// able to savepoint for some reason.
	FindExternalizedCheckpoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error)

	// Logs an event to the FlinkApplication resource and to the operator log
	LogEvent(ctx context.Context, app *v1beta1.FlinkApplication, eventType string, reason string, message string)

	// Compares and updates new cluster status with current cluster status
	// Returns true if there is a change in ClusterStatus
	CompareAndUpdateClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error)

	// Compares and updates new job status with current job status
	// Returns true if there is a change in JobStatus
	CompareAndUpdateJobStatus(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (bool, error)

	// Gets the last updated cluster status
	GetLatestClusterStatus(ctx context.Context, app *v1beta1.FlinkApplication) v1beta1.FlinkClusterStatus

	// Gets the last updated job status
	GetLatestJobStatus(ctx context.Context, app *v1beta1.FlinkApplication) v1beta1.FlinkJobStatus

	// Gets the last updated job ID
	GetLatestJobID(ctx context.Context, app *v1beta1.FlinkApplication) string

	// Updates the jobID on the latest jobStatus
	UpdateLatestJobID(ctx context.Context, app *v1beta1.FlinkApplication, jobID string)

	// Update jobStatus on the latest VersionStatuses
	UpdateLatestJobStatus(ctx context.Context, app *v1beta1.FlinkApplication, jobStatus v1beta1.FlinkJobStatus)

	// Update clusterStatus on the latest VersionStatuses
	UpdateLatestClusterStatus(ctx context.Context, app *v1beta1.FlinkApplication, jobStatus v1beta1.FlinkClusterStatus)

	// Update Version and Hash for application
	UpdateLatestVersionAndHash(application *v1beta1.FlinkApplication, version v1beta1.FlinkApplicationVersion, hash string)

	// Delete Resources with Hash
	DeleteResourcesForAppWithHash(ctx context.Context, application *v1beta1.FlinkApplication, hash string) error
	// Delete status for torn down cluster/job
	DeleteStatusPostTeardown(ctx context.Context, application *v1beta1.FlinkApplication, hash string)
	// Get job given hash
	GetJobToDeleteForApplication(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error)
	// Get hash given the version
	GetVersionAndJobIDForHash(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, string, error)
	// Get version and hash after teardown is complete
	GetVersionAndHashPostTeardown(ctx context.Context, application *v1beta1.FlinkApplication) (v1beta1.FlinkApplicationVersion, string)
}

func NewController(k8sCluster k8.ClusterInterface, eventRecorder record.EventRecorder, config controllerConfig.RuntimeConfig) ControllerInterface {
	metrics := newControllerMetrics(config.MetricsScope)
	return &Controller{
		k8Cluster:     k8sCluster,
		jobManager:    NewJobManagerController(k8sCluster, config),
		taskManager:   NewTaskManagerController(k8sCluster, config),
		flinkClient:   client.NewFlinkJobManagerClient(config),
		metrics:       metrics,
		eventRecorder: eventRecorder,
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
	k8Cluster     k8.ClusterInterface
	jobManager    JobManagerControllerInterface
	taskManager   TaskManagerControllerInterface
	flinkClient   client.FlinkAPIInterface
	metrics       *controllerMetrics
	eventRecorder record.EventRecorder
}

func (f *Controller) getURLFromApp(application *v1beta1.FlinkApplication, hash string) string {
	service := VersionedJobManagerServiceName(application, hash)
	cfg := controllerConfig.GetConfig()
	if cfg.UseProxy {
		return fmt.Sprintf(proxyURL, cfg.ProxyPort.Port, application.Namespace, service)
	}
	return fmt.Sprintf("http://%s.%s:%d", service, application.Namespace, port)
}

func (f *Controller) getClusterOverviewURL(app *v1beta1.FlinkApplication, version string) string {
	externalURL := f.getExternalURLFromApp(app, version)
	if externalURL != "" {
		return fmt.Sprintf(externalURL + client.WebUIAnchor + client.GetClusterOverviewURL)
	}
	return ""
}

func (f *Controller) getJobOverviewURL(app *v1beta1.FlinkApplication, version string, jobID string) string {
	externalURL := f.getExternalURLFromApp(app, version)
	if externalURL != "" {
		return fmt.Sprintf(externalURL+client.WebUIAnchor+client.GetJobsOverviewURL, jobID)
	}
	return ""
}

func (f *Controller) getExternalURLFromApp(application *v1beta1.FlinkApplication, version string) string {
	cfg := controllerConfig.GetConfig()
	// Local environment
	if cfg.UseProxy {
		if version != "" {
			return fmt.Sprintf(proxyVersionURL, cfg.ProxyPort.Port, application.Namespace, application.Name, version)
		}
		return fmt.Sprintf(proxyURL, cfg.ProxyPort.Port, application.Namespace, application.Name)
	}
	if version != "" {
		return GetFlinkUIIngressURL(fmt.Sprintf(externalVersionURL, application.Name, version))
	}
	return GetFlinkUIIngressURL(application.Name)
}

func GetActiveFlinkJobs(jobs []client.FlinkJob) []client.FlinkJob {
	var activeJobs []client.FlinkJob

	for _, job := range jobs {
		if job.Status != client.Canceled &&
			job.Status != client.Failed {
			activeJobs = append(activeJobs, job)
		}
	}

	return activeJobs
}

func (f *Controller) GetJobsForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) ([]client.FlinkJob, error) {
	jobResponse, err := f.flinkClient.GetJobs(ctx, f.getURLFromApp(application, hash))
	if err != nil {
		return nil, err
	}

	return jobResponse.Jobs, nil
}

func (f *Controller) GetJobForApplication(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
	if f.GetLatestJobID(ctx, application) == "" {
		return nil, nil
	}

	jobResponse, err := f.flinkClient.GetJobOverview(ctx, f.getURLFromApp(application, hash), f.GetLatestJobID(ctx, application))
	if err != nil {
		return nil, err
	}

	return jobResponse, nil
}

func (f *Controller) Savepoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string, isCancel bool, jobID string) (string, error) {
	if isCancel {
		return f.flinkClient.CancelJobWithSavepoint(ctx, f.getURLFromApp(application, hash), jobID)
	}
	return f.flinkClient.SavepointJob(ctx, f.getURLFromApp(application, hash), jobID)
}

func (f *Controller) ForceCancel(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) error {
	return f.flinkClient.ForceCancelJob(ctx, f.getURLFromApp(application, hash), jobID)
}

func (f *Controller) CreateCluster(ctx context.Context, application *v1beta1.FlinkApplication) error {
	newlyCreatedJm, err := f.jobManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Job manager cluster creation did not succeed %v", err)
		f.LogEvent(ctx, application, corev1.EventTypeWarning, "CreateClusterFailed",
			fmt.Sprintf("Failed to create job managers for deploy %s: %v",
				HashForApplication(application), err))

		return err
	}
	newlyCreatedTm, err := f.taskManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Task manager cluster creation did not succeed %v", err)
		f.LogEvent(ctx, application, corev1.EventTypeWarning, "CreateClusterFailed",
			fmt.Sprintf("Failed to create task managers for deploy %s: %v",
				HashForApplication(application), err))
		return err
	}

	if newlyCreatedJm || newlyCreatedTm {
		f.LogEvent(ctx, application, corev1.EventTypeNormal, "CreatingCluster",
			fmt.Sprintf("Creating Flink cluster for deploy %s", HashForApplication(application)))
	}
	return nil
}

func (f *Controller) StartFlinkJob(ctx context.Context, application *v1beta1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool,
	savepointPath string) (string, error) {
	response, err := f.flinkClient.SubmitJob(
		ctx,
		f.getURLFromApp(application, hash),
		jarName,
		client.SubmitJobRequest{
			Parallelism:           parallelism,
			SavepointPath:         savepointPath,
			EntryClass:            entryClass,
			ProgramArgs:           programArgs,
			AllowNonRestoredState: allowNonRestoredState,
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

func (f *Controller) GetSavepointStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string, jobID string) (*client.SavepointResponse, error) {
	return f.flinkClient.CheckSavepointStatus(ctx, f.getURLFromApp(application, hash), jobID, application.Status.SavepointTriggerID)
}

func (f *Controller) IsClusterReady(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	deployments, err := f.GetCurrentDeploymentsForApp(ctx, application)
	if deployments == nil || err != nil {
		return false, err
	}

	// TODO: Find if any events can be populated, that are useful to users
	if deployments.Jobmanager.Status.AvailableReplicas == 0 {
		return false, nil
	}

	if deployments.Taskmanager.Status.AvailableReplicas < *deployments.Taskmanager.Spec.Replicas {
		return false, nil
	}

	return true, nil
}

func (f *Controller) IsServiceReady(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
	resp, err := f.flinkClient.GetClusterOverview(ctx, f.getURLFromApp(application, hash))
	if err != nil {
		logger.Infof(ctx, "Error response indicating flink API is not ready to handle request %v", err)
		return false, err
	}

	// check that we have enough task slots to run the application
	if resp.NumberOfTaskSlots < application.Spec.Parallelism {
		return false, nil
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

func getCurrentHash(app *v1beta1.FlinkApplication) string {
	appHash := HashForApplication(app)

	if appHash == app.Status.FailedDeployHash || appHash == app.Status.TeardownHash {
		return app.Status.DeployHash
	}

	return appHash
}

// Gets the current deployment and any other deployments for the application. The current deployment will be the one
// that matches the FlinkApplication, unless the FailedDeployHash is set, in which case it will be the one with that
// hash.
func (f *Controller) GetCurrentDeploymentsForApp(ctx context.Context, application *v1beta1.FlinkApplication) (*common.FlinkDeployment, error) {
	return f.GetDeploymentsForHash(ctx, application, getCurrentHash(application))
}

func (f *Controller) GetDeploymentsForHash(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (*common.FlinkDeployment, error) {
	labels := k8.GetAppLabel(application.Name)
	labels[FlinkAppHash] = hash

	deployments, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, labels)
	if err != nil {
		return nil, err
	}

	cur := listToFlinkDeployment(deployments.Items, hash)

	return cur, nil
}

func (f *Controller) DeleteOldResourcesForApp(ctx context.Context, app *v1beta1.FlinkApplication) error {
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
			d.Labels[FlinkAppHash] != app.Status.InPlaceUpdatedFromHash &&
			// sanity check that this deployment matches the jobmanager or taskmanager naming format
			(strings.HasPrefix(d.Name, app.Name) &&
				(strings.HasSuffix(d.Name, "-tm") || strings.HasSuffix(d.Name, "-jm"))) {
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
			d.Labels[FlinkAppHash] != app.Status.InPlaceUpdatedFromHash &&
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
		f.LogEvent(ctx, app, corev1.EventTypeNormal, "ToreDownCluster",
			fmt.Sprintf("Deleted old cluster with hash %s", k))
	}

	return nil
}

func (f *Controller) FindExternalizedCheckpoint(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (string, error) {
	checkpoint, err := f.flinkClient.GetLatestCheckpoint(ctx, f.getURLFromApp(application, hash), f.GetLatestJobID(ctx, application))
	var checkpointPath string
	var checkpointTime int64
	if err != nil {
		jobStatus := f.GetLatestJobStatus(ctx, application)
		// we failed to query the JM, try to pull it out of the resource
		if jobStatus.LastCheckpointPath != "" && jobStatus.LastCheckpointTime != nil {
			checkpointPath = jobStatus.LastCheckpointPath
			checkpointTime = jobStatus.LastCheckpointTime.Unix()
			logger.Warnf(ctx, "Could not query JobManager for latest externalized checkpoint, using"+
				" last seen checkpoint")
		} else {
			return "", err
		}
	} else if checkpoint != nil {
		checkpointPath = checkpoint.ExternalPath
		checkpointTime = checkpoint.TriggerTimestamp
	} else {
		return "", nil
	}

	if isCheckpointOldToRecover(checkpointTime, getMaxCheckpointRestoreAgeSeconds(application)) {
		logger.Info(ctx, "Found checkpoint to restore from, but was too old")
		return "", nil
	}

	return checkpointPath, nil
}

func isCheckpointOldToRecover(checkpointTime int64, maxCheckpointRecoveryAgeSec int32) bool {
	return time.Since(time.Unix(checkpointTime, 0)) > (time.Duration(maxCheckpointRecoveryAgeSec) * time.Second)
}

func (f *Controller) LogEvent(ctx context.Context, app *v1beta1.FlinkApplication, eventType string, reason string, message string) {
	// Augment message with version for blue-green deployments
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		version := app.Status.UpdatingVersion
		message = fmt.Sprintf("%s for version %s", message, version)
	}

	f.eventRecorder.Event(app, eventType, reason, message)
	logger.Infof(ctx, "Logged %s event: %s: %s", eventType, reason, message)
}

// Gets and updates the cluster status
func (f *Controller) CompareAndUpdateClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication, hash string) (bool, error) {
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		return f.compareAndUpdateBlueGreenClusterStatus(ctx, application)
	}
	// Error retrieving cluster / taskmanagers overview (after startup/readiness) --> Red
	// If there is an error this loop will return with Health set to Red
	oldClusterStatus := application.Status.ClusterStatus
	application.Status.ClusterStatus.Health = v1beta1.Red

	deployment, err := f.GetCurrentDeploymentsForApp(ctx, application)
	if deployment == nil || err != nil {
		return false, err
	}

	application.Status.ClusterStatus.ClusterOverviewURL = f.getClusterOverviewURL(application, "")
	application.Status.ClusterStatus.NumberOfTaskManagers = deployment.Taskmanager.Status.AvailableReplicas
	// Get Cluster overview
	response, err := f.flinkClient.GetClusterOverview(ctx, f.getURLFromApp(application, hash))
	if err != nil {
		return false, err
	}
	// Update cluster overview
	application.Status.ClusterStatus.AvailableTaskSlots = response.SlotsAvailable
	application.Status.ClusterStatus.NumberOfTaskSlots = response.NumberOfTaskSlots

	// Get Healthy Taskmanagers
	tmResponse, tmErr := f.flinkClient.GetTaskManagers(ctx, f.getURLFromApp(application, hash))
	if tmErr != nil {
		return false, tmErr
	}
	application.Status.ClusterStatus.HealthyTaskManagers = getHealthyTaskManagerCount(tmResponse)

	// Determine Health of the cluster.
	// Healthy TaskManagers == Number of taskmanagers --> Green
	// Else --> Yellow
	if application.Status.ClusterStatus.HealthyTaskManagers == deployment.Taskmanager.Status.Replicas {
		application.Status.ClusterStatus.Health = v1beta1.Green
	} else {
		application.Status.ClusterStatus.Health = v1beta1.Yellow
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

func (f *Controller) CompareAndUpdateJobStatus(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (bool, error) {
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		return f.compareAndUpdateBlueGreenJobStatus(ctx, app)
	}
	if app.Status.JobStatus.LastFailingTime == nil {
		initTime := metav1.NewTime(time.Time{})
		app.Status.JobStatus.LastFailingTime = &initTime
	}

	oldJobStatus := app.Status.JobStatus
	app.Status.JobStatus.JobID = oldJobStatus.JobID
	jobResponse, err := f.flinkClient.GetJobOverview(ctx, f.getURLFromApp(app, hash), f.GetLatestJobID(ctx, app))
	if err != nil {
		return false, err
	}
	checkpoints, err := f.flinkClient.GetCheckpointCounts(ctx, f.getURLFromApp(app, hash), f.GetLatestJobID(ctx, app))
	if err != nil {
		return false, err
	}

	// Job status
	app.Status.JobStatus.JobOverviewURL = f.getJobOverviewURL(app, "", app.Status.JobStatus.JobID)
	app.Status.JobStatus.State = v1beta1.JobState(jobResponse.State)
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
		app.Status.JobStatus.LastCheckpointPath = latestCheckpoint.ExternalPath
		lastCheckpointAgeSeconds = app.Status.JobStatus.LastCheckpointTime.Second()
	}

	if checkpoints.Latest.Restored != nil {
		app.Status.JobStatus.RestorePath = checkpoints.Latest.Restored.ExternalPath
		restoreTime := metav1.NewTime(time.Unix(checkpoints.Latest.Restored.RestoredTimeStamp/1000, 0))
		app.Status.JobStatus.RestoreTime = &restoreTime
	}

	runningTasks := int32(0)
	totalTasks := int32(0)
	verticesInCreated := int32(0)

	for _, v := range jobResponse.Vertices {
		if v.Status == client.Created {
			verticesInCreated++
		}

		for k, v := range v.Tasks {
			if k == "RUNNING" {
				runningTasks += int32(v)
			}
			totalTasks += int32(v)
		}
	}

	app.Status.JobStatus.RunningTasks = runningTasks
	app.Status.JobStatus.TotalTasks = totalTasks

	// Health Status for job
	// Job is in FAILING state --> RED
	// Time since last successful checkpoint > maxCheckpointTime --> YELLOW
	// Else --> Green

	if app.Status.JobStatus.State == v1beta1.Failing ||
		time.Since(app.Status.JobStatus.LastFailingTime.Time) < failingIntervalThreshold ||
		verticesInCreated > 0 {
		app.Status.JobStatus.Health = v1beta1.Red
	} else if time.Since(time.Unix(int64(lastCheckpointAgeSeconds), 0)) < maxCheckpointTime ||
		runningTasks < totalTasks {
		app.Status.JobStatus.Health = v1beta1.Yellow
	} else {
		app.Status.JobStatus.Health = v1beta1.Green
	}
	// Update LastFailingTime
	if app.Status.JobStatus.State == v1beta1.Failing {
		currTime := metav1.Now()
		app.Status.JobStatus.LastFailingTime = &currTime
	}
	return !apiequality.Semantic.DeepEqual(oldJobStatus, app.Status.JobStatus), err
}

// Only used with the BlueGreen DeploymentMode
// A method to identify the current VersionStatus
func getCurrentStatusIndex(app *v1beta1.FlinkApplication) int32 {
	// The current VersionStatus is the first (or earlier) version when
	// 1. The application is a Running phase and there's only one job running
	// 2. First deploy ever
	// 3. When the savepoint is being taken on the existing job
	if v1beta1.IsRunningPhase(app.Status.Phase) || app.Status.DeployHash == "" ||
		app.Status.Phase == v1beta1.FlinkApplicationSavepointing {
		return 0
	}

	if app.Status.Phase == v1beta1.FlinkApplicationDualRunning {
		return 1
	}

	// activeJobs and maxRunningJobs would be different once a TearDownVersionHash has happened and
	// the app has moved back to a Running state.
	activeJobs := int32(len(app.Status.VersionStatuses))
	maxRunningJobs := v1beta1.GetMaxRunningJobs(app.Spec.DeploymentMode)
	index := Min(activeJobs, maxRunningJobs) - indexOffset
	return index
}

func Min(x, y int32) int32 {
	if x < y {
		return x
	}
	return y
}

func (f *Controller) GetLatestClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication) v1beta1.FlinkClusterStatus {
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		return application.Status.VersionStatuses[getCurrentStatusIndex(application)].ClusterStatus
	}
	return application.Status.ClusterStatus
}

func (f *Controller) GetLatestJobStatus(ctx context.Context, application *v1beta1.FlinkApplication) v1beta1.FlinkJobStatus {
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		return application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus
	}
	return application.Status.JobStatus

}

func (f *Controller) UpdateLatestJobStatus(ctx context.Context, app *v1beta1.FlinkApplication, jobStatus v1beta1.FlinkJobStatus) {
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		app.Status.VersionStatuses[getCurrentStatusIndex(app)].JobStatus = jobStatus
		return
	}
	app.Status.JobStatus = jobStatus
}

func (f *Controller) UpdateLatestClusterStatus(ctx context.Context, app *v1beta1.FlinkApplication, clusterStatus v1beta1.FlinkClusterStatus) {
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		app.Status.VersionStatuses[getCurrentStatusIndex(app)].ClusterStatus = clusterStatus
		return
	}
	app.Status.ClusterStatus = clusterStatus
}

func (f *Controller) GetLatestJobID(ctx context.Context, application *v1beta1.FlinkApplication) string {
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		return application.Status.VersionStatuses[getCurrentStatusIndex(application)].JobStatus.JobID
	}
	return application.Status.JobStatus.JobID
}

func (f *Controller) UpdateLatestJobID(ctx context.Context, app *v1beta1.FlinkApplication, jobID string) {
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		app.Status.VersionStatuses[getCurrentStatusIndex(app)].JobStatus.JobID = jobID
	}
	app.Status.JobStatus.JobID = jobID
}

func (f *Controller) compareAndUpdateBlueGreenClusterStatus(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	isEqual := false
	for currIndex := range application.Status.VersionStatuses {
		if application.Status.VersionStatuses[currIndex].VersionHash == "" {
			continue
		}
		hash := application.Status.VersionStatuses[currIndex].VersionHash
		oldClusterStatus := application.Status.VersionStatuses[currIndex].ClusterStatus
		application.Status.VersionStatuses[currIndex].ClusterStatus.Health = v1beta1.Red

		deployment, err := f.GetCurrentDeploymentsForApp(ctx, application)
		if deployment == nil || err != nil {
			return false, err
		}

		version := string(application.Status.VersionStatuses[currIndex].Version)
		application.Status.VersionStatuses[currIndex].ClusterStatus.ClusterOverviewURL = f.getClusterOverviewURL(application, version)
		application.Status.VersionStatuses[currIndex].ClusterStatus.NumberOfTaskManagers = deployment.Taskmanager.Status.AvailableReplicas
		// Get Cluster overview
		response, err := f.flinkClient.GetClusterOverview(ctx, f.getURLFromApp(application, hash))
		if err != nil {
			return false, err
		}
		// Update cluster overview
		application.Status.VersionStatuses[currIndex].ClusterStatus.AvailableTaskSlots = response.SlotsAvailable
		application.Status.VersionStatuses[currIndex].ClusterStatus.NumberOfTaskSlots = response.NumberOfTaskSlots

		// Get Healthy Taskmanagers
		tmResponse, tmErr := f.flinkClient.GetTaskManagers(ctx, f.getURLFromApp(application, hash))
		if tmErr != nil {
			return false, tmErr
		}
		application.Status.VersionStatuses[currIndex].ClusterStatus.HealthyTaskManagers = getHealthyTaskManagerCount(tmResponse)

		// Determine Health of the cluster.
		// Healthy TaskManagers == Number of taskmanagers --> Green
		// Else --> Yellow
		if application.Status.VersionStatuses[currIndex].ClusterStatus.HealthyTaskManagers == deployment.Taskmanager.Status.Replicas {
			application.Status.VersionStatuses[currIndex].ClusterStatus.Health = v1beta1.Green
		} else {
			application.Status.VersionStatuses[currIndex].ClusterStatus.Health = v1beta1.Yellow
		}
		isEqual = isEqual || !apiequality.Semantic.DeepEqual(oldClusterStatus, application.Status.VersionStatuses[currIndex].ClusterStatus)
	}

	return isEqual, nil
}

func (f *Controller) compareAndUpdateBlueGreenJobStatus(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	isEqual := false
	var err error
	for statusIndex := range app.Status.VersionStatuses {
		if app.Status.VersionStatuses[statusIndex].JobStatus.JobID == "" {
			continue
		}
		hash := app.Status.VersionStatuses[statusIndex].VersionHash
		if app.Status.VersionStatuses[statusIndex].JobStatus.LastFailingTime == nil {
			initTime := metav1.NewTime(time.Time{})
			app.Status.VersionStatuses[statusIndex].JobStatus.LastFailingTime = &initTime
		}
		oldJobStatus := app.Status.VersionStatuses[statusIndex].JobStatus
		app.Status.VersionStatuses[statusIndex].JobStatus.JobID = oldJobStatus.JobID
		jobResponse, err := f.flinkClient.GetJobOverview(ctx, f.getURLFromApp(app, hash), app.Status.VersionStatuses[statusIndex].JobStatus.JobID)
		if err != nil {
			return false, err
		}
		checkpoints, err := f.flinkClient.GetCheckpointCounts(ctx, f.getURLFromApp(app, hash), app.Status.VersionStatuses[statusIndex].JobStatus.JobID)
		if err != nil {
			return false, err
		}

		// Job status
		version := string(app.Status.VersionStatuses[statusIndex].Version)
		app.Status.VersionStatuses[statusIndex].JobStatus.JobOverviewURL = f.getJobOverviewURL(app, version, app.Status.VersionStatuses[statusIndex].JobStatus.JobID)
		app.Status.VersionStatuses[statusIndex].JobStatus.State = v1beta1.JobState(jobResponse.State)
		jobStartTime := metav1.NewTime(time.Unix(jobResponse.StartTime/1000, 0))
		app.Status.VersionStatuses[statusIndex].JobStatus.StartTime = &jobStartTime

		// Checkpoints status
		app.Status.VersionStatuses[statusIndex].JobStatus.FailedCheckpointCount = checkpoints.Counts["failed"]
		app.Status.VersionStatuses[statusIndex].JobStatus.CompletedCheckpointCount = checkpoints.Counts["completed"]
		app.Status.VersionStatuses[statusIndex].JobStatus.JobRestartCount = checkpoints.Counts["restored"]

		latestCheckpoint := checkpoints.Latest.Completed
		var lastCheckpointAgeSeconds int
		if latestCheckpoint != nil {
			lastCheckpointTimeMillis := metav1.NewTime(time.Unix(latestCheckpoint.LatestAckTimestamp/1000, 0))
			app.Status.VersionStatuses[statusIndex].JobStatus.LastCheckpointTime = &lastCheckpointTimeMillis
			app.Status.VersionStatuses[statusIndex].JobStatus.LastCheckpointPath = latestCheckpoint.ExternalPath
			lastCheckpointAgeSeconds = app.Status.VersionStatuses[statusIndex].JobStatus.LastCheckpointTime.Second()
		}

		if checkpoints.Latest.Restored != nil {
			app.Status.VersionStatuses[statusIndex].JobStatus.RestorePath = checkpoints.Latest.Restored.ExternalPath
			restoreTime := metav1.NewTime(time.Unix(checkpoints.Latest.Restored.RestoredTimeStamp/1000, 0))
			app.Status.VersionStatuses[statusIndex].JobStatus.RestoreTime = &restoreTime
		}

		runningTasks := int32(0)
		totalTasks := int32(0)
		verticesInCreated := int32(0)

		for _, v := range jobResponse.Vertices {
			if v.Status == client.Created {
				verticesInCreated++
			}

			for k, v := range v.Tasks {
				if k == "RUNNING" {
					runningTasks += int32(v)
				}
				totalTasks += int32(v)
			}
		}

		app.Status.VersionStatuses[statusIndex].JobStatus.RunningTasks = runningTasks
		app.Status.VersionStatuses[statusIndex].JobStatus.TotalTasks = totalTasks

		// Health Status for job
		// Job is in FAILING state --> RED
		// Time since last successful checkpoint > maxCheckpointTime --> YELLOW
		// Else --> Green

		if app.Status.VersionStatuses[statusIndex].JobStatus.State == v1beta1.Failing ||
			time.Since(app.Status.VersionStatuses[statusIndex].JobStatus.LastFailingTime.Time) < failingIntervalThreshold ||
			verticesInCreated > 0 {
			app.Status.VersionStatuses[statusIndex].JobStatus.Health = v1beta1.Red
		} else if time.Since(time.Unix(int64(lastCheckpointAgeSeconds), 0)) < maxCheckpointTime ||
			runningTasks < totalTasks {
			app.Status.VersionStatuses[statusIndex].JobStatus.Health = v1beta1.Yellow
		} else {
			app.Status.VersionStatuses[statusIndex].JobStatus.Health = v1beta1.Green
		}
		// Update LastFailingTime
		if app.Status.VersionStatuses[statusIndex].JobStatus.State == v1beta1.Failing {
			currTime := metav1.Now()
			app.Status.VersionStatuses[statusIndex].JobStatus.LastFailingTime = &currTime
		}
		isEqual = isEqual || !apiequality.Semantic.DeepEqual(oldJobStatus, app.Status.VersionStatuses[statusIndex].JobStatus)
	}
	return isEqual, err
}

func (f *Controller) UpdateLatestVersionAndHash(application *v1beta1.FlinkApplication, version v1beta1.FlinkApplicationVersion, hash string) {
	currIndex := getCurrentStatusIndex(application)
	application.Status.VersionStatuses[currIndex].Version = version
	application.Status.VersionStatuses[currIndex].VersionHash = hash
	application.Status.UpdatingHash = hash
}

func (f *Controller) DeleteResourcesForAppWithHash(ctx context.Context, app *v1beta1.FlinkApplication, hash string) error {
	appLabel := k8.GetAppLabel(app.Name)
	deployments, err := f.k8Cluster.GetDeploymentsWithLabel(ctx, app.Namespace, appLabel)
	if err != nil {
		return err
	}

	oldObjects := make([]metav1.Object, 0)

	for _, d := range deployments.Items {
		if d.Labels[FlinkAppHash] == hash &&
			// verify that this deployment matches the jobmanager or taskmanager naming format
			(d.Name == fmt.Sprintf(JobManagerVersionNameFormat, app.Name, d.Labels[FlinkAppHash], d.Labels[FlinkApplicationVersion]) ||
				d.Name == fmt.Sprintf(TaskManagerVersionNameFormat, app.Name, d.Labels[FlinkAppHash], d.Labels[FlinkApplicationVersion])) {
			oldObjects = append(oldObjects, d.DeepCopy())
		}
	}

	services, err := f.k8Cluster.GetServicesWithLabel(ctx, app.Namespace, appLabel)

	if err != nil {
		return err
	}

	for _, d := range services.Items {
		if d.Labels[FlinkAppHash] == hash || d.Spec.Selector[FlinkAppHash] == hash {
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
		f.LogEvent(ctx, app, corev1.EventTypeNormal, "ToreDownCluster",
			fmt.Sprintf("Deleted old cluster with hash %s", k))
	}
	return nil
}

func (f *Controller) DeleteStatusPostTeardown(ctx context.Context, application *v1beta1.FlinkApplication, hash string) {
	var indexToDelete int
	for index, status := range application.Status.VersionStatuses {
		if status.VersionHash == hash {
			indexToDelete = index
		}
	}
	application.Status.VersionStatuses[0] = application.Status.VersionStatuses[indexOffset-indexToDelete]
	application.Status.VersionStatuses[1] = v1beta1.FlinkApplicationVersionStatus{}
}

func (f *Controller) GetJobToDeleteForApplication(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (*client.FlinkJobOverview, error) {
	jobID := ""
	for _, status := range app.Status.VersionStatuses {
		if status.VersionHash == hash {
			jobID = status.JobStatus.JobID
		}
	}
	if jobID == "" {
		return nil, nil
	}

	jobResponse, err := f.flinkClient.GetJobOverview(ctx, f.getURLFromApp(app, hash), jobID)
	if err != nil {
		return nil, err
	}

	return jobResponse, nil
}

func (f *Controller) GetVersionAndJobIDForHash(ctx context.Context, app *v1beta1.FlinkApplication, hash string) (string, string, error) {
	version := ""
	jobID := ""
	for _, status := range app.Status.VersionStatuses {
		if status.VersionHash == hash {
			version = string(status.Version)
			jobID = status.JobStatus.JobID
		}
	}
	if hash == "" || jobID == "" {
		return "", "", errors.New("could not find jobID and hash for application")
	}

	return version, jobID, nil
}

func (f *Controller) GetVersionAndHashPostTeardown(ctx context.Context, application *v1beta1.FlinkApplication) (v1beta1.FlinkApplicationVersion, string) {
	versionStatus := application.Status.VersionStatuses[0]
	return versionStatus.Version, versionStatus.VersionHash
}
