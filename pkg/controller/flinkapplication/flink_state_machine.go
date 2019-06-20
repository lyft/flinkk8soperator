package flinkapplication

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	jobFinalizer = "job.finalizers.flink.k8s.io"
	maxRetries   = 3
)

// The core state machine that manages Flink clusters and jobs. See docs/state_machine.md for a description of the
// states and transitions.
type FlinkHandlerInterface interface {
	Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

type FlinkStateMachine struct {
	flinkController flink.ControllerInterface
	k8Cluster       k8.ClusterInterface
	clock           clock.Clock
	metrics         *stateMachineMetrics
}

type stateMachineMetrics struct {
	scope                             promutils.Scope
	stateMachineHandlePhaseMap        map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch
	stateMachineHandleSuccessPhaseMap map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch
	errorCounterPhaseMap              map[v1alpha1.FlinkApplicationPhase]labeled.Counter
}

func newStateMachineMetrics(scope promutils.Scope) *stateMachineMetrics {
	stateMachineScope := scope.NewSubScope("state_machine")
	stateMachineHandlePhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch{}
	stateMachineHandleSuccessPhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch{}
	errorCounterPhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.Counter{}

	for _, phase := range v1alpha1.FlinkApplicationPhases {
		phaseName := phase.VerboseString()
		stateMachineHandleSuccessPhaseMap[phase] = labeled.NewStopWatch(phaseName+"_"+"handle_time_success",
			fmt.Sprintf("Total time to handle the %s application state on success", phaseName), time.Millisecond, stateMachineScope)
		stateMachineHandlePhaseMap[phase] = labeled.NewStopWatch(phaseName+"_"+"handle_time",
			fmt.Sprintf("Total time to handle the %s application state", phaseName), time.Millisecond, stateMachineScope)
		errorCounterPhaseMap[phase] = labeled.NewCounter(phaseName+"_"+"error",
			fmt.Sprintf("Failure to handle the %s application state", phaseName), stateMachineScope)
	}
	return &stateMachineMetrics{
		scope:                             scope,
		stateMachineHandlePhaseMap:        stateMachineHandlePhaseMap,
		stateMachineHandleSuccessPhaseMap: stateMachineHandleSuccessPhaseMap,
		errorCounterPhaseMap:              errorCounterPhaseMap,
	}
}

func (s *FlinkStateMachine) updateApplicationPhase(ctx context.Context, application *v1alpha1.FlinkApplication, phase v1alpha1.FlinkApplicationPhase) error {
	application.Status.Phase = phase
	now := v1.NewTime(s.clock.Now())
	application.Status.LastUpdatedAt = &now

	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func (s *FlinkStateMachine) shouldRollback(ctx context.Context, application *v1alpha1.FlinkApplication) bool {
	if application.Status.DeployHash == "" && application.Status.LastSeenError == "" {
		// TODO: we may want some more sophisticated way of handling this case
		// there's no previous deploy for this application, so nothing to roll back to
		return false
	}

	if application.Status.LastSeenError == "" {
		return false
	}
	// Check if the error is retryable and we have retries left
	if client.IsErrorRetryable(application.Status.LastSeenError) && application.Status.RetryCount <= maxRetries {
		application.Status.RetryCount++
		// Reset error to always record latest error
		application.Status.LastSeenError = ""
		return false
	}

	// If the error is not retryable, fail fast
	s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeWarning, fmt.Sprintf("Application failed to progress in the %v phase", application.Status.Phase))
	return true
}

func (s *FlinkStateMachine) Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	currentPhase := application.Status.Phase
	if _, ok := s.metrics.stateMachineHandlePhaseMap[currentPhase]; !ok {
		errMsg := fmt.Sprintf("Invalid state %s for the application", currentPhase)
		return errors.New(errMsg)
	}
	timer := s.metrics.stateMachineHandlePhaseMap[currentPhase].Start(ctx)
	successTimer := s.metrics.stateMachineHandleSuccessPhaseMap[currentPhase].Start(ctx)

	defer timer.Stop()
	err := s.handle(ctx, application)
	if err != nil {
		s.metrics.errorCounterPhaseMap[currentPhase].Inc(ctx)
	} else {
		successTimer.Stop()
	}
	return err
}

func (s *FlinkStateMachine) handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if !application.ObjectMeta.DeletionTimestamp.IsZero() && application.Status.Phase != v1alpha1.FlinkApplicationDeleting {
		// Always perform a single application update per callback
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationDeleting)
	}

	if !v1alpha1.IsRunningPhase(application.Status.Phase) {
		logger.Infof(ctx, "Handling state %s for application", application.Status.Phase)
	}

	switch application.Status.Phase {
	case v1alpha1.FlinkApplicationNew, v1alpha1.FlinkApplicationUpdating:
		// Currently just transitions to the next state
		return s.handleNewOrUpdating(ctx, application)
	case v1alpha1.FlinkApplicationClusterStarting:
		return s.handleClusterStarting(ctx, application)
	case v1alpha1.FlinkApplicationSubmittingJob:
		return s.handleSubmittingJob(ctx, application)
	case v1alpha1.FlinkApplicationRunning, v1alpha1.FlinkApplicationDeployFailed:
		return s.handleApplicationRunning(ctx, application)
	case v1alpha1.FlinkApplicationSavepointing:
		return s.handleApplicationSavepointing(ctx, application)
	case v1alpha1.FlinkApplicationRollingBackJob:
		return s.handleRollingBack(ctx, application)
	case v1alpha1.FlinkApplicationDeleting:
		return s.handleApplicationDeleting(ctx, application)
	}
	return nil
}

// In this state we create a new cluster, either due to an entirely new FlinkApplication or due to an update.
func (s *FlinkStateMachine) handleNewOrUpdating(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	// TODO: add up-front validation on the FlinkApplication resource
	if s.shouldRollback(ctx, application) {
		// we've failed to make progress; move to deploy failed
		return s.deployFailed(ctx, application)
	}

	// Create the Flink cluster
	err := s.flinkController.CreateCluster(ctx, application)
	if err != nil {
		s.getAndUpdateError(ctx, application, err)

		logger.Errorf(ctx, "Cluster creation failed with error: %v", err)
		return err
	}

	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
}

func (s *FlinkStateMachine) deployFailed(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeWarning, "Deployment failed, rolled back successfully")
	app.Status.FailedDeployHash = flink.HashForApplication(app)

	// Reset errors and retry count
	app.Status.LastSeenError = ""
	app.Status.RetryCount = 0

	return s.updateApplicationPhase(ctx, app, v1alpha1.FlinkApplicationDeployFailed)
}

// Create the underlying Kubernetes objects for the new cluster
func (s *FlinkStateMachine) handleClusterStarting(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if s.shouldRollback(ctx, application) {
		// we've failed to make progress; move to deploy failed
		// TODO: this will need different logic in single mode
		return s.deployFailed(ctx, application)
	}

	// Wait for all to be running
	ready, err := s.flinkController.IsClusterReady(ctx, application)
	if err != nil {
		s.getAndUpdateError(ctx, application, err)
		return err
	}
	if !ready {
		return nil
	}

	logger.Infof(ctx, "Flink cluster has started successfully")
	// TODO: in single mode move to submitting job
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationSavepointing)
}

func (s *FlinkStateMachine) handleApplicationSavepointing(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	// we've already savepointed (or this is our first deploy), continue on
	if application.Spec.SavepointInfo.SavepointLocation != "" || application.Status.DeployHash == "" {
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationSubmittingJob)
	}

	// we haven't started savepointing yet; do so now
	// TODO: figure out the idempotence of this
	if application.Spec.SavepointInfo.TriggerID == "" {
		if s.shouldRollback(ctx, application) {
			// we were unable to start savepointing for our failure period, so roll back
			// TODO: we should think about how to handle the case where the cluster has started savepointing, but does
			//       not finish within some time frame. Currently, we just wait indefinitely for the JM to report its
			//       status. It's not clear what the right answer is.
			return s.deployFailed(ctx, application)
		}

		triggerID, err := s.flinkController.CancelWithSavepoint(ctx, application, application.Status.DeployHash)
		if err != nil {
			s.getAndUpdateError(ctx, application, err)
			return err
		}

		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal, fmt.Sprintf("Cancelling job %s with a final savepoint", application.Status.JobStatus.JobID))

		application.Spec.SavepointInfo.TriggerID = triggerID
		return s.k8Cluster.UpdateK8Object(ctx, application)
	}

	// check the savepoints in progress
	savepointStatusResponse, err := s.flinkController.GetSavepointStatus(ctx, application, application.Status.DeployHash)
	if err != nil {
		return err
	}

	var restorePath string
	if savepointStatusResponse.Operation.Location == "" &&
		savepointStatusResponse.SavepointStatus.Status != client.SavePointInProgress {
		// Savepointing failed
		// TODO: we should probably retry this a few times before failing
		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeWarning, fmt.Sprintf("Failed to take savepoint: %v",
			savepointStatusResponse.Operation.FailureCause))

		// try to find an externalized checkpoint
		path, err := s.flinkController.FindExternalizedCheckpoint(ctx, application, application.Status.DeployHash)
		if err != nil {
			logger.Infof(ctx, "error while fetching externalized checkpoint path: %v", err)
			return s.deployFailed(ctx, application)
		} else if path == "" {
			logger.Infof(ctx, "no externalized checkpoint found")
			return s.deployFailed(ctx, application)
		}

		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal, fmt.Sprintf("Restoring from externalized checkpoint %s", path))

		restorePath = path
	} else if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal, fmt.Sprintf("Canceled job with savepoint %s",
			savepointStatusResponse.Operation.Location))
		restorePath = savepointStatusResponse.Operation.Location
	}

	if restorePath != "" {
		application.Spec.SavepointInfo.SavepointLocation = restorePath
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationSubmittingJob)
	}

	return nil
}

func (s *FlinkStateMachine) submitJobIfNeeded(ctx context.Context, app *v1alpha1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string) (*client.FlinkJob, error) {
	isReady, _ := s.flinkController.IsServiceReady(ctx, app, hash)
	// Ignore errors
	if !isReady {
		return nil, nil
	}

	// add the job running finalizer if necessary
	if err := s.addFinalizerIfMissing(ctx, app, jobFinalizer); err != nil {
		return nil, err
	}

	// Check that there are no jobs running before starting the job
	jobs, err := s.flinkController.GetJobsForApplication(ctx, app, hash)
	if err != nil {
		return nil, err
	}

	// TODO: check if there are multiple active jobs
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob == nil {
		logger.Infof(ctx, "No active job found for the application %v", jobs)
		jobID, err := s.flinkController.StartFlinkJob(ctx, app, hash,
			jarName, parallelism, entryClass, programArgs)
		if err != nil {
			s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeWarning, fmt.Sprintf("Failed to submit job to cluster: %v", err))

			// TODO: we probably want some kind of back-off here
			return nil, err
		}

		s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeNormal, fmt.Sprintf("Flink job submitted to cluster with id %s", jobID))
		app.Status.JobStatus.JobID = jobID
		activeJob = flink.GetActiveFlinkJob(jobs)
	} else {
		app.Status.JobStatus.JobID = activeJob.JobID
	}

	return activeJob, nil
}

func (s *FlinkStateMachine) updateGenericService(ctx context.Context, app *v1alpha1.FlinkApplication, newHash string) error {
	service, err := s.k8Cluster.GetService(ctx, app.Namespace, app.Name)
	if err != nil {
		return err
	}
	if service == nil {
		// this is bad... if the service is somehow deleted between the previous call to CreateCluster and here
		// recovery will not be possible
		// TODO: handle this case better
		return errors.New("service does not exist")
	}

	if service.Spec.Selector[flink.FlinkAppHash] != newHash {
		// the service hasn't yet been updated
		service.Spec.Selector[flink.FlinkAppHash] = newHash
		err = s.k8Cluster.UpdateK8Object(ctx, service)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *FlinkStateMachine) handleSubmittingJob(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	if s.shouldRollback(ctx, app) {
		// Something's gone wrong; roll back
		return s.updateApplicationPhase(ctx, app, v1alpha1.FlinkApplicationRollingBackJob)
	}

	// switch the service to point to the new jobmanager
	hash := flink.HashForApplication(app)
	err := s.updateGenericService(ctx, app, hash)
	if err != nil {
		return err
	}

	activeJob, err := s.submitJobIfNeeded(ctx, app, hash,
		app.Spec.JarName, app.Spec.Parallelism, app.Spec.EntryClass, app.Spec.ProgramArgs)
	if err != nil {
		s.getAndUpdateError(ctx, app, err)
		return err
	}

	if activeJob != nil && activeJob.Status == client.Running {
		// Clear the savepoint info
		app.Spec.SavepointInfo = v1alpha1.SavepointInfo{}
		// Update the application status with the running job info
		app.Status.DeployHash = hash
		app.Status.JobStatus.JarName = app.Spec.JarName
		app.Status.JobStatus.Parallelism = app.Spec.Parallelism
		app.Status.JobStatus.EntryClass = app.Spec.EntryClass
		app.Status.JobStatus.ProgramArgs = app.Spec.ProgramArgs

		return s.updateApplicationPhase(ctx, app, v1alpha1.FlinkApplicationRunning)
	}

	return nil
}

// Something has gone wrong during the update, post job-cancellation (and cluster tear-down in single mode). We need
// to try to get things back into a working state
func (s *FlinkStateMachine) handleRollingBack(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	if s.shouldRollback(ctx, app) {
		// we've failed in our roll back attempt (presumably because something's now wrong with the original cluster)
		// move immediately to the DeployFailed state so that the user can recover.
		return s.deployFailed(ctx, app)
	}

	s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeWarning, "Deployment failed, rolling back")

	// TODO: handle single mode

	// TODO: it's possible that a job is successfully running in the new cluster at this point -- should cancel it
	//       so that we never have two jobs running at once.

	// update the service to point back to the old deployment if needed
	err := s.updateGenericService(ctx, app, app.Status.DeployHash)
	if err != nil {
		return err
	}

	// wait until the service is ready
	isReady, _ := s.flinkController.IsServiceReady(ctx, app, app.Status.DeployHash)
	// Ignore errors
	if !isReady {
		return nil
	}

	// submit the old job
	activeJob, err := s.submitJobIfNeeded(ctx, app, app.Status.DeployHash,
		app.Status.JobStatus.JarName, app.Status.JobStatus.Parallelism,
		app.Status.JobStatus.EntryClass, app.Status.JobStatus.ProgramArgs)

	if err != nil {
		s.getAndUpdateError(ctx, app, err)
		return err
	}

	if activeJob != nil {
		app.Spec.SavepointInfo = v1alpha1.SavepointInfo{}
		// move to the deploy failed state
		return s.deployFailed(ctx, app)
	}

	return nil
}

// Check if the application is Running.
// This is a stable state. Keep monitoring if the underlying CRD reflects the Flink cluster
func (s *FlinkStateMachine) handleApplicationRunning(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	jobs, err := s.flinkController.GetJobsForApplication(ctx, application, application.Status.DeployHash)
	if err != nil {
		// TODO: think more about this case
		return err
	}

	// The jobid in Flink can change if there is a Job manager failover.
	// The Operator needs to update its state with the right value.
	// In the Running state, there must be a job already started in the cluster.
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob != nil {
		application.Status.JobStatus.JobID = activeJob.JobID
	}

	logger.Debugf(ctx, "Application running with job %v", activeJob)

	cur, err := s.flinkController.GetCurrentDeploymentsForApp(ctx, application)
	if err != nil {
		return err
	}

	// If the application has changed (i.e., there are no current deployments), and we haven't already failed trying to
	// do the update, move to the cluster starting phase to create the new cluster
	if cur == nil {
		logger.Infof(ctx, "Application resource has changed. Moving to Updating")
		// TODO: handle single mode
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationUpdating)
	}

	// If there are old resources left-over from a previous version, clean them up
	err = s.flinkController.DeleteOldResourcesForApp(ctx, application)
	if err != nil {
		logger.Warn(ctx, "Failed to clean up old resources: %v", err)
	}

	// Update status of the cluster
	hasClusterStatusChanged, clusterErr := s.flinkController.CompareAndUpdateClusterStatus(ctx, application, application.Status.DeployHash)
	if clusterErr != nil {
		logger.Errorf(ctx, "Updating cluster status failed with %v", clusterErr)
	}

	// Update status of jobs on the cluster
	hasJobStatusChanged, jobsErr := s.flinkController.CompareAndUpdateJobStatus(ctx, application, application.Status.DeployHash)
	if jobsErr != nil {
		logger.Errorf(ctx, "Updating jobs status failed with %v", jobsErr)
	}

	// Update k8s object if either job or cluster status has changed
	if hasJobStatusChanged || hasClusterStatusChanged {
		return s.k8Cluster.UpdateK8Object(ctx, application)
	}

	return nil
}

func (s *FlinkStateMachine) addFinalizerIfMissing(ctx context.Context, application *v1alpha1.FlinkApplication, finalizer string) error {
	for _, f := range application.Finalizers {
		if f == finalizer {
			return nil
		}
	}

	// finalizer not present; add
	application.Finalizers = append(application.Finalizers, finalizer)
	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func removeString(list []string, target string) []string {
	ret := make([]string, 0)
	for _, s := range list {
		if s != target {
			ret = append(ret, s)
		}
	}

	return ret
}

func (s *FlinkStateMachine) clearFinalizers(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	app.Finalizers = removeString(app.Finalizers, jobFinalizer)
	return s.k8Cluster.UpdateK8Object(ctx, app)
}

func jobFinished(jobs []client.FlinkJob, id string) bool {
	for _, job := range jobs {
		if job.JobID == id {
			return job.Status == client.Canceled ||
				job.Status == client.Failed ||
				job.Status == client.Finished
		}
	}

	return true
}

func (s *FlinkStateMachine) handleApplicationDeleting(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	// There should be a way for the user to force deletion (e.g., if the job is failing and they can't
	// savepoint). However, this seems dangerous to do automatically.
	// If https://github.com/kubernetes/kubernetes/issues/56567 is fixed users will be able to use
	// kubectl delete --force, but for now they will need to update the DeleteMode.

	// If the delete mode is none or there's no deployhash set (which means we failed to submit the job on the
	// first deploy) just delete the finalizer so the cluster can be torn down
	if app.Spec.DeleteMode == v1alpha1.DeleteModeNone || app.Status.DeployHash == "" {
		return s.clearFinalizers(ctx, app)
	}

	jobs, err := s.flinkController.GetJobsForApplication(ctx, app, app.Status.DeployHash)
	if err != nil {
		return err
	}

	finished := jobFinished(jobs, app.Status.JobStatus.JobID)

	if len(jobs) == 0 || finished {
		// there are no running jobs for this application, we can just tear down
		return s.clearFinalizers(ctx, app)
	}

	switch app.Spec.DeleteMode {
	case v1alpha1.DeleteModeForceCancel:
		logger.Infof(ctx, "Force cancelling job as part of cleanup")
		return s.flinkController.ForceCancel(ctx, app, app.Status.DeployHash)
	case v1alpha1.DeleteModeSavepoint, "":
		if app.Spec.SavepointInfo.SavepointLocation != "" {
			// we've already created the savepoint, now just waiting for the job to be cancelled
			return nil
		}

		if app.Spec.SavepointInfo.TriggerID == "" {
			// delete with savepoint
			triggerID, err := s.flinkController.CancelWithSavepoint(ctx, app, app.Status.DeployHash)
			if err != nil {
				s.getAndUpdateError(ctx, app, err)
				return err
			}
			s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeNormal, fmt.Sprintf("Cancelling job with savepoint %v", triggerID))
			app.Spec.SavepointInfo.TriggerID = triggerID
		} else {
			// we've already started savepointing; check the status
			status, err := s.flinkController.GetSavepointStatus(ctx, app, app.Status.DeployHash)
			if err != nil {
				s.getAndUpdateError(ctx, app, err)
				return err
			}

			if status.Operation.Location == "" && status.SavepointStatus.Status != client.SavePointInProgress {
				// savepointing failed
				s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeWarning, fmt.Sprintf("Failed to take savepoint %v", status.Operation.FailureCause))
				// clear the trigger id so that we can try again
				app.Spec.SavepointInfo.TriggerID = ""
			} else if status.SavepointStatus.Status == client.SavePointCompleted {
				// we're done, clean up
				s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeNormal, fmt.Sprintf("Cancelled job with savepoint '%s'", status.Operation.Location))
				app.Spec.SavepointInfo.SavepointLocation = status.Operation.Location
				app.Spec.SavepointInfo.TriggerID = ""
			}
		}

		return s.k8Cluster.UpdateK8Object(ctx, app)
	default:
		logger.Errorf(ctx, "Unsupported DeleteMode %s", app.Spec.DeleteMode)
	}

	return nil
}

func (s *FlinkStateMachine) getAndUpdateError(ctx context.Context, application *v1alpha1.FlinkApplication, err error) {
	application.Status.LastSeenError = client.GetErrorKey(err)
	updateErr := s.k8Cluster.UpdateK8Object(ctx, application)
	if updateErr != nil {
		logger.Errorf(ctx, "Updating last seen error failed with %v", updateErr)
	}
}

func NewFlinkStateMachine(k8sCluster k8.ClusterInterface, config config.RuntimeConfig) FlinkHandlerInterface {

	metrics := newStateMachineMetrics(config.MetricsScope)
	return &FlinkStateMachine{
		k8Cluster:       k8sCluster,
		flinkController: flink.NewController(k8sCluster, config),
		clock:           clock.RealClock{},
		metrics:         metrics,
	}
}
