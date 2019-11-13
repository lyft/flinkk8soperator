package flinkapplication

import (
	"context"
	"math"
	"time"

	"k8s.io/client-go/tools/record"

	"github.com/pkg/errors"

	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
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
	jobFinalizer    = "job.finalizers.flink.k8s.io"
	statusChanged   = true
	statusUnchanged = false
)

// The core state machine that manages Flink clusters and jobs. See docs/state_machine.md for a description of the
// states and transitions.
type FlinkHandlerInterface interface {
	Handle(ctx context.Context, application *v1beta1.FlinkApplication) error
}

type FlinkStateMachine struct {
	flinkController flink.ControllerInterface
	k8Cluster       k8.ClusterInterface
	clock           clock.Clock
	metrics         *stateMachineMetrics
	retryHandler    client.RetryHandlerInterface
}

type stateMachineMetrics struct {
	scope                             promutils.Scope
	stateMachineHandlePhaseMap        map[v1beta1.FlinkApplicationPhase]labeled.StopWatch
	stateMachineHandleSuccessPhaseMap map[v1beta1.FlinkApplicationPhase]labeled.StopWatch
	errorCounterPhaseMap              map[v1beta1.FlinkApplicationPhase]labeled.Counter
}

func newStateMachineMetrics(scope promutils.Scope) *stateMachineMetrics {
	stateMachineScope := scope.NewSubScope("state_machine")
	stateMachineHandlePhaseMap := map[v1beta1.FlinkApplicationPhase]labeled.StopWatch{}
	stateMachineHandleSuccessPhaseMap := map[v1beta1.FlinkApplicationPhase]labeled.StopWatch{}
	errorCounterPhaseMap := map[v1beta1.FlinkApplicationPhase]labeled.Counter{}

	for _, phase := range v1beta1.FlinkApplicationPhases {
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

func (s *FlinkStateMachine) updateApplicationPhase(application *v1beta1.FlinkApplication, phase v1beta1.FlinkApplicationPhase) {
	application.Status.Phase = phase
}

func (s *FlinkStateMachine) shouldRollback(ctx context.Context, application *v1beta1.FlinkApplication) (bool, string) {
	if application.Spec.ForceRollback && application.Status.Phase != v1beta1.FlinkApplicationRollingBackJob {
		return true, "forceRollback is set in the resource"
	}
	if application.Status.DeployHash == "" {
		// TODO: we may want some more sophisticated way of handling this case
		// there's no previous deploy for this application, so nothing to roll back to
		return false, ""
	}

	// Check if the error is retryable
	if application.Status.LastSeenError != nil && s.retryHandler.IsErrorRetryable(application.Status.LastSeenError) {
		if s.retryHandler.IsRetryRemaining(application.Status.LastSeenError, application.Status.RetryCount) {
			logger.Warnf(ctx, "Application in phase %v retrying with error %v",
				application.Status.Phase, application.Status.LastSeenError)
			return false, ""
		}
		// Retryable error with retries exhausted
		error := application.Status.LastSeenError
		application.Status.LastSeenError = nil
		return true, fmt.Sprintf("exhausted retries handling error %v", error)
	}

	// For non-retryable errors, always fail fast
	if application.Status.LastSeenError != nil {
		error := application.Status.LastSeenError
		application.Status.LastSeenError = nil
		return true, fmt.Sprintf("will not retry error %v", error)
	}

	// As a default, use a time based wait to determine whether to rollback or not.
	if application.Status.LastUpdatedAt != nil {
		if _, ok := s.retryHandler.WaitOnError(s.clock, application.Status.LastUpdatedAt.Time); !ok {
			application.Status.LastSeenError = nil
			return true, fmt.Sprintf("failed to make progress after %v", config.GetConfig().MaxErrDuration)
		}
	}
	return false, ""
}

func (s *FlinkStateMachine) Handle(ctx context.Context, application *v1beta1.FlinkApplication) error {
	currentPhase := application.Status.Phase
	if _, ok := s.metrics.stateMachineHandlePhaseMap[currentPhase]; !ok {
		errMsg := fmt.Sprintf("Invalid state %s for the application", currentPhase)
		return errors.New(errMsg)
	}
	timer := s.metrics.stateMachineHandlePhaseMap[currentPhase].Start(ctx)
	successTimer := s.metrics.stateMachineHandleSuccessPhaseMap[currentPhase].Start(ctx)

	defer timer.Stop()
	updateStatus, err := s.handle(ctx, application)

	// Update k8s object
	if updateStatus {
		now := v1.NewTime(s.clock.Now())
		application.Status.LastUpdatedAt = &now
		updateAppErr := s.k8Cluster.UpdateStatus(ctx, application)
		if updateAppErr != nil {
			s.metrics.errorCounterPhaseMap[currentPhase].Inc(ctx)
			return updateAppErr
		}
	}
	if err != nil {
		s.metrics.errorCounterPhaseMap[currentPhase].Inc(ctx)
	} else {
		successTimer.Stop()
	}
	return err
}

func (s *FlinkStateMachine) handle(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	var appErr error
	updateApplication := false
	updateLastSeenError := false
	appPhase := application.Status.Phase

	if !application.ObjectMeta.DeletionTimestamp.IsZero() && appPhase != v1beta1.FlinkApplicationDeleting {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationDeleting)
		// Always perform a single application update per callback
		return statusChanged, nil
	}

	if s.IsTimeToHandlePhase(application) {
		if !v1beta1.IsRunningPhase(application.Status.Phase) {
			logger.Infof(ctx, "Handling state for application")
		}
		switch application.Status.Phase {
		case v1beta1.FlinkApplicationNew, v1beta1.FlinkApplicationUpdating:
			// Currently just transitions to the next state
			updateApplication, appErr = s.handleNewOrUpdating(ctx, application)
		case v1beta1.FlinkApplicationClusterStarting:
			updateApplication, appErr = s.handleClusterStarting(ctx, application)
		case v1beta1.FlinkApplicationSubmittingJob:
			updateApplication, appErr = s.handleSubmittingJob(ctx, application)
		case v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed:
			updateApplication, appErr = s.handleApplicationRunning(ctx, application)
		case v1beta1.FlinkApplicationSavepointing:
			updateApplication, appErr = s.handleApplicationSavepointing(ctx, application)
		case v1beta1.FlinkApplicationRollingBackJob:
			updateApplication, appErr = s.handleRollingBack(ctx, application)
		case v1beta1.FlinkApplicationDeleting:
			updateApplication, appErr = s.handleApplicationDeleting(ctx, application)
		}

		if !v1beta1.IsRunningPhase(appPhase) {
			// Only update LastSeenError and thereby invoke error handling logic for
			// non-Running phases
			updateLastSeenError = s.compareAndUpdateError(application, appErr)
		}
	} else {
		logger.Infof(ctx, "Handle state skipped for application, lastSeenError %v", application.Status.LastSeenError)
	}
	return updateApplication || updateLastSeenError, appErr
}

func (s *FlinkStateMachine) IsTimeToHandlePhase(application *v1beta1.FlinkApplication) bool {
	lastSeenError := application.Status.LastSeenError
	if application.Status.LastSeenError == nil || !s.retryHandler.IsErrorRetryable(application.Status.LastSeenError) {
		return true
	}
	// If for some reason, the error update time is nil, set it so that retries can proceed
	if lastSeenError.LastErrorUpdateTime == nil {
		now := v1.NewTime(s.clock.Now())
		application.Status.LastSeenError.LastErrorUpdateTime = &now
		return true
	}
	retryCount := application.Status.RetryCount
	if s.retryHandler.IsRetryRemaining(lastSeenError, retryCount) {
		if s.retryHandler.IsTimeToRetry(s.clock, lastSeenError.LastErrorUpdateTime.Time, retryCount) {
			application.Status.RetryCount++
			return true
		}
	}

	return false
}

// In this state we create a new cluster, either due to an entirely new FlinkApplication or due to an update.
func (s *FlinkStateMachine) handleNewOrUpdating(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	// TODO: add up-front validation on the FlinkApplication resource
	if rollback, reason := s.shouldRollback(ctx, application); rollback {
		// we've failed to make progress; move to deploy failed
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "ClusterCreationFailed",
			fmt.Sprintf("Failed to create Flink Cluster: %s", reason))
		return s.deployFailed(ctx, application)
	}

	// Create the Flink cluster
	err := s.flinkController.CreateCluster(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Cluster creation failed with error: %v", err)
		return statusUnchanged, err
	}

	s.updateApplicationPhase(application, v1beta1.FlinkApplicationClusterStarting)
	return statusChanged, nil
}

func (s *FlinkStateMachine) deployFailed(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	hash := flink.HashForApplication(app)
	s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "RolledBackDeploy",
		fmt.Sprintf("Successfully rolled back deploy %s", hash))
	app.Status.FailedDeployHash = hash
	// set rollbackHash to deployHash
	app.Status.RollbackHash = app.Status.DeployHash
	// Reset error and retry count
	app.Status.LastSeenError = nil
	app.Status.RetryCount = 0

	s.updateApplicationPhase(app, v1beta1.FlinkApplicationDeployFailed)
	return statusChanged, nil
}

// Create the underlying Kubernetes objects for the new cluster
func (s *FlinkStateMachine) handleClusterStarting(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	if rollback, reason := s.shouldRollback(ctx, application); rollback {
		// we've failed to make progress; move to deploy failed
		// TODO: this will need different logic in single mode
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "ClusterCreationFailed",
			fmt.Sprintf(
				"Flink cluster failed to become available: %s", reason))
		return s.deployFailed(ctx, application)
	}

	// Wait for all to be running
	ready, err := s.flinkController.IsClusterReady(ctx, application)
	if err != nil {
		return statusUnchanged, err
	}
	if !ready {
		return statusUnchanged, nil
	}

	logger.Infof(ctx, "Flink cluster has started successfully")
	// TODO: in single mode move to submitting job
	s.updateApplicationPhase(application, v1beta1.FlinkApplicationSavepointing)
	return statusChanged, nil
}

func (s *FlinkStateMachine) handleApplicationSavepointing(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	// we've already savepointed (or this is our first deploy), continue on
	if application.Status.SavepointPath != "" || application.Status.DeployHash == "" {
		application.Status.JobStatus.JobID = ""
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
		return statusChanged, nil
	}

	// we haven't started savepointing yet; do so now
	// TODO: figure out the idempotence of this
	if application.Status.SavepointTriggerID == "" {
		if rollback, reason := s.shouldRollback(ctx, application); rollback {
			// we were unable to start savepointing for our failure period, so roll back
			// TODO: we should think about how to handle the case where the cluster has started savepointing, but does
			//       not finish within some time frame. Currently, we just wait indefinitely for the JM to report its
			//       status. It's not clear what the right answer is.
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "SavepointFailed",
				fmt.Sprintf("Could not start savepointing: %s", reason))
			return s.deployFailed(ctx, application)
		}

		triggerID, err := s.flinkController.CancelWithSavepoint(ctx, application, application.Status.DeployHash)
		if err != nil {
			return statusUnchanged, err
		}

		s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "CancellingJob",
			fmt.Sprintf("Cancelling job %s with a final savepoint", application.Status.JobStatus.JobID))

		application.Status.SavepointTriggerID = triggerID
		return statusChanged, nil
	}

	// check the savepoints in progress
	savepointStatusResponse, err := s.flinkController.GetSavepointStatus(ctx, application, application.Status.DeployHash)
	if err != nil {
		return statusUnchanged, err
	}

	var restorePath string
	if savepointStatusResponse.Operation.Location == "" &&
		savepointStatusResponse.SavepointStatus.Status != client.SavePointInProgress {
		// Savepointing failed
		// TODO: we should probably retry this a few times before failing
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "SavepointFailed",
			fmt.Sprintf("Failed to take savepoint for job %s: %v",
				application.Status.JobStatus.JobID, savepointStatusResponse.Operation.FailureCause))

		// try to find an externalized checkpoint
		path, err := s.flinkController.FindExternalizedCheckpoint(ctx, application, application.Status.DeployHash)
		if err != nil {
			logger.Infof(ctx, "error while fetching externalized checkpoint path: %v", err)
			return s.deployFailed(ctx, application)
		} else if path == "" {
			logger.Infof(ctx, "no externalized checkpoint found")
			return s.deployFailed(ctx, application)
		}

		s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "RestoringExternalizedCheckpoint",
			fmt.Sprintf("Restoring from externalized checkpoint %s for deploy %s",
				path, flink.HashForApplication(application)))

		restorePath = path
	} else if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "CanceledJob",
			fmt.Sprintf("Canceled job with savepoint %s",
				savepointStatusResponse.Operation.Location))
		restorePath = savepointStatusResponse.Operation.Location
	}

	if restorePath != "" {
		application.Status.SavepointPath = restorePath
		application.Status.JobStatus.JobID = ""
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
		return statusChanged, nil
	}

	return statusUnchanged, nil
}

func (s *FlinkStateMachine) submitJobIfNeeded(ctx context.Context, app *v1beta1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool,
	savepointPath string) (string, error) {
	isReady, _ := s.flinkController.IsServiceReady(ctx, app, hash)
	// Ignore errors
	if !isReady {
		return "", nil
	}

	// add the job running finalizer if necessary
	if err := s.addFinalizerIfMissing(ctx, app, jobFinalizer); err != nil {
		return "", err
	}

	// Check if the job id has already been set on our application
	if app.Status.JobStatus.JobID != "" {
		return app.Status.JobStatus.JobID, nil
	}

	// Check that there are no jobs running before starting the job
	jobs, err := s.flinkController.GetJobsForApplication(ctx, app, hash)
	if err != nil {
		return "", err
	}

	activeJobs := flink.GetActiveFlinkJobs(jobs)

	switch n := len(activeJobs); n {
	case 0:
		// no active jobs, we need to submit a new one
		logger.Infof(ctx, "No job found for the application")

		jobID, err := s.flinkController.StartFlinkJob(ctx, app, hash,
			jarName, parallelism, entryClass, programArgs, allowNonRestoredState, savepointPath)
		if err != nil {
			s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "JobSubmissionFailed",
				fmt.Sprintf("Failed to submit job to cluster for deploy %s: %v", hash, err))
			return "", err
		}

		s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "JobSubmitted",
			fmt.Sprintf("Flink job submitted to cluster with id %s", jobID))
		return jobID, nil
	case 1:
		// there's one active job, we must have failed to save the update after job submission in a previous cycle
		logger.Warnf(ctx, "Found already-submitted job for application with id %s", activeJobs[0].JobID)
		return activeJobs[0].JobID, nil
	default:
		// there's more than one active jobs, something has gone terribly wrong
		return "", errors.New("found multiple active jobs for application")
	}
}

func (s *FlinkStateMachine) updateGenericService(ctx context.Context, app *v1beta1.FlinkApplication, newHash string) error {
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

func (s *FlinkStateMachine) handleSubmittingJob(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	if rollback, reason := s.shouldRollback(ctx, app); rollback {
		// Something's gone wrong; roll back
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "JobsSubmissionFailed",
			fmt.Sprintf("Failed to submit job: %s", reason))
		s.updateApplicationPhase(app, v1beta1.FlinkApplicationRollingBackJob)
		return statusChanged, nil
	}

	// switch the service to point to the new jobmanager
	hash := flink.HashForApplication(app)
	err := s.updateGenericService(ctx, app, hash)
	if err != nil {
		return statusUnchanged, err
	}

	// Update status of the cluster
	_, clusterErr := s.flinkController.CompareAndUpdateClusterStatus(ctx, app, hash)
	if clusterErr != nil {
		logger.Errorf(ctx, "Updating cluster status failed with error: %v", clusterErr)
	}

	if app.Status.JobStatus.JobID == "" {
		savepointPath := ""
		if app.Status.DeployHash == "" {
			// this is the first deploy, use the user-provided savepoint
			savepointPath = app.Spec.SavepointPath
			if savepointPath == "" {
				//nolint // fall back to the old config for backwards-compatibility
				savepointPath = app.Spec.SavepointInfo.SavepointLocation
			}
		} else {
			// otherwise use the savepoint created by the operator
			savepointPath = app.Status.SavepointPath
		}

		appJobID, err := s.submitJobIfNeeded(ctx, app, hash,
			app.Spec.JarName, app.Spec.Parallelism, app.Spec.EntryClass, app.Spec.ProgramArgs,
			app.Spec.AllowNonRestoredState, savepointPath)
		if err != nil {
			return statusUnchanged, err
		}

		if appJobID != "" {
			app.Status.JobStatus.JobID = appJobID
			return statusChanged, nil
		}

		// we weren't ready to submit yet
		return statusUnchanged, nil
	}

	// get the state of the current application
	job, err := s.flinkController.GetJobForApplication(ctx, app, hash)
	if err != nil {
		return statusUnchanged, err
	}

	if job.State == client.Running {
		// Update the application status with the running job info
		app.Status.DeployHash = hash
		app.Status.SavepointPath = ""
		app.Status.SavepointTriggerID = ""
		app.Status.JobStatus.JarName = app.Spec.JarName
		app.Status.JobStatus.Parallelism = app.Spec.Parallelism
		app.Status.JobStatus.EntryClass = app.Spec.EntryClass
		app.Status.JobStatus.ProgramArgs = app.Spec.ProgramArgs
		app.Status.JobStatus.AllowNonRestoredState = app.Spec.AllowNonRestoredState

		s.updateApplicationPhase(app, v1beta1.FlinkApplicationRunning)
		return statusChanged, nil
	}

	return statusUnchanged, nil
}

// Something has gone wrong during the update, post job-cancellation (and cluster tear-down in single mode). We need
// to try to get things back into a working state
func (s *FlinkStateMachine) handleRollingBack(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	if rollback, reason := s.shouldRollback(ctx, app); rollback {
		// we've failed in our roll back attempt (presumably because something's now wrong with the original cluster)
		// move immediately to the DeployFailed state so that the user can recover.
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "RollbackFailed",
			fmt.Sprintf("Failed to rollback to origin deployment, manual intervention needed: %s", reason))
		return s.deployFailed(ctx, app)
	}

	s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "DeployFailed",
		fmt.Sprintf("Deployment %s failed, rolling back", flink.HashForApplication(app)))

	// TODO: handle single mode

	// TODO: it's possible that a job is successfully running in the new cluster at this point -- should cancel it
	//       so that we never have two jobs running at once.

	// update the service to point back to the old deployment if needed
	err := s.updateGenericService(ctx, app, app.Status.DeployHash)
	if err != nil {
		return statusUnchanged, err
	}

	// wait until the service is ready
	isReady, _ := s.flinkController.IsServiceReady(ctx, app, app.Status.DeployHash)
	// Ignore errors
	if !isReady {
		return statusUnchanged, nil
	}

	// submit the old job
	jobID, err := s.submitJobIfNeeded(ctx, app, app.Status.DeployHash,
		app.Status.JobStatus.JarName, app.Status.JobStatus.Parallelism,
		app.Status.JobStatus.EntryClass, app.Status.JobStatus.ProgramArgs,
		app.Status.JobStatus.AllowNonRestoredState,
		app.Status.SavepointPath)

	// set rollbackHash
	app.Status.RollbackHash = app.Status.DeployHash
	if err != nil {
		return statusUnchanged, err
	}

	if jobID != "" {
		app.Status.JobStatus.JobID = jobID
		app.Status.SavepointPath = ""
		app.Status.SavepointTriggerID = ""
		// move to the deploy failed state
		return s.deployFailed(ctx, app)
	}

	return statusUnchanged, nil
}

// Check if the application is Running.
// This is a stable state. Keep monitoring if the underlying CRD reflects the Flink cluster
func (s *FlinkStateMachine) handleApplicationRunning(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	job, err := s.flinkController.GetJobForApplication(ctx, application, application.Status.DeployHash)
	if err != nil {
		// TODO: think more about this case
		return statusUnchanged, err
	}

	if job == nil {
		logger.Warnf(ctx, "Could not find active job {}", application.Status.JobStatus.JobID)
	} else {
		logger.Debugf(ctx, "Application running with job %v", job.JobID)
	}

	cur, err := s.flinkController.GetCurrentDeploymentsForApp(ctx, application)
	if err != nil {
		return statusUnchanged, err
	}

	// If the application has changed (i.e., there are no current deployments), and we haven't already failed trying to
	// do the update, move to the cluster starting phase to create the new cluster
	if cur == nil {
		logger.Infof(ctx, "Application resource has changed. Moving to Updating")
		// TODO: handle single mode
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationUpdating)
		return statusChanged, nil
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
		return statusChanged, nil
	}

	return statusUnchanged, nil
}

func (s *FlinkStateMachine) addFinalizerIfMissing(ctx context.Context, application *v1beta1.FlinkApplication, finalizer string) error {
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

func (s *FlinkStateMachine) clearFinalizers(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	app.Finalizers = removeString(app.Finalizers, jobFinalizer)
	return statusUnchanged, s.k8Cluster.UpdateK8Object(ctx, app)
}

func jobFinished(job *client.FlinkJobOverview) bool {
	return job == nil ||
		job.State == client.Canceled ||
		job.State == client.Failed ||
		job.State == client.Finished
}

func (s *FlinkStateMachine) handleApplicationDeleting(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	// There should be a way for the user to force deletion (e.g., if the job is failing and they can't
	// savepoint). However, this seems dangerous to do automatically.
	// If https://github.com/kubernetes/kubernetes/issues/56567 is fixed users will be able to use
	// kubectl delete --force, but for now they will need to update the DeleteMode.

	// If the delete mode is none or there's no deployhash set (which means we failed to submit the job on the
	// first deploy) just delete the finalizer so the cluster can be torn down
	if app.Spec.DeleteMode == v1beta1.DeleteModeNone || app.Status.DeployHash == "" {
		return s.clearFinalizers(ctx, app)
	}

	job, err := s.flinkController.GetJobForApplication(ctx, app, app.Status.DeployHash)
	if err != nil {
		return statusUnchanged, err
	}

	switch app.Spec.DeleteMode {
	case v1beta1.DeleteModeForceCancel:
		if job.State == client.Cancelling {
			// we've already cancelled the job, waiting for it to finish
			return statusUnchanged, nil
		} else if jobFinished(job) {
			// job was successfully cancelled
			return s.clearFinalizers(ctx, app)
		}

		logger.Infof(ctx, "Force-cancelling job without a savepoint")
		return statusUnchanged, s.flinkController.ForceCancel(ctx, app, app.Status.DeployHash)
	case v1beta1.DeleteModeSavepoint, "":
		if app.Status.SavepointPath != "" {
			// we've already created the savepoint, now just waiting for the job to be cancelled
			if jobFinished(job) {
				return s.clearFinalizers(ctx, app)
			}

			return statusUnchanged, nil
		}

		if app.Status.SavepointTriggerID == "" {
			// delete with savepoint
			triggerID, err := s.flinkController.CancelWithSavepoint(ctx, app, app.Status.DeployHash)
			if err != nil {
				return statusUnchanged, err
			}
			s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "CancellingJob",
				fmt.Sprintf("Cancelling job with savepoint %v", triggerID))
			app.Status.SavepointTriggerID = triggerID
		} else {
			// we've already started savepointing; check the status
			status, err := s.flinkController.GetSavepointStatus(ctx, app, app.Status.DeployHash)
			if err != nil {
				return statusUnchanged, err
			}

			if status.Operation.Location == "" && status.SavepointStatus.Status != client.SavePointInProgress {
				// savepointing failed
				s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "SavepointFailed",
					fmt.Sprintf("Failed to take savepoint %v", status.Operation.FailureCause))
				// clear the trigger id so that we can try again
				app.Status.SavepointTriggerID = ""
				return true, client.GetRetryableError(errors.New("failed to take savepoint"),
					v1beta1.CancelJobWithSavepoint, "500", math.MaxInt32)
			} else if status.SavepointStatus.Status == client.SavePointCompleted {
				// we're done, clean up
				s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "CanceledJob",
					fmt.Sprintf("Cancelled job with savepoint '%s'", status.Operation.Location))
				app.Status.SavepointPath = status.Operation.Location
				app.Status.SavepointTriggerID = ""
			}
		}

		return statusChanged, nil
	default:
		logger.Errorf(ctx, "Unsupported DeleteMode %s", app.Spec.DeleteMode)
	}

	return statusUnchanged, nil
}

func (s *FlinkStateMachine) compareAndUpdateError(application *v1beta1.FlinkApplication, err error) bool {
	oldErr := application.Status.LastSeenError

	if err == nil && oldErr == nil {
		return statusUnchanged
	}

	if err == nil {
		application.Status.LastSeenError = nil
	} else {
		if flinkAppError, ok := err.(*v1beta1.FlinkApplicationError); ok {
			application.Status.LastSeenError = flinkAppError
		} else {
			err = client.GetRetryableError(err, "UnknownMethod", client.GlobalFailure, client.DefaultRetries)
			application.Status.LastSeenError = err.(*v1beta1.FlinkApplicationError)
		}

		now := v1.NewTime(s.clock.Now())
		application.Status.LastSeenError.LastErrorUpdateTime = &now
	}

	return statusChanged

}

func createRetryHandler() client.RetryHandlerInterface {
	return client.NewRetryHandler(config.GetConfig().BaseBackoffDuration.Duration, config.GetConfig().MaxErrDuration.Duration,
		config.GetConfig().MaxBackoffDuration.Duration)
}

func NewFlinkStateMachine(k8sCluster k8.ClusterInterface, eventRecorder record.EventRecorder, config config.RuntimeConfig) FlinkHandlerInterface {

	metrics := newStateMachineMetrics(config.MetricsScope)
	return &FlinkStateMachine{
		k8Cluster:       k8sCluster,
		flinkController: flink.NewController(k8sCluster, eventRecorder, config),
		clock:           clock.RealClock{},
		metrics:         metrics,
		retryHandler:    createRetryHandler(),
	}
}
