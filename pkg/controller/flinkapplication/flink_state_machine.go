package flinkapplication

import (
	"context"
	"math"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	k8_err "k8s.io/apimachinery/pkg/api/errors"

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
	// initialize application status array if it's not yet been initialized
	s.initializeAppStatusIfEmpty(application)

	if !application.ObjectMeta.DeletionTimestamp.IsZero() && appPhase != v1beta1.FlinkApplicationDeleting {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationDeleting)
		// Always perform a single application update per callback
		return statusChanged, nil
	}

	if s.IsTimeToHandlePhase(application, appPhase) {
		if !v1beta1.IsRunningPhase(application.Status.Phase) {
			logger.Infof(ctx, "Handling state for application")
		}
		switch application.Status.Phase {
		case v1beta1.FlinkApplicationNew, v1beta1.FlinkApplicationUpdating:
			// Currently just transitions to the next state
			updateApplication, appErr = s.handleNewOrUpdating(ctx, application)
		case v1beta1.FlinkApplicationRescaling:
			updateApplication, appErr = s.handleRescaling(ctx, application)
		case v1beta1.FlinkApplicationClusterStarting:
			updateApplication, appErr = s.handleClusterStarting(ctx, application)
		case v1beta1.FlinkApplicationSubmittingJob:
			updateApplication, appErr = s.handleSubmittingJob(ctx, application)
		case v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed:
			updateApplication, appErr = s.handleApplicationRunning(ctx, application)
		case v1beta1.FlinkApplicationCancelling:
			updateApplication, appErr = s.handleApplicationCancelling(ctx, application)
		case v1beta1.FlinkApplicationSavepointing:
			updateApplication, appErr = s.handleApplicationSavepointing(ctx, application)
		case v1beta1.FlinkApplicationRecovering:
			updateApplication, appErr = s.handleApplicationRecovering(ctx, application)
		case v1beta1.FlinkApplicationRollingBackJob:
			updateApplication, appErr = s.handleRollingBack(ctx, application)
		case v1beta1.FlinkApplicationDeleting:
			updateApplication, appErr = s.handleApplicationDeleting(ctx, application)
		case v1beta1.FlinkApplicationDualRunning:
			updateApplication, appErr = s.handleDualRunning(ctx, application)

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

func (s *FlinkStateMachine) IsTimeToHandlePhase(application *v1beta1.FlinkApplication, phase v1beta1.FlinkApplicationPhase) bool {
	if phase == v1beta1.FlinkApplicationDeleting {
		// reset lastSeenError and retryCount in case the application was failing in its previous phase
		// We always want a Deleting phase to be handled
		application.Status.LastSeenError = nil
		application.Status.RetryCount = 0
		return true
	}
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
		return s.deployFailed(application)
	}

	// Update version if blue/green deploy
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		application.Status.UpdatingVersion = getUpdatingVersion(application)
		// First deploy both versions are the same
		if application.Status.DeployHash == "" {
			application.Status.DeployVersion = application.Status.UpdatingVersion
		}
		// Reset teardown hash if set
		application.Status.TeardownHash = ""
	}

	// Reset the in-place update hash if set
	hash := flink.HashForApplication(application)
	if application.Status.InPlaceUpdatedFromHash == hash {
		// This means that we went from parallelism P -> P' -> P (P < P') without any other changes in the spec. This
		// will not work, because we are still using the deployments with this hash, and we can't scale them
		// down without disrupting the existing job. All we can do is tell the user how to fix the situation by
		// making a change that will give us a different hash.
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "DeployFailed",
			fmt.Sprintf("Could not scale down cluster due to previous in-place scale up. To continue this "+
				"deploy, modify the application's restartNonce."))
		return s.deployFailed(application)
	}
	application.Status.InPlaceUpdatedFromHash = ""

	// Create the Flink cluster
	err := s.flinkController.CreateCluster(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Cluster creation failed with error: %v", err)
		return statusUnchanged, err
	}

	s.updateApplicationPhase(application, v1beta1.FlinkApplicationClusterStarting)
	return statusChanged, nil
}

// In this phase we attempt to increase the scale of the application in-place without creating a new cluster
func (s *FlinkStateMachine) handleRescaling(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	if rollback, reason := s.shouldRollback(ctx, app); rollback {
		// we've failed to make progress; move to deploy failed
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "ClusterScaleUpFailed",
			fmt.Sprintf("New taskmanagers failed to become available: %s", reason))
		return s.deployFailed(app)
	}

	labels := k8.GetAppLabel(app.Name)
	oldHash := app.Status.DeployHash

	labels[flink.FlinkAppHash] = oldHash
	app.Status.InPlaceUpdatedFromHash = oldHash

	deployments, err := s.k8Cluster.GetDeploymentsWithLabel(ctx, app.Namespace, labels)
	if err != nil {
		return statusUnchanged, err
	}

	// If we have old (pre-scaled deployments), update their hashes our new hash to transform them into our new
	// replicas, and also update the TM replica count to our new parallelism
	var tmDeployment *appsv1.Deployment
	var jmDeployment *appsv1.Deployment

	for i, d := range deployments.Items {
		if flink.DeploymentIsJobmanager(&d) {
			jmDeployment = &deployments.Items[i]
		} else if flink.DeploymentIsTaskmanager(&d) {
			tmDeployment = &deployments.Items[i]
		}
	}

	newHash := flink.HashForApplication(app)

	if jmDeployment != nil {
		jmDeployment.Labels[flink.FlinkAppHash] = newHash
		if s.k8Cluster.UpdateK8Object(ctx, jmDeployment) != nil {
			return statusUnchanged, err
		}
	}

	if tmDeployment != nil {
		tmCount := flink.ComputeTaskManagerReplicas(app)
		if *tmDeployment.Spec.Replicas != tmCount {
			s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "ClusterScaleUp",
				fmt.Sprintf(
					"Increasing TaskManager replica count from %d to %d in preparation for scale up",
					*tmDeployment.Spec.Replicas, tmCount))

			*tmDeployment.Spec.Replicas = tmCount
		}

		tmDeployment.Labels[flink.FlinkAppHash] = newHash
		if s.k8Cluster.UpdateK8Object(ctx, tmDeployment) != nil {
			return statusUnchanged, err
		}
	}

	// Create a new versioned service by copying the existing one
	oldService, err := s.k8Cluster.GetService(ctx, app.Namespace, app.Name, oldHash)
	oldService = oldService.DeepCopy()
	if err != nil {
		return statusUnchanged, err
	}
	serviceLabels := flink.GetCommonAppLabels(app)
	serviceLabels[flink.FlinkAppHash] = newHash
	newService := corev1.Service{
		TypeMeta: v1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       k8.Service,
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      flink.VersionedJobManagerServiceName(app, newHash),
			Namespace: app.Namespace,
			OwnerReferences: []v1.OwnerReference{
				*v1.NewControllerRef(app, app.GroupVersionKind()),
			},
			Labels: serviceLabels,
		},
		Spec: corev1.ServiceSpec{
			Ports:    oldService.Spec.Ports,
			Selector: oldService.Spec.Selector,
		},
	}
	err = s.k8Cluster.CreateK8Object(ctx, &newService)
	if err != nil && !k8_err.IsAlreadyExists(err) {
		return statusUnchanged, err
	}

	// Wait for all to be running
	clusterReady, err := s.flinkController.IsClusterReady(ctx, app)
	if err != nil || !clusterReady {
		return statusUnchanged, err
	}

	// Wait for all TMs to be registered with the JobManager
	serviceReady, _ := s.flinkController.IsServiceReady(ctx, app, newHash)
	if !serviceReady {
		return statusUnchanged, nil
	}

	// once we're ready, continue on to job submission
	// TODO: should think more about how this should interact with blue/green, since doing things this way violates
	//       the normal blue/green non-downtime guarantee
	if app.Spec.SavepointDisabled {
		s.updateApplicationPhase(app, v1beta1.FlinkApplicationCancelling)
	} else {
		s.updateApplicationPhase(app, v1beta1.FlinkApplicationSavepointing)
	}

	return statusChanged, nil
}

func (s *FlinkStateMachine) deployFailed(app *v1beta1.FlinkApplication) (bool, error) {
	hash := flink.HashForApplication(app)
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
		return s.deployFailed(application)
	}

	// Wait for all to be running
	clusterReady, err := s.flinkController.IsClusterReady(ctx, application)
	if err != nil || !clusterReady {
		return statusUnchanged, err
	}

	// ignore the error, we just care whether it's ready or not
	serviceReady, _ := s.flinkController.IsServiceReady(ctx, application, flink.HashForApplication(application))
	if !serviceReady {
		return statusUnchanged, nil
	}

	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		// Update hashes
		s.flinkController.UpdateLatestVersionAndHash(application, application.Status.UpdatingVersion, flink.HashForApplication(application))
	}

	logger.Infof(ctx, "Flink cluster has started successfully")
	// TODO: in single mode move to submitting job
	if application.Spec.SavepointDisabled && !v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationCancelling)
	} else if application.Spec.SavepointDisabled && v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		// Blue Green deployment and no savepoint required implies, we directly transition to submitting job
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
	} else {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSavepointing)
	}
	return statusChanged, nil
}

func (s *FlinkStateMachine) initializeAppStatusIfEmpty(application *v1beta1.FlinkApplication) {
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		if len(application.Status.VersionStatuses) == 0 {
			application.Status.VersionStatuses = make([]v1beta1.FlinkApplicationVersionStatus, v1beta1.GetMaxRunningJobs(application.Spec.DeploymentMode))
		}
	}
	// Set the deployment mode if it's never been set
	if application.Status.DeploymentMode == "" {
		application.Status.DeploymentMode = application.Spec.DeploymentMode
	}
}

func (s *FlinkStateMachine) handleApplicationSavepointing(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	// we've already savepointed (or this is our first deploy), continue on
	if application.Status.SavepointPath != "" || application.Status.DeployHash == "" {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
		return statusChanged, nil
	}

	if rollback, reason := s.shouldRollback(ctx, application); rollback {
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "SavepointFailed",
			fmt.Sprintf("Could not savepoint existing job: %s", reason))
		application.Status.RetryCount = 0
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationRecovering)
		return statusChanged, nil
	}
	cancelFlag := getCancelFlag(application)
	// we haven't started savepointing yet; do so now
	// TODO: figure out the idempotence of this
	if application.Status.SavepointTriggerID == "" {
		triggerID, err := s.flinkController.Savepoint(ctx, application, application.Status.DeployHash, cancelFlag, s.flinkController.GetLatestJobID(ctx, application))
		if err != nil {
			return statusUnchanged, err
		}
		if cancelFlag {
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "CancellingJob",
				fmt.Sprintf("Cancelling job %s with a final savepoint", s.flinkController.GetLatestJobID(ctx, application)))
		} else {
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "SavepointingJob",
				fmt.Sprintf("Savepointing job %s with a final savepoint", s.flinkController.GetLatestJobID(ctx, application)))

		}

		application.Status.SavepointTriggerID = triggerID
		return statusChanged, nil
	}

	// check the savepoints in progress
	savepointStatusResponse, err := s.flinkController.GetSavepointStatus(ctx, application, application.Status.DeployHash, s.flinkController.GetLatestJobID(ctx, application))
	if err != nil {
		return statusUnchanged, err
	}

	if savepointStatusResponse.Operation.Location == "" &&
		savepointStatusResponse.SavepointStatus.Status != client.SavePointInProgress {
		// Savepointing failed
		// TODO: we should probably retry this a few times before failing
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "SavepointFailed",
			fmt.Sprintf("Failed to take savepoint for job %s: %v",
				s.flinkController.GetLatestJobID(ctx, application), savepointStatusResponse.Operation.FailureCause))
		application.Status.RetryCount = 0
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationRecovering)
		return statusChanged, nil
	} else if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		if cancelFlag {
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "CanceledJob",
				fmt.Sprintf("Canceled job with savepoint %s",
					savepointStatusResponse.Operation.Location))
		} else {
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "SavepointCompleted",
				fmt.Sprintf("Completed savepoint at %s",
					savepointStatusResponse.Operation.Location))
		}

		application.Status.SavepointPath = savepointStatusResponse.Operation.Location
		// We haven't cancelled the job in this case, so don't reset job ID
		if !v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
			s.flinkController.UpdateLatestJobID(ctx, application, "")
		}
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
		return statusChanged, nil
	}

	return statusUnchanged, nil
}

func (s *FlinkStateMachine) handleApplicationCancelling(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {

	// this is the first deploy
	if application.Status.DeployHash == "" {
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
		return statusChanged, nil
	}

	if rollback, reason := s.shouldRollback(ctx, application); rollback {
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "CancelFailed",
			fmt.Sprintf("Could not cancel existing job: %s", reason))
		application.Status.RetryCount = 0
		application.Status.JobStatus.JobID = ""
		s.updateApplicationPhase(application, v1beta1.FlinkApplicationRollingBackJob)
		return statusChanged, nil
	}

	job, err := s.flinkController.GetJobForApplication(ctx, application, application.Status.DeployHash)
	if err != nil {
		return statusUnchanged, err
	}

	if job != nil && job.State != client.Canceled &&
		job.State != client.Failed {
		err := s.flinkController.ForceCancel(ctx, application, application.Status.DeployHash, s.flinkController.GetLatestJobID(ctx, application))
		if err != nil {
			return statusUnchanged, err
		}
	}

	application.Status.JobStatus.JobID = ""
	s.updateApplicationPhase(application, v1beta1.FlinkApplicationSubmittingJob)
	return statusChanged, nil
}

func (s *FlinkStateMachine) handleApplicationRecovering(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	// we're in the middle of a deploy, and savepointing has failed in some way... we're going to try to recover
	// and push through if possible
	if rollback, reason := s.shouldRollback(ctx, app); rollback {
		// we failed to recover, attempt to rollback
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "RecoveryFailed",
			fmt.Sprintf("Failed to recover with externalized checkpoint: %s", reason))
		s.updateApplicationPhase(app, v1beta1.FlinkApplicationRollingBackJob)
		return statusChanged, nil
	}

	// TODO: the old job might still be running at this point... we should maybe try to force cancel it
	//       (but if the JM is unavailable, our options there might be limited)

	// try to find an externalized checkpoint
	path, err := s.flinkController.FindExternalizedCheckpoint(ctx, app, app.Status.DeployHash)
	if err != nil {
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "RecoveryFailed",
			"Failed to get externalized checkpoint config, could not recover. "+
				"Manual intervention is needed.")
		return s.deployFailed(app)
	} else if path == "" {
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "RecoveryFailed",
			"No externalized checkpoint found, could not recover. Make sure that "+
				"externalized checkpoints are enabled in your job's checkpoint configuration. Manual intervention "+
				"is needed to recover.")
		return s.deployFailed(app)
	}

	s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "RestoringExternalizedCheckpoint",
		fmt.Sprintf("Restoring from externalized checkpoint %s for deploy %s",
			path, flink.HashForApplication(app)))

	app.Status.SavepointPath = path
	s.flinkController.UpdateLatestJobID(ctx, app, "")
	s.updateApplicationPhase(app, v1beta1.FlinkApplicationSubmittingJob)
	return statusChanged, nil
}

func (s *FlinkStateMachine) submitJobIfNeeded(ctx context.Context, app *v1beta1.FlinkApplication, hash string,
	jarName string, parallelism int32, entryClass string, programArgs string, allowNonRestoredState bool,
	savepointPath string) (string, error) {

	// add the job running finalizer if necessary
	if err := s.addFinalizerIfMissing(ctx, app, jobFinalizer); err != nil {
		return "", err
	}

	// Check if the job id has already been set on our application
	if s.flinkController.GetLatestJobID(ctx, app) != "" {
		return s.flinkController.GetLatestJobID(ctx, app), nil
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
	service, err := s.k8Cluster.GetService(ctx, app.Namespace, app.Name, string(app.Status.UpdatingVersion))
	if err != nil {
		return err
	}
	if service == nil {
		// this is bad... if the service is somehow deleted between the previous call to CreateCluster and here
		// recovery will not be possible
		// TODO: handle this case better
		return errors.New("service does not exist")
	}

	deployments, err := s.flinkController.GetDeploymentsForHash(ctx, app, newHash)
	if deployments == nil {
		return errors.New("Could not find deployments for service " + service.Name)
	}
	if err != nil {
		return err
	}

	selector := deployments.Jobmanager.Spec.Selector.MatchLabels[flink.PodDeploymentSelector]
	if service.Spec.Selector[flink.PodDeploymentSelector] != selector {
		// the service hasn't yet been updated
		service.Spec.Selector[flink.PodDeploymentSelector] = selector
		// remove the old app hash selector if it's still present
		delete(service.Spec.Selector, flink.FlinkAppHash)
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
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "JobSubmissionFailed",
			fmt.Sprintf("Failed to submit job: %s", reason))
		s.flinkController.UpdateLatestJobID(ctx, app, "")
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

	if s.flinkController.GetLatestJobID(ctx, app) == "" {
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
			s.flinkController.UpdateLatestJobID(ctx, app, appJobID)
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
	if job == nil {
		return statusUnchanged, errors.Errorf("Could not find job %s", s.flinkController.GetLatestJobID(ctx, app))
	}

	// wait until all vertices have been scheduled and started
	allVerticesStarted := true
	for _, v := range job.Vertices {
		allVerticesStarted = allVerticesStarted && (v.StartTime > 0)
	}

	if job.State == client.Running && allVerticesStarted {
		// Update job status
		jobStatus := s.flinkController.GetLatestJobStatus(ctx, app)
		jobStatus.JarName = app.Spec.JarName
		jobStatus.Parallelism = app.Spec.Parallelism
		jobStatus.EntryClass = app.Spec.EntryClass
		jobStatus.ProgramArgs = app.Spec.ProgramArgs
		jobStatus.AllowNonRestoredState = app.Spec.AllowNonRestoredState
		s.flinkController.UpdateLatestJobStatus(ctx, app, jobStatus)
		// Update the application status with the running job info
		app.Status.SavepointPath = ""
		app.Status.SavepointTriggerID = ""
		if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) && app.Status.DeployHash != "" {
			s.updateApplicationPhase(app, v1beta1.FlinkApplicationDualRunning)
			return statusChanged, nil
		}
		app.Status.DeployHash = hash
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
			fmt.Sprintf("Failed to rollback to original deployment, manual intervention needed: %s", reason))
		return s.deployFailed(app)
	}

	s.flinkController.LogEvent(ctx, app, corev1.EventTypeWarning, "DeployFailed",
		fmt.Sprintf("Deployment %s failed, rolling back", flink.HashForApplication(app)))

	// In the case of blue green deploys, we don't try to submit a new job
	// and instead transition to a deploy failed state
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) && app.Status.DeployHash != "" {
		return s.deployFailed(app)
	}
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
	jobStatus := s.flinkController.GetLatestJobStatus(ctx, app)
	jobID, err := s.submitJobIfNeeded(ctx, app, app.Status.DeployHash,
		jobStatus.JarName, jobStatus.Parallelism,
		jobStatus.EntryClass, jobStatus.ProgramArgs,
		jobStatus.AllowNonRestoredState,
		app.Status.SavepointPath)

	// set rollbackHash
	app.Status.RollbackHash = app.Status.DeployHash
	if err != nil {
		return statusUnchanged, err
	}

	if jobID != "" {
		s.flinkController.UpdateLatestJobID(ctx, app, jobID)
		app.Status.SavepointPath = ""
		app.Status.SavepointTriggerID = ""
		// move to the deploy failed state
		s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "RollbackSucceeded",
			"Successfully rolled back to previous deploy")
		return s.deployFailed(app)
	}

	return statusUnchanged, nil
}

// Check if the application is Running.
// This is a stable state. Keep monitoring if the underlying CRD reflects the Flink cluster
func (s *FlinkStateMachine) handleApplicationRunning(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	cur, err := s.flinkController.GetCurrentDeploymentsForApp(ctx, application)
	if err != nil {
		return statusUnchanged, err
	}

	// If the application has changed (i.e., there are no current deployments), and we haven't already failed trying to
	// do the update, move to the cluster starting phase to create the new cluster
	if cur == nil {
		if s.isIncompatibleDeploymentModeChange(application) {
			s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "UnsupportedChange",
				fmt.Sprintf("Changing deployment mode from %s to %s is unsupported", application.Status.DeploymentMode, application.Spec.DeploymentMode))
			return s.deployFailed(application)
		}
		// TODO: handle single mode

		// if our scale mode is InPlace and this is a suitable update (the only change is an increase in parallelism)
		// move to Rescaling
		if application.Spec.ScaleMode == v1beta1.ScaleModeInPlace && isScaleUp(application) {
			if application.Spec.DeploymentMode == v1beta1.DeploymentModeBlueGreen {
				s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "NotScalingInPlace",
					"Not using configured InPlace scaling mode because it is incompatible with BlueGreen"+
						" deploy mode")
				s.updateApplicationPhase(application, v1beta1.FlinkApplicationUpdating)
			} else {
				logger.Infof(ctx, "Application scale has increased. Moving to Rescaling.")
				s.updateApplicationPhase(application, v1beta1.FlinkApplicationRescaling)
			}
		} else {
			logger.Infof(ctx, "Application resource has changed. Moving to Updating")
			s.updateApplicationPhase(application, v1beta1.FlinkApplicationUpdating)
		}

		return statusChanged, nil
	}

	job, err := s.flinkController.GetJobForApplication(ctx, application, application.Status.DeployHash)
	if err != nil {
		// TODO: think more about this case
		return statusUnchanged, err
	}

	if job == nil {
		logger.Warnf(ctx, "Could not find active job {}", s.flinkController.GetLatestJobID(ctx, application))
	} else {
		logger.Debugf(ctx, "Application running with job %v", job.JobID)
	}

	// For blue-green deploys, specify the hash to be deleted
	if application.Status.FailedDeployHash != "" && v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		err = s.flinkController.DeleteResourcesForAppWithHash(ctx, application, application.Status.FailedDeployHash)
		// Delete status object for the failed hash
		s.flinkController.DeleteStatusPostTeardown(ctx, application, application.Status.FailedDeployHash)
	} else if !v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		// If there are old resources left-over from a previous version, clean them up
		err = s.flinkController.DeleteOldResourcesForApp(ctx, application)
	}

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
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		return s.deleteBlueGreenApplication(ctx, app)
	}
	job, err := s.flinkController.GetJobForApplication(ctx, app, app.Status.DeployHash)
	if err != nil {
		return statusUnchanged, err
	}

	if job == nil {
		return s.clearFinalizers(ctx, app)
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
		return statusUnchanged, s.flinkController.ForceCancel(ctx, app, app.Status.DeployHash, s.flinkController.GetLatestJobID(ctx, app))
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
			triggerID, err := s.flinkController.Savepoint(ctx, app, app.Status.DeployHash, getCancelFlag(app), s.flinkController.GetLatestJobID(ctx, app))
			if err != nil {
				return statusUnchanged, err
			}
			s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "CancellingJob",
				fmt.Sprintf("Cancelling job with savepoint %v", triggerID))
			app.Status.SavepointTriggerID = triggerID
		} else {
			// we've already started savepointing; check the status
			status, err := s.flinkController.GetSavepointStatus(ctx, app, app.Status.DeployHash, s.flinkController.GetLatestJobID(ctx, app))
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

func getUpdatingVersion(application *v1beta1.FlinkApplication) v1beta1.FlinkApplicationVersion {
	if getDeployedVersion(application) == v1beta1.BlueFlinkApplication {
		return v1beta1.GreenFlinkApplication
	}

	return v1beta1.BlueFlinkApplication
}

func getDeployedVersion(application *v1beta1.FlinkApplication) v1beta1.FlinkApplicationVersion {
	// First deploy, set the version to Blue
	if application.Status.DeployVersion == "" {
		application.Status.DeployVersion = v1beta1.BlueFlinkApplication
	}
	return application.Status.DeployVersion
}

func getCancelFlag(app *v1beta1.FlinkApplication) bool {
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) && app.Status.Phase != v1beta1.FlinkApplicationDeleting {
		return false
	}
	return true
}

// Two applications are running in this phase. This phase is only ever reached when the
// DeploymentMode is set to BlueGreen
func (s *FlinkStateMachine) handleDualRunning(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	if application.Spec.TearDownVersionHash != "" {
		versionHashToTeardown := application.Spec.TearDownVersionHash
		_, _, err := s.flinkController.GetVersionAndJobIDForHash(ctx, application, versionHashToTeardown)
		if err != nil {
			logger.Warnf(ctx, "Cannot find flink application with tearDownVersionhash %s. The hash may be obsolete; Ignoring hash", versionHashToTeardown)
		} else {
			return s.teardownApplicationVersion(ctx, application)
		}
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

func (s *FlinkStateMachine) teardownApplicationVersion(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	versionHashToTeardown := application.Spec.TearDownVersionHash
	versionToTeardown, jobID, _ := s.flinkController.GetVersionAndJobIDForHash(ctx, application, versionHashToTeardown)

	s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "TeardownInitated",
		fmt.Sprintf("Tearing down application with hash %s and version %v", versionHashToTeardown,
			versionToTeardown))
	// Force-cancel job first
	s.flinkController.LogEvent(ctx, application, corev1.EventTypeNormal, "ForceCanceling",
		fmt.Sprintf("Force-canceling application with version %v and hash %s",
			versionToTeardown, versionHashToTeardown))

	err := s.flinkController.ForceCancel(ctx, application, versionHashToTeardown, jobID)
	if err != nil {
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "TeardownFailed",
			fmt.Sprintf("Failed to force-cancel application version %v and hash %s; will attempt to tear down cluster immediately: %s",
				versionToTeardown, versionHashToTeardown, err))
		return s.deployFailed(application)
	}

	// Delete all resources associated with the teardown version
	err = s.flinkController.DeleteResourcesForAppWithHash(ctx, application, versionHashToTeardown)
	if err != nil {
		s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "TeardownFailed",
			fmt.Sprintf("Failed to teardown application with hash %s and version %v, manual intervention needed: %s", versionHashToTeardown,
				versionToTeardown, err))
		return s.deployFailed(application)
	}
	s.flinkController.LogEvent(ctx, application, corev1.EventTypeWarning, "TeardownCompleted",
		fmt.Sprintf("Tore down application with hash %s and version %v", versionHashToTeardown,
			versionToTeardown))

	s.flinkController.DeleteStatusPostTeardown(ctx, application, versionHashToTeardown)
	versionPostTeardown, versionHashPostTeardown := s.flinkController.GetVersionAndHashPostTeardown(ctx, application)
	application.Status.DeployVersion = versionPostTeardown
	application.Status.UpdatingVersion = ""
	application.Status.DeployHash = versionHashPostTeardown
	application.Status.UpdatingHash = ""
	application.Status.TeardownHash = flink.HashForApplication(application)
	s.updateApplicationPhase(application, v1beta1.FlinkApplicationRunning)
	return statusChanged, nil
}

func (s *FlinkStateMachine) deleteBlueGreenApplication(ctx context.Context, app *v1beta1.FlinkApplication) (bool, error) {
	// Cancel deployed job
	deployedJob, err := s.flinkController.GetJobToDeleteForApplication(ctx, app, app.Status.DeployHash)
	if err != nil {
		return statusUnchanged, nil
	}
	if !jobFinished(deployedJob) {
		isFinished, err := s.cancelAndDeleteJob(ctx, app, deployedJob, app.Status.DeployHash)
		if err != nil {
			return statusUnchanged, nil
		}
		return isFinished, nil
	}

	deploySavepointPath := app.Status.SavepointPath
	// Cancel Updating job
	updatingJob, err := s.flinkController.GetJobToDeleteForApplication(ctx, app, app.Status.UpdatingHash)
	if err != nil {
		return statusUnchanged, nil
	}
	if !jobFinished(updatingJob) {
		if app.Status.SavepointPath == deploySavepointPath {
			app.Status.SavepointPath = ""
		}
		isFinished, err := s.cancelAndDeleteJob(ctx, app, updatingJob, app.Status.UpdatingHash)
		if err != nil {
			return statusUnchanged, nil
		}
		return isFinished, nil
	}

	if jobFinished(deployedJob) && jobFinished(updatingJob) {
		return s.clearFinalizers(ctx, app)
	}
	return statusUnchanged, nil
}

func (s *FlinkStateMachine) cancelAndDeleteJob(ctx context.Context, app *v1beta1.FlinkApplication, job *client.FlinkJobOverview, hash string) (bool, error) {
	switch app.Spec.DeleteMode {
	case v1beta1.DeleteModeForceCancel:
		if job.State == client.Cancelling {
			// we've already cancelled the job, waiting for it to finish
			return statusUnchanged, nil
		} else if jobFinished(job) {
			return statusUnchanged, nil
		}

		logger.Infof(ctx, "Force-cancelling job without a savepoint")
		return statusUnchanged, s.flinkController.ForceCancel(ctx, app, hash, s.flinkController.GetLatestJobID(ctx, app))
	case v1beta1.DeleteModeSavepoint, "":
		if app.Status.SavepointPath != "" {
			return statusChanged, nil
		}

		if app.Status.SavepointTriggerID == "" {
			// delete with savepoint
			triggerID, err := s.flinkController.Savepoint(ctx, app, hash, getCancelFlag(app), job.JobID)
			if err != nil {
				return statusUnchanged, err
			}
			s.flinkController.LogEvent(ctx, app, corev1.EventTypeNormal, "CancellingJob",
				fmt.Sprintf("Cancelling job with savepoint %v", triggerID))
			app.Status.SavepointTriggerID = triggerID
		} else {
			// we've already started savepointing; check the status
			status, err := s.flinkController.GetSavepointStatus(ctx, app, hash, job.JobID)
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

func (s *FlinkStateMachine) isIncompatibleDeploymentModeChange(application *v1beta1.FlinkApplication) bool {
	return application.Spec.DeploymentMode != application.Status.DeploymentMode
}

// Returns true if the only change between the current status and spec is an _increase_ in parallelism
func isScaleUp(app *v1beta1.FlinkApplication) bool {
	if app.Spec.Parallelism > app.Status.JobStatus.Parallelism {
		appWithOldScale := app.DeepCopy()
		appWithOldScale.Spec.Parallelism = app.Status.JobStatus.Parallelism
		hash := flink.HashForApplication(appWithOldScale)

		// this implies that everything _except_ for parallelism is the same
		return hash == app.Status.DeployHash || hash == app.Status.FailedDeployHash
	}
	return false
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
