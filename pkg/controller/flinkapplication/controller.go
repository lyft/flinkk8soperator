package flinkapplication

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"time"

	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ReconcileFlinkApplication reconciles a FlinkApplication resource
type ReconcileFlinkApplication struct {
	client            client.Client
	cache             cache.Cache
	metrics           *reconcilerMetrics
	flinkStateMachine FlinkHandlerInterface
}

type reconcilerMetrics struct {
	scope          promutils.Scope
	cacheHit       labeled.Counter
	cacheMiss      labeled.Counter
	reconcileError labeled.Counter
}

func newReconcilerMetrics(scope promutils.Scope) *reconcilerMetrics {
	reconcilerScope := scope.NewSubScope("reconciler")
	return &reconcilerMetrics{
		scope:          reconcilerScope,
		cacheHit:       labeled.NewCounter("cache_hit", "Flink application resource fetched from cache", reconcilerScope),
		cacheMiss:      labeled.NewCounter("cache_miss", "Flink application resource missing from cache", reconcilerScope),
		reconcileError: labeled.NewCounter("reconcile_error", "Reconcile for application failed", reconcilerScope),
	}
}

func (r *ReconcileFlinkApplication) getResource(ctx context.Context, key types.NamespacedName, obj client.Object) error {
	err := r.cache.Get(ctx, key, obj)
	if err != nil && k8.IsK8sObjectDoesNotExist(err) {
		r.metrics.cacheMiss.Inc(ctx)
		return r.client.Get(ctx, key, obj)
	}
	if err == nil {
		r.metrics.cacheHit.Inc(ctx)
	}
	return err
}

// For failures, we do not want to retry immediately, as we want the underlying resource to recover.
// At the same time, we want to retry faster than the regular success interval.
func (r *ReconcileFlinkApplication) getFailureRetryInterval() time.Duration {
	return config.GetConfig().ResyncPeriod.Duration / 2
}

func (r *ReconcileFlinkApplication) getReconcileResultForError(err error) reconcile.Result {
	if err == nil {
		return reconcile.Result{}
	}
	return reconcile.Result{
		RequeueAfter: r.getFailureRetryInterval(),
	}
}

func (r *ReconcileFlinkApplication) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = contextutils.WithNamespace(ctx, request.Namespace)
	ctx = contextutils.WithAppName(ctx, request.Name)
	typeMeta := metaV1.TypeMeta{
		Kind:       v1beta1.FlinkApplicationKind,
		APIVersion: v1beta1.SchemeGroupVersion.String(),
	}
	// Fetch the FlinkApplication instance
	instance := &v1beta1.FlinkApplication{
		TypeMeta: typeMeta,
	}

	err := r.getResource(ctx, request.NamespacedName, instance)
	if err != nil {
		if k8.IsK8sObjectDoesNotExist(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - we will check again in next loop
		return r.getReconcileResultForError(err), nil
	}
	// We are seeing instances where getResource is removing TypeMeta
	instance.TypeMeta = typeMeta
	ctx = contextutils.WithPhase(ctx, string(instance.Status.Phase))
	err = r.flinkStateMachine.Handle(ctx, instance)
	if err != nil {
		r.metrics.reconcileError.Inc(ctx)
		logger.Warnf(ctx, "Failed to reconcile resource %v: %v", request.NamespacedName, err)
	}
	return r.getReconcileResultForError(err), err
}

// Add creates a new FlinkApplication Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(ctx context.Context, mgr manager.Manager, cfg config.RuntimeConfig) error {
	k8sCluster := k8.NewK8Cluster(mgr, cfg)
	eventRecorder := mgr.GetEventRecorderFor(config.AppName)
	flinkStateMachine := NewFlinkStateMachine(k8sCluster, eventRecorder, cfg)

	metrics := newReconcilerMetrics(cfg.MetricsScope)
	reconciler := ReconcileFlinkApplication{
		client:            mgr.GetClient(),
		cache:             mgr.GetCache(),
		metrics:           metrics,
		flinkStateMachine: flinkStateMachine,
	}

	c, err := controller.New(config.AppName, mgr, controller.Options{
		MaxConcurrentReconciles: config.GetConfig().Workers,
		Reconciler:              &reconciler,
	})

	if err != nil {
		return err
	}

	if err = c.Watch(&source.Kind{Type: &v1beta1.FlinkApplication{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	// Watch deployments and services for the application
	if err := c.Watch(&source.Kind{Type: &v1.Deployment{}}, &handler.Funcs{}, getPredicateFuncs()); err != nil {
		return err
	}

	if err := c.Watch(&source.Kind{Type: &coreV1.Service{}}, &handler.Funcs{}, getPredicateFuncs()); err != nil {
		return err
	}
	return nil
}

func isOwnedByFlinkApplication(ownerReferences []metaV1.OwnerReference) bool {
	for _, ownerReference := range ownerReferences {
		if ownerReference.APIVersion == v1beta1.SchemeGroupVersion.String() &&
			ownerReference.Kind == v1beta1.FlinkApplicationKind {
			return true
		}
	}
	return false
}

// Predicate filters events before enqueuing the keys.
// We are only interested in kubernetes objects that are owned by the FlinkApplication
// This filters all the objects not owned by the flinkApplication, and ensures only subset reaches event handlers
func getPredicateFuncs() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return isOwnedByFlinkApplication(e.Object.GetOwnerReferences())
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return isOwnedByFlinkApplication(e.ObjectNew.GetOwnerReferences())
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return isOwnedByFlinkApplication(e.Object.GetOwnerReferences())
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return isOwnedByFlinkApplication(e.Object.GetOwnerReferences())
		},
	}
}
