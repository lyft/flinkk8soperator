package k8

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	AppKey = "flink-app"
)

func IsK8sObjectDoesNotExist(err error) bool {
	return k8serrors.IsNotFound(err) || k8serrors.IsGone(err) || k8serrors.IsResourceExpired(err)
}

func GetAppLabel(appName string) map[string]string {
	return map[string]string{
		AppKey: appName,
	}
}

func GetDeploymentWithName(deployments []v1.Deployment, name string) *v1.Deployment {
	if len(deployments) == 0 {
		return nil
	}
	for _, deployment := range deployments {
		if deployment.Name == name {
			return &deployment
		}
	}
	return nil
}

func CreateEvent(app *v1alpha1.FlinkApplication, fieldPath string, eventType string, reason string, message string) corev1.Event {
	eventTime := metav1.Now()

	objectReference := corev1.ObjectReference{
		Kind:       app.Kind,
		Name:       app.Name,
		Namespace:  app.Namespace,
		UID:        app.UID,
		APIVersion: app.APIVersion,
		FieldPath:  fieldPath,
	}

	return corev1.Event{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Event",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    app.Namespace,
			GenerateName: "event",
		},
		Reason:         reason,
		Message:        message,
		InvolvedObject: objectReference,
		Source: corev1.EventSource{
			Component: "flinkk8soperator",
		},
		FirstTimestamp: eventTime,
		LastTimestamp:  eventTime,
		Type:           eventType,
	}

}
