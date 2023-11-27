package flink

import (
	"fmt"
	"regexp"

	flinkapp "github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	networkV1 "k8s.io/api/networking/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const AppIngressName = "%s-%s"

var inputRegex = regexp.MustCompile(`{{[$]jobCluster}}`)

func ReplaceJobURL(value string, input string) string {
	return inputRegex.ReplaceAllString(value, input)
}

func GetFlinkUIIngressURL(jobName string) string {
	return ReplaceJobURL(config.GetConfig().FlinkIngressURLFormat, jobName)
}

func FetchJobManagerIngressCreateObj(app *flinkapp.FlinkApplication) *networkV1.Ingress {
	podLabels := common.DuplicateMap(app.Labels)
	podLabels = common.CopyMap(podLabels, k8.GetAppLabel(app.Name))

	ingressMeta := v1.ObjectMeta{
		Name:      getJobManagerServiceName(app),
		Labels:    podLabels,
		Namespace: app.Namespace,
		OwnerReferences: []v1.OwnerReference{
			*v1.NewControllerRef(app, app.GroupVersionKind()),
		},
	}

	backend := networkV1.IngressBackend{
		Service: &networkV1.IngressServiceBackend{
			Name: getJobManagerServiceName(app),
			Port: networkV1.ServiceBackendPort{
				Name:   getJobManagerServiceName(app),
				Number: getUIPort(app),
			},
		},
	}

	ingressSpec := networkV1.IngressSpec{
		Rules: []networkV1.IngressRule{{
			Host: GetFlinkUIIngressURL(getIngressName(app)),
			IngressRuleValue: networkV1.IngressRuleValue{
				HTTP: &networkV1.HTTPIngressRuleValue{
					Paths: []networkV1.HTTPIngressPath{{
						Backend: backend,
					}},
				},
			},
		}},
	}
	return &networkV1.Ingress{
		ObjectMeta: ingressMeta,
		TypeMeta: v1.TypeMeta{
			APIVersion: networkV1.SchemeGroupVersion.String(),
			Kind:       k8.Ingress,
		},
		Spec: ingressSpec,
	}

}

func getIngressName(app *flinkapp.FlinkApplication) string {
	if flinkapp.IsBlueGreenDeploymentMode(app.Spec.DeploymentMode) {
		return fmt.Sprintf(AppIngressName, app.Name, string(app.Status.UpdatingVersion))
	}
	return app.Name
}
