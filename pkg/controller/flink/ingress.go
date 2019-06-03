package flink

import (
	"regexp"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"k8s.io/api/extensions/v1beta1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var inputRegex = regexp.MustCompile(`{{[$]jobCluster}}`)

func ReplaceJobURL(value string, input string) string {
	return inputRegex.ReplaceAllString(value, input)
}

func GetFlinkUIIngressURL(jobName string) string {
	return ReplaceJobURL(config.GetConfig().FlinkIngressURLFormat, jobName)
}

func FetchJobManagerIngressCreateObj(app *v1alpha1.FlinkApplication) *v1beta1.Ingress {
	podLabels := common.DuplicateMap(app.Labels)
	podLabels = common.CopyMap(podLabels, k8.GetAppLabel(app.Name))

	ingressMeta := v1.ObjectMeta{
		Name:        app.Name,
		Labels:      podLabels,
		Namespace:   app.Namespace,
		Annotations: app.Spec.IngressConfig.Annotations,
		OwnerReferences: []v1.OwnerReference{
			*v1.NewControllerRef(app, app.GroupVersionKind()),
		},
	}

	backend := v1beta1.IngressBackend{
		ServiceName: app.Name,
		ServicePort: intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: getUIPort(app),
		},
	}

	ingressSpec := v1beta1.IngressSpec{
		Rules: []v1beta1.IngressRule{{
			Host: GetFlinkUIIngressURL(app.Name),
			IngressRuleValue: v1beta1.IngressRuleValue{
				HTTP: &v1beta1.HTTPIngressRuleValue{
					Paths: []v1beta1.HTTPIngressPath{{
						Backend: backend,
					}},
				},
			},
		}},
	}
	return &v1beta1.Ingress{
		ObjectMeta: ingressMeta,
		TypeMeta: v1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       k8.Ingress,
		},
		Spec: ingressSpec,
	}

}
