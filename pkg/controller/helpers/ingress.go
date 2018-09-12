package helpers

import (
	"regexp"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/config"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var inputRegex = regexp.MustCompile("{{\\s*[$]job\\s*}}")

func ReplaceJobUrl(value string, input string) string {
	return inputRegex.ReplaceAllString(value, input)
}

func GetFlinkUIIngressURL(jobName string) string {
	return ReplaceJobUrl(config.FlinkIngressUrlFormat, jobName)
}

func FetchJobManagerIngressCreateObj(job *v1alpha1.FlinkJob) *v1beta1.Ingress {
	jobManagerName := GetJobManagerName(job.Name)
	podLabels := CopyMap(job.Labels)
	podLabels[FlinkProcessRoleKey] = JobManagerProcessRole

	ingressMeta := v1.ObjectMeta{
		Name:      jobManagerName,
		Labels:    podLabels,
		Namespace: job.Namespace,
		OwnerReferences: []v1.OwnerReference{
			*v1.NewControllerRef(job, job.GroupVersionKind()),
		},
	}

	backend := v1beta1.IngressBackend{
		ServiceName: jobManagerName,
		ServicePort: intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: FlinkUIDefaultPort,
		},
	}

	ingressSpec := v1beta1.IngressSpec{
		Rules: []v1beta1.IngressRule{{
			Host: GetFlinkUIIngressURL(job.Name),
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
			Kind:       Ingress,
		},
		Spec: ingressSpec,
	}

}

func FetchJobManagerIngressIdentityObj(job *v1alpha1.FlinkJob) *v1beta1.Ingress {
	jmName := GetJobManagerName(job.Name)

	return &v1beta1.Ingress{
		TypeMeta: v1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       Ingress,
		},
		ObjectMeta: v1.ObjectMeta{
			Name:      jmName,
			Namespace: job.Namespace,
		},
	}
}
