package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-resty/resty"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	v1alpha12 "github.com/lyft/flinkk8soperator/pkg/client/clientset/versioned/typed/app/v1alpha1"
	"github.com/prometheus/common/log"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsClientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)
import clientset "github.com/lyft/flinkk8soperator/pkg/client/clientset/versioned"

type TestUtil struct {
	KubeClient             kubernetes.Interface
	FlinkApplicationClient clientset.Interface
	APIExtensionsClient    apiextensionsClientset.Interface
	Namespace              *v1.Namespace
	Image                  string
	CheckpointDir          string
}

func New(namespaceName string, kubeconfig string, image string, checkpointDir string) (*TestUtil, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	var namespace *v1.Namespace
	if namespaceName == "default" {
		namespace, err = client.CoreV1().Namespaces().Get("default", metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
	} else {
		namespace, err = client.CoreV1().Namespaces().Create(&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespaceName,
			},
		})
		if err != nil {
			return nil, err
		}
	}

	crdClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	apiextensionsClient, err := apiextensionsClientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &TestUtil{
		KubeClient:             client,
		FlinkApplicationClient: crdClient,
		APIExtensionsClient:    apiextensionsClient,
		Namespace:              namespace,
		Image:                  image,
		CheckpointDir:          checkpointDir,
	}, nil
}

func (f *TestUtil) Cleanup() {
	if f.Namespace.Name != "default" {
		flinkApps, err := f.FlinkApps().List(metav1.ListOptions{})
		if err != nil {
			log.Errorf("Failed to fetch flink apps during cleanup: %v", err)
		} else {
			// make sure none of the apps have left-over finalizers
			for _, app := range flinkApps.Items {
				if len(app.Finalizers) != 0 {
					app.Finalizers = []string{}
					_, _ = f.FlinkApps().Update(&app)
				}
			}
		}

		err = f.KubeClient.CoreV1().Namespaces().Delete(f.Namespace.Name, &metav1.DeleteOptions{})
		if err != nil {
			log.Errorf("Failed to clean up after test: %v", err)
		}
	}
}

func getFile(relativePath string) (*os.File, error) {
	path, err := filepath.Abs(relativePath)
	if err != nil {
		return nil, err
	}

	return os.Open(path)
}

func (f *TestUtil) CreateCRD() error {
	file, err := getFile("../deploy/crd.yaml")
	if err != nil {
		return err
	}

	crd := v1beta1.CustomResourceDefinition{}
	err = yaml.NewYAMLOrJSONDecoder(file, 1024).Decode(&crd)
	if err != nil {
		return err
	}

	crd.Namespace = f.Namespace.Name
	fmt.Printf("crd %v", crd)

	_, err = f.APIExtensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(&crd)
	if err != nil {
		return err
	}

	return nil
}

func (f *TestUtil) CreateOperator() error {
	configValue := make(map[string]string)
	configValue["development"] = "operator:\n  containerNameFormat: \"%s-unknown\"\n  statemachineStalenessDuration: 40s\n  resyncPeriod: 5s"

	configMap := v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "flink-operator-config",
			Namespace: f.Namespace.Name,
		},
		Data: configValue,
	}

	if _, err := f.KubeClient.CoreV1().ConfigMaps(f.Namespace.Name).Create(&configMap); err != nil {
		return err
	}

	var replicas int32 = 1
	resources := make(map[v1.ResourceName]resource.Quantity)
	resources[v1.ResourceCPU] = resource.MustParse("0.2")
	resources[v1.ResourceMemory] = resource.MustParse("0.5Gi")
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "flinkk8soperator",
			Namespace: f.Namespace.Name,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "flinkk8soperator",
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "flinkk8soperator",
					},
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "config-volume",
							VolumeSource: v1.VolumeSource{
								ConfigMap: &v1.ConfigMapVolumeSource{
									LocalObjectReference: v1.LocalObjectReference{
										Name: "flink-operator-config",
									},
									Items: []v1.KeyToPath{
										{
											Key:  "development",
											Path: "config.yaml",
										},
									},
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name:  "flinkk8soperator",
							Image: f.Image,
							Env: []v1.EnvVar{
								{Name: "OPERATOR_NAME", Value: "flinkk8soperator"},
							},
							Command: []string{"flinkoperator"},
							Args:    []string{"--config", "/etc/flinkk8soperator/config/config.yaml"},
							Ports: []v1.ContainerPort{
								{ContainerPort: 10254},
							},
							Resources: v1.ResourceRequirements{
								Requests: resources,
							},
							VolumeMounts: []v1.VolumeMount{
								{Name: "config-volume", MountPath: "/etc/flinkk8soperator/config"},
							},
							ImagePullPolicy: v1.PullIfNotPresent,
						},
					},
				},
			},
		},
	}

	if _, err := f.KubeClient.AppsV1().Deployments(f.Namespace.Name).Create(&deployment); err != nil {
		return err
	}

	return nil
}

func (f *TestUtil) GetJobManagerPod() (string, error) {
	pods, err := f.KubeClient.CoreV1().Pods(f.Namespace.Name).List(metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, p := range pods.Items {
		if strings.Contains(p.Name, "-jm-") {
			return p.Name, nil
		}
	}

	return "", errors.New("no jobmanager pod found")
}

func (f *TestUtil) GetTaskManagerPods() ([]string, error) {
	tms := make([]string, 0)
	pods, err := f.KubeClient.CoreV1().Pods(f.Namespace.Name).List(metav1.ListOptions{})

	if err != nil {
		return tms, err
	}

	for _, p := range pods.Items {
		if strings.Contains(p.Name, "-tm-") {
			tms = append(tms, p.Name)
		}
	}

	return tms, nil
}

func (f *TestUtil) GetLogs(podName string, lines *int64) error {
	req := f.KubeClient.CoreV1().Pods(f.Namespace.Name).
		GetLogs(podName,
			&v1.PodLogOptions{
				TailLines: lines,
				Follow:    false,
			})

	readCloser, err := req.Stream()
	if err != nil {
		return err
	}

	defer readCloser.Close()
	_, err = io.Copy(os.Stdout, readCloser)

	if err != nil {
		return err
	}

	return nil
}

func (f *TestUtil) TailOperatorLogs() error {
	var podName string
	for {
		pods, err := f.KubeClient.CoreV1().Pods(f.Namespace.Name).List(metav1.ListOptions{
			LabelSelector: "app=flinkk8soperator",
		})

		if err != nil {
			return err
		}

		if len(pods.Items) == 0 || len(pods.Items[0].Status.ContainerStatuses) == 0 || !pods.Items[0].Status.ContainerStatuses[0].Ready {
			time.Sleep(500 * time.Millisecond)
			log.Info("Waiting for operator container to be ready...")
		} else {
			podName = pods.Items[0].Name
			break
		}
	}

	log.Infof("Found operator pod %s, starting to tail logs...", podName)

	req := f.KubeClient.CoreV1().RESTClient().Get().
		Namespace(f.Namespace.Name).
		Name(podName).
		Resource("pods").
		SubResource("log").
		Param("follow", "true")

	readerCloser, err := req.Stream()
	if err != nil {
		return err
	}

	go func() {
		defer readerCloser.Close()
		_, err = io.Copy(os.Stderr, readerCloser)
		if err != nil {
			log.Errorf("Lost connection to operator logs %v", err)
		}
	}()

	return nil
}

func (f *TestUtil) ReadFlinkApplication(path string) (*v1alpha1.FlinkApplication, error) {
	file, err := getFile(path)
	if err != nil {
		return nil, err
	}

	app := v1alpha1.FlinkApplication{}
	err = yaml.NewYAMLOrJSONDecoder(file, 2048).Decode(&app)
	if err != nil {
		return nil, err
	}

	app.Spec.Volumes[0].HostPath.Path = f.CheckpointDir

	return &app, nil
}

func (f *TestUtil) FlinkApps() v1alpha12.FlinkApplicationInterface {
	return f.FlinkApplicationClient.FlinkV1alpha1().FlinkApplications(f.Namespace.Name)
}

func (f *TestUtil) CreateFlinkApplication(application *v1alpha1.FlinkApplication) error {
	_, err := f.FlinkApps().Create(application)
	return err
}

func (f *TestUtil) GetFlinkApplication(name string) (*v1alpha1.FlinkApplication, error) {
	return f.FlinkApps().Get(name, metav1.GetOptions{})
}

func (f *TestUtil) WaitForPhase(name string, phase v1alpha1.FlinkApplicationPhase, failurePhases ...v1alpha1.FlinkApplicationPhase) error {
	for {
		app, err := f.FlinkApps().Get(name, metav1.GetOptions{})

		if err != nil {
			return err
		}

		if app.Status.Phase == phase {
			return nil
		}

		for _, p := range failurePhases {
			if app.Status.Phase == p {
				return fmt.Errorf("application entered %s phase", p)
			}
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func (f *TestUtil) FlinkAPIGet(app *v1alpha1.FlinkApplication, endpoint string) (interface{}, error) {
	url := fmt.Sprintf("http://localhost:8001/api/v1/namespaces/%s/"+
		"services/%s:8081/proxy/%s",
		f.Namespace.Name, app.Name, endpoint)

	resp, err := resty.SetRedirectPolicy(resty.FlexibleRedirectPolicy(5)).R().Get(url)
	if err != nil {
		return nil, err
	}

	if !resp.IsSuccess() {
		return nil, fmt.Errorf("request failed with code %d", resp.StatusCode())
	}

	var result interface{}
	err = json.Unmarshal(resp.Body(), &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (f *TestUtil) WaitForAllTasksInState(name string, state string) error {
	flinkApp, err := f.GetFlinkApplication(name)
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("jobs/%s", flinkApp.Status.JobStatus.JobID)
	for {
		res, err := f.FlinkAPIGet(flinkApp, endpoint)
		if err != nil {
			return err
		}

		body := res.(map[string]interface{})
		vertices := body["vertices"].([]interface{})

		var allRunning = true
		for _, vertex := range vertices {
			allRunning = allRunning && (vertex.(map[string]interface{})["status"] == state)
		}

		if allRunning && len(vertices) > 0 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// wait a little bit longer, as sometimes the flink api reports tasks as running
	// just before they actually are
	time.Sleep(5 * time.Second)

	return nil
}
