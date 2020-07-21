package integ

import (
	"fmt"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/prometheus/common/log"
	. "gopkg.in/check.v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *IntegSuite) TestInPlaceScaleUp(c *C) {
	const finalizer = "scaleup.finalizers.test.com"
	const testName = "test_in_place_scale_up"

	// start a simple app
	config, err := s.Util.ReadFlinkApplication("test_app.yaml")
	c.Assert(err, IsNil, Commentf("Failed to read test app yaml"))

	config.Spec.ScaleMode = "InPlace"
	config.Spec.Parallelism = 2
	config.ObjectMeta.Name = "inplace"
	config.ObjectMeta.Labels["integTest"] = testName
	// add a finalizer so that the flinkapplication won't be deleted until we've had a chance to look at it
	config.Finalizers = append(config.Finalizers, finalizer)

	c.Assert(s.Util.CreateFlinkApplication(config), IsNil,
		Commentf("Failed to create flink application"))

	c.Assert(s.Util.WaitForPhase(config.Name, v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning(config.Name), IsNil)

	pods, err := s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 2)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, config.Spec.Image)
	}

	deployments, err := s.Util.KubeClient.AppsV1().Deployments(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "flink-app=inplace,flink-deployment-type=taskmanager"})
	c.Assert(err, IsNil)
	c.Assert(len(deployments.Items), Equals, 1)
	deployment := deployments.Items[0]

	log.Info("Application started successfully")

	// test updating the app with a new scale
	_, err = s.Util.Update("inplace", func(app *v1beta1.FlinkApplication) {
		app.Spec.Parallelism = 4
	})
	c.Assert(err, IsNil)

	c.Assert(s.Util.WaitForPhase("inplace", v1beta1.FlinkApplicationRescaling, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForPhase("inplace", v1beta1.FlinkApplicationSavepointing, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForPhase("inplace", v1beta1.FlinkApplicationRunning, v1beta1.FlinkApplicationDeployFailed), IsNil)
	c.Assert(s.Util.WaitForAllTasksRunning("inplace"), IsNil)

	log.Info("Rescaled job started successfully")
	newApp, err := s.Util.GetFlinkApplication(config.Name)
	c.Assert(err, IsNil)

	// check that we savepointed and restored correctly
	endpoint := fmt.Sprintf("jobs/%s/checkpoints", newApp.Status.JobStatus.JobID)
	res, err := s.Util.FlinkAPIGet(newApp, endpoint)
	c.Assert(err, IsNil)

	body := res.(map[string]interface{})
	restored := (body["latest"].(map[string]interface{}))["restored"]
	c.Assert(restored, NotNil)

	c.Assert(restored.(map[string]interface{})["is_savepoint"], Equals, true)

	// check that we have the correct number of total pods
	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)

	// check that we are still using the same deploymnet
	deployments2, err := s.Util.KubeClient.AppsV1().Deployments(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "flink-app=inplace,flink-deployment-type=taskmanager"})
	c.Assert(err, IsNil)
	c.Assert(len(deployments2.Items), Equals, 1)
	deployment2 := deployments.Items[0]
	c.Assert(deployment2.Name, Equals, deployment.Name)

	// ensure that we can now proceed to a normal deployment
	newApp = updateAndValidate(c, s, config.Name, func(app *v1beta1.FlinkApplication) {
		app.Spec.Image = NewImage
	}, v1beta1.FlinkApplicationDeployFailed)
	c.Assert(newApp.Spec.Image, Equals, NewImage)
	pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
		List(v1.ListOptions{LabelSelector: "integTest=" + testName})
	c.Assert(err, IsNil)
	c.Assert(len(pods.Items), Equals, 3)
	for _, pod := range pods.Items {
		c.Assert(pod.Spec.Containers[0].Image, Equals, NewImage)
	}

	// delete the application and ensure everything is cleaned up successfully
	c.Assert(s.Util.FlinkApps().Delete(config.Name, &v1.DeleteOptions{}), IsNil)

	// validate that a savepoint was taken and the job was cancelled
	var app *v1beta1.FlinkApplication
	for {
		app, err = s.Util.GetFlinkApplication(config.Name)
		c.Assert(err, IsNil)

		if len(app.Finalizers) == 1 && app.Finalizers[0] == finalizer {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c.Assert(app.Status.SavepointPath, NotNil)
	job := func() map[string]interface{} {
		jobs, _ := s.Util.FlinkAPIGet(app, "/jobs")
		jobMap := jobs.(map[string]interface{})
		jobList := jobMap["jobs"].([]interface{})
		for _, j := range jobList {
			job := j.(map[string]interface{})
			if job["id"] == app.Status.JobStatus.JobID {
				return job
			}
		}
		return nil
	}()

	fmt.Printf("test job = %v", job)
	c.Assert(job["status"], Equals, "CANCELED")

	// delete our finalizer
	app.Finalizers = []string{}
	_, err = s.Util.FlinkApps().Update(app)
	c.Assert(err, IsNil)

	// wait until all pods are gone
	for {
		pods, err = s.Util.KubeClient.CoreV1().Pods(s.Util.Namespace.Name).
			List(v1.ListOptions{LabelSelector: "integTest=" + testName})
		c.Assert(err, IsNil)
		if len(pods.Items) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Info("All pods torn down")
}
